import os
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR, DECIMAL
from sqlalchemy.exc import OperationalError
from urllib.parse import quote_plus
import pandas as pd
import numpy as np
from utils.tools import get_last_ingested, get_logger, fix_order_checkout, update_last_ingested
from utils.fks_mapper import get_bays, get_custom, get_users, get_locations, get_cars, get_customers
from utils.custom_err import IncrementalDependencyError


warnings.filterwarnings('ignore')
load_dotenv()

log = get_logger("Orders")

# -------------------- Connections --------------------
def get_engine(server_env, db_env, user_env, pw_env) -> Engine:
    conn_string = (
        f"DRIVER={os.getenv('AZURE_ODBC_DRIVER', '{ODBC Driver 18 for SQL Server}')};"
        f"SERVER={os.getenv(server_env)};"
        f"DATABASE={os.getenv(db_env)};"
        f"UID={os.getenv(user_env)};"
        f"PWD={os.getenv(pw_env)};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=yes;"
    )
    quoted = quote_plus(conn_string)
    engine = create_engine(f'mssql+pyodbc:///?odbc_connect={quoted}')
    log.info(f'Connected to {os.getenv(db_env)} at {os.getenv(server_env)}')
    return engine

def source_db_conn(): return get_engine('AZURE_SERVER','AZURE_DATABASE','AZURE_USERNAME','AZURE_PASSWORD')
def target_db_conn(): return get_engine('STAGE_SERVER','STAGE_DATABASE','STAGE_USERNAME','STAGE_PASSWORD')

# -------------------- Extract --------------------
def extract(user_id:int, engine: Engine) -> pd.DataFrame:
    """Extract data based on UserID."""

    last_id = get_last_ingested(user_id, 'dbo.Orders')

    location_ids = pd.read_sql(f'SELECT LocationID FROM dbo.Locations WHERE UserID={user_id}', engine)
    location_ids = (0,0) + tuple(location_ids['LocationID'].values.tolist())
  
    query = f"SELECT OrderID, LocationID, TransactionNo, OrderNo, CarID, CustomerID, BayID, OrderType, OrderMode, OrderTakerID, StatusID, CreatedOn, LastUpdateDT FROM dbo.Orders WHERE LocationID IN {location_ids} AND OrderID > {last_id}"
    df = pd.read_sql_query(query, engine)

    order_checkout = pd.read_sql(f'SELECT OrderID, OrderStatus, AmountTotal, AmountDiscount, Tax, GrandTotal, AmountPaid, DiscountPercent, RefundedAmount FROM dbo.OrderCheckout WHERE LocationID IN {location_ids} AND OrderID > {last_id}', engine)
    order_checkout = order_checkout.groupby('OrderID', as_index=False).agg({k:('sum' if k not in ['DiscountPercent','OrderStatus'] else 'max') for k in order_checkout.columns})


    order_details = pd.read_sql(f'SELECT OrderID, DiscountAmount AS ItemDiscountTotal FROM dbo.OrderDetail WHERE OrderID IN (SELECT OrderID FROM dbo.Orders WHERE LocationID IN {location_ids} AND OrderID > {last_id})', engine)
    order_details = order_details.groupby('OrderID', as_index=False).sum()

    df = pd.merge(df, order_checkout, on='OrderID', how='left')
    df = pd.merge(df, order_details, on='OrderID', how='left')

    log.info(f'Extracted {len(df)} rows from dbo.Orders')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame, target: Engine) -> pd.DataFrame:
    """Clean and transform Orders data."""

    # rename columns
    df.rename(columns={
        "OrderID":'OldOrderID',
        'LocationID':'OldLocationID',
        'CarID':'OldCarID',
        'BayID':'OldBayID',
        'OrderTakerID':'OldID',
        'StatusID':'LastServiceStatusID',
        'OrderStatus':'LastOrderPaymentStatusID',
        'LastUpdateDT':'UpdatedAt',
        'CreatedOn':'CreatedAt',
        'AmountTotal':'Subtotal',
        'AmountDiscount':'OrderDiscountTotal',
        'ServiceCharges':'AdditionalCharges',
        'Tax':'ItemTaxTotal',
        'RefundedAmount':'RefundAmountTotal',
        'AmountPaid':'AmountPaidTotal',
        'DiscountPercent':'OrderDiscountPercent'
        }, inplace=True)


    # Clean strings: strip & lowercase
    for col in df.select_dtypes(include='object').columns:
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) else x)
    df['OldCarID'] = pd.to_numeric(df['OldCarID'], errors='coerce')

    # Clean nulls
    df['UpdatedAt'] = df['UpdatedAt'].fillna(datetime.now())
    df['LastServiceStatusID'] = df['LastServiceStatusID'].fillna(0)
    df['LastServiceStatusID'] = df['LastServiceStatusID'].map(lambda x: {103:105, 105:100}.get(x, x)) # type: ignore
    df['Subtotal'] = df['Subtotal'].fillna(0)
    df['OrderDiscountTotal'] = df['OrderDiscountTotal'].fillna(0)
    df['ItemDiscountTotal'] = df['ItemDiscountTotal'].fillna(0)
    df['ItemTaxTotal'] = df['ItemTaxTotal'].fillna(0)
    df['OrderDiscountPercent'] = df['OrderDiscountPercent'].fillna(0)
    df['GrandTotal'] = df['GrandTotal'].fillna(0)
    df['AmountPaidTotal'] = df['AmountPaidTotal'].fillna(0)
    df['RefundAmountTotal'] = df['RefundAmountTotal'].fillna(0)
    df['LastOrderPaymentStatusID'] = df['LastOrderPaymentStatusID'].map({103:303,105:308,106:306,108:307})
    df['LastOrderPaymentStatusID'] = df[['LastOrderPaymentStatusID', 'RefundAmountTotal']].apply(lambda row: 305 if row['LastOrderPaymentStatusID']==303 and row['RefundAmountTotal'] > 0 else row['LastOrderPaymentStatusID'], axis=1)
    df['LastOrderPaymentStatusID'] = df['LastOrderPaymentStatusID'].fillna(308)
    df['OldBayID'] = pd.to_numeric(df['OldBayID'], errors='coerce')
    df['OrderType'] = df['OldCarID'].map(lambda x: 0 if isinstance(x, int) else 1)
    df['AmountDueTotal'] = df['GrandTotal'] - df['AmountPaidTotal']


    # Fix OrderNo
    df.sort_values(by='CreatedAt', inplace=True)
    df['OrderNo'] = range(1,len(df)+1)


    # Foreign Keys Mapping
    df = pd.merge(df, get_locations(target), on='OldLocationID', how='left')
    missing_locs = df['LocationID'].isna().sum()
    if missing_locs:
        raise IncrementalDependencyError(f'Missing LocationIDs: {missing_locs}. Update Locations Table.')


    df = pd.merge(df, get_cars(target), on='OldCarID', how='left')


    df = pd.merge(df, get_users(target), on='OldID', how='left')
    df.rename(columns={'Id':'OrderTakerID'}, inplace=True)
    df.drop(columns='OldID', inplace=True)
    missing_ot = df['OrderTakerID'].isna().sum()
    if missing_ot:
        log.info(f'Missing OrderTakerIDs: {missing_ot}. Update AspNetUsers Table.')
        # raise IncrementalDependencyError(f'Missing OrderTakerIDs: {missing_ot}. Update AspNetUsers Table.')
    
    df.rename(columns={'CustomerID':'OldID'}, inplace=True)
    df = pd.merge(df, get_customers(target, df['OldID']), on='OldID', how='left')

        
    df = pd.merge(df, get_bays(target), on='OldBayID', how='left')


    df.drop(columns={'OldLocationID', 'OldCarID', 'OldBayID', 'OldID', 'OrderMode', 'RefundAmountTotal'}, inplace=True)
    df.sort_values(by='OldOrderID', inplace=True)

    log.info(f'Transformation complete. df rows: {len(df)}')
    return df

# -------------------- Load --------------------
def load(df: pd.DataFrame, user_id:int, engine: Engine):

    dtype_mapping = {col:NVARCHAR(None) for col in df.select_dtypes(include='object').columns}
    
    try:
        with engine.begin() as conn:  # Transaction-safe

            conn.execute(text("""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.columns
                    WHERE Name = 'OldOrderID'
                    AND Object_ID = Object_ID('app.Orders')
                )
                BEGIN
                    ALTER TABLE app.Orders
                    ADD OldOrderID BIGINT NULL;
                END
            """))
            log.info("Verified/Added OldOrderID column.")

        i = 0
        while i < len(df)/5000:
            df.iloc[5000*i:5000*(i+1)].to_sql('Orders', con=engine, schema='app', if_exists='append', index=False, dtype=dtype_mapping) # type: ignore
            update_last_ingested(user_id, 'dbo.Orders', int(df.iloc[5000*i:5000*(i+1)]['OldOrderID'].max()))
            log.info(f'Batch {i+1} inserted')
            i+=1
        log.info(f'dbo.Orders loaded successfully')

    except Exception as e:
        log.error(f'Failed to load dbo.Orders: {e}')
        raise

# -------------------- Main --------------------
def main(user_id:int, if_load:bool=True):
    source = source_db_conn()
    target = target_db_conn()

    df = extract(user_id, source)
    if df.empty:
        log.info('No data to load.')
        return 

    df = transform(df, target)
    print(df)

    if if_load:
        load(df, user_id, target)
        

# if __name__ == '__main__':
#     main()
