import os
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR, DECIMAL
from sqlalchemy.exc import OperationalError
from urllib.parse import quote_plus
import pandas as pd
import numpy as np
from utils.tools import get_logger
from utils.fks_mapper import get_custom, get_users, get_locations, get_cars, get_customers
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
def extract(source_db: Engine, target_db: Engine) -> pd.DataFrame:
    """Extract data based on CDC."""
    with target_db.begin() as conn:
        max_id = conn.execute(
            text("SELECT ISNULL(MaxIndex,0) FROM app.EtlCDC WHERE TableName=:table_name"),
            {"table_name": 'dbo.Orders'}
        ).scalar()
    
    max_id = max_id if not max_id is None else 0
    log.info(f'Current CDC for dbo.Orders: {max_id}')

    query = f"SELECT TOP 100 OrderID, LocationID, TransactionNo, OrderNo, CarID, CustomerID, BayID, OrderType, OrderMode, OrderTakerID, StatusID, CreatedOn, LastUpdateDT FROM dbo.Orders WHERE OrderID > {max_id} ORDER BY OrderID"
    df = pd.read_sql_query(query, source_db)

    order_ids = tuple(df['OrderID'].values.tolist()) + (0,0)
    order_checkout = pd.read_sql(f'SELECT OrderID, AmountTotal, AmountDiscount, Tax, ServiceCharges, GrandTotal, AmountPaid, TaxPercent, DiscountPercent, RefundedAmount FROM dbo.OrderCheckout WHERE OrderID IN {order_ids}', source_db)
    order_checkout = order_checkout.groupby('OrderID').sum()

    df = pd.merge(df, order_checkout, on='OrderID', how='left')

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
        'StatusID':'ServiceStatusID',
        'LastUpdateDT':'UpdatedAt',
        'CreatedOn':'CreatedAt',
        'AmountTotal':'Subtotal',
        'AmountDiscount':'DiscountAmount',
        'ServiceCharges':'AdditionalCharges',
        'GrandTotal':'Total',
        'RefundedAmount':'RefundAmount',
        }, inplace=True)


    # Clean strings: strip & lowercase
    for col in df.select_dtypes(include='object').columns:
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) else x)
            

    # Clean nulls
    df['UpdatedAt'] = df['UpdatedAt'].fillna(datetime.now())
    df['ServiceStatusID'] = df['ServiceStatusID'].fillna(1)
    df['Subtotal'] = df['Subtotal'].fillna(0)
    df['DiscountAmount'] = df['DiscountAmount'].fillna(0)
    df['Tax'] = df['Tax'].fillna(0)
    df['TaxPercent'] = df['TaxPercent'].fillna(0)
    df['AdditionalCharges'] = df['AdditionalCharges'].fillna(0)
    df['Total'] = df['Total'].fillna(0)
    df['AmountPaid'] = df['AmountPaid'].fillna(0)
    df['DiscountPercent'] = df['DiscountPercent'].fillna(0)
    df['RefundAmount'] = df['RefundAmount'].fillna(0)
    df['OrderPaymentStatusID'] = 1
    df['OldBayID'] = pd.to_numeric(df['OldBayID'], errors='coerce')
    df['OrderType'] = df['OrderType'].map({'New': 0})

    

    # Foreign Keys Mapping
    df = pd.merge(df, get_locations(target), on='OldLocationID', how='left')
    missing_locs = df['LocationID'].isna().sum()
    if missing_locs:
        raise IncrementalDependencyError(f'Missing LocationIDs: {missing_locs}. Update Locations Table.')
    
    df = pd.merge(df, get_cars(target, df['OldCarID']), on='OldCarID', how='left')
    missing_cars = df['CarID'].isna().sum()
    if missing_cars:
        raise IncrementalDependencyError(f'Missing CarIDs: {missing_cars}. Update Cars Table.')
    
    df = pd.merge(df, get_users(target, df['OldID']), on='OldID', how='left')
    df.rename(columns={'Id':'OrderTakerID'}, inplace=True)
    df.drop(columns='OldID', inplace=True)
    missing_ot = df['OrderTakerID'].isna().sum()
    if missing_ot:
        raise IncrementalDependencyError(f'Missing OrderTakerIDs: {missing_ot}. Update AspNetUsers Table.')
    
    df.rename(columns={'CustomerID':'OldID'}, inplace=True)
    df = pd.merge(df, get_customers(target, df['OldID']), on='OldID', how='left')

    df = pd.merge(df, get_custom(target, ['BayID', 'OldBayID'], 'app.Bays'), on='OldBayID', how='left')


    df.drop(columns={'OldLocationID', 'OldCarID', 'OldBayID', 'OldID', 'OrderMode'}, inplace=True)

    log.info(f'Transformation complete, df\'s Length is {len(df)}')
    return df

# -------------------- Load --------------------
def load(df: pd.DataFrame, engine: Engine):

    dtype_mapping = {col:NVARCHAR(None) for col in df.select_dtypes(include='object').columns}
    
    max_id = df['OldOrderID'].max()

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

            df.to_sql('Orders', con=conn, schema='app', if_exists='append', index=False, dtype=dtype_mapping) # type: ignore
            log.info(f'dbo.Orders loaded successfully')

            conn.execute(
                text("""
                    MERGE app.[EtlCDC] AS target
                    USING (SELECT :table_name AS [TableName], :max_index AS [MaxIndex]) AS source
                    ON target.[TableName] = source.[TableName]
                    WHEN MATCHED THEN UPDATE SET target.[MaxIndex] = source.[MaxIndex]
                    WHEN NOT MATCHED THEN INSERT ([TableName],[MaxIndex]) VALUES (source.[TableName],source.[MaxIndex]);
                """),
                {"table_name": f'dbo.Orders', "max_index": int(max_id)}
            )
            log.info(f'dbo.Orders loaded successfully, CDC updated to {max_id}')
    except Exception as e:
        log.error(f'Failed to load dbo.Orders: {e}')
        raise

# -------------------- Main --------------------
def main():
    source = source_db_conn()
    target = target_db_conn()

    while True:
        df = extract(source, target)
        if df.empty:
            log.info('No new data to load.')
            break
        df = transform(df, target)
        # return
        load(df, target)
        return

if __name__ == '__main__':
    main()
