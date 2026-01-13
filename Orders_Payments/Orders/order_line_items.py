import os
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR, DECIMAL
from urllib.parse import quote_plus
import pandas as pd
from utils.tools import get_logger
from utils.fks_mapper import get_items, get_packages, get_custom
from utils.custom_err import IncrementalDependencyError


warnings.filterwarnings('ignore')
load_dotenv()

log = get_logger("OrderLineItems")

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
def extract(user_id: int, engine: Engine) -> pd.DataFrame:
    """Extract data based on UserID."""

    order_query = f"""
        SELECT OrderID
        FROM dbo.Orders
        WHERE LocationID IN (
            SELECT LocationID
            FROM dbo.Locations
            WHERE UserID={user_id}
        )
    """

    order_ids = pd.read_sql(order_query, engine)
    order_ids = (0,0) + tuple(order_ids['OrderID'].values.tolist())
    
    
    query = f"SELECT OrderDetailID, OrderID, ItemID, PackageID, Description, Quantity, Price, Cost, DiscountAmount, RefundAmount, RefundQty, StatusID, CreatedOn, CreatedBy, LastUpdateDT, LastUpdateBy  FROM dbo.OrderDetail WHERE OrderID IN {order_ids}"
    df = pd.read_sql_query(query, engine)
    log.info(f'Extracted {len(df)} rows from dbo.OrderDetail')

    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame, engine: Engine) -> pd.DataFrame:
    """Clean and transform OrderLineItems data."""
    # Keep only necessary columns and rename

    df.rename(columns={
        "OrderDetailID":'OldOrderDetailID',
        'OrderID':'OldOrderID',
        'ItemID':'OldItemID',
        'PackageID':'OldPackageID',
        'StatusID':'LineItemStatus',
        'LastUpdateDT':'UpdatedAt',
        'LastUpdateBy':'LastUpdatedBy',
        'RefundAmount':'RefundedAmount',
        'RefundQty':'RefundedQuantity',
        'Price':'UnitPrice',
        'Cost':'UnitCost',
        'Description':'Notes'
        }, inplace=True)


    # Clean strings: strip & lowercase
    df['Notes'] = df['Notes'].map(lambda x: x.strip() if isinstance(x,str) and x.strip() != 'NULL' else None)
    for col in df.select_dtypes(include='object').columns:
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) else x)
            

    # Clean nulls
    df['UpdatedAt'] = df['UpdatedAt'].fillna(datetime.now())
    df['LineItemStatus'] = df['LineItemStatus'].fillna(1)
    df['TaxAmount'] = 0
    df['TaxPercent'] = 0
    df['IsInclusiveVAT'] = 0
    df['RefundedTaxAmount'] = 0
    df['UnitCost'] = df['UnitCost'].fillna(0)
    df['DiscountAmount'] = df['DiscountAmount'].fillna(0)
    df['RefundedAmount'] = df['RefundedAmount'].fillna(0)
    df['RefundedQuantity'] = df['RefundedQuantity'].fillna(0)
  
    df['LastUpdatedBy'] = df['LastUpdatedBy'].isna()
    df['CreatedBy'] = df['CreatedBy'].isna()

    df['UnitPrice'] = df['UnitPrice'] / df['Quantity']
    df['UnitCost'] = df['UnitCost'] / df['Quantity']

    df['Subtotal'] = df['UnitPrice'] * df['Quantity']
    df['GrandTotal'] = df['Subtotal'] - df['DiscountAmount'] + df['TaxAmount']
    df['Subtotal'] = df['Subtotal'].fillna(0) 
    df['GrandTotal'] = df['GrandTotal'].fillna(0) 

    df['DiscountPercent'] = (df['DiscountAmount'] / df['Subtotal']) * 100

    df['OldPackageID'] = df['OldPackageID'].fillna(1)
    df['OldItemID'] = df['OldItemID'].fillna(1)

    df['IsFreeItem'] = df['DiscountPercent'] == 100


    df = pd.merge(df, get_custom(engine, ['OrderID', 'OldOrderID', 'OrderDiscountTotal'], 'app.Orders'), on='OldOrderID', how='left')
    missing_orders = df['OrderID'].isna().sum()
    if missing_orders:
        log.warning(f'Missing OrderIDs: {missing_orders}')
        raise IncrementalDependencyError('Update Orders Table.')
    

    df['OrderDiscountAllocation'] = df.apply(lambda row: 0 if row['OrderDiscountTotal'] == 0 else (row['DiscountAmount']/row['OrderDiscountTotal'])*100, axis=1)


    df = pd.merge(df, get_packages(engine), on='OldPackageID', how='left')
    
    df = pd.merge(df, get_items(engine, df['OldItemID']), on='OldItemID', how='left')


    df.drop(columns={'OldItemID', 'OldPackageID', 'OldOrderID', 'OrderDiscountTotal'}, inplace=True)


    log.info(f'Transformation complete, df rows: {len(df)}')
    return df

# -------------------- Load --------------------
def load(df: pd.DataFrame, engine: Engine):

    dtype_mapping = {col:NVARCHAR(None) for col in df.select_dtypes(include='object').columns}
    
    try:
        with engine.begin() as conn:  # Transaction-safe

            conn.execute(text("""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.columns
                    WHERE Name = 'OldOrderDetailID'
                    AND Object_ID = Object_ID('app.OrderLineItems')
                )
                BEGIN
                    ALTER TABLE app.OrderLineItems
                    ADD OldOrderDetailID BIGINT NULL;
                END
            """))
            log.info("Verified/Added OldOrderDetailID column.")

            df.to_sql('OrderLineItems', con=conn, schema='app', if_exists='append', index=False, dtype=dtype_mapping) # type: ignore
            log.info(f'dbo.OrderDetail loaded successfully')

    except Exception as e:
        log.error(f'Failed to load dbo.OrderDetail: {e}')
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
        load(df, target)
        

# if __name__ == '__main__':
#     main()
