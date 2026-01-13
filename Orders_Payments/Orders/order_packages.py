import os
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR, DECIMAL
from urllib.parse import quote_plus
import pandas as pd
from utils.tools import get_logger
from utils.fks_mapper import get_order_details, get_items
from utils.custom_err import IncrementalDependencyError


warnings.filterwarnings('ignore')
load_dotenv()

log = get_logger("OrderDetailPackages")

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

    query = f"SELECT  * FROM dbo.OrderPackageDetail WHERE OrderID IN {order_ids} ORDER BY OrderPkgDetailID"
    df = pd.read_sql_query(query, engine)
    log.info(f'Extracted {len(df)} rows from dbo.OrderPackageDetail')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame, engine: Engine) -> pd.DataFrame:
    """Clean and transform OrderDetailPackages data."""
    # Keep only necessary columns and rename

    df.rename(columns={
        "OrderPkgDetailID":'OldOrderPackageDetailID',
        'OrderDetailID':'OldOrderDetailID',
        'ItemID':'OldItemID'
        }, inplace=True)


    df['Name'] = df['Name'].fillna('')
    df['Name'] = df['Name'].map(lambda x: x.strip() if x.strip() != 'NULL' else '')


    df = pd.merge(df, get_order_details(engine, df['OldOrderDetailID']), on='OldOrderDetailID', how='left')
    missing_order_details = df['OrderDetailID'].isna().sum()
    if missing_order_details:
        log.warning(f'Missing OrderDetailIDs: {missing_order_details}')
        raise IncrementalDependencyError('Update OrderDetails Table.')
    
    df = pd.merge(df, get_items(engine, df['OldItemID']), on='OldItemID', how='left')
    missing_items = df['ItemID'].isna().sum()
    if missing_items:
        log.warning(f'Missing ItemIDs: {missing_items}')
        raise IncrementalDependencyError('Update Items Table.')


    df.drop(columns={'OldOrderDetailID', 'OldItemID'}, inplace=True)


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
                    WHERE Name = 'OldOrderPackageDetailID'
                    AND Object_ID = Object_ID('app.OrderDetailPackages')
                )
                BEGIN
                    ALTER TABLE app.OrderDetailPackages
                    ADD OldOrderPackageDetailID BIGINT NULL;
                END
            """))
            log.info("Verified/Added OldOrderPackageDetailID column.")

            df.to_sql('OrderDetailPackages', con=conn, schema='app', if_exists='append', index=False, dtype=dtype_mapping) # type: ignore
            log.info(f'dbo.OrderPackageDetail loaded successfully')

    except Exception as e:
        log.error(f'Failed to load dbo.OrderPackageDetail: {e}')
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
        
        load(df, target)

# if __name__ == '__main__':
#     main()
