import os
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR, DECIMAL
from urllib.parse import quote_plus
import pandas as pd
from utils.tools import get_logger
from utils.fks_mapper import get_orders, get_custom
from utils.custom_err import IncrementalDependencyError


warnings.filterwarnings('ignore')
load_dotenv()

log = get_logger("OrderPayments")

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

    query = f"SELECT OrderCheckOutID, OrderID, PaymentMode, Remarks, OrderStatus, CreatedOn, CreatedBy, AppSourceID, AmountPaid FROM dbo.OrderCheckout WHERE OrderID IN {order_ids} ORDER BY OrderCheckOutID"
    df = pd.read_sql_query(query, engine)
    log.info(f'Extracted {len(df)} rows from dbo.OrderCheckout')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame, engine: Engine) -> pd.DataFrame:
    """Clean and transform OrderPayments data."""
    # Keep only necessary columns and rename

    df.rename(columns={
        "OrderCheckOutID":'OldPaymentID',
        'OrderID':'OldOrderID',
        'OrderStatus':'StatusID',
        'CreatedOn':'CreatedAt',
        'Remarks':'Notes',
        'PaymentMode':'PaymentModeID',
        'AppSourceID':'OldAppSourceID'
        }, inplace=True)
    
    df['CreatedBy'] = 0
    df['PaymentModeID'] = df['PaymentModeID'].fillna(1)
    df['OldAppSourceID'] = pd.to_numeric(df['OldAppSourceID'], errors='coerce')

    df = pd.merge(df, get_orders(engine, df['OldOrderID']), on='OldOrderID', how='left')
    missing_orders = df['OrderID'].isna().sum()
    if missing_orders:
        log.warning(f'Missing OrderIDs: {missing_orders}')
        raise IncrementalDependencyError('Update Orders Table.')
   
    df = pd.merge(df, get_custom(engine, '*', 'app.SyncAppSources'), how='left', on='OldAppSourceID')

    df.drop(columns={'OldOrderID', 'OldAppSourceID'}, inplace=True)

    log.info(f'Transformation complete. df rows: {len(df)}')
    return df

# -------------------- Load --------------------
def load(df: pd.DataFrame, engine: Engine):

    dtype_mapping = {col:NVARCHAR(None) for col in df.select_dtypes(include='object').columns}

    try:
        with engine.begin() as conn:  # Transaction-safe

            conn.execute(text("""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.columns
                    WHERE Name = 'OldPaymentID'
                    AND Object_ID = Object_ID('app.OrderPayments')
                )
                BEGIN
                    ALTER TABLE app.OrderPayments
                    ADD OldPaymentID BIGINT NULL;
                END
            """))
            log.info("Verified/Added OldPaymentID column.")

            df.to_sql('OrderPayments', con=conn, schema='app', if_exists='append', index=False, dtype=dtype_mapping) # type: ignore
            log.info(f'dbo.OrderCheckout loaded successfully')

    except Exception as e:
        log.error(f'Failed to load dbo.OrderCheckout: {e}')
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
