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
def extract(source_db: Engine, target_db: Engine) -> pd.DataFrame:
    """Extract data based on CDC."""
    with target_db.begin() as conn:
        max_id = conn.execute(
            text("SELECT ISNULL(MaxIndex,0) FROM app.EtlCDC WHERE TableName=:table_name"),
            {"table_name": 'dbo.OrderCheckout'}
        ).scalar()
    
    max_id = max_id if not max_id is None else 0
    # max_id = 14948
    log.info(f'Current CDC for dbo.OrderCheckout: {max_id}')

    order_ids = pd.read_sql(f"SELECT TOP 3000 OrderID, OldOrderID FROM app.Orders WHERE OrderID > {max_id}", target_db)
    print(order_ids)
    order_ids = tuple(order_ids['OldOrderID'].values.tolist()) + (0,0)

    # query = f"SELECT TOP 5000 OrderCheckOutID, OrderID, PaymentMode, Remarks, OrderStatus, CreatedOn, CreatedBy, AppSourceID, AmountPaid FROM dbo.OrderCheckout WHERE OrderCheckOutID > {max_id} ORDER BY OrderCheckOutID"
    query = f"SELECT OrderCheckOutID, OrderID, PaymentMode, Remarks, OrderStatus, CreatedOn, CreatedBy, AppSourceID, AmountPaid FROM dbo.OrderCheckout WHERE OrderID IN {order_ids} ORDER BY OrderCheckOutID"
    df = pd.read_sql_query(query, source_db)
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

    log.info(f'Transformation complete, df\'s Length is {len(df)}')
    return df

# -------------------- Load --------------------
def load(df: pd.DataFrame, engine: Engine):

    dtype_mapping = {col:NVARCHAR(None) for col in df.select_dtypes(include='object').columns}
    
    max_id = df['OrderID'].max()

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

            conn.execute(
                text("""
                    MERGE app.[EtlCDC] AS target
                    USING (SELECT :table_name AS [TableName], :max_index AS [MaxIndex]) AS source
                    ON target.[TableName] = source.[TableName]
                    WHEN MATCHED THEN UPDATE SET target.[MaxIndex] = source.[MaxIndex]
                    WHEN NOT MATCHED THEN INSERT ([TableName],[MaxIndex]) VALUES (source.[TableName],source.[MaxIndex]);
                """),
                {"table_name": f'dbo.OrderCheckout', "max_index": int(max_id)}
            )
            log.info(f'dbo.OrderCheckout loaded successfully, CDC updated to {max_id}')
    except Exception as e:
        log.error(f'Failed to load dbo.OrderCheckout: {e}')
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
        # print(df.head(20))
        # return
        load(df, target)
        # return

if __name__ == '__main__':
    main()
