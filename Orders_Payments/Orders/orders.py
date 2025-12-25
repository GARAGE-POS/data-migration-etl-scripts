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

    query = f"SELECT TOP 15000 * FROM dbo.Orders WHERE OrderID > {max_id} ORDER BY OrderID"
    df = pd.read_sql_query(query, source_db)
    log.info(f'Extracted {len(df)} rows from dbo.Orders')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame, engine: Engine) -> pd.DataFrame:
    """Clean and transform Orders data."""

    # Keep only necessary columns and rename
    df = df[['OrderID', 'LocationID', 'TransactionNo', 'OrderNo', 'CarID', 'CustomerID', 'BayID', 'OrderType', 'OrderCreatedDT', 'OrderMode', 'OrderTakerID', 'StatusID','CreatedOn', 'LastUpdateDT']]

    df.rename(columns={
        "OrderID":'OldOrderID',
        'LocationID':'OldLocationID',
        'CarID':'OldCarID',
        'BayID':'OldBayID',
        'OrderTakerID':'OldID',
        'LastUpdateDT':'UpdatedAt',
        'CreatedOn':'CreatedAt',
        'OrderCreatedDT':'OrderCreatedDate'
        }, inplace=True)


    # Clean strings: strip & lowercase
    for col in df.select_dtypes(include='object').columns:
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) else x)
            

    # Clean nulls
    df['UpdatedAt'] = df['UpdatedAt'].fillna(datetime.now())
    df['StatusID'] = df['StatusID'].fillna(1)
    df['CustomerID'] = df['CustomerID'].fillna(np.nan)

    # OrderType and Mode
    df['OrderType'] = df['OrderType'].map({'New': 0})
    df['OrderMode'] = df['OrderMode'].map({'New':0, 'Update':1})
        


    df = pd.merge(df, get_locations(engine), on='OldLocationID', how='left')
    df = pd.merge(df, get_cars(engine, df['OldCarID']), on='OldCarID', how='left')
    df = pd.merge(df, get_custom(engine, ['BayID', 'OldBayID'], 'app.Bays'), on='OldBayID', how='left')
    df = pd.merge(df, get_users(engine, df['OldID']), on='OldSubUserID', how='left')
    df = pd.merge(df, get_customers(engine, df['OldID']), on='OldSubUserID', how='left')



    df.drop(columns={'OldLocationID', 'OldCarID', 'OldBayID', 'OldID'}, inplace=True)
    df.rename(columns={'Id':'OrderTakerID'}, inplace=True)

    # Missing Order Takers we should fix this
    # df['OrderTakerID'] = df['OrderTakerID'].fillna(339439)

    # print(df[['OrderTakerID', 'LocationID','CarID', 'BayID','CustomerID', 'OrderMode']])

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
        # print(df.head(20))
        # return
        load(df, target)

if __name__ == '__main__':
    main()
