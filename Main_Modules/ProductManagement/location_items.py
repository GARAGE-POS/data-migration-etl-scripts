import os
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR, DECIMAL
from urllib.parse import quote_plus
import pandas as pd
from utils.tools import get_logger
from utils.fks_mapper import get_custom

warnings.filterwarnings('ignore')
load_dotenv()
log = get_logger('LocationItems')

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
def extract(engine: Engine) -> pd.DataFrame:
    """Extract data based on CDC."""
    with engine.begin() as conn:
        max_id = conn.execute(
            text("SELECT ISNULL(MaxIndex,0) FROM app.EtlCDC WHERE TableName=:table_name"),
            {"table_name": 'app.LocationItems'}
        ).scalar()
    max_id = max_id if not max_id is None else 0
    log.info(f'Current CDC for app.LocationItems: {max_id}')
    
    query = f"SELECT TOP 5000 ItemID, CategoryID, Price, UpdatedAt, CreatedAt, StatusID FROM app.Items WHERE ItemID > {max_id} ORDER BY ItemID"
    # query = f"SELECT ItemID, CategoryID, Price, CreatedAt, UpdatedAt, StatusID FROM app.Items WHERE CategoryID IN (1938, 1939, 1940, 1941, 1942, 1943, 1944, 1971, 1972, 1973)"
    df = pd.read_sql_query(query, engine)
    log.info(f'Extracted {len(df)} rows from app.Items')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame, engine: Engine) ->  pd.DataFrame:
    """Clean and transform Category data."""
    # Keep only necessary columns and rename
    df = pd.merge(df, get_custom(engine, ['AccountID', 'CategoryID'], 'app.Categories'), how='left', on='CategoryID')
    
    location_ids = pd.read_sql(f"SELECT LocationID, AccountID FROM app.Locations WHERE AccountID IN {tuple(df['AccountID'].values.tolist())+(0,0)}", engine)
    df = pd.merge(df, location_ids, on='AccountID', how='left')

    df.drop(columns=['CategoryID', 'AccountID'], inplace=True)

    log.info(f'Transformation complete, output rows: {len(df)}')
    return df

# -------------------- Load --------------------
def load(df: pd.DataFrame, engine: Engine):

    max_id = df['ItemID'].max()

    try:
        with engine.begin() as conn: 

            # Inserting the Data
            df.to_sql('LocationItems', con=conn, schema='app', if_exists='append', index=False) # type: ignore
            log.info(f'app.LocationItems loaded successfully')

            # # Updating the CDC
            conn.execute(
                text("""
                    MERGE app.[EtlCDC] AS target
                    USING (SELECT :table_name AS [TableName], :max_index AS [MaxIndex]) AS source
                    ON target.[TableName] = source.[TableName]
                    WHEN MATCHED THEN UPDATE SET target.[MaxIndex] = source.[MaxIndex]
                    WHEN NOT MATCHED THEN INSERT ([TableName],[MaxIndex]) VALUES (source.[TableName],source.[MaxIndex]);
                """),
                {"table_name": f'app.LocationItems', "max_index": int(max_id)}
            )
            log.info(f'app.LocationItems loaded successfully, CDC updated to {max_id}')
    except Exception as e:
        log.error(f'Failed to load app.LocationItems: {e}')
        raise

# -------------------- Main --------------------
def main():

    target = target_db_conn()
    while True:
        df = extract(target)
        if df.empty:
            log.info('No new data to load.')
            return
        df = transform(df, target)
        # print(df)
        # return
        load(df, target)
        # return
    
if __name__ == '__main__':
    main()
