import os
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR, DECIMAL
from urllib.parse import quote_plus
import pandas as pd
from utils.custom_err import IncrementalDependencyError
from utils.tools import clean_contact, get_last_ingested, get_logger, update_last_ingested
from utils.fks_mapper import get_locations

warnings.filterwarnings('ignore')
load_dotenv()

log = get_logger('Warehouses')

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

    last_id = get_last_ingested(user_id, 'dbo.Stores')

    query = f"SELECT * FROM dbo.Stores WHERE UserID={user_id} AND StoreID > {last_id} ORDER BY StoreID"
    df = pd.read_sql_query(query, engine)
    log.info(f'Extracted {len(df)} rows from dbo.Stores')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame, engine: Engine) -> pd.DataFrame:
    """Clean and transform Stores data."""
    # Keep only necessary columns and rename
    df = df[['StoreID', 'Name', 'StoreLocationID', 'Contact', 'Address', 'StatusID', 'Type', 'LastUpdatedDate']]

    df.rename(columns={
        "StoreID":'OldStoreID',
        "StoreLocationID":"OldLocationID",
        "CreatedOn":"CreatedAt",
        "LastUpdatedDate":"UpdatedAt"
        }, inplace=True)

    # Clean strings
    for col in df.select_dtypes(include='object').columns:
            if col !='Name':
                df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) and x.strip()!='' else None)
            else: 
                df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) else x)

    df['Contact'] = df['Contact'].map(clean_contact)
    
    # Filling Null Values
    df['StatusID'] = df['StatusID'].fillna(1)
    df['UpdatedAt'] = df['UpdatedAt'].fillna(datetime.now())
    df['CreatedAt'] = df['UpdatedAt']

    df["IsMainStore"] = df['Type'].apply(lambda x: 1 if x == 'Main Store' else 0)

    df['OldLocationID'] = df['OldLocationID'].fillna(df['OldLocationID'].min())

    df = pd.merge(df, get_locations(engine), on='OldLocationID', how='left')
    missing_loc = df['LocationID'].isna().sum()
    if missing_loc:
        log.warning(f'Missing LocationIDs: {missing_loc}')
        raise IncrementalDependencyError("Update Locations Table.")


    df.drop(columns=['Type', 'OldLocationID'], inplace=True)


    log.info(f'Transformation complete, df\'s Length is {len(df)}')
    return df

# -------------------- Load --------------------
def load(df: pd.DataFrame, user_id: int, engine: Engine):

    dtype_mapping = {col:NVARCHAR(None) for col in df.select_dtypes(include='object').columns}
    
    try:
        with engine.begin() as conn:  # Transaction-safe

            conn.execute(text("""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.columns
                    WHERE Name = 'OldStoreID'
                    AND Object_ID = Object_ID('app.Warehouses')
                )
                BEGIN
                    ALTER TABLE app.Warehouses
                    ADD OldStoreID BIGINT NULL;
                END
            """))
            log.info("Verified/Added OldStoreID column.")

            df.to_sql('Warehouses', con=conn, schema='app', if_exists='append', index=False, dtype=dtype_mapping) # type: ignore
            update_last_ingested(user_id, 'dbo.Stores', int(df['OldStoreID'].max()))
            log.info(f'dbo.Stores loaded successfully')

    except Exception as e:
        log.error(f'Failed to load dbo.Stores: {e}')
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
