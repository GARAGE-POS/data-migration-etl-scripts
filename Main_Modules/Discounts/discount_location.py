import os
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR, DECIMAL
from urllib.parse import quote_plus
import pandas as pd
from utils.custom_err import IncrementalDependencyError
from utils.tools import fill_useraccounts, get_last_ingested, get_logger,  clean_contact, update_last_ingested
from utils.fks_mapper import get_cities, get_accounts, get_custom, get_discounts, get_locations, get_users


warnings.filterwarnings('ignore')
load_dotenv()
log = get_logger('DiscountLocations')

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

    last_id = get_last_ingested(user_id, 'dbo.DiscLocationJunc')

    query = f"SELECT * FROM dbo.DiscLocationJunc WHERE LocationID IN (SELECT LocationID FROM dbo.Locations WHERE UserID={user_id}) AND ID > {last_id} ORDER BY DiscountID"
    df = pd.read_sql_query(query, engine)
    log.info(f'Extracted {len(df)} rows from dbo.DiscLocationJunc')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame, engine: Engine) -> pd.DataFrame:
    """Clean and transform Discount data."""


    df.rename(columns={
        "ID":'OldDiscountLocationID',
        "DiscountID":'OldDiscountID',
        "LocationID":'OldLocationID',
        'LastUpdatedDate':'UpdatedAt'
        }, inplace=True)

    df['UpdatedAt'] = df['UpdatedAt'].fillna(datetime.now())
    df['CreatedAt'] = df['UpdatedAt']
    df['StatusID'] = 2
    df = pd.merge(df, get_discounts(engine), on='OldDiscountID', how='left')
    df.dropna(subset='DiscountID', inplace=True)
    missing_disc = df['DiscountID'].isna().sum()
    if missing_disc:
        log.warning(f'Missing DiscountIDs: {missing_disc}')
        raise IncrementalDependencyError("Update Discounts Table.")
    

    df = pd.merge(df, get_locations(engine), on='OldLocationID', how='left')
    missing_loc = df['LocationID'].isna().sum()
    if missing_loc:
        log.warning(f'Missing LocationIDs: {missing_loc}')
        raise IncrementalDependencyError("Update Locations Table.")
    
    df.drop(columns=['OldLocationID', 'OldDiscountID'], inplace=True)

    log.info(f'Transformation complete. df rows: {len(df)}')
    return df

# -------------------- Load --------------------
def load(df: pd.DataFrame, user_id: int, engine: Engine):

    dtype_mapping = {col:NVARCHAR(None) for col in df.select_dtypes(include='object').columns}

    last_id = int(df['OldDiscountLocationID'].max())
    df.drop(columns=['OldDiscountLocationID'], inplace=True)

    try:
        with engine.begin() as conn:  # Transaction-safe


            df.to_sql('DiscountLocations', con=conn, schema='app', if_exists='append', index=False, dtype=dtype_mapping) # type: ignore
            log.info(f'dbo.DiscLocationJunc loaded successfully')

            update_last_ingested(user_id, 'dbo.DiscLocationJunc', last_id)

    except Exception as e:
        log.error(f'Failed to load dbo.Users: {e}')
        raise

# -------------------- Main --------------------
def main(user_id: int, if_load:bool=True):
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
