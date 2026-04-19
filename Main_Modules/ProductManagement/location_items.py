import os
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR, DECIMAL
from urllib.parse import quote_plus
import pandas as pd
from utils.custom_err import IncrementalDependencyError
from utils.tools import get_last_ingested, get_logger, update_last_ingested
from utils.fks_mapper import get_custom, get_items, get_locations

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
def extract(user_id:int, engine: Engine) -> pd.DataFrame:
    """Extract data based on UserID."""

    last_id = get_last_ingested(user_id, 'app.LocationItems')

    query = f"""
        SELECT
            i.ItemID OldItemID,
            l.LocationID OldLocationID,
            i.Price,
            i.LastUpdatedDate UpdatedAt,
            i.StatusID
        FROM Items i
        JOIN SubCategory sc ON sc.SubCategoryID = i.SubCatID
        JOIN Category c ON sc.CategoryID = c.CategoryID
        JOIN Locations l ON c.LocationID = l.LocationID
        JOIN Users u ON l.UserID = u.UserID
        WHERE u.UserID = {user_id} and i.ItemID > {last_id}
        ORDER BY ItemID
    """

    df = pd.read_sql_query(query, engine)
    log.info(f'Extracted {len(df)} rows from dbo.Items')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame, engine: Engine) ->  pd.DataFrame:
    """Clean and transform Category data."""

    # Fix Null values
    df['UpdatedAt'] = df['UpdatedAt'].fillna(datetime.now())
    df['CreatedAt'] = df['UpdatedAt']
    df['StatusID'] = df['StatusID'].fillna(2)

    # IDs mapping
    df = pd.merge(df, get_items(engine), on='OldItemID', how='left')
    missing_items = df['ItemID'].isna().sum()
    if missing_items:
        log.warning(f'Missing ItemIDs: {missing_items}')
        raise IncrementalDependencyError('Update Items Table.')
        
    df = pd.merge(df, get_locations(engine), on='OldLocationID', how='left')
    missing_loc = df['LocationID'].isna().sum()
    if missing_loc:
        log.warning(f'Missing LocationIDs: {missing_loc}')
        raise IncrementalDependencyError('Update Locations Table.')
    

    log.info(f'Transformation complete, df rows: {len(df)}')
    return df

# -------------------- Load --------------------
def load(df: pd.DataFrame, user_id: int, engine: Engine):

    cols = ['ItemID', 'LocationID', 'Price', 'StatusID', 'CreatedAt', 'UpdatedAt']

    try:
        i = 0
        while i < len(df)/5000:

            df[5000*i : 5000*(i+1)][cols].to_sql('LocationItems', con=engine, schema='app', if_exists='append', index=False) # type: ignore
            update_last_ingested(user_id, 'app.LocationItems', int(df[5000*i : 5000*(i+1)]['OldItemID'].max()))
            log.info(f"Batch {i+1} inserted.")
            i+=1
            
        log.info(f'app.LocationItems loaded successfully')

    except Exception as e:
        log.error(f'Failed to load app.LocationItems: {e}')
        raise

# -------------------- Main --------------------
def main(user_id:int, if_load:bool=True):

    source = source_db_conn()
    target = target_db_conn()

    df = extract(user_id, source)
    if df.empty:
        log.info('No new data to load.')
        return
        
    df = transform(df, target)
    print(df)
    
    if if_load:
        load(df, user_id, target)
    
# if __name__ == '__main__':
#     main()
