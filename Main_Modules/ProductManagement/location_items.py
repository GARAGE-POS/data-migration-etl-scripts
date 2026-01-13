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
def extract(user_id:int, engine: Engine) -> pd.DataFrame:
    """Extract data based on UserID."""

    category_query = f"""
        SELECT CategoryID
        FROM app.Categories
        WHERE AccountID IN (
            SELECT AccountID 
            FROM app.Accounts 
            WHERE OldUserID={user_id}    
        )
    """

    category_ids = pd.read_sql(category_query,engine)
    category_ids = (0,0) + tuple(category_ids['CategoryID'].values.tolist())

    query = f"SELECT ItemID, CategoryID, Price, CreatedAt, UpdatedAt, StatusID FROM app.Items WHERE CategoryID IN {category_ids}"
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

    log.info(f'Transformation complete, df rows: {len(df)}')
    return df

# -------------------- Load --------------------
def load(df: pd.DataFrame, engine: Engine):

    try:
        with engine.begin() as conn: 

            # Inserting the Data
            df.to_sql('LocationItems', con=conn, schema='app', if_exists='append', index=False) # type: ignore
            log.info(f'app.LocationItems loaded successfully')

    except Exception as e:
        log.error(f'Failed to load app.LocationItems: {e}')
        raise

# -------------------- Main --------------------
def main(user_id:int, if_load:bool=True):

    target = target_db_conn()

    df = extract(user_id, target)
    if df.empty:
        log.info('No new data to load.')
        return
        
    df = transform(df, target)
    print(df)
    
    if if_load:
        load(df, target)
    
# if __name__ == '__main__':
#     main()
