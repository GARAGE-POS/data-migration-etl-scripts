import os
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR, DECIMAL
from urllib.parse import quote_plus
import pandas as pd
from utils.tools import get_logger
from utils.custom_err import IncrementalDependencyError

warnings.filterwarnings('ignore')
load_dotenv()
log = get_logger('Categories')

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

    location_ids = pd.read_sql(f'SELECT LocationID FROM dbo.Locations WHERE UserID={user_id}', engine)
    location_ids = (0,0) + tuple(location_ids['LocationID'].values.tolist())
    
    query = f"SELECT * FROM dbo.Category WHERE LocationID IN {location_ids} ORDER BY CategoryID"
    df = pd.read_sql_query(query, engine)
    log.info(f'Extracted {len(df)} rows from dbo.Category')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame, engine: Engine) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Clean and transform Category data."""
    # Keep only necessary columns and rename
    df.drop(
        columns=['RowID','SortByAlpha','LastUpdatedBy'], inplace=True
    )
    df = df.rename(columns={
        'AlternateName':'NameAr',
        'CategoryID':'OldCategoryID',
        'LocationID':'OldLocationID',
        'LastUpdatedDate':'UpdatedAt',
        'Image':'ImagePath'
    })

    df['UpdatedAt'] = df['UpdatedAt'].fillna(datetime.now())
    df['CreatedAt'] = df['UpdatedAt']

    for col in df.select_dtypes(include='object').columns:
        if col != 'Name':
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) and x.strip() != '' else None)
        else:
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) else x)

    df['StatusID'] = df['StatusID'].fillna(1)
    df = df[~df['OldLocationID'].isna()]


    # GET AccountIDs
    current_acc_ids = pd.read_sql("SELECT AccountID, OldLocationID FROM app.Locations WHERE OldLocationID IS NOT NULL", engine)
    df = pd.merge(df, current_acc_ids, on='OldLocationID', how='left')
    missing_acc = df['AccountID'].isna()
    if missing_acc.sum():
        log.warning(f"Missing AccountID: {missing_acc.sum()}")
        raise IncrementalDependencyError(f"Update Accounts and Locations tables.")
    
    # DROP DUPLICATED 
    sync_table = df[['OldCategoryID', 'AccountID', 'Name']].copy()
    df.sort_values(by=['AccountID', 'StatusID'], inplace=True)
    df.drop_duplicates(subset=['AccountID', 'Name'], inplace=True)


    df.drop(columns=['OldLocationID'], inplace=True)

    log.info(f'Transformation complete. df rows: {len(df)}')
    return df, sync_table

# -------------------- Load --------------------
def load(df: pd.DataFrame, sync_table: pd.DataFrame, engine: Engine):

    dtype_mapping = {'Name':NVARCHAR(None), 'NameAr':NVARCHAR(None), 'ImagePath':NVARCHAR(None), 'Description':NVARCHAR(None)}

    df.drop(columns='OldCategoryID', inplace=True)

    try:
        with engine.begin() as conn: 

            # Inserting the Data
            df.to_sql('Categories', con=conn, schema='app', if_exists='append', index=False, dtype=dtype_mapping) # type: ignore
            log.info(f'dbo.Category loaded successfully')

            sync_table.to_sql('SyncCategories', con=conn, schema='app', if_exists='append', index=False, dtype={'Name':NVARCHAR(None)}) # type: ignore
            log.info(f'app.SyncCategories updated successfully')

    
    except Exception as e:
        log.error(f'Failed to load dbo.Category: {e}')
        raise

# -------------------- Main --------------------
def main(user_id:int, if_load:bool=True):
    source = source_db_conn()
    target = target_db_conn()

    df = extract(user_id, source)
    if df.empty:
        log.info('No data to load.')
        return
    
    df, sync_table = transform(df, target)
    print(df)

    if if_load:
        load(df, sync_table, target)
        
    
# if __name__ == '__main__':
#     main()
