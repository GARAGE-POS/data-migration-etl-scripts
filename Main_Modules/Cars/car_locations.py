import os
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine
from urllib.parse import quote_plus
import pandas as pd
from utils.tools import get_logger
from utils.fks_mapper import get_locations, get_custom
from utils.custom_err import IncrementalDependencyError

warnings.filterwarnings('ignore')
load_dotenv()

log = get_logger('CarLocations')

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

    car_ids = pd.read_sql(f'SELECT CarID FROM dbo.Cars WHERE UserID={user_id}', engine)
    car_ids = (0,0) + tuple(car_ids['CarID'].values.tolist())
 
    query = f"SELECT * FROM dbo.CarsLocation_Junc WHERE CarID IN {car_ids} AND UserID={user_id} ORDER BY CarLocationID"
    df = pd.read_sql_query(query, engine)
    log.info(f'Extracted {len(df)} rows from dbo.CarsLocation_Junc')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame, engine: Engine) -> pd.DataFrame:
    """Clean and transform data."""

    # Keep only necessary columns and rename
    df = df[['CarLocationID','CarID', 'LocationID', 'StatusID','CreatedOn', 'LastUpdatedDate']]

    df.rename(columns={
        "CarLocationID":'OldCarLocationID',
        'CarID':'OldCarID',
        'LocationID':'OldLocationID',
        'LastUpdatedDate':'UpdatedAt',
        'CreatedOn':'CreatedAt'}
        ,inplace=True)
    
    
    df = pd.merge(df, get_locations(engine), on='OldLocationID', how='left')
    missing_locs = df['LocationID'].isna().sum()
    if missing_locs:
        log.warning(f'Missing LocationIDs: {missing_locs}.')
        raise IncrementalDependencyError(f'Update Locations Table.')
        

    df = pd.merge(df, get_custom(engine, ['CarID', 'OldCarID'], 'app.Cars'), on='OldCarID', how='left')
    missing_cars = df['CarID'].isna().sum()
    if missing_cars:
        log.warning(f'Missing CarIDs: {missing_cars}.')
        raise IncrementalDependencyError(f'Update Cars Table.')
        

    df.drop(columns={'OldLocationID','OldCarID'}, inplace=True)

    df.loc[df['CreatedAt'].isna(), 'CreatedAt'] = df['UpdatedAt']
    df['CreatedAt'] = df['CreatedAt'].fillna(datetime(2000, 1,1,0,0,0))
    df['UpdatedAt'] = df['UpdatedAt'].fillna(datetime.now())
    df['StatusID'] = df['StatusID'].fillna(1)


    log.info(f'Transformation complete, length of df is {len(df)}')
    return df

# -------------------- Load --------------------
def load(df: pd.DataFrame, engine: Engine):

    try:
        with engine.begin() as conn:  # Transaction-safe

            conn.execute(text("""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.columns
                    WHERE Name = 'OldCarLocationID'
                    AND Object_ID = Object_ID('app.CarLocations')
                )
                BEGIN
                    ALTER TABLE app.CarLocations
                    ADD OldCarLocationID BIGINT NULL;
                END
            """))
            log.info("Verified/Added OldCarLocationID column.")

            df.to_sql('CarLocations', con=conn, schema='app', if_exists='append', index=False) # type: ignore
            log.info(f'dbo.CarsLocation_Junc loaded successfully')

    except Exception as e:
        log.error(f'Failed to load dbo.CarsLocation_Junc: {e}')
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
        load(df, target)
        

# if __name__ == '__main__':
#     main()
