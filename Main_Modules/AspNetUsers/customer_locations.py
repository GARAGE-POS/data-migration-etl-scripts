import os
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR, DECIMAL
from urllib.parse import quote_plus
import pandas as pd
from utils.tools import get_logger
from utils.fks_mapper import get_locations, get_customers
from utils.custom_err import IncrementalDependencyError


warnings.filterwarnings('ignore')
load_dotenv()
log = get_logger('CustomerLocations')


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

    customer_ids = pd.read_sql(f'SELECT CustomerID FROM dbo.Customers WHERE UserID={user_id}', engine)
    customer_ids = (0,0) + tuple(customer_ids['CustomerID'].values.tolist())
    

    query = f"SELECT * FROM dbo.CustomerLocation_Junc WHERE CustomerID IN {customer_ids} AND UserID={user_id} ORDER BY CustomerLocationID"
    df = pd.read_sql_query(query, engine)
    log.info(f'Extracted {len(df)} rows from dbo.CustomerLocation_Junc')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame, engine: Engine) -> pd.DataFrame:
    """Clean and transform Customers data."""

    # Keep only necessary columns and rename
    df = df[['CustomerLocationID','CustomerID', 'LocationId', 'StatusID','CreatedOn', 'LastUpdatedDate']]

    df.rename(columns={
        "CustomerLocationID":'OldCustomerLocationID',
        'CustomerID':'OldID',
        'LastUpdatedDate':'UpdatedAt',
        'LocationId':'OldLocationID',
        'CreatedOn':'CreatedAt'
        }, inplace=True)

    df['OldLocationID'] = df['OldLocationID'].fillna(16)
    df['OldLocationID'] = df['OldLocationID'].map(lambda x: x if x!=0 else 16)


    df = pd.merge(df, get_locations(engine), on='OldLocationID', how='left')
    missing_loc = df['LocationID'].isna().sum()
    if missing_loc:
        raise IncrementalDependencyError(f'Missing LocationIDs: {missing_loc}. Update Locations Table.')
    
    df = pd.merge(df, get_customers(engine, df['OldID']), on='OldID', how='left')
    missing_cust = df['CustomerID'].isna().sum()
    if missing_cust:
        log.warning(f'Missing CustomerIDs: {missing_cust}.')
        raise IncrementalDependencyError(f'Update Customers in AspNetUsers Table.')

        

    df.drop(columns={'OldLocationID','OldID'}, inplace=True)

    df['UpdatedAt'] = df['UpdatedAt'].fillna(datetime.now())
    df.loc[df['CreatedAt'].isna(),  'CreatedAt'] = df['UpdatedAt']

    log.info(f'Transformation complete. df rows: {len(df)}')
    return df

# -------------------- Load --------------------
def load(df: pd.DataFrame, engine: Engine):
    
    try:
        with engine.begin() as conn:  # Transaction-safe

            conn.execute(text("""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.columns
                    WHERE Name = 'OldCustomerLocationID'
                    AND Object_ID = Object_ID('app.CustomerLocations')
                )
                BEGIN
                    ALTER TABLE app.CustomerLocations
                    ADD OldCustomerLocationID BIGINT NULL;
                END
            """))
            log.info("Verified/Added OldCustomerLocationID column.")

            df.to_sql('CustomerLocations', con=conn, schema='app', if_exists='append', index=False) # type: ignore
            log.info(f'dbo.CustomerLocation_Junc loaded successfully')

    except Exception as e:
        log.error(f'Failed to load dbo.CustomerLocation_Junc: {e}')
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
