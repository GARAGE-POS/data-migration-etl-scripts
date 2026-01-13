import os
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR
from urllib.parse import quote_plus
import pandas as pd
from utils.fks_mapper import get_customers, get_custom
from utils.tools import get_logger, parse_date
from utils.custom_err import IncrementalDependencyError

log = get_logger('Cars')
warnings.filterwarnings('ignore')
load_dotenv()


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

    query = f"SELECT * FROM dbo.Cars WHERE UserID={user_id} ORDER BY CarID"
    df = pd.read_sql_query(query, engine)
    log.info(f'Extracted {len(df)} rows from dbo.Cars')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame, source_db: Engine, target_db: Engine) -> pd.DataFrame:
    """Clean and transform Cars data."""
    # Keep only necessary columns and rename
    df = df[['CarID', 'CustomerID' , 'MakeID', 'ModelID', 'Year', 'Color', 'VinNo', 'Description', 'RegistrationNo', 'ImagePath', 'CarType', 'StatusID','CreatedOn', 'LastUpdatedDate']]

    df.rename(columns={
        "CarID":'OldCarID',
        "CustomerID":'OldID',
        'ModelID':'OldModelID',
        'LastUpdatedDate':'UpdatedAt',
        'CreatedOn':'CreatedAt',
        'MakeID':'OldMakeID'
        }, inplace=True)


    # Clean strings: strip & lowercase
    for col in df.select_dtypes(include='object').columns:
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) and x.strip()!='' else None)
            df[col] = df[col].apply(lambda x: x if isinstance(x,str) and x != 'NULL' else None)

    # Filling Null Values in StatusID, CarType and CarPlateType
    df['StatusID'] = df['StatusID'].fillna(1)
    df['CarType'] = df['CarType'].fillna(0)
    df['CarPlateType'] = 0


    # Sync CustomerID and ModelID
    df = pd.merge(df, get_customers(target_db, df['OldID']), on='OldID', how='left')
    missing_cust = df['CustomerID'].isna().sum()
    if missing_cust:
        log.warning(f'Missing CustomerIDs: {missing_cust}.')
        raise IncrementalDependencyError('Update Customers in AspNetUsers Table')

    df = pd.merge(df, get_custom(target_db, ['MakeID', 'ModelID', 'OldModelID'], 'app.Models'), on='OldModelID', how='left')


    # Fixing Date columns
    df.set_index('OldCarID', drop=False, inplace=True)
    df.index.name = None

    missing_update = df['UpdatedAt'].isna()
    log.info(f'Missing UpdatedAt is {missing_update.sum()}')

    df['CreatedAt'] = df['UpdatedAt']
    if int(missing_update.sum()) > 0:
        car_ids = tuple(df[df['UpdatedAt'].isna()]['OldCarID'].values.tolist()) + (0,0)
        ids = str(car_ids)
        dates = pd.read_sql(f"SELECT CarID, LastUpdatedDate, CreatedOn FROM dbo.CarsLocation_Junc WHERE CarID IN {ids} ORDER BY CarID, CreatedOn", source_db)

        if len(dates):
            dates.set_index('CarID', drop=False, inplace=True)
            dates.index.name = None
            dates = dates.drop_duplicates(subset='CarID', keep='first')
            dates.loc[dates['CreatedOn'].isna(), 'CreatedOn'] = dates['LastUpdatedDate']
            df.loc[df['UpdatedAt'].isna(), 'CreatedAt'] = dates['CreatedOn']

    df['UpdatedAt'] = df['UpdatedAt'].fillna(datetime.now())
    df['CreatedAt'] = df['CreatedAt'].fillna(datetime(2000, 1,1,0,0,0))


    for col in ['CreatedAt', 'UpdatedAt']:
        df[col] = df[col].apply(parse_date) # type: ignore


    missing_date = df[(df['CreatedAt'].isna()) | (df['UpdatedAt'].isna())]
    if len(missing_date):
        log.warning(f"Missing dates: {len(missing_date)}")
        raise ValueError("Some of 'CreatedAt' or 'UpdatedAt' values are missing.")

    df.drop(columns={'OldID', 'OldMakeID', 'OldModelID'}, inplace=True)

    log.info(f'Transformation complete. df rows: {len(df)}')
    return df

# -------------------- Load --------------------
def load(df: pd.DataFrame, engine: Engine):

    dtype_mapping = {col:NVARCHAR(None) for col in df.select_dtypes(include='object').columns}
    
    try:
        with engine.begin() as conn:  # Transaction-safe

            conn.execute(text("""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.columns
                    WHERE Name = 'OldCarID'
                    AND Object_ID = Object_ID('app.Cars')
                )
                BEGIN
                    ALTER TABLE app.Cars
                    ADD OldCarID BIGINT NULL;
                END
            """))
            log.info("Verified/Added OldCarID column.")

            df.to_sql('Cars', con=conn, schema='app', if_exists='append', index=False, dtype=dtype_mapping) # type: ignore
            log.info(f'dbo.Cars loaded successfully')


    except Exception as e:
        log.error(f'Failed to load dbo.Cars: {e}')
        raise

# -------------------- Main --------------------
def main(user_id:int, if_load:bool=True):
    source = source_db_conn()
    target = target_db_conn()

    df = extract(user_id, source)
    if df.empty:
        log.info('No data to load.')
        return
        
    df = transform(df, source, target)
    print(df)

    if if_load:
        load(df, target)
        

# if __name__ == '__main__':
#     main()
