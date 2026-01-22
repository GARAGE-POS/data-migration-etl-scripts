import os
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR, DECIMAL
from urllib.parse import quote_plus
import pandas as pd
from utils.tools import get_logger
from utils.fks_mapper import get_accounts
from utils.custom_err import IncrementalDependencyError 

log = get_logger('Subscriptions')
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
def extract(user_id: int, source_db: Engine) -> pd.DataFrame:
    """Extract data based on UserID."""


    query = f"SELECT * FROM dbo.UserPackageDetails WHERE UserID={user_id} ORDER BY UserPackageDetailID"
    df = pd.read_sql_query(query, source_db)
    log.info(f'Extracted {len(df)} rows from dbo.UserPackageDetails')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame, engine: Engine) -> pd.DataFrame:
    """Clean and transform UserPackageDetails data."""
    # Keep only necessary columns and rename
    df.rename(columns={
        "UserPackageDetailID":'OldUserPackageDetailID',
        'UserID':'OldUserID',
        'PackageInfoID':'SubscriptionType',
        "CreatedDate": "CreatedAt",
        'LastUpdatedDate':'UpdatedAt'
        }, inplace=True)


    df['UpdatedAt'] = df['UpdatedAt'].fillna(datetime.now())
    df.loc[df['ExpiryDate'].isna(), 'ExpiryDate'] = df['CreatedAt'] + pd.DateOffset(years=1)
    df['SubscriptionType'] = df['SubscriptionType'].fillna(1)
    df['StatusID'] = df['StatusID'].fillna(1)

    df['SubscriptionName'] = df['SubscriptionType'].map({1:'FREE', 2:'PROF'})

    df['StartDate'] = df['CreatedAt']
    df['PaymentTerm'] = 0
    df['NumberOfTerminals'] = 0


    df = pd.merge(df, get_accounts(engine), on='OldUserID', how='left')
    missing_accs = df['AccountID'].isna().sum()
    if missing_accs:
        log.warning(f'Missing AccountIDs: {missing_accs}')
        raise IncrementalDependencyError('Update Accounts Table.')

    df.drop(columns='OldUserID', inplace=True)


    log.info(f'Transformation complete, output: {len(df)}')
    return df

# -------------------- Load --------------------
def load(df: pd.DataFrame, engine: Engine):
    dtype_mapping = {col:NVARCHAR(None) for col in df.select_dtypes(include='object').columns}

    max_id = df['OldUserPackageDetailID'].max()

    try:
        with engine.begin() as conn:  # Transaction-safe

            conn.execute(text("""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.columns
                    WHERE Name = 'OldUserPackageDetailID'
                    AND Object_ID = Object_ID('app.Subscriptions')
                )
                BEGIN
                    ALTER TABLE app.Subscriptions
                    ADD OldUserPackageDetailID BIGINT NULL;
                END
            """))
            log.info("Verified/Added OldUserPackageDetailID column.")

            df.to_sql('Subscriptions', con=conn, schema='app', if_exists='append', index=False, dtype=dtype_mapping) # type: ignore
            log.info(f'dbo.UserPackageDetails loaded successfully')

    except Exception as e:
        log.error(f'Failed to load dbo.UserPackageDetails: {e}')
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
