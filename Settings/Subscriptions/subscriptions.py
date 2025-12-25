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
def extract(source_db: Engine, target_db: Engine) -> pd.DataFrame:
    """Extract data based on CDC."""
    with target_db.begin() as conn:
        max_id = conn.execute(
            text("SELECT ISNULL(MaxIndex,0) FROM app.ETLcdc WHERE TableName=:table_name"),
            {"table_name": 'dbo.UserPackageDetails'}
        ).scalar()
    max_id = max_id if not max_id is None else 0
    log.info(f'Current CDC for dbo.UserPackageDetails: {max_id}')

    query = f"SELECT top 1000 * FROM dbo.UserPackageDetails WHERE UserPackageDetailID > {max_id} ORDER BY UserPackageDetailID"
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

            conn.execute(
                text("""
                    MERGE app.[ETLcdc] AS target
                    USING (SELECT :table_name AS [TableName], :max_index AS [MaxIndex]) AS source
                    ON target.[TableName] = source.[TableName]
                    WHEN MATCHED THEN UPDATE SET target.[MaxIndex] = source.[MaxIndex]
                    WHEN NOT MATCHED THEN INSERT ([TableName],[MaxIndex]) VALUES (source.[TableName],source.[MaxIndex]);
                """),
                {"table_name": f'dbo.UserPackageDetails', "max_index": int(max_id)}
            )
            log.info(f'dbo.UserPackageDetails loaded successfully, CDC updated to {max_id}')
    except Exception as e:
        log.error(f'Failed to load dbo.UserPackageDetails: {e}')
        raise

# -------------------- Main --------------------
def main():
    source = source_db_conn()
    target = target_db_conn()
    # return
    while True:
        df = extract(source, target)
        if df.empty:
            log.info('No new data to load.')
            return
        # return
        df = transform(df, target)
        # print(df)
        # return
        load(df, target)

if __name__ == '__main__':
    main()
