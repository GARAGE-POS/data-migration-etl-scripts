import os
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR
from urllib.parse import quote_plus
import pandas as pd
from utils.fks_mapper import get_accounts
from utils.tools import get_logger

log = get_logger('AppSources')
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
    """Extract new rows based on CDC."""
    with target_db.connect() as conn:
        max_id = conn.execute(
            text("SELECT ISNULL(MaxIndex,0) FROM app.EtlCDC WHERE TableName=:table_name"),
            {"table_name": 'dbo.AppSource'}
        ).scalar()
    
    max_id = max_id if not max_id is None else 0
    log.info(f'Current CDC for dbo.AppSource: {max_id}')

    query = f"SELECT * FROM dbo.AppSource WHERE SourceID > {max_id}"
    df = pd.read_sql_query(query, source_db)
    log.info(f'Extracted {len(df)} rows from dbo.AppSource')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame, target_db:Engine) -> pd.DataFrame:
    """Clean and transform AppSource data."""
    # Keep only necessary columns and rename
    df.drop(columns='LastUpdatedBy', inplace=True)
    df = df.rename(columns={
        'UserID':'OldUserID',
        'ArabicName':'NameAr',
        'SourceID':'OldSourceID',
        'LastUpdatedDate':'UpdatedAt'
    })

    df['UpdatedAt'] = df['UpdatedAt'].fillna(datetime.now())
    df['CreatedAt'] = df['UpdatedAt']

    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) else x)



    # Map Old to New UserIDs
    df = pd.merge(df,get_accounts(target_db), on='OldUserID', how='left')
    df = df.drop(columns='UserID')
    df.rename(columns={'AccountId':'UserID'}, inplace=True)



    log.info('Transformation complete')
    return df

# -------------------- Load --------------------
def load(df: pd.DataFrame, engine: Engine):

    dtype_mapping = {'Name':NVARCHAR(None), 'NameAr':NVARCHAR(None)}

    max_id = df['OldSourceID'].max()

    try:
        with engine.begin() as conn:  # Transaction-safe

            conn.execute(text("""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.columns
                    WHERE Name = 'OldSourceID'
                    AND Object_ID = Object_ID('app.AppSources')
                )
                BEGIN
                    ALTER TABLE app.AppSources
                    ADD OldSourceID BIGINT NULL;
                END
            """))
            log.info("Verified/Added OldSourceID column.")

            df.to_sql('AppSources', con=conn, schema='app', if_exists='append', index=False, dtype=dtype_mapping) # type: ignore
            log.info(f'dbo.AppSource loaded successfully')

            # Updating the CDC
            conn.execute(
                text("""
                    MERGE app.[EtlCDC] AS target
                    USING (SELECT :table_name AS [TableName], :max_index AS [MaxIndex]) AS source
                    ON target.[TableName] = source.[TableName]
                    WHEN MATCHED THEN UPDATE SET target.[MaxIndex] = source.[MaxIndex]
                    WHEN NOT MATCHED THEN INSERT ([TableName],[MaxIndex]) VALUES (source.[TableName],source.[MaxIndex]);
                """),
                {"table_name": f'dbo.AppSource', "max_index": int(max_id)}
            )
            log.info(f'dbo.AppSource loaded successfully, CDC updated to {max_id}')
    except Exception as e:
        log.error(f'Failed to load dbo.AppSource: {e}')
        raise

# -------------------- Main --------------------
def main():
    source = source_db_conn()
    target = target_db_conn()
    df = extract(source, target)
    if df.empty:
        log.info('No new data to load.')
        return
    df = transform(df, target)
    # return
    load(df, target)

if __name__ == '__main__':
    main()
