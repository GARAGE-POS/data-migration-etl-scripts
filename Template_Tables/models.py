import os
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR, DECIMAL
from urllib.parse import quote_plus
import pandas as pd
from utils.fks_mapper import get_makes
from utils.tools import get_logger

log = get_logger('Models')
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
    """Extract data."""

    query = f"SELECT * FROM dbo.Model ORDER BY ModelID"
    df = pd.read_sql_query(query, source_db)
    log.info(f'Extracted {len(df)} rows from dbo.Model')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame, target_db: Engine) -> pd.DataFrame:
    """Clean and transform Model data."""
    # Keep only necessary columns and rename

    df.drop(
        columns=['RowID','CreatedBy','LastUpdatedBy'], inplace=True
    )
    df = df.rename(columns={
        'ArabicName':'NameAr',
        'ModelID':'OldModelID',
        'MakeID':'OldMakeID',
        'CreatedOn':'CreatedAt',
        'LastUpdatedDate':'UpdatedAt',
        'RecommendedLitres':'RecommendedLiters'
    })

    df['UpdatedAt'] = df['UpdatedAt'].fillna(datetime.now())
    df.loc[df['CreatedAt'].isna(), 'CreatedAt'] = df['UpdatedAt']

    df['RecommendedLiters'] = pd.to_numeric(df['RecommendedLiters'], errors='coerce')

    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) and x.strip() != '' else x)

    df['Year'] = df['Year'].fillna(0)


    # Sync ModelIDs
    df = pd.merge(df, get_makes(target_db), on='OldMakeID', how='left')
    df.drop(columns="OldMakeID", inplace=True)

    print(df)

    log.info('Transformation complete')
    return df

# -------------------- Load --------------------
def load(df: pd.DataFrame, engine: Engine):

    dtype_mapping = {'Name':NVARCHAR(None), 'NameAr':NVARCHAR(None), 'RecommendedLiters':DECIMAL(18,2), 'ImagePath':NVARCHAR(None)}
    max_id = df['OldModelID'].max()

    try:
        with engine.begin() as conn:  # Transaction-safe

            conn.execute(text("""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.columns
                    WHERE Name = 'OldModelID'
                    AND Object_ID = Object_ID('app.Models')
                )
                BEGIN
                    ALTER TABLE app.Models
                    ADD OldModelID BIGINT NULL;
                END
            """))
            log.info("Verified/Added OldModelID column.")

            df.to_sql('Models', con=conn, schema='app', if_exists='append', index=False, dtype=dtype_mapping) # type: ignore
            log.info(f'dbo.Model loaded successfully')

    except Exception as e:
        log.error(f'Failed to load dbo.Model: {e}')
        raise

# -------------------- Main --------------------
def main():
    source = source_db_conn()
    target = target_db_conn()
    while True:
        df = extract(source, target)
        if df.empty:
            log.info('No new data to load.')
            return
        df = transform(df, target)
        # print(df)
        # return
        load(df, target)
        return
if __name__ == '__main__':
    main()
