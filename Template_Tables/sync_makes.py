import os
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR
from urllib.parse import quote_plus
import pandas as pd
from utils.tools import get_logger

log = get_logger('SyncMakes')
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

    df_1 = pd.read_sql("SELECT MakeID AS OldMakeID, Trim(Name) AS Name FROM dbo.Make WHERE StatusID = 1 ORDER BY MakeID", source_db)

    df_2 = pd.read_sql("SELECT MakeID, Name FROM app.Makes", target_db)


    df = pd.merge(df_1, df_2, how='inner', on='Name')

    return df

def transform(df: pd.DataFrame) -> str:

    df.drop(columns='Name', inplace=True)

    values = str(df.values.tolist()).replace('[','(').replace(']', ')')


    expression = f'''

        MERGE app.Makes o
        USING (
            VALUES {values} 
            ) n (OldMakeID, MakeID)
        ON o.MakeID = n.MakeID
        WHEN MATCHED THEN
            UPDATE SET o.OldMakeID = n.OldMakeID
    '''


    return expression




# -------------------- Load --------------------
def load(expression: str, engine: Engine):


    try:
        with engine.begin() as conn:  # Transaction-safe

            conn.execute(text("""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.columns
                    WHERE Name = 'OldModelID'
                    AND Object_ID = Object_ID('app.Makes')
                )
                BEGIN
                    ALTER TABLE app.Makes
                    ADD OldMakeID BIGINT NULL;
                END
            """))
            log.info("Verified/Added OldMakeID column.")


            conn.execute(text(expression))
            log.info(f'Makes sync successfully')

    except Exception as e:
        log.error(f'Failed to load dbo.Model: {e}')
        raise

# -------------------- Main --------------------
def main():
    source = source_db_conn()
    target = target_db_conn()

    df = extract(source, target)
    if df.empty:
        log.info('No data to load.')
        return

    print(df)

    expression = transform(df)
   
    # load(expression, target)

if __name__ == '__main__':
    main()
