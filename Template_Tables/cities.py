import os
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR
from urllib.parse import quote_plus
import pandas as pd
from utils.tools import get_logger

log = get_logger('Cities')

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

    query = f"SELECT * FROM dbo.City"
    df = pd.read_sql_query(query, source_db)
    log.info(f'Extracted {len(df)} rows from dbo.City')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame, engine: Engine) -> pd.DataFrame:
    """Clean and transform Cities data."""
    # Keep only necessary columns and rename
    df = df[['ID', 'Name', 'District', 'CountryCode']]
    df = df.rename(columns={
        'ID':'OldCityID',
        'Name':'CityName',
        'CountryCode':'Code',
    })

    df['Timezone'] = ''
    df['District'] = df['District'].fillna('')
    df['Code'] = df['Code'].map(lambda x: 'SAU' if x == 'SA' else x)

    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) else x)

    countries = pd.read_sql(f"SELECT CountryID, Code FROM app.Countries", engine)
    df = pd.merge(df, countries, on='Code', how='left')

    print(df.head(20))
    print(df.tail(20))

    mask = ~df['CountryID'].isna()
    df = df[mask]


    df = df.drop(columns='Code')


    log.info('Transformation complete')
    return df

# -------------------- Load --------------------
def load(df: pd.DataFrame, engine: Engine):

    dtype_mapping = {'Code':NVARCHAR(None), 'Name':NVARCHAR(None), 'NameAr':NVARCHAR(None)}
    # max_id = df['OldCountryID'].max()

    try:
        with engine.begin() as conn:  # Transaction-safe

            conn.execute(text("""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.columns
                    WHERE Name = 'OldCityID'
                    AND Object_ID = Object_ID('app.Cities')
                )
                BEGIN
                    ALTER TABLE app.Cities
                    ADD OldCityID BIGINT NULL;
                END
            """))
            log.info("Verified/Added OldCityID column.")

            df.to_sql('Cities', con=conn, schema='app', if_exists='append', index=False, dtype=dtype_mapping) # type: ignore
            log.info(f'dbo.City loaded successfully')

    except Exception as e:
        log.error(f'Failed to load dbo.City: {e}')
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







