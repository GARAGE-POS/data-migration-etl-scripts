import os
import warnings
from dotenv import load_dotenv
from sqlalchemy import create_engine, Engine
from urllib.parse import quote_plus
import pandas as pd
from utils.tools import get_logger

warnings.filterwarnings('ignore')
load_dotenv()

log = get_logger('SyncServices')

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
def extract_old(engine: Engine) -> pd.DataFrame:
    """Extract data."""

    query = f"SELECT ServiceID AS OldServiceID, ServiceTitle AS Name FROM dbo.Service"
    df = pd.read_sql_query(query, engine)
    log.info(f'Extracted {len(df)} rows from dbo.Service')
    return df

def extract_new(engine: Engine) -> pd.DataFrame:
    """Extract data."""

    query = f"SELECT ServiceID, Name FROM app.Services"
    df = pd.read_sql_query(query, engine)
    log.info(f'Extracted {len(df)} rows from app.Services')
    return df

# -------------------- Transform --------------------
def join(old_data: pd.DataFrame, new_data: pd.DataFrame) -> pd.DataFrame:



    old_data['Name'] = old_data['Name'].map(lambda x: x.replace('Car', '').strip().lower())
    new_data['Name'] = new_data['Name'].map(lambda x: x.replace('Service', '').replace('Car', '').strip().lower())


    joined_data = pd.merge(new_data, old_data, how='inner', on='Name')

    return joined_data


# -------------------- Main --------------------
def main(if_load:bool=True):
    source = source_db_conn()
    target = target_db_conn()

    old = extract_old(source)
    new = extract_new(target)

    df = join(old, new)
    print(df)
    df.drop(columns='Name', inplace=True)

    if if_load:
        df.to_sql(
            name='SyncServices',
            con=target,
            schema='app',
            if_exists='append',
            index=False,
        )
        log.info('Services are Synchronized')

# if __name__ == '__main__':
#     main()
