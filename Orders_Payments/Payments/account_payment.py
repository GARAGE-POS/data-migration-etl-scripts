import os
import sys
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine
from urllib.parse import quote_plus
import pandas as pd
from utils.tools import get_logger

warnings.filterwarnings('ignore')
load_dotenv()

log = get_logger('AccountPaymentModes')

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

    df = pd.read_sql_query( f"SELECT AccountID, StatusID FROM app.Accounts WHERE OldUserID={user_id}", engine)
    log.info(f'Extracted {len(df)} rows from app.Accounts')

    payment_modes = pd.read_sql_query(f"SELECT PaymentModeID FROM app.PaymentModes", engine)


    df = pd.merge(df, payment_modes, how='cross')

    df.sort_values('AccountID', inplace=True)

    df['UpdatedAt'] = datetime.now()
    df['CreatedAt'] = df['UpdatedAt']


    log.info(f'Transformation completed. df rows: {len(df)}')
    return df



# -------------------- Load --------------------
def load(df: pd.DataFrame, engine: Engine):
    
    try:
        with engine.begin() as conn: 

            # Inserting the Data
            df.to_sql('AccountPaymentModes', con=conn, schema='app', if_exists='append', index=False) # type: ignore
            log.info(f'app.AccountPaymentModes loaded successfully')

    except Exception as e:
        log.error(f'Failed to load app.LocationItems: {e}')
        raise

# -------------------- Main --------------------
def main(user_id:int, if_load:bool=True):

    target = target_db_conn()

    df = extract(user_id, target)
    if df.empty:
        log.info('No data to load.')
        return
        
    print(df)
    
    if if_load:
        load(df, target)

# if __name__ == '__main__':
#     main()
