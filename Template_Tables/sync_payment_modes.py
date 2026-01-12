import os
import warnings
from dotenv import load_dotenv
import logging
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, BIGINT
from urllib.parse import quote_plus
import pandas as pd

warnings.filterwarnings('ignore')
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s:%(lineno)d | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

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
    logging.info(f'Connected to {os.getenv(db_env)} at {os.getenv(server_env)}')
    return engine

def source_db_conn(): return get_engine('AZURE_SERVER','AZURE_DATABASE','AZURE_USERNAME','AZURE_PASSWORD')
def target_db_conn(): return get_engine('STAGE_SERVER','STAGE_DATABASE','STAGE_USERNAME','STAGE_PASSWORD')

# -------------------- Extract --------------------
def extract_old(engine: Engine) -> pd.DataFrame:
    """Extract data."""

    query = f"SELECT PaymentModeID AS OldPaymentModeID, Name FROM dbo.PaymentModes"
    df = pd.read_sql_query(query, engine)
    logging.info(f'Extracted {len(df)} rows from dbo.PaymentModes')
    return df

def extract_new(engine: Engine) -> pd.DataFrame:
    """Extract data."""

    query = f"SELECT PaymentModeID, Name FROM app.PaymentModes"
    df = pd.read_sql_query(query, engine)
    logging.info(f'Extracted {len(df)} rows from app.PaymentModes')
    return df

# -------------------- Transform --------------------
def join(old_data: pd.DataFrame, new_data: pd.DataFrame) -> pd.DataFrame:

    new_map = {
        'STC Pay':'StcPay',
        'Bank Transfer':'BankTransfer',
        'Credit Card': 'Credit',
        'Debit Card': 'Card'
    }

    old_data['Name'] = old_data['Name'].map(lambda x: x.strip())
    new_data['Name'] = new_data['Name'].map(lambda x: new_map.get(x) if new_map.get(x) else x)


    joined_data = pd.merge(new_data, old_data, how='right', on='Name')
    joined_data.drop_duplicates(subset='OldPaymentModeID', inplace=True)
    joined_data.dropna(inplace=True)

    return joined_data


# -------------------- Main --------------------
def main():
    source = source_db_conn()
    target = target_db_conn()

    old = extract_old(source)
    new = extract_new(target)

    df = join(old, new)
    print(df)
    df.drop(columns='Name', inplace=True)
    # return
    df.to_sql(
        name='SyncPaymentModes',
        con=target,
        schema='app',
        if_exists='append',
        index=False,
    )
    logging.info('PaymentModes are Synchronized')

if __name__ == '__main__':
    main()
