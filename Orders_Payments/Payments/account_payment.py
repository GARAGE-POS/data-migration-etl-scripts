import os
import sys
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR, DECIMAL
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
def extract(engine: Engine) -> pd.DataFrame:

    with engine.begin() as conn:
        max_id = conn.execute(
            text("SELECT ISNULL(MaxIndex,0) FROM app.EtlCDC WHERE TableName=:table_name"),
            {"table_name": 'app.AccountPaymentModes'}
        ).scalar()
    max_id = max_id if not max_id is None else 0
    log.info(f'Current CDC for app.AccountPaymentModes: {max_id}')

    df = pd.read_sql_query( f"SELECT TOP 1000 AccountID, StatusID FROM app.Accounts WHERE AccountID > {max_id} ORDER BY AccountID", engine)
    log.info(f'Extracted {len(df)} rows from app.Accounts')


    payment_modes = pd.read_sql_query( f"SELECT PaymentModeID FROM app.PaymentModes", engine)


    df = pd.merge(df, payment_modes, how='cross')

    df.sort_values('AccountID', inplace=True)

    df['UpdatedAt'] = datetime.now()
    df['CreatedAt'] = df['UpdatedAt']


    log.info(f'Transformation completed. Output: {len(df)} rows.')
    return df



# -------------------- Load --------------------
def load(df: pd.DataFrame, engine: Engine):
    
    max_id = df['AccountID'].max()

    try:
        with engine.begin() as conn: 

            # Inserting the Data
            df.to_sql('AccountPaymentModes', con=conn, schema='app', if_exists='append', index=False) # type: ignore
            log.info(f'app.AccountPaymentModes loaded successfully')

            conn.execute(
                text("""
                    MERGE app.[EtlCDC] AS target
                    USING (SELECT :table_name AS [TableName], :max_index AS [MaxIndex]) AS source
                    ON target.[TableName] = source.[TableName]
                    WHEN MATCHED THEN UPDATE SET target.[MaxIndex] = source.[MaxIndex]
                    WHEN NOT MATCHED THEN INSERT ([TableName],[MaxIndex]) VALUES (source.[TableName],source.[MaxIndex]);
                """),
                {"table_name": f'app.AccountPaymentModes', "max_index": int(max_id)}
            )
            log.info(f'app.AccountPaymentModes loaded successfully, CDC updated to {max_id}')
    except Exception as e:
        log.error(f'Failed to load app.LocationItems: {e}')
        raise

# -------------------- Main --------------------
def main():
    target = target_db_conn()

    
    while True:
        df = extract(target)
        if df.empty:
            log.info('No new data to load.')
            return
        load(df, target)
    
if __name__ == '__main__':
    main()
