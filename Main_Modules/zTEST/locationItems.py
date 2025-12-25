import os
import sys
import warnings
from dotenv import load_dotenv
import logging
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR, DECIMAL
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
def extract(account_id: int, engine: Engine) -> pd.DataFrame:

    cat_ids = str(tuple(pd.read_sql(f"SELECT CategoryID FROM app.Categories WHERE AccountID={account_id}", engine)['CategoryID'].values.tolist())+(0,0))

    df = pd.read_sql_query( f"SELECT ItemID, CategoryID, Price, CreatedAt, UpdatedAt, StatusID FROM app.Items WHERE CategoryID IN {cat_ids} AND StatusID <> 3", engine)
    logging.info(f'Extracted {len(df)} rows from app.Items')


    loc_ids = pd.read_sql(f"SELECT LocationID FROM app.Locations WHERE AccountID = {account_id}", engine)
    df = pd.merge(df, loc_ids, how='cross')

    df.drop(columns='CategoryID', inplace=True)

    logging.info(f'Transformation complete, output: {len(df)}')

    # print(df)

    return df



# -------------------- Load --------------------
def load(df: pd.DataFrame, account_id: int, engine: Engine):


    try:
        with engine.begin() as conn: 

            # Inserting the Data
            df.to_sql('LocationItems', con=conn, schema='app', if_exists='append', index=False) # type: ignore
            logging.info(f'app.LocationItems loaded successfully')

            conn.execute(
                text("""
                    MERGE app.[EtlCDC] AS target
                    USING (SELECT :table_name AS [TableName], :max_index AS [MaxIndex]) AS source
                    ON target.[TableName] = source.[TableName]
                    WHEN MATCHED THEN UPDATE SET target.[MaxIndex] = source.[MaxIndex]
                    WHEN NOT MATCHED THEN INSERT ([TableName],[MaxIndex]) VALUES (source.[TableName],source.[MaxIndex]);
                """),
                {"table_name": f'app.LocationItems', "max_index": int(account_id)}
            )
            logging.info(f'app.LocationItems loaded successfully, CDC updated to {account_id}')
    except Exception as e:
        logging.error(f'Failed to load app.LocationItems: {e}')
        raise

# -------------------- Main --------------------
def main():
    target = target_db_conn()
    # account_id = int(sys.argv[1])
    # print(pd.read_sql(f"SELECT CategoryID FROM app.Categories WHERE AccountID={account_id}",target)['CategoryID'].values.tolist())
    with target.begin() as conn:
        max_id = conn.execute(
            text("SELECT ISNULL(MaxIndex,0) FROM app.EtlCDC WHERE TableName=:table_name"),
            {"table_name": 'app.LocationItems'}
        ).scalar()
    max_id = max_id if not max_id is None else 0
    logging.info(f'Current CDC for app.LocationItems: {max_id}')

    account_ids : list = pd.read_sql(f'SELECT AccountID FROM app.Accounts WHERE AccountID > {max_id} ORDER BY AccountID', target)['AccountID'].values.tolist()
    for account_id in account_ids:
        logging.info(f'Current AccountID: {account_id}')
        df = extract(account_id, target)
        if df.empty:
            logging.info('No new data to load.')
            continue
        # return
        load(df, account_id, target)
        # if account_id == 1435:
        #     return
    
if __name__ == '__main__':
    main()
