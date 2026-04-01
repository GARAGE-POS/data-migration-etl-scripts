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
def extract(user_id: int, engine: Engine) -> str:
    """Extract data based on UserID."""
    
    txt = ''

    subs_df = pd.read_sql('SELECT AccountID, CAST(CRMID AS BIGINT) CRNo FROM app.Subscriptions WHERE AccountID BETWEEN 40 AND 50', engine)

    df = pd.read_csv('Settings/Subscriptions/Accounts.csv')

    df = pd.merge(df, subs_df, on='CRNo', how='left')[['AccountID','CompanyName', 'CRNo', 'RepresentativeContactNo', 'BusinessServiceJson']]


    for _, row in df.iterrows():
        txt += f"({row['AccountID']}, '{row['CRNo']}', N'{row['CompanyName']}', '+966{row['RepresentativeContactNo']}', N'{row['BusinessServiceJson']}'),"

    return txt[:-1]



# -------------------- Load --------------------
def load(values: str, engine: Engine):

    try:
        with engine.begin() as conn:  # Transaction-safe

            txt= f"""
                MERGE INTO app.Accounts o
                USING (VALUES {values}) n (AccountID, CRNo, CompanyName, RepresentativeContactNo, BusinessServiceJson)
                ON o.AccountID = n.AccountID
                WHEN MATCHED THEN
                    UPDATE SET o.CRNo = n.CRNo, o.CompanyName = n.CompanyName,  o.RepresentativeContactNo = n.RepresentativeContactNo, o.BusinessServiceJson = n.BusinessServiceJson;
            """
            conn.execute(text(txt))
            log.info(f'Upserted successfully')


    except Exception as e:
        log.error(f'Failed to update app.Accounts: {e}')
        raise

# -------------------- Main --------------------
def main(user_id:int, if_load:bool=True):
    target = target_db_conn()

    df = extract(user_id, target)

    if if_load:
        load(df, target)

# if __name__ == '__main__':
#     main()
