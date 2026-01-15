import os
import warnings
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, Engine, NVARCHAR
from urllib.parse import quote_plus
import pandas as pd
from utils.tools import get_logger
from utils.fks_mapper import get_permissions

log = get_logger('AspNetUserRoles')
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
def extract(user_id:int, engine: Engine) -> pd.DataFrame:
    """Extract data based on UserID."""

    account_id = pd.read_sql(f'SELECT AccountID FROM app.Accounts WHERE OldUserID={user_id}', engine)
    account_id = int(account_id['AccountID']) # type: ignore



    df = pd.read_sql(f"SELECT Id AS SubUserID, Designation AS RoleName, AccountID FROM app.AspNetUsers WHERE AccountID={account_id} AND UserType='User'", engine)

    roles = pd.read_sql(f'SELECT ID AS RoleID, Name AS RoleName FROM app.AspNetRoles WHERE AccountID={account_id}', engine)

    df = df.merge(roles, on='RoleName', how='left')

    missing_roles = df['RoleID'].isna()
    if missing_roles.sum():
        log.warning(f"Missing RoleIDs for Roles: {df[missing_roles]['RoleName'].drop_duplicates().values}")
        raise ValueError('Check Designation in AspNetUsers or RoleName in AspNetRoles.')
    
    df.drop(columns='RoleName', inplace=True)

    log.info(f'Extracted {len(df)} rows from asp.AspNetUsers')
    return df



# -------------------- Load --------------------
def load(df: pd.DataFrame, engine: Engine):

    try:
        with engine.begin() as conn:

            df.to_sql(
                name='AspNetUserRoles', con=conn, schema='app', if_exists='append', index=False
            )
            log.info(f'app.AspNetUserRoles loaded successfully')
    
    except Exception as e:
        log.error(f'Failed to load app.AspNetUserRoles: {e}')
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
