import os
import warnings
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, Engine, NVARCHAR
from urllib.parse import quote_plus
import pandas as pd
from utils.tools import fill_useraccounts, get_last_ingested, get_logger, update_last_ingested
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

    last_id = get_last_ingested(user_id, 'app.AspNetUserRoles')
    df = pd.read_sql(f"SELECT Id AS UserID, Designation AS RoleName FROM app.AspNetUsers WHERE Id IN (SELECT UserID FROM app.UserAccounts WHERE AccountID={account_id} AND UserID <> 1) AND ID > {last_id}", engine)

    if len(df) == 0:
        return pd.DataFrame()

    roles = pd.read_sql(f'SELECT ID AS RoleID, Name AS RoleName FROM app.AspNetRoles WHERE AccountID={account_id}', engine)

    df = df.merge(roles, on='RoleName', how='left')

    missing_roles = df['RoleID'].isna()
    if missing_roles.sum():
        log.warning(f"Missing RoleIDs for Roles: {df[missing_roles]['RoleName'].drop_duplicates().values}")
    df = df[~missing_roles]
    
    df.drop(columns='RoleName', inplace=True)

    df['AccountID'] = account_id

    log.info(f'Extracted {len(df)} rows from asp.AspNetUsers')
    return df



# -------------------- Load --------------------
def load(df: pd.DataFrame, user_id: int, engine: Engine):

    try:
        with engine.begin() as conn:

            df.to_sql(
                name='AspNetUserRoles', con=conn, schema='app', if_exists='append', index=False
            )
            update_last_ingested(user_id, 'app.AspNetUserRoles', int(df['UserID'].max()))
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
        load(df, user_id, target)

# if __name__ == '__main__':
#     main()
