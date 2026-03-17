import os
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR, DECIMAL
from urllib.parse import quote_plus
import pandas as pd
from utils.custom_err import IncrementalDependencyError
from utils.tools import get_logger
from utils.fks_mapper import get_addons

log = get_logger('SubscriptionAddOns')
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
def extract(user_id: int, engine: Engine) -> pd.DataFrame:
    """Extract data based on UserID."""


    df = pd.read_sql(f'SELECT AccountID, SubscriptionID, CAST(CRMID AS BIGINT) CRMID, StartDate, ExpiryDate, StatusID FROM app.Subscriptions WHERE AccountID = (SELECT AccountID FROM app.Accounts WHERE OldUserID={user_id})', engine)
    if len(df) == 0:
        raise IncrementalDependencyError("Subscription doesn't exist for this account.")    
    df = pd.merge(df, pd.read_csv('Settings/Subscriptions/Subs.csv')[['CRMID', 'AddOns']], on='CRMID', how='left')
    df.dropna(subset=['AddOns'], inplace=True)
    if len(df) == 0:
        raise ValueError("AddOns don't exist for this account.")


    addons_df = pd.DataFrame(columns=['AccountID', 'AddOnName'])

    addons = ', '.join(df['AddOns'].values.tolist()).lower() # type: ignore

    addons_map = {
        'zatca':'ZATCA E-Invoice Compliance',
        'surepay':'SurePay',
        'tamara':'Tamara',
        'tabby':'Tabby'
    }

    for k,v in addons_map.items():
        if k in addons:
            addons_df.loc[len(addons_df)] = {
                'AccountID':df.iloc[0,0],
                'AddOnName': v
            }
    

    addons_df = pd.merge(addons_df, get_addons(engine),on='AddOnName', how='inner')


    df = pd.merge(df.iloc[[0]], addons_df, on='AccountID', how='left')


    df['CreatedAt'] = df['UpdatedAt'] = datetime.now()

    df.rename(columns={'ExpiryDate':'EndDate'}, inplace=True)
    df.drop(columns=['AccountID', 'CRMID', 'AddOns', 'AddOnName'], inplace=True)

    if df['AddOnID'].isna().sum():
        return pd.DataFrame()
    return df




# -------------------- Load --------------------
def load(df: pd.DataFrame, engine: Engine):
    dtype_mapping = {col:NVARCHAR(None) for col in df.select_dtypes(include='object').columns}

    try:
        with engine.begin() as conn:  # Transaction-safe

            df.to_sql('SubscriptionAddOns', con=conn, schema='app', if_exists='append', index=False, dtype=dtype_mapping) # type: ignore
            log.info(f'app.SubscriptionAddOns loaded successfully')

    except Exception as e:
        log.error(f'Failed to load app.SubscriptionAddOns: {e}')
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
