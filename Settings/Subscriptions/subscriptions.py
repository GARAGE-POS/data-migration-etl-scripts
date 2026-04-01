import os
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR, DECIMAL
from urllib.parse import quote_plus
import pandas as pd
from utils.tools import get_last_ingested, get_logger, update_last_ingested
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
def extract(user_id: int, engine: Engine) -> pd.DataFrame:
    """Extract data based on UserID."""

    ingested = get_last_ingested(user_id, 'app.Subscriptions')

    if ingested:
        return pd.DataFrame()
    
    
    df = pd.DataFrame()
    
    account = pd.read_sql(f'SELECT AccountID, TRIM(CompanyCode) AS CompanyCode FROM app.Accounts WHERE OldUserID={user_id}', engine)
    df['CompanyCode'] = account['CompanyCode']

    df = pd.merge(df, pd.read_csv('Settings/Subscriptions/CRMs.csv'), on='CompanyCode', how='left')
    df = pd.merge(df, pd.read_csv('Settings/Subscriptions/Subs.csv'), on='CRMID', how='left')
    df = pd.merge(df, pd.read_csv('Settings/Subscriptions/Deals.csv'), on='DealID', how='left')
    df.dropna(subset='SubscriptionName', inplace=True)

    
    df['AccountID'] = int(account['AccountID']) # type: ignore


    df['StartDate'] = pd.to_datetime(df['StartDate'], errors='coerce')
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], errors='coerce')
    df.loc[df['StartDate'].isna(), 'StartDate'] = df['InvoiceDate']

    df['StartDate'] = df['StartDate'].min()
    df['InvoiceDate'] = df['InvoiceDate'].max()
    if len(df) == 1:
        df['ExpiryDate'] = df['StartDate'] + pd.DateOffset(years=1)
    elif len(df) > 1:
        df['ExpiryDate'] = df['InvoiceDate'] + pd.DateOffset(years=1)
        for _, row in df.iterrows():
            if 'monthly' in row['SubscriptionName'].lower():
                raise ValueError('Monthly Subscription exists.')

    df['CreatedAt'] = df['UpdatedAt'] = datetime.now()
    df['Terminals'] = df['Terminals'].sum()


    df['PaymentTerm'] = df['SubscriptionName'].map(lambda x: 30 if 'monthly' in x.lower() else 365)
    df['StatusID'] = 1
    df['SubscriptionType'] = 1
    df['CRMID'] = df['CRMID'].astype(int).astype(str)

    df.drop(columns={'CompanyCode', 'InvoiceDate', 'DealID', 'AddOns', 'InvoiceNumber'}, inplace=True)
    df.rename(columns={'Terminals':'NumberOfTerminals'}, inplace=True)

    if df.empty:
        return pd.DataFrame()

    return df.iloc[[0]]



# -------------------- Load --------------------
def load(df: pd.DataFrame, user_id: int, engine: Engine):
    dtype_mapping = {col:NVARCHAR(None) for col in df.select_dtypes(include='object').columns}

    try:
        with engine.begin() as conn:  # Transaction-safe

            df.to_sql('Subscriptions', con=conn, schema='app', if_exists='append', index=False, dtype=dtype_mapping) # type: ignore
            update_last_ingested(user_id, 'app.Subscriptions', 1)
            log.info(f'app.Subscriptions loaded successfully')

    except Exception as e:
        log.error(f'Failed to load app.Subscriptions: {e}')
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
