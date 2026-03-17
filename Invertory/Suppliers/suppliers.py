import os
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR, DECIMAL
from urllib.parse import quote_plus
import pandas as pd
from utils.custom_err import IncrementalDependencyError
from utils.fks_mapper import get_accounts
from utils.tools import get_last_ingested, get_logger, clean_contact, update_last_ingested


warnings.filterwarnings('ignore')
load_dotenv()

log = get_logger('Suppliers')

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

    last_id = get_last_ingested(user_id, 'dbo.Supplier')

    query = f"SELECT * FROM dbo.Supplier WHERE UserID={user_id} AND SupplierID > {last_id} ORDER BY SupplierID"
    df = pd.read_sql_query(query, engine)
    log.info(f'Extracted {len(df)} rows from dbo.Supplier')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame, engine: Engine) -> pd.DataFrame:
    """Clean and transform Suppliers data."""
    # Keep only necessary columns and rename
    df = df[['SupplierID', 'UserID', 'Name' , 'Email', 'Phone', 'ContactPerson', 'Address', 'StatusID', 'CreatedOn', 'LastUpdatedDate']]

    df.rename(columns={
        "SupplierID":'OldSupplierID',
        'UserID':'OldUserID',
        "Email":"ContactEmail",
        "Phone":"ContactPhone",
        "Address":"NationalAddress",
        "CreatedOn":"CreatedAt",
        "LastUpdatedDate":"UpdatedAt"
        }, inplace=True)

    # Clean strings
    for col in df.select_dtypes(include='object').columns:
            if col !='Name':
                df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) and x.strip()!='' else None)
            else: 
                df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) else x)

    df['ContactPhone'] = df['ContactPhone'].map(clean_contact)

    # Filling Null Values in StatusID
    df['StatusID'] = df['StatusID'].fillna(1)
    df['UpdatedAt'] = df['UpdatedAt'].fillna(datetime.now())
    df['CreatedAt'] = df['UpdatedAt']
    df["IsVATRegistered"] = 0


    df = pd.merge(df, get_accounts(engine), on='OldUserID', how='left')
    missing_acc = df['AccountID'].isna().sum()
    if missing_acc:
        log.warning(f'Missing AccountIDs: {missing_acc}')
        raise IncrementalDependencyError("Update Accounts Table.")
    
    df.drop(columns='OldUserID', inplace=True)

    log.info(f'Transformation complete, df\'s Length is {len(df)}')
    return df

# -------------------- Load --------------------
def load(df: pd.DataFrame, user_id: int, engine: Engine):

    dtype_mapping = {col:NVARCHAR(None) for col in df.select_dtypes(include='object').columns}
    
    try:
        with engine.begin() as conn:  # Transaction-safe

            conn.execute(text("""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.columns
                    WHERE Name = 'OldSupplierID'
                    AND Object_ID = Object_ID('app.Suppliers')
                )
                BEGIN
                    ALTER TABLE app.Suppliers
                    ADD OldSupplierID BIGINT NULL;
                END
            """))
            log.info("Verified/Added OldSupplierID column.")

            df.to_sql('Suppliers', con=conn, schema='app', if_exists='append', index=False, dtype=dtype_mapping) # type: ignore
            update_last_ingested(user_id, 'dbo.Supplier', int(df['OldSupplierID'].max()))
            log.info(f'dbo.Supplier loaded successfully')

    
    except Exception as e:
        log.error(f'Failed to load dbo.Supplier: {e}')
        raise

# -------------------- Main --------------------
def main(user_id:int, if_load:bool=True):
    source = source_db_conn()
    target = target_db_conn()

    df = extract(user_id, source)
    if df.empty:
        log.info('No data to load.')
        return 

    df = transform(df, target)
    print(df)

    if if_load:
        load(df, user_id, target)

# if __name__ == '__main__':
#     main()
