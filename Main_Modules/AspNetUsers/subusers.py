import os
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR, DECIMAL
from urllib.parse import quote_plus
import pandas as pd
from utils.tools import get_logger,  clean_contact
from utils.fks_mapper import get_cities, get_accounts


warnings.filterwarnings('ignore')
load_dotenv()
log = get_logger('SubUsers')

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

    query = f"SELECT * FROM dbo.SubUsers WHERE UserID={user_id} ORDER BY SubUserID"
    df = pd.read_sql_query(query, engine)
    log.info(f'Extracted {len(df)} rows from dbo.SubUsers')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame, engine: Engine) -> pd.DataFrame:
    """Clean and transform SubUsers data."""

    # Keep only necessary columns and rename
    df = df[['UserID','SubUserID', 'UserName', 'FirstName','UserType', 'LastName', 'Address', 'Designation', 'ImagePath', 'Password', 'Email', 'ContactNo', 'CityID', 'StatusID', 'LastUpdatedDate']]

    df.rename(columns={
        "SubUserID":'OldID',
        "UserID":'OldUserID',
        'LastUpdatedDate':'UpdatedAt',
        'Password':'PasswordHash',
        'CityID':'OldCityID'
        }, inplace=True)

    # Clean strings: strip & lowercase
    for col in df.select_dtypes(include='object').columns:
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) and x.strip()!='' else None)
            
    df['ContactNo'] = df['ContactNo'].apply(clean_contact)

    df['UserType'] = 'User'
    df['StatusID'] = df['StatusID'].fillna(1)
    df['UpdatedAt'] = df['UpdatedAt'].fillna(datetime.now())
    df['CreatedAt'] = df['UpdatedAt']
    df['IsEmailVerified'] = 0  
    df['IsContactNoVerified'] = 0
    df['EmailConfirmed'] = 0
    df['PhoneNumberConfirmed'] = 0
    df['TwoFactorEnabled'] = 0
    df['LockoutEnabled'] = 0
    df['AccessFailedCount'] = 0


    df['Designation'] = 'AccountManager'


    df['NormalizedEmail'] = df['Email'].map(lambda x: x.upper() if isinstance(x,str) else None)
    df['NormalizedUserName'] = df['NormalizedEmail']

    df['OldCityID'] = pd.to_numeric(df['OldCityID'], errors='coerce')


    df = pd.merge(df, get_cities(engine), on='OldCityID', how='left')
    df = pd.merge(df, get_accounts(engine, df['OldUserID']), on='OldUserID', how='left')

    df.drop(columns={'OldCityID', 'OldUserID'}, inplace=True)

    log.info(f'Transformation complete. df rows: {len(df)}')
    return df

# -------------------- Load --------------------
def load(df: pd.DataFrame, engine: Engine):

    dtype_mapping = {col:NVARCHAR(None) for col in df.select_dtypes(include='object').columns}
    
    try:
        with engine.begin() as conn:  # Transaction-safe

            conn.execute(text("""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.columns
                    WHERE Name = 'OldID'
                    AND Object_ID = Object_ID('app.AspNetUsers')
                )
                BEGIN
                    ALTER TABLE app.AspNetUsers
                    ADD OldID BIGINT NULL;
                END
            """))
            log.info("Verified/Added OldID column.")

            df.to_sql('AspNetUsers', con=conn, schema='app', if_exists='append', index=False, dtype=dtype_mapping) # type: ignore
            log.info(f'dbo.SubUsers loaded successfully')

    except Exception as e:
        log.error(f'Failed to load dbo.Users: {e}')
        raise

# -------------------- Main --------------------
def main(user_id: int, if_load:bool=True):
    source = source_db_conn()
    target = target_db_conn()

    df = extract(user_id, source)
    if df.empty:
        log.info('No data to load.')
        return
    
    df = transform(df, target)
    print(df)
    
    if if_load:
        load(df, target)
    

# if __name__ == '__main__':
#     main()
