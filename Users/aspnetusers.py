import os
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
def extract(source_db: Engine, target_db: Engine) -> pd.DataFrame:
    """Extract data based on CDC."""
    with target_db.begin() as conn:
        max_id = conn.execute(
            text("SELECT ISNULL(MaxIndex,0) FROM app.EtlCDC WHERE TableName=:table_name"),
            {"table_name": 'dbo.SubUsers'}
        ).scalar()
    logging.info(f'Current CDC for dbo.SubUsers: {max_id}')

    query = f"SELECT * FROM dbo.SubUsers WHERE SubUserID > {max_id}"
    df = pd.read_sql_query(query, source_db)
    logging.info(f'Extracted {len(df)} rows from dbo.SubUsers')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame, engine: Engine) -> pd.DataFrame:
    """Clean and transform SubUsers data."""

    # Keep only necessary columns and rename
    df = df[['SubUserID', 'UserName', 'FirstName','UserType', 'LastName', 'Address', 'Designation', 'ImagePath', 'Password', 'Email','CountryID', 'ContactNo', 'CityID', 'StatusID', 'LastUpdatedDate']]

    df.rename(columns={
        "SubUserID":'OldSubUserID',
        'LastUpdatedDate':'LastUpdateDate',
        'Password':'PasswordHash',
        'CityID':'OldCityID'
        }, inplace=True)



    def clean_contact(num: str):
        if pd.isna(num): return None
        num = num.replace(' ','')
        while num.startswith('0'): num = num[1:]
        if num.startswith('5'): return '+966'+num[:12]
        elif num.startswith('9'): return '+'+num[:14]
        return num[:15]
    df['ContactNo'] = df['ContactNo'].apply(clean_contact)

    # Clean strings: strip & lowercase
    for col in df.select_dtypes(include='object').columns:
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) and x.strip()!='' else None)
            

    df['CreatedDate']=datetime.now()
    df['UserType'] = df['UserType'].fillna('')
    df['StatusID'] = df['StatusID'].fillna(1)
    df['LastUpdateDate'] = df['LastUpdateDate'].fillna(datetime.now())
    df['IsEmailVerified'] = 0  
    df['IsContactNoVerified'] = 0
    df['EmailConfirmed'] = 0
    df['PhoneNumberConfirmed'] = 0
    df['TwoFactorEnabled'] = 0
    df['LockoutEnabled'] = 0
    df['AccessFailedCount'] = 0


    # df['NormalizedUserName'] = df['UserName'].map(lambda x: x.upper() if isinstance(x,str) else None)
    # df['NormalizedUserName'] = df['NormalizedUserName'].mask(df.duplicated('NormalizedUserName', keep='first'))

    df['NormalizedEmail'] = df['Email'].map(lambda x: x.upper() if isinstance(x,str) else None)
    df['UserType'] = df['UserType'].map(lambda x: x[:8])
    df['CountryID'] = pd.to_numeric(df['CountryID'], errors='coerce')



    current_city_ids = pd.read_sql('Select * from app.SyncCities', engine)

    df = pd.merge(df, current_city_ids, on='OldCityID', how='left')

    df.drop(columns={'OldCityID'}, inplace=True)



    logging.info('Transformation complete')
    return df

# -------------------- Load --------------------
def load(df: pd.DataFrame, engine: Engine):

    dtype_mapping = {col:NVARCHAR(None) for col in df.select_dtypes(include='object').columns}
    
    max_id = df['OldSubUserID'].max()

    try:
        with engine.begin() as conn:  # Transaction-safe

            conn.execute(text("""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.columns
                    WHERE Name = 'OldSubUserID'
                    AND Object_ID = Object_ID('app.AspNetUsers')
                )
                BEGIN
                    ALTER TABLE app.AspNetUsers
                    ADD OldSubUserID BIGINT NULL;
                END
            """))
            logging.info("Verified/Added OldSubUserID column.")

            df.to_sql('AspNetUsers', con=conn, schema='app', if_exists='append', index=False, dtype=dtype_mapping) # type: ignore
            logging.info(f'dbo.SubUsers loaded successfully')

            conn.execute(
                text("""
                    MERGE app.[EtlCDC] AS target
                    USING (SELECT :table_name AS [TableName], :max_index AS [MaxIndex]) AS source
                    ON target.[TableName] = source.[TableName]
                    WHEN MATCHED THEN UPDATE SET target.[MaxIndex] = source.[MaxIndex]
                    WHEN NOT MATCHED THEN INSERT ([TableName],[MaxIndex]) VALUES (source.[TableName],source.[MaxIndex]);
                """),
                {"table_name": f'dbo.SubUsers', "max_index": int(max_id)}
            )
            logging.info(f'dbo.SubUsers loaded successfully, CDC updated to {max_id}')
    except Exception as e:
        logging.error(f'Failed to load dbo.Users: {e}')
        raise

# -------------------- Main --------------------
def main():
    source = source_db_conn()
    target = target_db_conn()
    df = extract(source, target)
    if df.empty:
        logging.info('No new data to load.')
        return
    df = transform(df, target)
    # print(df.head(5))
    # return
    load(df, target)

if __name__ == '__main__':
    main()
