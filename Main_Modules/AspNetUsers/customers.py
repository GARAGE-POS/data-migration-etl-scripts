import os
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR, DECIMAL
from urllib.parse import quote_plus
import pandas as pd
from utils.tools import get_logger, clean_contact
from utils.fks_mapper import get_cities, get_custom

warnings.filterwarnings('ignore')
load_dotenv()
log = get_logger('Customers')

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
def extract(source_db: Engine, target_db: Engine) -> pd.DataFrame:
    """Extract data based on CDC."""
    with target_db.begin() as conn:
        max_id = conn.execute(
            text("SELECT ISNULL(MaxIndex,0) FROM app.EtlCDC WHERE TableName=:table_name"),
            {"table_name": 'dbo.Customers'}
        ).scalar()
    max_id = max_id if not max_id is None else 0
    log.info(f'Current CDC for dbo.Customers: {max_id}')

    query = f"SELECT TOP 5000 * FROM dbo.Customers WHERE CustomerID > {max_id} ORDER BY CustomerID"
    df = pd.read_sql_query(query, source_db)
    log.info(f'Extracted {len(df)} rows from dbo.Customers')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame, engine: Engine) -> pd.DataFrame:
    """Clean and transform Customers data."""

    # Keep only necessary columns and rename
    df = df[['CustomerID', 'FullName', 'ImagePath', 'Password', 'Email', 'Mobile', 'LocationID', 'StatusID','CreatedOn', 'LastUpdatedDate']]

    df.rename(columns={
        "CustomerID":'OldID',
        'LastUpdatedDate':'UpdatedAt',
        'Password':'PasswordHash',
        'LocationID':'OldLocationID',
        'FullName':'FirstName',
        'Mobile':'ContactNo',
        'CreatedOn':'CreatedAt'
        }, inplace=True)

    # Clean strings: strip & lowercase
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) and x.strip()!='' else None)
            

    df['ContactNo'] = df['ContactNo'].apply(clean_contact)

    df['UpdatedAt'] = df['UpdatedAt'].fillna(datetime.now())
    df.loc[df['CreatedAt'].isna(), 'CreatedAt'] = df['UpdatedAt']
    df['StatusID'] = df['StatusID'].fillna(1)
    df['IsEmailVerified'] = 0  
    df['IsContactNoVerified'] = 0
    df['EmailConfirmed'] = 0
    df['PhoneNumberConfirmed'] = 0
    df['TwoFactorEnabled'] = 0
    df['LockoutEnabled'] = 0
    df['AccessFailedCount'] = 0
    df['UserType'] = 'Customer'

    df['NormalizedEmail'] = df['Email'].map(lambda x: x.upper() if isinstance(x,str) else None)


    # current_city_ids = pd.read_sql('SELECT CityID, OldLocationID FROM app.Locations WHERE OldLocationID IS NOT NULL', engine)
    # current_country_ids = pd.read_sql('SELECT CityID, CountryID FROM app.Cities', engine)

    df = pd.merge(df, get_custom(engine, ['OldLocationID, CityID'], 'app.Locations'), on='OldLocationID', how='left')
    df = pd.merge(df, get_cities(engine), on='CityID', how='left')

    df.drop(columns={'OldLocationID', 'OldCityID'}, inplace=True)


    log.info(f'Transformation complete, df len is {len(df)}')
    return df

# -------------------- Load --------------------
def load(df: pd.DataFrame, engine: Engine):

    dtype_mapping = {col:NVARCHAR(None) for col in df.select_dtypes(include='object').columns}
    
    max_id = df['OldID'].max()

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
            log.info(f'dbo.Customers loaded successfully')

            conn.execute(
                text("""
                    MERGE app.[EtlCDC] AS target
                    USING (SELECT :table_name AS [TableName], :max_index AS [MaxIndex]) AS source
                    ON target.[TableName] = source.[TableName]
                    WHEN MATCHED THEN UPDATE SET target.[MaxIndex] = source.[MaxIndex]
                    WHEN NOT MATCHED THEN INSERT ([TableName],[MaxIndex]) VALUES (source.[TableName],source.[MaxIndex]);
                """),
                {"table_name": f'dbo.Customers', "max_index": int(max_id)}
            )
            log.info(f'dbo.Customers loaded successfully, CDC updated to {max_id}')
    except Exception as e:
        log.error(f'Failed to load dbo.Customers: {e}')
        raise

# -------------------- Main --------------------
def main():
    source = source_db_conn()
    target = target_db_conn()

    while True:
        df = extract(source, target)
        if df.empty:
            log.info('No new data to load.')
            break
        df = transform(df, target)
        load(df, target)
        # return

if __name__ == '__main__':
    main()
