import os
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR, DECIMAL
from urllib.parse import quote_plus
import pandas as pd
from utils.tools import get_logger
from utils.fks_mapper import get_categories, get_custom, get_accounts
from utils.custom_err import IncrementalDependencyError

log = get_logger('Packages')
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
def extract(source_db: Engine, target_db: Engine) -> pd.DataFrame:
    """Extract data based on CDC."""
    with target_db.begin() as conn:
        max_id = conn.execute(
            text("SELECT ISNULL(MaxIndex,0) FROM app.EtlCDC WHERE TableName=:table_name"),
            {"table_name": 'dbo.Packages'}
        ).scalar()
    
    max_id = max_id if not max_id is None else 0
    log.info(f'Current CDC for dbo.Packages: {max_id}')

    query = f"SELECT TOP 1000 * FROM dbo.Packages WHERE PackageID > {max_id} ORDER BY PackageID "
    df = pd.read_sql_query(query, source_db)
    log.info(f'Extracted {len(df)} rows from dbo.Packages')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame, source_db: Engine, target_db: Engine) -> pd.DataFrame:
    """Clean and transform Packages data."""
    # Keep only necessary columns and rename
    df = df[['PackageID', 'SubCategoryID' , 'Name', 'ArabicName', 'Description', 'Price', 'Cost', 'SKU','Barcode', 'Image', 'UserID', 'StatusID', 'LastUpdatedDate']]

    df.rename(columns={
        "PackageID":'OldPackageID',
        "UserID":'OldUserID',
        'LastUpdatedDate':'UpdatedAt',
        'ArabicName':'NameAr',
        'Image':'ImagePath'
        }, inplace=True)
    

    # Clean strings: strip & lowercase
    for col in df.select_dtypes(include='object').columns:
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) and x.strip()!='' else None)
            df[col] = df[col].apply(lambda x: x if isinstance(x,str) and x != 'NULL' else None)


    # Sync AccountID and CategoryID
    df = pd.merge(df, get_accounts(target_db), on='OldUserID', how='left')
    missing_accs = df['AccountID'].isna().sum()
    if missing_accs:
        log.warning(f'Missing AccountIDs: {missing_accs}')
        raise IncrementalDependencyError('Update Accounts Table.')
    
    df = pd.merge(df, get_custom(source_db, ['CategoryID', 'SubCategoryID'], 'dbo.SubCategory'), on='SubCategoryID', how='left')
    df.rename(columns={'CategoryID':'OldCategoryID'}, inplace=True)

    df = pd.merge(df, get_categories(target_db, df['OldCategoryID']), on='OldCategoryID', how='left')
    missing_cats = df['CategoryID'].isna().sum()
    if missing_cats:
        log.warning(f'Missing CategoryIDs: {missing_cats}')
        raise IncrementalDependencyError('Update Categories Table.')


    df.drop(columns={'OldUserID', 'OldCategoryID', 'SubCategoryID'}, inplace=True)

    # Fixing Date columns
    df['UpdatedAt'] = df['UpdatedAt'].fillna(datetime.now())
    df['CreatedAt'] = df['UpdatedAt']

    log.info(f'Transformation complete, df\'s Length is {len(df)}')
    return df

# -------------------- Load --------------------
def load(df: pd.DataFrame, engine: Engine):

    dtype_mapping = {col:NVARCHAR(None) for col in df.select_dtypes(include='object').columns}
    
    max_id = df['OldPackageID'].max()

    try:
        with engine.begin() as conn:  # Transaction-safe

            conn.execute(text("""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.columns
                    WHERE Name = 'OldPackageID'
                    AND Object_ID = Object_ID('app.Packages')
                )
                BEGIN
                    ALTER TABLE app.Packages
                    ADD OldPackageID BIGINT NULL;
                END
            """))
            log.info("Verified/Added OldPackageID column.")

            df.to_sql('Packages', con=conn, schema='app', if_exists='append', index=False, dtype=dtype_mapping) # type: ignore
            log.info(f'dbo.Packages loaded successfully')

            conn.execute(
                text("""
                    MERGE app.[EtlCDC] AS target
                    USING (SELECT :table_name AS [TableName], :max_index AS [MaxIndex]) AS source
                    ON target.[TableName] = source.[TableName]
                    WHEN MATCHED THEN UPDATE SET target.[MaxIndex] = source.[MaxIndex]
                    WHEN NOT MATCHED THEN INSERT ([TableName],[MaxIndex]) VALUES (source.[TableName],source.[MaxIndex]);
                """),
                {"table_name": f'dbo.Packages', "max_index": int(max_id)}
            )
            log.info(f'dbo.Packages loaded successfully, CDC updated to {max_id}')
    except Exception as e:
        log.error(f'Failed to load dbo.Packages: {e}')
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
        df = transform(df, source, target)
        # print(df.head(20))
        # return
        load(df, target)
        # return

if __name__ == '__main__':
    main()
