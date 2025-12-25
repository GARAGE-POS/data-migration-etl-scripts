import os
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR, DECIMAL
from urllib.parse import quote_plus
import pandas as pd
from utils.tools import get_logger

log = get_logger('LocationPackages')
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
def extract(engine: Engine) -> pd.DataFrame:
    """Extract data based on CDC."""
    with engine.begin() as conn:
        max_id = conn.execute(
            text("SELECT ISNULL(MaxIndex,0) FROM app.EtlCDC WHERE TableName=:table_name"),
            {"table_name": 'app.LocationPackages'}
        ).scalar()
    
    max_id = max_id if not max_id is None else 0
    log.info(f'Current CDC for app.LocationPackages: {max_id}')
    query = f"SELECT TOP 500 PackageID, CategoryID, Price, CreatedAt, UpdatedAt, StatusID FROM app.Packages WHERE PackageID > {max_id} ORDER BY PackageID "
    df = pd.read_sql_query(query, engine)
    log.info(f'Extracted {len(df)} rows from app.Packages')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame, engine: Engine) -> pd.DataFrame:
    """Clean and transform Packages data."""

    print(df)

    # Sync AccountID and CategoryID
    cat_acc_ids = pd.read_sql("SELECT AccountID, CategoryID FROM app.Categories", engine)
    loc_acc_ids = pd.read_sql(f"SELECT AccountID, LocationID FROM app.Locations", engine)


    df = pd.merge(df, cat_acc_ids, on='CategoryID', how='left')
    df = pd.merge(df, loc_acc_ids, on='AccountID', how='left')



    df.drop(columns={'CategoryID', 'AccountID'}, inplace=True)


    print(df)


    log.info(f'Transformation complete, df\'s Length is {len(df)}')
    return df

# -------------------- Load --------------------
def load(df: pd.DataFrame, engine: Engine):

    dtype_mapping = {col:NVARCHAR(None) for col in df.select_dtypes(include='object').columns}
    
    max_id = df['PackageID'].max()

    try:
        with engine.begin() as conn:  # Transaction-safe

            df.to_sql('LocationPackages', con=conn, schema='app', if_exists='append', index=False, dtype=dtype_mapping) # type: ignore
            log.info(f'app.LocationPackages loaded successfully')

            conn.execute(
                text("""
                    MERGE app.[EtlCDC] AS target
                    USING (SELECT :table_name AS [TableName], :max_index AS [MaxIndex]) AS source
                    ON target.[TableName] = source.[TableName]
                    WHEN MATCHED THEN UPDATE SET target.[MaxIndex] = source.[MaxIndex]
                    WHEN NOT MATCHED THEN INSERT ([TableName],[MaxIndex]) VALUES (source.[TableName],source.[MaxIndex]);
                """),
                {"table_name": f'app.LocationPackages', "max_index": int(max_id)}
            )
            log.info(f'app.LocationPackages loaded successfully, CDC updated to {max_id}')
    except Exception as e:
        log.error(f'Failed to load app.LocationPackages: {e}')
        raise

# -------------------- Main --------------------
def main():
    target = target_db_conn()

    while True:
        df = extract(target)
        if df.empty:
            log.info('No new data to load.')
            break
        df = transform(df, target)
        # print(df.head(20))
        # return
        load(df, target)
        # return

if __name__ == '__main__':
    main()
