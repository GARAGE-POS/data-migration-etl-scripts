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
            {"table_name": 'dbo.Model'}
        ).scalar()
    logging.info(f'Current CDC for dbo.Model: {max_id}')

    query = f"SELECT * FROM dbo.Model WHERE ModelID > {max_id}"
    df = pd.read_sql_query(query, source_db)
    logging.info(f'Extracted {len(df)} rows from dbo.Model')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame, target_db: Engine) -> pd.DataFrame:
    """Clean and transform Model data."""
    # Keep only necessary columns and rename

    df.drop(
        columns=['RowID','CreatedBy','LastUpdatedBy'], inplace=True
    )
    df = df.rename(columns={
        'ArabicName':'NameAr',
        'ModelID':'OldModelID',
        'CreatedOn':'CreatedDate',
        'LastUpdatedDate':'LastUpdateDate'
    })

    df['CreatedDate'] = df['CreatedDate'].fillna(datetime.now())
    df['LastUpdateDate'] = df['LastUpdateDate'].fillna(datetime.now())

    df['RecommendedLitres'] = pd.to_numeric(df['RecommendedLitres'], errors='coerce')

    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) and x.strip() != '' else x)

    df['Year'] = df['Year'].fillna(0)


    # Sync ModelIDs
    make_ids = pd.read_sql("SELECT MakeID, OldMakeID FROM app.Makes WHERE OldMakeID IS NOT NULL", target_db)
    df.rename(columns={'MakeID':'OldMakeID'}, inplace=True)

    df = pd.merge(df, make_ids, on='OldMakeID', how='left')
    df.drop(columns="OldMakeID", inplace=True)

    print(df)

    logging.info('Transformation complete')
    return df

# -------------------- Load --------------------
def load(df: pd.DataFrame, engine: Engine):

    dtype_mapping = {'Name':NVARCHAR(None), 'NameAr':NVARCHAR(None), 'RecommendedLitres':DECIMAL(18,2), 'ImagePath':NVARCHAR(None)}
    max_id = df['OldModelID'].max()

    try:
        with engine.begin() as conn:  # Transaction-safe

            conn.execute(text("""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.columns
                    WHERE Name = 'OldModelID'
                    AND Object_ID = Object_ID('app.Models')
                )
                BEGIN
                    ALTER TABLE app.Models
                    ADD OldModelID BIGINT NULL;
                END
            """))
            logging.info("Verified/Added OldModelID column.")

            df.to_sql('Models', con=conn, schema='app', if_exists='append', index=False, dtype=dtype_mapping) # type: ignore
            logging.info(f'dbo.Model loaded successfully')

            conn.execute(
                text("""
                    MERGE app.[EtlCDC] AS target
                    USING (SELECT :table_name AS [TableName], :max_index AS [MaxIndex]) AS source
                    ON target.[TableName] = source.[TableName]
                    WHEN MATCHED THEN UPDATE SET target.[MaxIndex] = source.[MaxIndex]
                    WHEN NOT MATCHED THEN INSERT ([TableName],[MaxIndex]) VALUES (source.[TableName],source.[MaxIndex]);
                """),
                {"table_name": f'dbo.Model', "max_index": int(max_id)}
            )
            logging.info(f'dbo.Model loaded successfully, CDC updated to {max_id}')
    except Exception as e:
        logging.error(f'Failed to load dbo.Model: {e}')
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
    # print(df)
    # return
    load(df, target)

if __name__ == '__main__':
    main()
