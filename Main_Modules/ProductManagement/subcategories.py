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
            {"table_name": 'dbo.SubCategory'}
        ).scalar()
    max_id = max_id if not max_id is None else 0
    logging.info(f'Current CDC for dbo.SubCategory: {max_id}')
    
    query = f"SELECT * FROM dbo.SubCategory WHERE CategoryID > {max_id}"
    df = pd.read_sql_query(query, source_db)
    logging.info(f'Extracted {len(df)} rows from dbo.SubCategory')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame, engine: Engine) -> pd.DataFrame:
    """Clean and transform Category data."""
    # Keep only necessary columns and rename
    df = df[['SubCategoryID', 'CategoryID']]

    df.rename(columns={
        'CategoryID': 'OldCategoryID'
        }, inplace=True)
    
    joint_table = pd.read_sql("SELECT AccountID, Name, OldCategoryID FROM app.SyncCategories", engine)
    current_cat_ids = pd.read_sql("SELECT CategoryID, AccountID, Name FROM app.Categories", engine)

    cat_ids = pd.merge(joint_table, current_cat_ids, how='left', on=['AccountID','Name'])[['CategoryID', 'OldCategoryID']]


    df = pd.merge(df, cat_ids, how='left', on='OldCategoryID')


    print(df)


    print(len(df[df['CategoryID'].isna()]))

    return df 

# -------------------- Load --------------------
def load(df: pd.DataFrame, engine: Engine):

    dtype_mapping = {'Name':NVARCHAR(None), 'NameAr':NVARCHAR(None), 'ImagePath':NVARCHAR(None), 'Description':NVARCHAR(None)}
    max_id = df['OldCategoryID'].max()

    try:
        with engine.begin() as conn: 

            # Adding Old CategoryID Column 
            # conn.execute(text("""
            #     IF NOT EXISTS (
            #         SELECT 1 FROM sys.columns
            #         WHERE Name = 'OldCategoryID'
            #         AND Object_ID = Object_ID('app.Categories')
            #     )
            #     BEGIN
            #         ALTER TABLE app.Categories
            #         ADD OldCategoryID BIGINT NULL;
            #     END
            # """))
            # logging.info("Verified/Added OldCategoryID column.")

            # Inserting the Data
            # df.to_sql('Categories', con=conn, schema='app', if_exists='append', index=False, dtype=dtype_mapping) # type: ignore
            # logging.info(f'dbo.SubCategory loaded successfully')

            sync_table.to_sql('SyncCategories', con=conn, schema='app', if_exists='append', index=False, dtype={'Name':NVARCHAR(None)}) # type: ignore
            logging.info(f'app.SyncCategories updated successfully')

            # # Updating the CDC
            # conn.execute(
            #     text("""
            #         MERGE app.[EtlCDC] AS target
            #         USING (SELECT :table_name AS [TableName], :max_index AS [MaxIndex]) AS source
            #         ON target.[TableName] = source.[TableName]
            #         WHEN MATCHED THEN UPDATE SET target.[MaxIndex] = source.[MaxIndex]
            #         WHEN NOT MATCHED THEN INSERT ([TableName],[MaxIndex]) VALUES (source.[TableName],source.[MaxIndex]);
            #     """),
            #     {"table_name": f'dbo.SubCategory', "max_index": int(max_id)}
            # )
            # logging.info(f'dbo.SubCategory loaded successfully, CDC updated to {max_id}')
    except Exception as e:
        logging.error(f'Failed to load dbo.SubCategory: {e}')
        raise

# -------------------- Main --------------------
def main():
    source = source_db_conn()
    target = target_db_conn()
    while True:
        df = extract(source, target)
        if df.empty:
            logging.info('No new data to load.')
            return
        df = transform(df, target)
        # print(df)
        return
        load(df, target)
        return
    
if __name__ == '__main__':
    main()
