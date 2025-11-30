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
            {"table_name": 'dbo.Category'}
        ).scalar()
    logging.info(f'Current CDC for dbo.Category: {max_id}')

    query = f"SELECT TOP 5000 * FROM dbo.Category WHERE CategoryID > {max_id}"
    df = pd.read_sql_query(query, source_db)
    logging.info(f'Extracted {len(df)} rows from dbo.Category')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and transform Category data."""
    # Keep only necessary columns and rename

    df.drop(
        columns=['RowID','SortByAlpha','LastUpdatedBy', 'LocationID'], inplace=True
    )
    df = df.rename(columns={
        'AlternateName':'NameAr',
        'CategoryID':'OldCategoryID',
        'LastUpdatedDate':'LastUpdateDate',
        'Image':'ImagePath'
    })

    df['CreatedDate'] = datetime.now()
    df['LastUpdateDate'] = df['LastUpdateDate'].fillna(datetime.now())

    for col in df.select_dtypes(include='object').columns:
        if col != 'Name':
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) and x.strip() != '' else None)
        else:
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) else x)


    df['StatusID'] = df['StatusID'].fillna(1)

    logging.info('Transformation complete')
    return df

# -------------------- Load --------------------
def load(df: pd.DataFrame, engine: Engine):

    dtype_mapping = {'Name':NVARCHAR(None), 'NameAr':NVARCHAR(None), 'ImagePath':NVARCHAR(None), 'Description':NVARCHAR(None)}
    max_id = df['OldCategoryID'].max()

    try:
        with engine.begin() as conn: 

            # Adding Old CategoryID Column 
            conn.execute(text("""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.columns
                    WHERE Name = 'OldCategoryID'
                    AND Object_ID = Object_ID('app.Categories')
                )
                BEGIN
                    ALTER TABLE app.Categories
                    ADD OldCategoryID BIGINT NULL;
                END
            """))
            logging.info("Verified/Added OldCategoryID column.")

            # Inserting the Data
            df.to_sql('Categories', con=conn, schema='app', if_exists='append', index=False, dtype=dtype_mapping) # type: ignore
            logging.info(f'dbo.Category loaded successfully')

            # Updating the CDC
            conn.execute(
                text("""
                    MERGE app.[EtlCDC] AS target
                    USING (SELECT :table_name AS [TableName], :max_index AS [MaxIndex]) AS source
                    ON target.[TableName] = source.[TableName]
                    WHEN MATCHED THEN UPDATE SET target.[MaxIndex] = source.[MaxIndex]
                    WHEN NOT MATCHED THEN INSERT ([TableName],[MaxIndex]) VALUES (source.[TableName],source.[MaxIndex]);
                """),
                {"table_name": f'dbo.Category', "max_index": int(max_id)}
            )
            logging.info(f'dbo.Category loaded successfully, CDC updated to {max_id}')
    except Exception as e:
        logging.error(f'Failed to load dbo.Category: {e}')
        raise

# -------------------- Main --------------------
def main():
    source = source_db_conn()
    target = target_db_conn()
    df = extract(source, target)
    if df.empty:
        logging.info('No new data to load.')
        return
    df = transform(df)
    # print(df)
    # return
    load(df, target)

if __name__ == '__main__':
    main()
