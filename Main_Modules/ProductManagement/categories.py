import os
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR, DECIMAL
from urllib.parse import quote_plus
import pandas as pd
from utils.tools import get_logger
from utils.custom_err import IncrementalDependencyError

warnings.filterwarnings('ignore')
load_dotenv()
log = get_logger('Categories')

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
            {"table_name": 'dbo.Category'}
        ).scalar()
    max_id = max_id if not max_id is None else 0
    log.info(f'Current CDC for dbo.Category: {max_id}')
    
    query = f"SELECT top 1000 * FROM dbo.Category WHERE CategoryID > {max_id} and CategoryID <> 2400 ORDER BY CategoryID"
    df = pd.read_sql_query(query, source_db)
    log.info(f'Extracted {len(df)} rows from dbo.Category')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame, engine: Engine) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Clean and transform Category data."""
    # Keep only necessary columns and rename

    df.drop(
        columns=['RowID','SortByAlpha','LastUpdatedBy'], inplace=True
    )
    df = df.rename(columns={
        'AlternateName':'NameAr',
        'CategoryID':'OldCategoryID',
        'LocationID':'OldLocationID',
        'LastUpdatedDate':'UpdatedAt',
        'Image':'ImagePath'
    })

    df['UpdatedAt'] = df['UpdatedAt'].fillna(datetime.now())
    df['CreatedAt'] = df['UpdatedAt']

    for col in df.select_dtypes(include='object').columns:
        if col != 'Name':
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) and x.strip() != '' else None)
        else:
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) else x)


    df['StatusID'] = df['StatusID'].fillna(1)
    df = df[~df['OldLocationID'].isna()]

    current_acc_ids = pd.read_sql("SELECT AccountID, OldLocationID FROM app.Locations WHERE OldLocationID IS NOT NULL", engine)
    df = pd.merge(df, current_acc_ids, on='OldLocationID', how='left')

    df.drop(columns=['OldLocationID'], inplace=True)

    sync_table = df[['OldCategoryID', 'AccountID', 'Name']].copy()

    df.sort_values(by=['AccountID', 'StatusID'], inplace=True)

    # print(df[df['Name']=='Big Car'])


    df.drop_duplicates(subset=['AccountID', 'Name'], inplace=True)
    # print(df[df['Name']=='Big Car'])


    existing_records = pd.read_sql(f"SELECT AccountID, Name FROM app.Categories", engine)
    existing_records['Duplicated'] = 1

    df = pd.merge(df, existing_records, how='left', on=['AccountID','Name'])

    df = df[df['Duplicated'].isna()]
    df.drop(columns='Duplicated', inplace=True)


    missing_acc = df['AccountID'].isna()
    log.info(f"Missing AccountID: {missing_acc.sum()}")
    if missing_acc.sum():
        print(df[missing_acc])
        raise IncrementalDependencyError(f"Update Accounts and Locations tables.")
    
    log.info(f'Transformation complete, output rows: {len(df)}')
    return df, sync_table

# -------------------- Load --------------------
def load(df: pd.DataFrame, sync_table: pd.DataFrame, engine: Engine):

    dtype_mapping = {'Name':NVARCHAR(None), 'NameAr':NVARCHAR(None), 'ImagePath':NVARCHAR(None), 'Description':NVARCHAR(None)}
    max_id = sync_table['OldCategoryID'].max()

    df.drop(columns='OldCategoryID', inplace=True)

    try:
        with engine.begin() as conn: 

            # Inserting the Data
            df.to_sql('Categories', con=conn, schema='app', if_exists='append', index=False, dtype=dtype_mapping) # type: ignore
            log.info(f'dbo.Category loaded successfully')

            sync_table.to_sql('SyncCategories', con=conn, schema='app', if_exists='append', index=False, dtype={'Name':NVARCHAR(None)}) # type: ignore
            log.info(f'app.SyncCategories updated successfully')

            # # Updating the CDC
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
            log.info(f'dbo.Category loaded successfully, CDC updated to {max_id}')
    except Exception as e:
        log.error(f'Failed to load dbo.Category: {e}')
        raise

# -------------------- Main --------------------
def main():
    source = source_db_conn()
    target = target_db_conn()
    while True:
        df = extract(source, target)
        if df.empty:
            log.info('No new data to load.')
            return
        df, sync_table = transform(df, target)
        # return
        # print(sync_table)
        # print(df)
        # return
        load(df, sync_table, target)
        # return
    
if __name__ == '__main__':
    main()
