import os
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR
from urllib.parse import quote_plus
import pandas as pd
from utils.tools import get_logger
from utils.custom_err import IncrementalDependencyError
from utils.fks_mapper import get_custom

warnings.filterwarnings('ignore')
load_dotenv()
log = get_logger('Items')

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
            {"table_name": 'dbo.Items'}
        ).scalar()
    max_id = max_id if not max_id is None else 0
    # max_id=0
    log.info(f'Current CDC for dbo.Items: {max_id}')

    query = f"SELECT TOP 10000 * FROM dbo.Items WHERE ItemID > {max_id} ORDER BY ItemID"
    # query = f"SELECT * FROM dbo.Items WHERE SubCatID in (10764, 10765, 10763, 10762, 10761, 10658, 10657, 10656, 10655, 10654, 10653, 10652)"
    df = pd.read_sql_query(query, source_db)
    log.info(f'Extracted {len(df)} rows from dbo.Items')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame, source_db: Engine, target_db: Engine) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Clean and transform Items data."""
    # Keep only necessary columns and rename
    df = df[['ItemID','SubCatID','Name','NameOnReceipt','Description','ItemImage','Barcode','SKU','DisplayOrder','Price', 'Cost','ItemType','IsInventoryItem','IsOpenItem','MinOpenPrice','LastUpdatedDate','StatusID','UnitID']]

    df = df.rename(columns={
        'NameOnReceipt':'NameAr',
        'ItemImage':'ImagePath',
        'ItemID':'OldItemID',
        'UnitID':'OldUnitID',
        'SubCatID':'SubCategoryID',
        'LastUpdatedDate':'UpdatedAt',
    })

    # Fix Null values
    df['UpdatedAt'] = df['UpdatedAt'].fillna(datetime.now())
    df['CreatedAt'] = df['UpdatedAt']
    df['IsInclusiveVAT'] = 0
    df['StatusID'] = df['StatusID'].fillna(1)

    for col in df.select_dtypes(include='object').columns:
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) and x.strip()!='' else None)
            df[col] = df[col].apply(lambda x: x if isinstance(x,str) and x != 'NULL' else None)


    log.info(f'{len(df[df['IsInventoryItem'].isna()])} rows with missing InventoryItem')
    df['IsInventoryItem'] = df['IsInventoryItem'].fillna(False)

    log.info(f'{len(df[df['IsOpenItem'].isna()])} rows with missing OpenItem')
    df['IsOpenItem'] = df['IsOpenItem'].fillna(False)

    log.info(f'{len(df[df['Cost'].isna()])} rows with missing Cost')
    df['Cost'] = df['Cost'].fillna(0)


    log.info(f'{len(df[df['SubCategoryID'].isna()])} rows with missing SubCategoryID')
    # print(df[df['SubCategoryID'].isna()])
    df = df[~df['SubCategoryID'].isna()]

    log.info(f'{len(df[df['Price'].isna()])} rows with missing Price')
    df['Price'] = df['Price'].fillna(0)



    # Fix String columns
    for col in df.select_dtypes(include='object').columns:
        if col != 'Name':
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) and x.strip() != '' else None)
        else: 
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) else '')
    
    
    # ItemTypeID HardCoded
    item_types = {'oil':1, 'oilfilter':2, 'service':3, 'other':4, 'carwash':5}
    df['ItemTypeID'] = df['ItemType'].apply(lambda x: x.lower().replace(' ', '').strip() if x else None).map(lambda x: item_types.get(x, 4))


    # CategoryID Matching    
    cat_ids = pd.read_sql(f"SELECT CategoryID, SubCategoryID FROM dbo.SubCategory", source_db)
    df = pd.merge(df, cat_ids, on='SubCategoryID', how='left')

    df = df.rename(columns={'CategoryID': 'OldCategoryID'})

    old_cat_ids = pd.read_sql(f"SELECT AccountID, Name, OldCategoryID FROM app.SyncCategories", target_db)
    new_cat_ids = pd.read_sql(f"SELECT AccountID, Name, CategoryID FROM app.Categories", target_db)
    # new_cat_ids = pd.read_sql(f"SELECT AccountID, Name, CategoryID FROM app.Categories WHERE CategoryID in (1938, 1939, 1940, 1941, 1942, 1943, 1944, 1971, 1972, 1973)", target_db)

    current_cat_ids = pd.merge(old_cat_ids, new_cat_ids, how='left', on=['AccountID', 'Name'])
    current_cat_ids.drop(columns=['AccountID', 'Name'], inplace=True)

    df = pd.merge(df, current_cat_ids, on='OldCategoryID', how='left')


    # UnitID Matching
    current_unit_ids = pd.read_sql(f"SELECT UnitID, OldUnitID FROM app.Units WHERE OldUnitID IS NOT NULL", target_db)
    df = pd.merge(df, current_unit_ids, on='OldUnitID', how='left')

    log.info(f'{df['CategoryID'].isna().sum()} rows with missing CategoryID')
    if df['CategoryID'].isna().sum():
        raise IncrementalDependencyError("Update Categories Table.")
    # print(df[df['CategoryID'].isna()][['CategoryID', 'OldCategoryID']])


    # Handling Duplicates
    sync_table = df[['OldItemID', 'CategoryID','Name']].copy()
    
    existing_records = pd.read_sql(f"SELECT CategoryID, Name FROM app.Items", target_db)
    existing_records['Duplicated'] = 1

    df = pd.merge(df, existing_records, how='left', on=['CategoryID','Name'])
    


    df = df[df['Duplicated'].isna()]

    df.sort_values(by=['CategoryID', 'StatusID', 'Price'], ascending=[True, True, False], inplace=True)


    df.drop(columns=['SubCategoryID', 'OldCategoryID', 'OldUnitID', 'ItemType', 'Duplicated', 'OldItemID'], inplace=True)


    df.drop_duplicates(subset=['CategoryID','Name'], inplace=True)
    

    log.info(f'Transformation complete, output: {len(df)}')
    return df, sync_table

# -------------------- Load --------------------
def load(df: pd.DataFrame, sync_t: pd.DataFrame, engine: Engine):

    dtype_mapping = {'Name':NVARCHAR(None), 'NameAr':NVARCHAR(None), 'Description':NVARCHAR(None), 'DescriptionAr':NVARCHAR(None), 'ImagePath':NVARCHAR(None)}
    max_id = sync_t['OldItemID'].max()

    # df.drop(columns='OldItemID', inplace=True)

    try:
        with engine.begin() as conn:  # Transaction-safe

            df.to_sql('Items', con=conn, schema='app', if_exists='append', index=False, dtype=dtype_mapping) # type: ignore
            log.info(f'dbo.Items loaded successfully')

            sync_t.to_sql('SyncItems', con=conn, schema='app', if_exists='append', index=False, dtype={'Name':NVARCHAR(None)}) # type: ignore
            log.info(f'app.SyncItems updated successfully')

            conn.execute(
                text("""
                    MERGE app.[EtlCDC] AS target
                    USING (SELECT :table_name AS [TableName], :max_index AS [MaxIndex]) AS source
                    ON target.[TableName] = source.[TableName]
                    WHEN MATCHED THEN UPDATE SET target.[MaxIndex] = source.[MaxIndex]
                    WHEN NOT MATCHED THEN INSERT ([TableName],[MaxIndex]) VALUES (source.[TableName],source.[MaxIndex]);
                """),
                {"table_name": f'dbo.Items', "max_index": int(max_id)}
            )
            log.info(f'dbo.Items loaded successfully, CDC updated to {max_id}')
    except Exception as e:
        log.error(f'Failed to load dbo.Items: {e}')
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
        df,sync_table = transform(df, source, target)
        # return
        load(df, sync_table, target)
        # return 

if __name__ == '__main__':
    main()
