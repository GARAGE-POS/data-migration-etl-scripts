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
def extract(user_id: int, engine: Engine) -> pd.DataFrame:
    """Extract data based on UserID."""

    subcat_query = f"""
        SELECT SubCategoryID 
        FROM dbo.SubCategory
        WHERE CategoryID IN (
            SELECT CategoryID
            FROM dbo.Category
            WHERE LocationID IN (
                SELECT LocationID
                FROM dbo.Locations
                WHERE UserID={user_id}
            )
        )
    """

    subcat_ids = pd.read_sql(subcat_query, engine)
    subcat_ids = (0,0) + tuple(subcat_ids['SubCategoryID'].values.tolist())

    query = f"SELECT * FROM dbo.Items WHERE SubCatID IN {subcat_ids}"
    df = pd.read_sql_query(query, engine)
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

    # Fix String columns
    for col in df.select_dtypes(include='object').columns:
        if col != 'Name':
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) and x.strip() != '' else None)
        else: 
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) else '')
    

    # Fix Null values
    df['UpdatedAt'] = df['UpdatedAt'].fillna(datetime.now())
    df['CreatedAt'] = df['UpdatedAt']
    df['IsInclusiveVAT'] = 0
    df['StatusID'] = df['StatusID'].fillna(1)

    for col in df.select_dtypes(include='object').columns:
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) and x.strip()!='' else None)
            df[col] = df[col].apply(lambda x: x if isinstance(x,str) and x != 'NULL' else None)


    log.info(f'{df['IsInventoryItem'].isna().sum()} rows with missing InventoryItem')
    df['IsInventoryItem'] = df['IsInventoryItem'].fillna(False)

    log.info(f'{df['IsOpenItem'].isna().sum()} rows with missing OpenItem')
    df['IsOpenItem'] = df['IsOpenItem'].fillna(False)

    log.info(f'{df['Cost'].isna().sum()} rows with missing Cost')
    df['Cost'] = df['Cost'].fillna(0)

    log.info(f'{df['SubCategoryID'].isna().sum()} rows with missing SubCategoryID')

    log.info(f'{df['Price'].isna().sum()} rows with missing Price')
    df['Price'] = df['Price'].fillna(0)


    
    # ItemTypeID MAP
    item_df = get_custom(target_db, ['ItemTypeID', 'Name'], 'app.ItemTypes')
    item_map = dict(zip(item_df['Name'].map(lambda x: x.lower().replace(' ', '').strip()), item_df['ItemTypeID']))
    df['ItemTypeID'] = df['ItemType'].apply(lambda x: x.lower().replace(' ', '').strip() if x else None).map(lambda x: item_map.get(x, 4))

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
    current_unit_ids = pd.read_sql(f"SELECT UnitID, OldUnitID FROM app.SyncUnits WHERE OldUnitID IS NOT NULL", target_db)
    df = pd.merge(df, current_unit_ids, on='OldUnitID', how='left')

    if df['CategoryID'].isna().sum():
        log.warning(f'{df['CategoryID'].isna().sum()} rows with missing CategoryID')
        raise IncrementalDependencyError("Update Categories Table.")


    # Handling Duplicates
    sync_table = df[['OldItemID', 'CategoryID','Name']].copy()

    df.sort_values(by=['CategoryID', 'StatusID', 'Price'], ascending=[True, True, False], inplace=True)

    df.drop(columns=['SubCategoryID', 'OldCategoryID', 'OldUnitID', 'ItemType', 'OldItemID'], inplace=True)

    df.drop_duplicates(subset=['CategoryID','Name'], inplace=True)
    

    log.info(f'Transformation complete, output: {len(df)}')
    return df, sync_table

# -------------------- Load --------------------
def load(df: pd.DataFrame, sync_t: pd.DataFrame, engine: Engine):

    dtype_mapping = {'Name':NVARCHAR(None), 'NameAr':NVARCHAR(None), 'Description':NVARCHAR(None), 'DescriptionAr':NVARCHAR(None), 'ImagePath':NVARCHAR(None)}

    try:
        with engine.begin() as conn:  # Transaction-safe

            df.to_sql('Items', con=conn, schema='app', if_exists='append', index=False, dtype=dtype_mapping) # type: ignore
            log.info(f'dbo.Items loaded successfully')

            sync_t.to_sql('SyncItems', con=conn, schema='app', if_exists='append', index=False, dtype={'Name':NVARCHAR(None)}) # type: ignore
            log.info(f'app.SyncItems updated successfully')

    except Exception as e:
        log.error(f'Failed to load dbo.Items: {e}')
        raise

# -------------------- Main --------------------
def main(user_id:int, if_load:bool=True):
    source = source_db_conn()
    target = target_db_conn()

    df = extract(user_id, source)
    if df.empty:
        log.info('No data to load.')
        return
    
    df,sync_table = transform(df, source, target)
    print(df)

    if if_load:
        load(df, sync_table, target)
        

# if __name__ == '__main__':
#     main()
