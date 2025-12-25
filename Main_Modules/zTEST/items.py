import os
import sys
import warnings
from dotenv import load_dotenv
import logging
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR
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
def extract(account_id: int, v1: Engine, v2: Engine) -> tuple[pd.DataFrame, pd.DataFrame]:

    current_unit_ids = pd.read_sql(f"SELECT UnitID, OldUnitID FROM app.Units WHERE OldUnitID IS NOT NULL", v2)


    current_cat_ids = tuple(pd.read_sql(f"SELECT CategoryID FROM app.Categories WHERE AccountID={account_id}", v2).values.tolist()) + (0,0)
    current_cat_ids = str(current_cat_ids).replace('[','').replace(']','')

    curr_old_cat_ids = pd.read_sql(f"""
                SELECT s.OldCategoryID, c.CategoryID
                FROM app.synccategories s
                JOIN app.categories c
                    ON s.accountid = c.AccountID
                AND c.Name COLLATE Latin1_General_CS_AS = s.Name COLLATE Latin1_General_CS_AS
                WHERE c.CategoryID IN {current_cat_ids}
                ORDER BY s.OldCategoryID
            """, v2)

    
    old_cat_ids = tuple(curr_old_cat_ids['OldCategoryID'].values.tolist())+(0,0)
    old_cat_ids = str(old_cat_ids).replace('[','').replace(']','')

    cat_subcat_ids = pd.read_sql(f"SELECT CategoryID, SubCategoryID FROM dbo.SubCategory WHERE CategoryID IN {old_cat_ids}", v1)
    cat_subcat_ids.rename(columns={'CategoryID':'OldCategoryID'}, inplace=True)
    
    old_subcat_ids = tuple(cat_subcat_ids['SubCategoryID'].values.tolist()) + (0,0)
    old_subcat_ids = str(old_subcat_ids).replace('[','').replace(']','')


    # Importing app.Items
    df = pd.read_sql_query(f"SELECT ItemID, SubCatID, Name, NameOnReceipt, Description, ItemImage, Barcode, SKU, DisplayOrder, Price,  Cost, ItemType, IsInventoryItem, IsOpenItem, MinOpenPrice, LastUpdatedDate, StatusID, UnitID FROM dbo.Items WHERE SubCatID in {old_subcat_ids}", v1)
    logging.info(f'Extracted {len(df)} rows from dbo.Items')

    df = df.rename(columns={
        'ItemID':'OldItemID',
        'NameOnReceipt':'NameAr',
        'ItemImage':'ImagePath',
        'UnitID':'OldUnitID',
        'LastUpdatedDate':'UpdatedAt',
        'SubCatID':'SubCategoryID'
    })



    # Fix Null values
    df['UpdatedAt'] = df['UpdatedAt'].fillna(datetime.now())
    df['CreatedAt'] = df['UpdatedAt']

    df['IsInclusiveVAT'] = 0
    df['StatusID'] = df['StatusID'].fillna(1)


    logging.info(f'{len(df[df['IsInventoryItem'].isna()])} rows with missing InventoryItem')
    df['IsInventoryItem'] = df['IsInventoryItem'].fillna(False)

    logging.info(f'{len(df[df['IsOpenItem'].isna()])} rows with missing OpenItem')
    df['IsOpenItem'] = df['IsOpenItem'].fillna(False)

    logging.info(f'{len(df[df['Cost'].isna()])} rows with missing Cost')
    df['Cost'] = df['Cost'].fillna(0)


    logging.info(f'{len(df[df['Price'].isna()])} rows with missing Price')
    df['Price'] = df['Price'].fillna(0)


    # Fix String columns
    for col in df.select_dtypes(include='object').columns:
        if col != 'Name':
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) and x.strip() != '' else None)
        else: 
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) else '')
    
    
    # ItemTypeID HardCoded
    df['ItemTypeID'] = df['ItemType'].apply(lambda x: x.lower().replace(' ', '').strip() if x else 'other').map({'oil':1, 'oilfilter':2, 'service':3, 'other':4, 'carwash':5})



    # ID Matching
    df = pd.merge(df, current_unit_ids, on='OldUnitID', how='left')

    df = pd.merge(df, cat_subcat_ids, on='SubCategoryID', how='left')

    df = pd.merge(df, curr_old_cat_ids, on='OldCategoryID', how='left')

    sync_table = df[['OldItemID','CategoryID', 'Name']]

    df.drop(columns=['ItemType', 'OldUnitID', 'SubCategoryID', 'OldItemID', 'OldCategoryID'], inplace=True)

    df.sort_values('Price', ascending=False, inplace=True)
    df.drop_duplicates(subset=['CategoryID','Name'], inplace=True)


    logging.info(f'Transformation complete, output: {len(df)}')
    # print(pd.read_sql(f"SELECT * FROM app.Categories WHERE AccountID={account_id} and StatusID <> 3", v2))

    return df,sync_table



# -------------------- Load --------------------
def load(df: pd.DataFrame, sync_t: pd.DataFrame, account_id: int, engine: Engine):

    dtype_mapping = {'Name':NVARCHAR(None), 'NameAr':NVARCHAR(None), 'Description':NVARCHAR(None), 'DescriptionAr':NVARCHAR(None), 'ImagePath':NVARCHAR(None)}

    try:
        with engine.begin() as conn:  # Transaction-safe

            df.to_sql('Items', con=conn, schema='app', if_exists='append', index=False, dtype=dtype_mapping) # type: ignore
            logging.info(f'dbo.Items loaded successfully')

            sync_t.to_sql('SyncItems', con=conn, schema='app', if_exists='append', index=False, dtype={'Name':NVARCHAR(None)}) # type: ignore
            logging.info(f'app.SyncItems updated successfully')

            conn.execute(
                text("""
                    MERGE app.[EtlCDC] AS target
                    USING (SELECT :table_name AS [TableName], :max_index AS [MaxIndex]) AS source
                    ON target.[TableName] = source.[TableName]
                    WHEN MATCHED THEN UPDATE SET target.[MaxIndex] = source.[MaxIndex]
                    WHEN NOT MATCHED THEN INSERT ([TableName],[MaxIndex]) VALUES (source.[TableName],source.[MaxIndex]);
                """),
                {"table_name": f'app.Items', "max_index": int(account_id)}
            )
            logging.info(f'app.Items loaded successfully, CDC updated to {account_id}')

    except Exception as e:
        logging.error(f'Failed to load dbo.Items: {e}')
        raise

# -------------------- Main --------------------
def main():
    source = source_db_conn()
    target = target_db_conn()

    # account_id = int(sys.argv[1])

    with target.begin() as conn:
        max_id = conn.execute(
            text("SELECT ISNULL(MaxIndex,0) FROM app.EtlCDC WHERE TableName=:table_name"),
            {"table_name": 'app.Items'}
        ).scalar()
    max_id = max_id if not max_id is None else 0
    logging.info(f'Current CDC for app.Items: {max_id}')

    account_ids : list = pd.read_sql(f'SELECT AccountID FROM app.Accounts WHERE AccountID > {max_id} ORDER BY AccountID', target)['AccountID'].values.tolist()
    for account_id in account_ids:
        logging.info(f'Current AccountID: {account_id}')
        df,sync_table = extract(account_id,source, target)
        if df.empty and sync_table.empty:
            logging.info('No new data to load.')
            continue
        load(df, sync_table, account_id, target)
        # return
if __name__ == '__main__':
    main()
