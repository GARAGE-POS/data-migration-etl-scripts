import os
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR
from urllib.parse import quote_plus
import pandas as pd
import json
from utils.tools import get_last_ingested, get_logger, update_last_ingested
from utils.custom_err import IncrementalDependencyError
from utils.fks_mapper import get_categories

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

    last_id = get_last_ingested(user_id, 'dbo.Items')

    query = f"""
        SELECT
            i.ItemID,
            c.CategoryID,
            sc.Name SubCatName,
            i.Name,
            i.NameOnReceipt,
            i.Description,
            i.ItemImage,
            i.Barcode,
            i.SKU,
            i.DisplayOrder,
            i.Price,
            i.Cost,
            i.ItemType,
            i.IsInventoryItem,
            i.IsOpenItem,
            i.MinOpenPrice,
            i.LastUpdatedDate,
            i.StatusID,
            i.UnitID,
            inv.SupplierID,
            inv.LastUpdatedDate InvUpdatedDate,
            l.AllowNegativeInventory ContinueSellingWhenOutOfStock
        FROM Items i
        JOIN SubCategory sc ON sc.SubCategoryID = i.SubCatID
        JOIN Category c ON sc.CategoryID = c.CategoryID
        JOIN Locations l ON c.LocationID = l.LocationID
        JOIN Users u ON l.UserID = u.UserID
        LEFT JOIN Inventory inv ON inv.ItemID = i.ItemID
        WHERE u.UserID = {user_id} AND i.ItemID > {last_id}
        ORDER BY ItemID
    """
    df = pd.read_sql_query(query, engine)

    df.sort_values(by=['ItemID', 'InvUpdatedDate'], inplace=True)
    df.drop_duplicates(subset='ItemID', keep='last', inplace=True)

    stock_df = pd.read_sql(f"SELECT CurrentStock, ItemID, LastUpdatedDate FROM inv_Stock WHERE UserID={user_id}", engine)
    stock_df.sort_values(by=['ItemID', 'LastUpdatedDate'], inplace=True)
    stock_df.drop_duplicates(subset='ItemID', keep='last', inplace=True)
    df = pd.merge(df, stock_df[['CurrentStock', 'ItemID']], on='ItemID', how='left')

    log.info(f'Extracted {len(df)} rows from dbo.Items')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame, engine: Engine) -> pd.DataFrame:
    """Clean and transform Items data."""
    # Keep only necessary columns and rename
    df = df.rename(columns={
        'NameOnReceipt':'NameAr',
        'ItemImage':'ImagePath',
        'ItemID':'OldItemID',
        'UnitID':'OldUnitID',
        'SupplierID': 'OldSupplierID',
        'CategoryID': 'OldCategoryID',
        'LastUpdatedDate':'UpdatedAt',
    })


    # Fix String columns
    df['IsInventoryItem']= df['IsInventoryItem'].astype('bool')
    df['IsOpenItem'] = df['IsOpenItem'].astype('bool')
    for col in df.select_dtypes(include='object').columns:
        if col != 'Name':
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) and x.strip() != '' else None)
        else: 
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) else '')
    df['OldSupplierID'] = pd.to_numeric(df['OldSupplierID'], errors='coerce')

    # IDs Matching    
    df = pd.merge(df, get_categories(engine, df['OldCategoryID']), how='left', on='OldCategoryID')
    if df['CategoryID'].isna().sum():
        log.warning(f'{df['CategoryID'].isna().sum()} rows with missing CategoryID')
        raise IncrementalDependencyError("Update Categories Table.")

    df = pd.merge(df, pd.read_sql(f"SELECT UnitID, OldUnitID FROM app.SyncUnits WHERE OldUnitID IS NOT NULL", engine), on='OldUnitID', how='left')

    df = pd.merge(df, pd.read_sql(f"SELECT SupplierID, OldSupplierID FROM app.Suppliers WHERE OldSupplierID IS NOT NULL", engine), on='OldSupplierID', how='left')


    # Fix Null values
    df['UpdatedAt'] = df['UpdatedAt'].fillna(datetime.now())
    df['CreatedAt'] = df['UpdatedAt']
    df['IsInclusiveVAT'] = 0
    df['StatusID'] = df['StatusID'].fillna(2)
    df['CurrentStock'] = df['CurrentStock'].fillna(0)
    df['IsInventoryItem'] = df['IsInventoryItem'].fillna(0)
    df['Cost'] = df['Cost'].fillna(0)
    df['IsInventoryItem'] = df['IsInventoryItem'].fillna(False)
    df['IsOpenItem'] = df['IsOpenItem'].fillna(False)
    df['ItemTypeID'] = df['ItemType'].map(lambda x: 1 if x == 'service' else 2)
    df['SubCatName'] = df['SubCatName'].map(lambda x: {'SubCategory': x})
    df['ContinueSellingWhenOutOfStock'] = df['ContinueSellingWhenOutOfStock'].map(lambda x: {'ContinueSellingWhenOutOfStock': bool(x)})
    df['SupplierID'] = df['SupplierID'].map(lambda x: {'SupplierIDs': [x]} if isinstance(x, int) else {'SupplierIDs': []})
    df["InvUpdatedDate"] = (
    pd.to_datetime(df["InvUpdatedDate"], utc=True)
      .dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    )
    df["InvUpdatedDate"] = df["InvUpdatedDate"].astype("object").fillna(None)
    df['PropertiesJson'] = df.apply(lambda row: json.dumps(row['SubCatName'] | {"Inventory": row['SupplierID'] | row['ContinueSellingWhenOutOfStock'] | {"StockInHand": row['CurrentStock']}} | {'Attributes':{}} | {'lastUpdated': row['InvUpdatedDate']}, ensure_ascii=False), axis=1)


    df.drop(columns=['SubCatName', 'OldCategoryID', 'OldUnitID', 'InvUpdatedDate',  'OldSupplierID', 'SupplierID', 'ContinueSellingWhenOutOfStock', 'CurrentStock', 'ItemType'], inplace=True)

    log.info(f'Transformation complete, output: {len(df)}')
    return df

# -------------------- Load --------------------
def load(df: pd.DataFrame, user_id:int, engine: Engine):

    dtype_mapping = {col:NVARCHAR(None) for col in df.select_dtypes(include='object').columns}

  
    try:
        with engine.begin() as conn: 

            conn.execute(text("""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.columns
                    WHERE Name = 'OldItemID'
                    AND Object_ID = Object_ID('app.Items')
                )
                BEGIN
                    ALTER TABLE app.Items
                    ADD OldItemID BIGINT NULL;
                END
            """))
            log.info("Verified/Added OldItemID column.")

        # Inserting the Data
        i = 0
        while i < len(df)/5000:
            df.iloc[5000*i:5000*(i+1)].to_sql('Items', con=engine, schema='app', if_exists='append', index=False, dtype=dtype_mapping) # type: ignore
            update_last_ingested(user_id, 'dbo.Items', int(df.iloc[5000*i:5000*(i+1)]['OldItemID'].max()))
            log.info(f'Batch {i+1} inserted')
            i+=1

        log.info(f'dbo.Items loaded successfully')
    

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
    
    df = transform(df, target)

    if if_load:
        load(df, user_id, target)
        

# if __name__ == '__main__':
#     main()
