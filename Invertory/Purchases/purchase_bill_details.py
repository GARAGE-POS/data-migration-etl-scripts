import os
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR, DECIMAL
from urllib.parse import quote_plus
import pandas as pd
from utils.tools import get_last_ingested, get_logger, update_last_ingested
from utils.fks_mapper import get_custom, get_items
from utils.custom_err import IncrementalDependencyError

warnings.filterwarnings('ignore')
load_dotenv()
log = get_logger('PurchaseBillDetails')

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

    last_id = get_last_ingested(user_id, 'dbo.Inv_BillDetail')

    query = f"SELECT * FROM dbo.Inv_BillDetail WHERE BillID IN (SELECT BillID FROM dbo.inv_Bill WHERE LocationID IN (SELECT LocationID FROM dbo.Locations WHERE UserID={user_id})) AND BillDetailID > {last_id} ORDER BY BillDetailID"
    df = pd.read_sql_query(query, engine)
    log.info(f'Extracted {len(df)} rows from dbo.Inv_BillDetail')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame, engine: Engine) -> pd.DataFrame:
    """Clean and transform Users data."""
    # Keep only necessary columns and rename
    df.rename(columns={
        "BillDetailID":'OldBillDetailID',
        "BillID":'OldBillID',
        "ItemID": "OldItemID",
        "Cost": "CostPerUnit",
        "Price": "PricePerUnit",
        "CreatedOn": "CreatedAt",
        'LastUpdatedDate':'UpdatedAt'
        }, inplace=True)
    

    df['StatusID'] = df['StatusID'].fillna(1)
    df['UpdatedAt'] = df['UpdatedAt'].fillna(datetime.now())
    df.loc[df['CreatedAt'].isna(), 'CreatedAt'] = df['UpdatedAt']

  
    # PurchaseBillID Mapping
    df = pd.merge(df, get_custom(engine, ['PurchaseBillID', 'OldBillID', 'TaxAmount'], 'app.PurchaseBills'), on='OldBillID', how='left')
    missing_bills = df['PurchaseBillID'].isna()
    if missing_bills.sum():
        log.warning(f'Missing PurchaseBillIDs: {missing_bills.sum()}')
        raise IncrementalDependencyError('Update PurchaseBills Table.')

    # ItemID Mapping
    df = pd.merge(df, get_items(engine, df['OldItemID']), on='OldItemID', how='left')
    missing_items = df['ItemID'].isna()
    if missing_items.sum():
        log.warning(f'Missing ItemIDs: {missing_items.sum()}')
        raise IncrementalDependencyError('Update Items Table.')


    df.drop(columns=[
        'OldBillID', 'OldItemID', 'CreatedBy', 'LastUpdatedBy', 'Remarks'
    ], inplace=True)

    log.info('Transformation complete')

    return df

# -------------------- Load --------------------
def load(df: pd.DataFrame, user_id: int, engine: Engine):

    dtype_mapping = {col:NVARCHAR(None) for col in df.select_dtypes(include='object').columns}    
    
    try:
        with engine.begin() as conn:  # Transaction-safe

            conn.execute(text("""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.columns
                    WHERE Name = 'OldBillDetailID'
                    AND Object_ID = Object_ID('app.PurchaseBillDetails')
                )
                BEGIN
                    ALTER TABLE app.PurchaseBillDetails
                    ADD OldBillDetailID BIGINT NULL;
                END
            """))
            log.info("Verified/Added OldBillDetailID column.")


        i = 0
        while i < len(df)/5000:
            df.iloc[5000*i:5000*(i+1)].to_sql('PurchaseBillDetails', con=engine, schema='app', if_exists='append', index=False, dtype=dtype_mapping) # type: ignore
            update_last_ingested(user_id, 'dbo.Inv_BillDetail', int(df.iloc[5000*i:5000*(i+1)]['OldBillDetailID'].max()))
            log.info(f'Batch {i+1} inserted')
            i+=1
        log.info(f'dbo.Inv_BillDetail loaded successfully')
    except Exception as e:
        log.error(f'Failed to load dbo.Inv_BillDetail: {e}')
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
    print(df)

    if if_load:
        load(df, user_id, target)

# if __name__ == '__main__':
#     main()