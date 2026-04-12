import os
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR, DECIMAL
from urllib.parse import quote_plus
import pandas as pd
from utils.fks_mapper import get_locations, get_custom, get_suppliers
from utils.tools import get_last_ingested, get_logger, update_last_ingested
from utils.custom_err import IncrementalDependencyError

warnings.filterwarnings('ignore')
load_dotenv()
log = get_logger('PurchaseOrders')

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

    last_id = get_last_ingested(user_id, 'dbo.inv_PurchaseOrder')


    query = f"SELECT * FROM dbo.inv_PurchaseOrder WHERE LocationID IN (SELECT LocationID FROM dbo.Locations WHERE UserID={user_id}) AND PurchaseOrderID > {last_id} ORDER BY PurchaseOrderID"
    df = pd.read_sql_query(query, engine)
    log.info(f'Extracted {len(df)} rows from dbo.inv_PurchaseOrder')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame, engine: Engine) -> pd.DataFrame:
    """Clean and transform Users data."""

    df.rename(columns={
        "PurchaseOrderID":'OldPurchaseOrderID',
        "PONo": "PONumber",
        "ReferenceNo": "ReferenceNumber",
        "Date": "PODate",
        "DeliveryDate": "ExpectedDeliveryDate",
        "SupplierID":"OldSupplierID",
        "Tax": "TaxAmount",
        "LocationID": "OldLocationID",
        "Remarks": "Notes",
        "CreateOn": "CreatedAt",
        'LastUpdatedDate':'UpdatedAt'
        
        }, inplace=True)


    # Clean strings: strip & lowercase
    for col in df.select_dtypes(include='object').columns:
        if col != 'PONumber':
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) and x.strip()!='' else None)
        else:
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) else x)

    df['StatusID'] = df['StatusID'].fillna(1)
    df['UpdatedAt'] = df['UpdatedAt'].fillna(datetime.now())
    df.loc[df['CreatedAt'].isna(), 'CreatedAt'] = df['UpdatedAt']
    df['PONumber'] = df['OldPurchaseOrderID'].map(lambda x: f'PO-{x:05d}')
    df['ReferenceNumber'] = df['OldPurchaseOrderID'].map(lambda x: f'REF-{x:05d}')


    acc_pay_modes = get_custom(engine, ['AccountID', 'AccountPaymentModeID'], 'app.AccountPaymentModes').drop_duplicates(subset='AccountID')

    df = pd.merge(df, get_custom(engine, ['AccountID', 'LocationID', 'OldLocationID'], 'app.Locations'), on='OldLocationID', how='left')
    missing_locs = df['LocationID'].isna().sum()
    if missing_locs:
        log.warning(f'Missing LocationIDs: {missing_locs}')
        raise IncrementalDependencyError('Update Locations Table.')
    
    df = pd.merge(df, acc_pay_modes, on='AccountID', how='left')
    missing_apmodes = df['AccountPaymentModeID'].isna().sum()
    if missing_apmodes:
        log.warning(f'Missing AccountPaymentModeIDs: {missing_apmodes}')
        raise IncrementalDependencyError('Update AccountPaymentModes Table.')
    
    df = pd.merge(df, get_suppliers(engine), on='OldSupplierID', how='left')
    missing_supps = df['SupplierID'].isna().sum()
    if missing_supps:
        log.warning(f'Missing SupplierIDs: {missing_supps}')
        raise IncrementalDependencyError('Update Suppliers Table.')
    

    # Keep only necessary columns and rename
    df.drop(columns=[
        'CreatedBy', 'LastUpdatedBy', 'OldLocationID', 'AccountID', 'OldSupplierID'
    ], inplace=True)


    log.info(f'Transformation complete. Output: {len(df)}')
    return df

# -------------------- Load --------------------
def load(df: pd.DataFrame, user_id: int, engine: Engine):

    dtype_mapping = {col:NVARCHAR(None) for col in df.select_dtypes(include='object').columns}

    try:
        with engine.begin() as conn:  # Transaction-safe

            conn.execute(text("""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.columns
                    WHERE Name = 'OldPurchaseOrderID'
                    AND Object_ID = Object_ID('app.PurchaseOrders')
                )
                BEGIN
                    ALTER TABLE app.PurchaseOrders
                    ADD OldPurchaseOrderID BIGINT NULL;
                END
            """))
            log.info("Verified/Added OldPurchaseOrderID column.")

            df.to_sql('PurchaseOrders', con=conn, schema='app', if_exists='append', index=False, dtype=dtype_mapping) # type: ignore
            update_last_ingested(user_id, 'dbo.inv_PurchaseOrder', int(df['OldPurchaseOrderID'].max()))
            log.info(f'dbo.inv_PurchaseOrder loaded successfully')

    except Exception as e:
        log.error(f'Failed to load dbo.inv_PurchaseOrder: {e}')
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
