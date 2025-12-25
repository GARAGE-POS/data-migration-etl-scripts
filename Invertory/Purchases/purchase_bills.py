import os
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR, DECIMAL
from urllib.parse import quote_plus
import pandas as pd
from utils.tools import get_logger
from utils.fks_mapper import get_custom, get_suppliers, get_warehouses
from utils.custom_err import IncrementalDependencyError

warnings.filterwarnings('ignore')
load_dotenv()
log = get_logger('PurchaseBills')

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
            text("SELECT ISNULL(MaxIndex,0) FROM app.ETLcdc WHERE TableName=:table_name"),
            {"table_name": 'dbo.inv_Bill'}
        ).scalar()
    max_id = max_id if not max_id is None else 0
    log.info(f'Current CDC for dbo.inv_Bill: {max_id}')

    query = f"SELECT top 1000 * FROM dbo.inv_Bill WHERE BillID > {max_id} ORDER BY BillID"
    df = pd.read_sql_query(query, source_db)
    log.info(f'Extracted {len(df)} rows from dbo.inv_Bill')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame, engine: Engine) -> pd.DataFrame:
    """Clean and transform Users data."""
    # Keep only necessary columns and rename

    df.rename(columns={
        "BillID":'OldBillID',
        "PurchaseOrderID":'OldPurchaseOrderID',
        "BillNo": "BillNumber",
        "DueDate": "DeliveryDate",
        "Tax": "TaxAmount",
        "ImagePath":"Attachments",
        "StoreID": "OldStoreID",
        "SupplierID": "OldSupplierID",
        "Remarks": "Notes",
        "CreateOn": "CreatedAt",
        'LastUpdatedDate':'UpdatedAt'
        }, inplace=True)

    # Clean strings: strip & lowercase
    for col in df.select_dtypes(include='object').columns:
        if col not in ['BillNumber', 'Attachments']:
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) and x.strip()!='' else None)
        else:
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) else x)

    df['StatusID'] = df['StatusID'].fillna(1)
    df['UpdatedAt'] = df['UpdatedAt'].fillna(datetime.now())
    df.loc[df['CreatedAt'].isna(), 'CreatedAt'] = df['UpdatedAt']

    # Temporary Filling
    df['OldPurchaseOrderID'] = df['OldPurchaseOrderID'].fillna(4)
    df['OldSupplierID'] = df['OldSupplierID'].fillna(4)
    df['AuditedByUserID'] = 0
    df['Attachments'] = df['Attachments'].fillna('')

    df = pd.merge(df, get_custom(engine, ['PurchaseOrderID', 'ReferenceNumber', 'AccountPaymentModeID', 'TermsAndConditions', 'PaymentTerms', 'OldPurchaseOrderID'], 'app.PurchaseOrders'), on='OldPurchaseOrderID', how='left')
    missing_purchase_orders = df['PurchaseOrderID'].isna().sum()
    if missing_purchase_orders:
        log.warning(f'Missing PurchaseOrderIDs: {missing_purchase_orders}')
        raise IncrementalDependencyError('Update PurchaseOrders Table.')  
    
    df = pd.merge(df, get_suppliers(engine), on='OldSupplierID', how='left')
    missing_supps = df['SupplierID'].isna().sum()
    if missing_supps:
        log.warning(f'Missing SupplierIDs: {missing_supps}')
        raise IncrementalDependencyError('Update Suppliers Table.')
    
    df = pd.merge(df, get_warehouses(engine), on='OldStoreID', how='left')
    missing_whs = df['WarehouseID'].isna().sum()
    if missing_whs:
        log.warning(f'Missing WarehouseIDs: {missing_whs}')
        raise IncrementalDependencyError('Update Warehouses Table.')
    
    # // TO DO: AuditedByUserID



    df.drop(columns=[
        'OldPurchaseOrderID', 'OldSupplierID', 'OldStoreID', 'CreatedBy', 'LastUpdatedBy', 'Date', 'LocationID', 'PaymentStatus'
    ], inplace=True)

    log.info('Transformation complete')

    return df

# -------------------- Load --------------------
def load(df: pd.DataFrame, engine: Engine):

    dtype_mapping = {col:NVARCHAR(None) for col in df.select_dtypes(include='object').columns}    
    
    max_id = df['OldBillID'].max()

    try:
        with engine.begin() as conn:  # Transaction-safe

            conn.execute(text("""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.columns
                    WHERE Name = 'OldBillID'
                    AND Object_ID = Object_ID('app.PurchaseBills')
                )
                BEGIN
                    ALTER TABLE app.PurchaseBills
                    ADD OldBillID BIGINT NULL;
                END
            """))
            log.info("Verified/Added OldBillID column.")

            df.to_sql('PurchaseBills', con=conn, schema='app', if_exists='append', index=False, dtype=dtype_mapping) # type: ignore
            log.info(f'dbo.inv_Bill loaded successfully')

            conn.execute(
                text("""
                    MERGE app.[ETLcdc] AS target
                    USING (SELECT :table_name AS [TableName], :max_index AS [MaxIndex]) AS source
                    ON target.[TableName] = source.[TableName]
                    WHEN MATCHED THEN UPDATE SET target.[MaxIndex] = source.[MaxIndex]
                    WHEN NOT MATCHED THEN INSERT ([TableName],[MaxIndex]) VALUES (source.[TableName],source.[MaxIndex]);
                """),
                {"table_name": f'dbo.inv_Bill', "max_index": int(max_id)}
            )
            log.info(f'dbo.inv_Bill loaded successfully, CDC updated to {max_id}')
    except Exception as e:
        log.error(f'Failed to load dbo.inv_Bill: {e}')
        raise

# -------------------- Main --------------------
def main():
    source = source_db_conn()
    target = target_db_conn()
    # return
    while True:
        df = extract(source, target)
        if df.empty:
            log.info('No new data to load.')
            return
        
        # return
        df = transform(df, target)
        # print(df)
        # return
        load(df, target)

if __name__ == '__main__':
    main()
