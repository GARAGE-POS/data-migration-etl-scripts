import os
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR, DECIMAL
from urllib.parse import quote_plus
import pandas as pd
from utils.tools import get_last_ingested, get_logger, update_last_ingested
from utils.fks_mapper import get_stock_transfers, get_items
from utils.custom_err import IncrementalDependencyError

warnings.filterwarnings('ignore')
load_dotenv()
log = get_logger('StockTransferDetails')

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

    last_id = get_last_ingested(user_id, 'dbo.inv_StockIssueDetail')

    query = f"SELECT * FROM dbo.inv_StockIssueDetail WHERE StockIssueID IN (SELECT StockIssueID FROM dbo.inv_StockIssue WHERE UserID={user_id}) AND StockIssueDetailID > {last_id} ORDER BY StockIssueDetailID"
    df = pd.read_sql_query(query, engine)
    log.info(f'Extracted {len(df)} rows from dbo.inv_StockIssueDetail')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame, engine: Engine) -> pd.DataFrame:
    """Clean and transform Users data."""
    # Keep only necessary columns and rename

    df.rename(columns={
        "StockIssueDetailID":'OldStockIssueDetailID',
        "ItemID": "OldItemID",
        "StockIssueID": "OldStockIssueID",
        "IssueQty": "TransferredQuantity",
        "RequestQty": "RequestedQuantity",
        "ReceiveQty": "ReceivedQuantity",
        "CreateOn": "CreatedAt",
        'LastUpdatedDate':'UpdatedAt'
        }, inplace=True)

    # Clean strings: strip & lowercase
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) and x.strip()!='' else None)
        
    df['StatusID'] = df['StatusID'].fillna(1)
    df['UpdatedAt'] = df['UpdatedAt'].fillna(datetime.now())
    df.loc[df['CreatedAt'].isna(), 'CreatedAt'] = df['UpdatedAt']


    df = pd.merge(df, get_items(engine, df['OldItemID']), on='OldItemID', how='left')
    missing_items = df['ItemID'].isna().sum()
    if missing_items:
        log.warning(f'Missing ItemIDs: {missing_items}')
        raise IncrementalDependencyError('Update Items Table.')   
        
    df = pd.merge(df, get_stock_transfers(engine), on='OldStockIssueID', how='left')
    missing_stock_transforms = df['StockTransferID'].isna().sum()
    if missing_stock_transforms:
        log.warning(f'Missing StockTransferIDs: {missing_stock_transforms}')
        raise IncrementalDependencyError('Update StockTransfers Table.')  


    df.drop(columns=[
        'OldItemID', 'OldStockIssueID', 'CreatedBy', 'LastUpdatedBy', 'Notes'
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
                    WHERE Name = 'OldStockIssueDetailID'
                    AND Object_ID = Object_ID('app.StockTransferDetails')
                )
                BEGIN
                    ALTER TABLE app.StockTransferDetails
                    ADD OldStockIssueDetailID BIGINT NULL;
                END
            """))
            log.info("Verified/Added OldStockIssueDetailID column.")

            df.to_sql('StockTransferDetails', con=conn, schema='app', if_exists='append', index=False, dtype=dtype_mapping) # type: ignore
            update_last_ingested(user_id, 'dbo.inv_StockIssueDetail', int(df['OldStockIssueDetailID'].max()))
            log.info(f'dbo.inv_StockIssueDetail loaded successfully')

    except Exception as e:
        log.error(f'Failed to load dbo.inv_StockIssueDetail: {e}')
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