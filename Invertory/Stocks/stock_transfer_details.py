import os
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR, DECIMAL
from urllib.parse import quote_plus
import pandas as pd
from utils.tools import get_logger
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
def extract(source_db: Engine, target_db: Engine) -> pd.DataFrame:
    """Extract data based on CDC."""
    with target_db.begin() as conn:
        max_id = conn.execute(
            text("SELECT ISNULL(MaxIndex,0) FROM app.ETLcdc WHERE TableName=:table_name"),
            {"table_name": 'dbo.inv_StockIssueDetail'}
        ).scalar()
    max_id = max_id if not max_id is None else 0
    log.info(f'Current CDC for dbo.inv_StockIssueDetail: {max_id}')

    query = f"SELECT top 1000 * FROM dbo.inv_StockIssueDetail WHERE StockIssueDetailID > {max_id} ORDER BY StockIssueDetailID"
    df = pd.read_sql_query(query, source_db)
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
def load(df: pd.DataFrame, engine: Engine):

    dtype_mapping = {col:NVARCHAR(None) for col in df.select_dtypes(include='object').columns}    
    
    max_id = df['OldStockIssueDetailID'].max()

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
            log.info(f'dbo.inv_StockIssueDetail loaded successfully')

            conn.execute(
                text("""
                    MERGE app.[ETLcdc] AS target
                    USING (SELECT :table_name AS [TableName], :max_index AS [MaxIndex]) AS source
                    ON target.[TableName] = source.[TableName]
                    WHEN MATCHED THEN UPDATE SET target.[MaxIndex] = source.[MaxIndex]
                    WHEN NOT MATCHED THEN INSERT ([TableName],[MaxIndex]) VALUES (source.[TableName],source.[MaxIndex]);
                """),
                {"table_name": f'dbo.inv_StockIssueDetail', "max_index": int(max_id)}
            )
            log.info(f'dbo.inv_StockIssueDetail loaded successfully, CDC updated to {max_id}')
    except Exception as e:
        log.error(f'Failed to load dbo.inv_StockIssueDetail: {e}')
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
