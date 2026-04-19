import os
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, Engine, NVARCHAR
from urllib.parse import quote_plus
import pandas as pd
from utils.tools import get_last_ingested, get_logger, update_last_ingested
from utils.fks_mapper import get_items
from utils.custom_err import IncrementalDependencyError

warnings.filterwarnings('ignore')
load_dotenv()
log = get_logger('QuotationLineItems')

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

    last_id = get_last_ingested(user_id, 'dbo.CompanyQuotationDetail')

    query = f"SELECT * FROM dbo.CompanyQuotationDetail WHERE CompanyQuotationID IN (SELECT CompanyQuotationID FROM dbo.CompanyQuotation WHERE UserID={user_id}) AND CQuotationDetailID > {last_id}"
    df = pd.read_sql_query(query, engine)
    log.info(f'Extracted {len(df)} rows from dbo.CompanyQuotationDetail')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame, engine: Engine) -> pd.DataFrame:
    """Clean and transform Users data."""

    # Keep only necessary columns and rename
    df.rename(columns={
        "CompanyQuotationID": "OldQuotationID",
        "ItemID": "OldItemID",
        "Discount":"DiscountAmount",
        "TaxRate": "TaxPercent",
        "Total": "GrandTotal",
        "Price": "Subtotal"
        }, inplace=True)


    df = pd.merge(df, pd.read_sql("SELECT QuotationID, Discount, CreatedAt, UpdatedAt, Notes, OldQuotationID FROM app.Quotations WHERE OldQuotationID IS NOT NULL", engine), on='OldQuotationID', how='left')
    df = df[~df['QuotationID'].isna()]
    missing_cq = df['QuotationID'].isna().sum()
    if missing_cq:
        log.warning(f'Missing QuotationIDs: {missing_cq}')
        raise IncrementalDependencyError('Update Quotations Table.')
    

    df = pd.merge(df, get_items(engine), on='OldItemID', how='left')
    missing_items = df['ItemID'].isna().sum()
    if missing_items:
        log.warning(f'Missing ItemIDs: {missing_items}')
        raise IncrementalDependencyError('Update Items Table.')

    df['IsInclusiveVAT'] = df['TaxPercent'].map(lambda x: 1 if x > 0 else 0)
    df['DiscountPercent'] = df[['DiscountAmount', 'Price']].apply(lambda row: 0 if row['Price'] == 0 else 100 * (row['DiscountAmount'] / (row['Price'] + row['DiscountAmount'])), axis=1)
    df['OrderDiscountAllocation'] =  df[['DiscountAmount', 'Discount']].apply(lambda row: 0 if row['Discount'] == 0 else 100 * (row['DiscountAmount'] / row['Discount']), axis=1)
    df['CreatedByUserId'] = 1

    df.drop(columns=[
        'OldItemID', 'OldQuotationID', 'ItemName', 'ItemNameArabic', 'Discount'
    ], inplace=True)

    log.info(f'Transformation complete, row={len(df)}.')

    return df

# -------------------- Load --------------------
def load(df: pd.DataFrame, user_id: int, engine: Engine):

    dtype_mapping = {col:NVARCHAR(None) for col in df.select_dtypes(include='object').columns}    
    
    max_id = int(df['CQuotationDetailID'].max())
    df.drop(columns='CQuotationDetailID', inplace=True)

    try:
        i = 0
        while i < len(df)/5000:
            df.iloc[5000*i:5000*(i+1)].to_sql('QuotationLineItems', con=engine, schema='app', if_exists='append', index=False, dtype=dtype_mapping) # type: ignore
            update_last_ingested(user_id, 'dbo.CompanyQuotationDetail', max_id)
            log.info(f'Batch {i+1} inserted')
            i+=1
        log.info(f'dbo.CompanyQuotationDetail loaded successfully')

    except Exception as e:
        log.error(f'Failed to load dbo.CompanyQuotationDetail: {e}')
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
