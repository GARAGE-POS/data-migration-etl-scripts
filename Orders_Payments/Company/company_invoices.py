import os
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR, DECIMAL
from urllib.parse import quote_plus
import pandas as pd
from utils.custom_err import IncrementalDependencyError
from utils.tools import fill_useraccounts, get_last_ingested, get_logger,  clean_contact, update_last_ingested
from utils.fks_mapper import get_cities, get_accounts, get_company_clients, get_custom, get_discounts, get_locations, get_users


warnings.filterwarnings('ignore')
load_dotenv()
log = get_logger('CompanyInvoices')

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

    last_id = get_last_ingested(user_id, 'dbo.CompanyInvoice')

    query = f'''SELECT 
                    ci.CompanyInvoiceID,
                    cc.CompanyClientID,
                    ci.InvoiceDate, 
                    ci.TotalDiscount, 
                    ci.TotalAmount, 
                    ci.TotalVAT, 
                    ci.GrandTotal, 
                    ci.StatusID, 
                    ci.LastUpdatedDate,
                    (SELECT MIN(LocationID) FROM dbo.Locations WHERE UserID={user_id}) LocationID 
                FROM dbo.CompanyInvoice ci
                JOIN dbo.CompanyClients cc
                ON ci.UserID = cc.UserID AND ci.BuyerName = cc.Name
                WHERE ci.UserID={user_id} AND ci.CompanyInvoiceID > {last_id} AND cc.StatusID = 1
                ORDER BY CompanyInvoiceID
            '''
    df = pd.read_sql_query(query, engine)
    log.info(f'Extracted {len(df)} rows from dbo.CompanyInvoice')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame, engine: Engine) -> pd.DataFrame:
    """Clean and transform CompanyInvoice data."""

    df.rename(columns={
        "CompanyClientID":'OldCompanyClientID',
        "LocationID":'OldLocationID',
        "TotalDiscount":'OrderDiscountTotal',
        "TotalVAT":'ItemTaxTotal',
        "TotalAmount":'Subtotal',
        "StatusID":'LastServiceStatusID',
        "InvoiceDate":'CreatedAt',
        'LastUpdatedDate':'UpdatedAt'
        }, inplace=True)
    


    df['UpdatedAt'] = df['UpdatedAt'].fillna(datetime.now())
    df['CreatedAt'] = df['CreatedAt'].bfill()

    df['ItemDiscountTotal'] = df['OrderDiscountTotal']
    df['ItemTaxTotal'] = df['ItemTaxTotal'].fillna(0)
    df['OrderDiscountPercent'] = (df['OrderDiscountTotal'] / df['OrderDiscountTotal']) * 100
    df['OrderDiscountPercent'] = df['OrderDiscountPercent'].fillna(0)
    df['AmountPaidTotal'] = df['GrandTotal']
    df['AmountDueTotal'] = 0
    df['OrderNo'] = 0
    df['TransactionNo'] = 0
    # df['LastServiceStatusID'] = df['LastServiceStatusID'].map(lambda x: {103:105, 105:100}.get(x, x)) # type: ignore
    # df['LastOrderPaymentStatusID'] = df['LastOrderPaymentStatusID'].map({103:303,105:308,106:306,108:307})
    df['LastOrderPaymentStatusID'] = df['LastServiceStatusID']
    

    df = pd.merge(df, get_locations(engine), on='OldLocationID', how='left')
    missing_loc = df['LocationID'].isna().sum()
    if missing_loc:
        log.warning(f'Missing LocationIDs: {missing_loc}')
        raise IncrementalDependencyError("Update Locations Table.")
    
    df = pd.merge(df, get_company_clients(engine), on='OldCompanyClientID', how='left')
    missing_cc = df['CompanyClientID'].isna().sum()
    if missing_cc:
        log.warning(f'Missing CompanyClientIDs: {missing_cc}')
        raise IncrementalDependencyError("Update CompanyClients Table.")
    
    df.drop(columns=['OldCompanyClientID', 'OldLocationID'], inplace=True)

    log.info(f'Transformation complete. df rows: {len(df)}')
    return df

# -------------------- Load --------------------
def load(df: pd.DataFrame, user_id: int, engine: Engine):

    dtype_mapping = {col:NVARCHAR(None) for col in df.select_dtypes(include='object').columns}

    last_id = int(df['CompanyInvoiceID'].max())
    df.drop(columns=['CompanyInvoiceID'], inplace=True)

    try:
        with engine.begin() as conn:  # Transaction-safe

            df.to_sql('Orders', con=conn, schema='app', if_exists='append', index=False, dtype=dtype_mapping) # type: ignore
            update_last_ingested(user_id, 'dbo.CompanyInvoice', last_id)
            log.info(f'dbo.CompanyInvoice loaded successfully')

    except Exception as e:
        log.error(f'Failed to load dbo.Users: {e}')
        raise

# -------------------- Main --------------------
def main(user_id: int, if_load:bool=True):
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
