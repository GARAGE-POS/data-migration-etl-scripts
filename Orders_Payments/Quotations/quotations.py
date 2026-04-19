import os
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR, DECIMAL
from urllib.parse import quote_plus
import pandas as pd
from utils.tools import get_last_ingested, get_logger, update_last_ingested
from utils.fks_mapper import get_company_clients, get_custom, get_items, get_suppliers, get_warehouses
from utils.custom_err import IncrementalDependencyError

warnings.filterwarnings('ignore')
load_dotenv()
log = get_logger('Quotations')

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

    last_id = get_last_ingested(user_id, 'dbo.CompanyQuotation')

    query = f"""
        SELECT
            cq.CompanyQuotationID,
            cq.QuotationNo,
            cc.CompanyClientID,
            cq.SupplyDate,
            cq.BuyerAddress,
            cq.BuyerVAT,
            cq.TotalDiscount,
            cq.TotalVAT,
            cq.TotalAmount,
            cq.GrandTotal,
            cq.Notes,
            cq.StatusID,
            cq.LastUpdatedDate 
        FROM CompanyQuotation cq
        JOIN CompanyClients cc ON cc.Name = cq.BuyerName AND cc.UserID = cq.UserID
        WHERE cc.UserID={user_id} AND cq.CompanyQuotationID > {last_id}
        ORDER BY cq.CompanyQuotationID
    """
    df = pd.read_sql_query(query, engine)
    log.info(f'Extracted {len(df)} rows from dbo.CompanyQuotation')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame, engine: Engine) -> pd.DataFrame:
    """Clean and transform Users data."""
    # Keep only necessary columns and rename

    df.rename(columns={
        "CompanyQuotationID": "OldQuotationID",
        "CompanyClientID":'OldCompanyClientID',
        "QuotationNo":"QuotationNumber",
        "SupplyDate": "Date",
        "BuyerAddress": "BillingAddress",
        "BuyerVAT": "VatNumber",
        "TotalDiscount": "Discount",
        "TotalAmount": "SubTotal",
        "TotalVAT": "TaxAmount",
        "GrandTotal": "Total",
        "LastUpdatedDate":"UpdatedAt"
        }, inplace=True)


    df['DueDate'] = df['Date']
    df['Attachments'] = '[]'
    df['Status'] = 0
    df['CreatedAt'] = df['UpdatedAt']
    df['CreatedByUserId']=1

    df = pd.merge(df, get_company_clients(engine), on='OldCompanyClientID', how='left')
    missing_cc = df['CompanyClientID'].isna().sum()
    if missing_cc:
        log.warning(f'Missing CompanyClientIDs: {missing_cc}')
        raise IncrementalDependencyError('Update CompanyClients Table.')


    df.drop(columns=[
        'OldCompanyClientID'
    ], inplace=True)

    log.info(f'Transformation complete, row={len(df)}.')

    return df

# -------------------- Load --------------------
def load(df: pd.DataFrame, user_id: int, engine: Engine):

    dtype_mapping = {col:NVARCHAR(None) for col in df.select_dtypes(include='object').columns}    
    
    try:
        with engine.begin() as conn:  # Transaction-safe

            conn.execute(text("""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.columns
                    WHERE Name = 'OldQuotationID'
                    AND Object_ID = Object_ID('app.Quotations')
                )
                BEGIN
                    ALTER TABLE app.Quotations
                    ADD OldQuotationID BIGINT NULL;
                END
            """))
            log.info("Verified/Added OldQuotationID column.")

        i = 0
        while i < len(df)/5000:
            df.iloc[5000*i:5000*(i+1)].to_sql('Quotations', con=engine, schema='app', if_exists='append', index=False, dtype=dtype_mapping) # type: ignore
            update_last_ingested(user_id, 'dbo.CompanyQuotation', int(df.iloc[5000*i:5000*(i+1)]['OldQuotationID'].max()))
            log.info(f'Batch {i+1} inserted')
            i+=1
        log.info(f'dbo.CompanyQuotation loaded successfully')

    except Exception as e:
        log.error(f'Failed to load dbo.CompanyQuotation: {e}')
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
