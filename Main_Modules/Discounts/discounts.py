import os
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR, DECIMAL
from urllib.parse import quote_plus
import pandas as pd
from utils.tools import fill_useraccounts, get_last_ingested, get_logger,  clean_contact, update_last_ingested
from utils.fks_mapper import get_cities, get_accounts, get_custom, get_users


warnings.filterwarnings('ignore')
load_dotenv()
log = get_logger('Discounts')

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

    last_id = get_last_ingested(user_id, 'dbo.Discount')

    query = f"SELECT * FROM dbo.Discount WHERE LocationID IN (SELECT LocationID FROM dbo.Locations WHERE UserID={user_id}) AND DiscountID > {last_id} ORDER BY DiscountID"
    df = pd.read_sql_query(query, engine)
    log.info(f'Extracted {len(df)} rows from dbo.Discount')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame, engine: Engine) -> pd.DataFrame:
    """Clean and transform Discount data."""

    # Keep only necessary columns and rename
    df = df[['DiscountID', 'Name', 'DiscountType', 'Value', 'FromDate', 'ToDate', 'FromTime', 'ToTime', 'LocationID', 'LastUpdatedDate', 'StatusID', 'Code', 'NoOfRedemption']]
    
    df.rename(columns={
        "DiscountID":'OldDiscountID',
        "LocationID":'OldLocationID',
        'LastUpdatedDate':'UpdatedAt',
        'Code':'DiscountCode',
        'NoOfRedemption':'MaximumRedemptionCount'
        }, inplace=True)

    # Clean strings: strip & lowercase
    for col in df.select_dtypes(include='object').columns:
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) and x.strip()!='' else None)
            

    
    df['DiscountMethod'] = 0
    df['RequirementType'] = 0
    df['DiscountValueType'] = 0
    df['LimitOnePerCustomer'] = 0
    

    df['DiscountType'] = df['DiscountType'].map({'Amount':1, 'Percent':2})
    df.loc[df['DiscountType']==1, 'FixedAmountValue'] = df['Value']
    df.loc[df['DiscountType']==2, 'PercentageValue'] = df['Value']
    df['DiscountCode'] = df['OldDiscountID'].map(lambda x: f'DUMMYCODE-{x}')

    df['FromTime'] = df['FromTime'].fillna('00:00:00')
    df['ToTime'] = df['ToTime'].fillna('00:00:00')

    df['FromDate'] = pd.to_datetime(df['FromDate'])
    df['ToDate'] = pd.to_datetime(df['ToDate'])
    df['UpdatedAt'] = pd.to_datetime(df['UpdatedAt'])
    df['FromTime'] = pd.to_timedelta(df['FromTime'].astype(str))
    df['ToTime'] = pd.to_timedelta(df['ToTime'].astype(str))

    df['CreatedAt'] = df[['UpdatedAt', 'FromDate']].min(axis=1)


    df['StartsAt'] = df['FromDate'] + df['FromTime']
    df['EndsAt'] = df['ToDate'] + df['ToTime']

    print(df[['StartsAt','EndsAt']])


    # FOREIN KEYS MAPPING
    df = pd.merge(df, get_custom(engine, ['OldLocationID', 'AccountID'], 'app.Locations'), on='OldLocationID', how='left')

    df.drop(columns={'OldLocationID', 'Value', 'FromTime', 'ToTime', 'FromDate', 'ToDate'}, inplace=True)


    log.info(f'Transformation complete. df rows: {len(df)}')
    return df

# -------------------- Load --------------------
def load(df: pd.DataFrame, user_id: int, engine: Engine):

    dtype_mapping = {col:NVARCHAR(None) for col in df.select_dtypes(include='object').columns}

    try:
        with engine.begin() as conn:  # Transaction-safe

            conn.execute(text("""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.columns
                    WHERE Name = 'OldDiscountID'
                    AND Object_ID = Object_ID('app.Discounts')
                )
                BEGIN
                    ALTER TABLE app.Discounts
                    ADD OldDiscountID BIGINT NULL;
                END
            """))
            log.info("Verified/Added OldDiscountID column.")

            df.to_sql('Discounts', con=conn, schema='app', if_exists='append', index=False, dtype=dtype_mapping) # type: ignore
            log.info(f'dbo.Discount loaded successfully')

            update_last_ingested(user_id, 'dbo.Discount', int(df['OldDiscountID'].max()))

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
