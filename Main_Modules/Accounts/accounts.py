import os
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR, DECIMAL
from urllib.parse import quote_plus
import pandas as pd
from utils.tools import get_logger, clean_contact

warnings.filterwarnings('ignore')
load_dotenv()
log = get_logger('Accounts')

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
            {"table_name": 'dbo.Users'}
        ).scalar()
    max_id = max_id if not max_id is None else 0
    log.info(f'Current CDC for dbo.Users: {max_id}')

    query = f"SELECT top 100 * FROM dbo.Users WHERE UserID > {max_id} ORDER BY UserID"
    df = pd.read_sql_query(query, source_db)
    log.info(f'Extracted {len(df)} rows from dbo.Users')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and transform Users data."""
    # Keep only necessary columns and rename

    df = df[["UserID","FirstName","LastName","ImagePath","Company","BusinessType","Email","ContactNo","LastUpdatedDate","StatusID","CompanyCode","CreatedDate","VATNO","BrandThumbnailImage"]]

    df.rename(columns={
        "UserID":'OldUserID',
        "FirstName": "RepresentativeFirstName",
        "LastName": "RepresentativeLastName",
        "ImagePath": "ImagePath",
        "Company": "CompanyName",
        "BusinessType":"PrimaryBusiness",
        "Email": "CompanyEmail",
        "ContactNo": "RepresentativeContactNo",
        "CreatedDate": "CreatedAt",
        "VATNO": "VATNo",
        "BrandThumbnailImage": "BrandThumbnailImage",
        'LastUpdatedDate':'UpdatedAt'
        }, inplace=True)

    # Clean strings: strip & lowercase
    for col in df.select_dtypes(include='object').columns:
        if col != 'CompanyName':
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) and x.strip()!='' else None)
        else:
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) else x)

    df['RepresentativeContactNo'] = df['RepresentativeContactNo'].apply(clean_contact)
    df['CompanyName'] = df['CompanyName'].fillna('')
    df['UpdatedAt'] = df['UpdatedAt'].fillna(datetime.now())
    df.loc[df['CreatedAt'].isna(), 'CreatedAt'] = df['UpdatedAt']

    df['CRNo'] = ''    
    df['CompanyCode'] = df['CompanyCode'].fillna('')
    df['VATNo'] = pd.to_numeric(df['VATNo'], errors='coerce')

    log.info('Transformation complete')
    return df

# -------------------- Load --------------------
def load(df: pd.DataFrame, engine: Engine):

    dtype_mapping = {
            'CompanyName': NVARCHAR(None),             # NVARCHAR(MAX)
            'RepresentativeFirstName': NVARCHAR(None),
            'RepresentativeLastName': NVARCHAR(None),
            'RepresentativeContactNo': NVARCHAR(None),
            'CompanyContactNo': NVARCHAR(None),
            'CompanyNameAr': NVARCHAR(None),
            'CRNo': NVARCHAR(None),
            'CompanyEmail': NVARCHAR(None),
            'PrimaryBusiness': NVARCHAR(None),
            'CompanyCode': NVARCHAR(None),
            'ExternalID': NVARCHAR(None),
            'SocialMediaJson': NVARCHAR(None),
            'BrandThumbnailImage': NVARCHAR(None),
            'ImagePath': NVARCHAR(None),
            }
    
    max_id = df['OldUserID'].max()

    try:
        with engine.begin() as conn:  # Transaction-safe

            conn.execute(text("""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.columns
                    WHERE Name = 'OldUserID'
                    AND Object_ID = Object_ID('app.Accounts')
                )
                BEGIN
                    ALTER TABLE app.Accounts
                    ADD OldUserID BIGINT NULL;
                END
            """))
            log.info("Verified/Added OldUserID column.")

            df.to_sql('Accounts', con=conn, schema='app', if_exists='append', index=False, dtype=dtype_mapping) # type: ignore
            log.info(f'dbo.Users loaded successfully')

            conn.execute(
                text("""
                    MERGE app.[ETLcdc] AS target
                    USING (SELECT :table_name AS [TableName], :max_index AS [MaxIndex]) AS source
                    ON target.[TableName] = source.[TableName]
                    WHEN MATCHED THEN UPDATE SET target.[MaxIndex] = source.[MaxIndex]
                    WHEN NOT MATCHED THEN INSERT ([TableName],[MaxIndex]) VALUES (source.[TableName],source.[MaxIndex]);
                """),
                {"table_name": f'dbo.Users', "max_index": int(max_id)}
            )
            log.info(f'dbo.Users loaded successfully, CDC updated to {max_id}')
    except Exception as e:
        log.error(f'Failed to load dbo.Users: {e}')
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
        df = transform(df)
        # print(df)
        # return
        load(df, target)

if __name__ == '__main__':
    main()
