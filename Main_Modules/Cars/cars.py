import os
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR, DECIMAL
from urllib.parse import quote_plus
import pandas as pd
from utils.fks_mapper import get_customers, get_custom
from utils.tools import get_logger

log = get_logger('Cars')
warnings.filterwarnings('ignore')
load_dotenv()


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
            text("SELECT ISNULL(MaxIndex,0) FROM app.EtlCDC WHERE TableName=:table_name"),
            {"table_name": 'dbo.Cars'}
        ).scalar()
    
    max_id = max_id if not max_id is None else 0
    log.info(f'Current CDC for dbo.Cars: {max_id}')

    # query = f"SELECT * FROM dbo.Cars WHERE CarID BETWEEN 1556 AND 23454 ORDER BY CarID"
    query = f"SELECT TOP 10000 * FROM dbo.Cars WHERE CarID > {max_id} ORDER BY CarID"
    df = pd.read_sql_query(query, source_db)
    log.info(f'Extracted {len(df)} rows from dbo.Cars')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame, source_db: Engine, target_db: Engine) -> pd.DataFrame:
    """Clean and transform Cars data."""
    # Keep only necessary columns and rename
    df = df[['CarID', 'CustomerID' , 'MakeID', 'ModelID', 'Year', 'Color', 'VinNo', 'Description', 'RegistrationNo','RecommendedAmount', 'ImagePath', 'CarType', 'StatusID','CreatedOn', 'LastUpdatedDate']]

    df.rename(columns={
        "CarID":'OldCarID',
        "CustomerID":'OldSubUserID',
        'ModelID':'OldModelID',
        'LastUpdatedDate':'UpdatedAt',
        'CreatedOn':'CreatedAt',
        'RecommendedAmount':'Odometer', /Wrong
        'MakeID':'OldMakeID'
        }, inplace=True)

    # Clean strings: strip & lowercase
    for col in df.select_dtypes(include='object').columns:
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) and x.strip()!='' else None)
            df[col] = df[col].apply(lambda x: x if isinstance(x,str) and x != 'NULL' else None)

    # Filling Null Values in StatusID, CarType and CarPlateType
    df['StatusID'] = df['StatusID'].fillna(1)
    df['CarType'] = df['CarType'].fillna(0)
    df['CarPlateType'] = 0

    # Fixing Odometer 
    df['Odometer'] = pd.to_numeric(df['Odometer'],errors='coerce')


    # Sync CustomerID and ModelID
    current_cust_ids = pd.read_sql("SELECT Id, OldSubUserID FROM app.AspNetUsers WHERE UserType='Customer' AND OldSubUserID IS NOT NULL", target_db)
    current_model_ids = pd.read_sql('SELECT MakeID, ModelID, OldModelID FROM app.Models WHERE OldModelID IS NOT NULL', target_db)

    df = pd.merge(df, get_customers(target_db), on='OldSubUserID', how='left')
    df = pd.merge(df, get_custom(target_db, ['MakeID', 'ModelID', 'OldModelID'], 'app.Models'), on='OldModelID', how='left')


    df.rename(columns={'Id':'CustomerID'},inplace=True)

    df.drop(columns={'OldSubUserID', 'OldMakeID', 'OldModelID'}, inplace=True)


    missing_cust = df['CustomerID'].isna().sum()
    if missing_cust:
        log.warning(f'Missing CustomerIDs: {missing_cust}. Update Customers in AspNetUsers Table.')
        raise

    # Fixing Date columns
    df.set_index('OldCarID', drop=False, inplace=True)
    df.index.name = None

    mask = df['UpdatedAt'].isna()
    log.info(f'Missing UpdatedAt is {mask.sum()}')

    df['CreatedAt'] = df['UpdatedAt']
    if int(mask.sum()) > 0:
        car_ids = tuple(df[df['UpdatedAt'].isna()]['OldCarID'].values.tolist()) + (0,0)
        ids = str(car_ids)
        dates = pd.read_sql(f"SELECT CarID, LastUpdatedDate, CreatedOn FROM dbo.CarsLocation_Junc WHERE CarID IN {ids} ORDER BY CarID, CreatedOn", source_db)

        if len(dates):
            dates.set_index('CarID', drop=False, inplace=True)
            dates.index.name = None
            dates = dates.drop_duplicates(subset='CarID', keep='first')
            dates.loc[dates['CreatedOn'].isna(), 'CreatedOn'] = dates['LastUpdatedDate']
            df.loc[df['UpdatedAt'].isna(), 'CreatedAt'] = dates['CreatedOn']

    df['UpdatedAt'] = df['UpdatedAt'].fillna(datetime.now())
    df['CreatedAt'] = df['CreatedAt'].fillna(datetime(2000, 1,1,0,0,0))

    def parse_date(s):
        formats = [
            '%b %d %Y %I:%M%p',        # May 29 2020 8:39AM
            '%m/%d/%Y %I:%M:%S %p',    # 3/3/2025 1:28:20 PM
        ]
        for fmt in formats:
            try:
                return pd.to_datetime(s, format=fmt)
            except (ValueError, TypeError):
                continue
        return pd.NaT
    for col in ['CreatedAt', 'UpdatedAt']:
        df[col] = df[col].apply(parse_date) # type: ignore

    # print(df[['CreatedAt', 'UpdatedAt']].head(20))
    missing_date = df[(df['CreatedAt'].isna()) | (df['UpdatedAt'].isna())]
    log.info(f"Missing dates: {len(missing_date)}")
    if len(missing_date):
        log.warning('Missing dates from bad parsing.')
        raise

    # if int(mask.sum()) > 0:
    #     print(df[mask][['OldCarID','UpdatedAt', 'CreatedAt']])

    log.info(f'Transformation complete, df\'s Length is {len(df)}')
    return df

# -------------------- Load --------------------
def load(df: pd.DataFrame, engine: Engine):

    dtype_mapping = {col:NVARCHAR(None) for col in df.select_dtypes(include='object').columns}
    
    max_id = df['OldCarID'].max()

    try:
        with engine.begin() as conn:  # Transaction-safe

            conn.execute(text("""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.columns
                    WHERE Name = 'OldCarID'
                    AND Object_ID = Object_ID('app.Cars')
                )
                BEGIN
                    ALTER TABLE app.Cars
                    ADD OldCarID BIGINT NULL;
                END
            """))
            log.info("Verified/Added OldCarID column.")

            df.to_sql('Cars', con=conn, schema='app', if_exists='append', index=False, dtype=dtype_mapping) # type: ignore
            log.info(f'dbo.Cars loaded successfully')

            conn.execute(
                text("""
                    MERGE app.[EtlCDC] AS target
                    USING (SELECT :table_name AS [TableName], :max_index AS [MaxIndex]) AS source
                    ON target.[TableName] = source.[TableName]
                    WHEN MATCHED THEN UPDATE SET target.[MaxIndex] = source.[MaxIndex]
                    WHEN NOT MATCHED THEN INSERT ([TableName],[MaxIndex]) VALUES (source.[TableName],source.[MaxIndex]);
                """),
                {"table_name": f'dbo.Cars', "max_index": int(max_id)}
            )
            log.info(f'dbo.Cars loaded successfully, CDC updated to {max_id}')
    except Exception as e:
        log.error(f'Failed to load dbo.Cars: {e}')
        raise

# -------------------- Main --------------------
def main():
    source = source_db_conn()
    target = target_db_conn()

    while True:
        df = extract(source, target)
        if df.empty:
            log.info('No new data to load.')
            break
        df = transform(df, source, target)
        load(df, target)
        return

if __name__ == '__main__':
    main()
