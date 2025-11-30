import os
import warnings
from dotenv import load_dotenv
import logging
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR, DECIMAL
from urllib.parse import quote_plus
import pandas as pd

warnings.filterwarnings('ignore')
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s:%(lineno)d | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

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
    logging.info(f'Connected to {os.getenv(db_env)} at {os.getenv(server_env)}')
    return engine

def source_db_conn(): return get_engine('AZURE_SERVER','AZURE_DATABASE','AZURE_USERNAME','AZURE_PASSWORD')
def target_db_conn(): return get_engine('STAGE_SERVER','STAGE_DATABASE','STAGE_USERNAME','STAGE_PASSWORD')

# -------------------- Extract --------------------
def extract(source_db: Engine, target_db: Engine) -> pd.DataFrame:
    """Extract new rows based on CDC."""
    with target_db.connect() as conn:
        max_id = conn.execute(
            text("SELECT ISNULL(MaxIndex,0) FROM app.EtlCDC WHERE TableName=:table_name"),
            {"table_name": 'dbo.Locations'}
        ).scalar()
    logging.info(f'Current CDC for dbo.Locations: {max_id}')

    query = f"SELECT * FROM dbo.Locations WHERE LocationID > {max_id} ORDER BY LocationID"
    df = pd.read_sql_query(query, source_db)
    logging.info(f'Extracted {len(df)} rows from dbo.Locations')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame, target_db: Engine) -> pd.DataFrame:
    """Clean and transform locations data."""
    # Keep only necessary columns and rename
    df = df[['LocationID','UserID', 'Name','LandmarkID', 'Descripiton', 'ArabicDescription','Open_Time', 'Close_Time', 'Email', 'ContactNo', 'Address','District', 'BuildingNumber','PostalCode', 'StreetName', 'ArabicName' , 'CityID','LastUpdatedDate', 'Gmaplink', 'Longitude','Latitude','IsFeatured','StatusID']]
    df = df.rename(columns={
        'LocationID':'OldLocationID',
        'CityID':'OldCityID',
        'UserID':'OldUserID',
        'ArabicName':'NameAr',
        'Descripiton':'Description',
        'ArabicDescription':'DescriptionAr',
        'Gmaplink':'GMapLink',
        'Address':'ShortAddress',
        'LastUpdatedDate':'LastUpdateDate'
    })

    # fixing column type
    df['Longitude'] = pd.to_numeric(df['Longitude'], errors='coerce')
    df['Latitude'] = pd.to_numeric(df['Latitude'], errors='coerce')
    df['Longitude'] = df['Longitude'].round(6)
    df['Latitude']  = df['Latitude'].round(6)
    df.loc[df['Longitude'].abs() > 999, 'Longitude'] = pd.NA
    df.loc[df['Latitude'].abs()  > 999, 'Latitude']  = pd.NA

    # Default values for missing columns
    df['IsHQ'] = 0
    df['IsActiveMyKarage'] = 0
    df['StatusID'] = df['StatusID'].fillna(1)
    df['IsFeatured'] = pd.to_numeric(df['IsFeatured'], errors='coerce')
    df['IsFeatured'] = df['IsFeatured'].fillna(0)


    # Dates
    now = datetime.now()
    for date_col in ['CreatedDate','LastUpdateDate']:
        if date_col not in df.columns:
            df[date_col] = now
        else:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce').fillna(now)


    # Normalize contacts
    def clean_contact(num: str):
        if pd.isna(num): return None
        num = num.replace(' ','')
        while num.startswith('0'): num = num[1:]
        if num.startswith('5'): return '+966'+num[:12]
        elif num.startswith('9'): return '+'+num[:14]
        return num[:15]
    df['ContactNo'] = df['ContactNo'].apply(clean_contact)

    # Clean strings: strip & lowercase
    for col in df.select_dtypes(include='object').columns:
        if col != 'Name': df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) and x.strip()!='' else None)
        else: df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) else x)


    # WorkingHours
    df['Open_Time'] = pd.to_datetime(df['Open_Time'], errors='coerce').dt.strftime('%H:%M')
    df['Close_Time'] = pd.to_datetime(df['Close_Time'], errors='coerce').dt.strftime('%H:%M')
    df['WorkingHours'] = df.apply(
        lambda row: f"{row['Open_Time']}-{row['Close_Time']}" if pd.notna(row['Open_Time']) and pd.notna(row['Close_Time']) else None, # type: ignore
        axis=1
    ) #type: ignore


    # CityID adjustments
    city_ids = pd.read_sql("SELECT * FROM app.SyncCities", target_db)
    df = pd.merge(df, city_ids, on='OldCityID', how='left')


    print(df[['CityID', 'OldCityID']])

    print(df[df['CityID'].isna()][['CityID', 'OldCityID']])

    # UserID adjustments
    user_ids = pd.read_sql("SELECT AccountID, OldUserID FROM app.Accounts", target_db)
    df = pd.merge(df, user_ids, on='OldUserID', how='left')

    # Dropping columns
    df.drop(columns=['Open_Time','Close_Time', 'OldUserID', 'OldCityID'], inplace=True, errors='ignore')

    logging.info('Transformation complete')
    return df

# -------------------- Load --------------------
def load(df: pd.DataFrame, engine: Engine):
    dtype_mapping = {col: NVARCHAR(None) for col in df.columns if df[col].dtype=='object'}
    dtype_mapping['Longitude'] = DECIMAL(9, 6) # type: ignore
    dtype_mapping['Latitude'] = DECIMAL(9, 6) # type: ignore

    max_id = df['OldLocationID'].max()

    try:
        with engine.begin() as conn:  # Transaction-safe
            conn.execute(text("""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.columns
                    WHERE Name = 'OldLocationID'
                    AND Object_ID = Object_ID('app.Locations')
                )
                BEGIN
                    ALTER TABLE app.Locations
                    ADD OldLocationID BIGINT NULL;
                END
            """))
            logging.info("Verified/Added OldLocationID column.")

            df.to_sql('Locations', con=conn, schema='app', if_exists='append', index=False, dtype=dtype_mapping) #type: ignore

            # Update CDC only after successful insert
            conn.execute(
                text("""
                    MERGE app.[EtlCDC] AS target
                    USING (SELECT :table_name AS [TableName], :max_index AS [MaxIndex]) AS source
                    ON target.[TableName] = source.[TableName]
                    WHEN MATCHED THEN UPDATE SET target.[MaxIndex] = source.[MaxIndex]
                    WHEN NOT MATCHED THEN INSERT ([TableName],[MaxIndex]) VALUES (source.[TableName],source.[MaxIndex]);
                """),
                {"table_name": f'dbo.Locations', "max_index": int(max_id)}
            )
        logging.info(f'dbo.Locations loaded successfully, CDC updated to {max_id}')
    except Exception as e:
        logging.error(f'Failed to load dbo.Locations: {e}')
        raise

# -------------------- Main --------------------
def main():
    source = source_db_conn()
    target = target_db_conn()
    df = extract(source, target)
    if df.empty:
        logging.info('No new data to load.')
        return
    df = transform(df, target)


    # print(df[~df['Longitude'].isna()][['Longitude','Latitude']])
    # return
    load(df, target)

if __name__ == '__main__':
    main()
