import os
import warnings
import json
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR, DECIMAL
from urllib.parse import quote_plus
import pandas as pd
from utils.tools import get_logger
from utils.custom_err import IncrementalDependencyError
from utils.fks_mapper import get_accounts, get_cities, get_custom

warnings.filterwarnings('ignore')
log = get_logger('Locations')
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
    """Extract new rows based on CDC."""
    with target_db.connect() as conn:
        max_id = conn.execute(
            text("SELECT ISNULL(MaxIndex,0) FROM app.EtlCDC WHERE TableName=:table_name"),
            {"table_name": 'dbo.Locations'}
        ).scalar()
    
    max_id = max_id if not max_id is None else 0
    # max_id=0
    log.info(f'Current CDC for dbo.Locations: {max_id}')

    query = f"SELECT top 100 * FROM dbo.Locations WHERE LocationID > {max_id} ORDER BY LocationID"
    df = pd.read_sql_query(query, source_db)
    log.info(f'Extracted {len(df)} rows from dbo.Locations')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame, source_db: Engine, target_db: Engine) -> pd.DataFrame:
    """Clean and transform locations data."""
    # Keep only necessary columns and rename
    df = df[['LocationID','UserID', 'CountryID', 'Name','Descripiton', 'ArabicDescription','Email', 'ContactNo', 'Address','ArabicAddress', 'District', 'BuildingNumber','PostalCode', 'StreetName', 'ArabicName' , 'CityID', 'LandmarkID', 'LastUpdatedDate', 'Gmaplink', 'Longitude','Latitude','IsFeatured','StatusID']]
    df = df.rename(columns={
        'LocationID':'OldLocationID',
        'CityID':'OldCityID',
        'UserID':'OldUserID',
        'ArabicName':'NameAr',
        'Descripiton':'Description',
        'ArabicDescription':'DescriptionAr',
        'Gmaplink':'GMapLink',
        'Address':'Address1',
        'ArabicAddress':'Address1Ar',
        'LastUpdatedDate':'UpdatedAt'
    })

    # fixing column type
    df['Longitude'] = pd.to_numeric(df['Longitude'], errors='coerce')
    df['Latitude'] = pd.to_numeric(df['Latitude'], errors='coerce')
    df['Longitude'] = df['Longitude'].round(6)
    df['Latitude']  = df['Latitude'].round(6)
    df.loc[df['Longitude'].abs() > 999, 'Longitude'] = pd.NA
    df.loc[df['Latitude'].abs()  > 999, 'Latitude']  = pd.NA

    # Default values for missing columns
    df['ShortAddress'] = df['Address1']
    df['IsHQ'] = 0
    df['IsActiveMyKarage'] = 0
    df['StatusID'] = df['StatusID'].fillna(1)
    df['IsFeatured'] = pd.to_numeric(df['IsFeatured'], errors='coerce')
    df['IsFeatured'] = df['IsFeatured'].fillna(0)
    df['LandmarkID'] = df['LandmarkID'].map(lambda x: x if x in [1,2] else None)
    df.loc[(df['CountryID'] == 'SA') & (df['OldCityID'].isna()), 'OldCityID'] = 4101


    # Dates
    df['UpdatedAt'] = df['UpdatedAt'].fillna(datetime.now())
    df['CreatedAt'] = df['UpdatedAt']

    # Clean strings: strip 
    for col in df.select_dtypes(include='object').columns:
        if col != 'Name': df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) and x.strip()!='' else None)
        else: df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) else x)


    # IDs adjustments
    df = pd.merge(df, get_cities(target_db)[['CityID','OldCityID']], on='OldCityID', how='left')
    df = pd.merge(df, get_accounts(target_db), on='OldUserID', how='left')

    # Amenities Adjustments
    amenities_junc = get_custom(source_db, ['LocationID', 'AmenitiesID'], 'dbo.LocationAmenitiesJunc')
    amenities_junc.drop_duplicates(subset=['LocationID', 'AmenitiesID'], inplace=True)
    amenities_junc.rename(columns={'AmenitiesID':'OldAmenitiesID', 'LocationID':'OldLocationID'}, inplace=True)
    amenities = get_custom(target_db, ['Name', 'NameAr', 'AmenitiesID', 'OldAmenitiesID'], 'app.Amenities')
    amenities = pd.merge(amenities, amenities_junc, how='right', on='OldAmenitiesID')
    amenities.drop(columns='OldAmenitiesID', inplace=True)
    amenities = amenities.groupby('OldLocationID').apply(lambda x: x.drop(columns="OldLocationID").to_dict(orient="records")).reset_index(name="AmenitiesJson")


    # Services Adjustements
    services_junc = get_custom(source_db, ['LocationID', 'ServiceID'], 'dbo.LocationServiceJunc')
    services_junc.drop_duplicates(subset=['LocationID', 'ServiceID'], inplace=True)
    services_junc.rename(columns={'ServiceID':'OldServiceID', 'LocationID':'OldLocationID'}, inplace=True)
    services = get_custom(target_db, ['Name', 'NameAr', 'ServiceID', 'OldServiceID'], 'app.Services')
    services = pd.merge(services, services_junc, how='right', on='OldServiceID')
    services.drop(columns='OldServiceID', inplace=True)
    services = services.groupby('OldLocationID').apply(lambda x: x.drop(columns="OldLocationID").to_dict(orient="records")).reset_index(name="ServicesJson")

    # SocialMedia Adjustements
    social_media = get_custom(source_db, ['LocationID', 'Facebook', 'Twitter', 'Instagram', 'TikTok', 'Snapchat'], 'dbo.Receipt')
    social_media.dropna(subset=['Facebook', 'Twitter', 'Instagram', 'TikTok', 'Snapchat'], how='all', inplace=True)
    social_media.drop_duplicates(subset=['LocationID', 'Facebook', 'Twitter', 'Instagram', 'TikTok', 'Snapchat'], inplace=True)
    social_media.rename(columns={'LocationID':'OldLocationID'}, inplace=True)
    social_media = social_media.groupby('OldLocationID').apply(lambda x: x.drop(columns="OldLocationID").to_dict(orient="records")).reset_index(name="SocialMediaJson")

    # WorkingHours Adjustements
    workinghours = get_custom(source_db, ['LocationID', 'Name', 'ArabicName', 'Time', 'ArabicTime'], 'dbo.LocationWorkingHours')
    workinghours.rename(columns={'LocationID':'OldLocationID'}, inplace=True)
    workinghours = workinghours.groupby('OldLocationID').apply(lambda x: x.drop(columns="OldLocationID").to_dict(orient="records")).reset_index(name="WorkingHours")

    # Images Adjustements
    images = get_custom(source_db, ['LocationID', 'Image'], 'dbo.LocationImages')
    images.rename(columns={'LocationID':'OldLocationID'}, inplace=True)
    images = images.groupby('OldLocationID').apply(lambda x: x.drop(columns="OldLocationID").to_dict(orient="records")).reset_index(name="LocationImagesJson")


    df = pd.merge(df, amenities, on='OldLocationID', how='left')
    df = pd.merge(df, services, on='OldLocationID', how='left')
    df = pd.merge(df, social_media, on='OldLocationID', how='left')
    df = pd.merge(df, workinghours, on='OldLocationID', how='left')
    df = pd.merge(df, images, on='OldLocationID', how='left')

    log.info(f'Null values in WorkingHours: {df['WorkingHours'].map(lambda x: False if isinstance(x,list) else True).sum()}')

    df['WorkingHours'] = df['WorkingHours'].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, list) else pd.NA) 
    df['LocationImagesJson'] = df['LocationImagesJson'].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, list) else pd.NA) 
    df['SocialMediaJson'] = df['SocialMediaJson'].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, list) else pd.NA) 
    df['ServicesJson'] = df['ServicesJson'].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, list) else pd.NA) 
    df['AmenitiesJson'] = df['AmenitiesJson'].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, list) else pd.NA) 

    df[['WorkingHours', "LocationImagesJson", "SocialMediaJson", "ServicesJson", "AmenitiesJson"]] = df[['WorkingHours', "LocationImagesJson", "SocialMediaJson", "ServicesJson", "AmenitiesJson"]].astype("string")


   # Dropping columns
    df.drop(
        columns=['OldUserID', 'OldCityID', 'CountryID'], 
        inplace=True, errors='ignore')


    missing_acc = df['AccountID'].isna()
    log.info(f"Missing AccountID: {missing_acc.sum()}")
    if missing_acc.sum():
        raise IncrementalDependencyError(f"Missing AccountIDs for UserID = {df[missing_acc]['OldUserID'].values.tolist()}. Update Accounts Table.")


    df.sort_values(by='OldLocationID', inplace=True)


    log.info(f'Transformation complete, output: {len(df)} rows')
    return df

# -------------------- Load --------------------
def load(df: pd.DataFrame, engine: Engine):
    dtype_mapping = {col: NVARCHAR(None) for col in df.select_dtypes(include=['object', 'string']).columns}
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
            log.info("Verified/Added OldLocationID column.")

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
        log.info(f'dbo.Locations loaded successfully, CDC updated to {max_id}')
    except Exception as e:
        log.error(f'Failed to load dbo.Locations: {e}')
        raise

# -------------------- Main --------------------
def main():
    source = source_db_conn()
    target = target_db_conn()

    while True:
        df = extract(source, target)
        if df.empty:
            log.info('No new data to load.')
            return
        df = transform(df, source, target)
        # return
        load(df, target)
        # return

if __name__ == '__main__':
    main()
