import os
import warnings
import json
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR, DECIMAL
from urllib.parse import quote_plus
import pandas as pd
from utils.tools import get_logger, clean_contact, normalize_ranges
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
def extract(user_id:int, engine: Engine) -> pd.DataFrame:
    """Extract new rows based on UserID."""
 
    query = f"SELECT * FROM dbo.Locations WHERE UserID={user_id} ORDER BY LocationID"
    df = pd.read_sql_query(query, engine)
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
    df['LandmarkID'] = df['LandmarkID'].map(lambda x: x if x in [1,2, 3] else None)
    df.loc[(df['CountryID'] == 'SA') & (df['OldCityID'].isna()), 'OldCityID'] = 4101


    # Dates
    df['UpdatedAt'] = df['UpdatedAt'].fillna(datetime.now())
    df['CreatedAt'] = df['UpdatedAt']

    # Clean strings: strip 
    for col in df.select_dtypes(include='object').columns:
        if col != 'Name': df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) and x.strip()!='' else None)
        else: df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) else x)
    df['ContactNo'] = df['ContactNo'].apply(clean_contact)


    # IDs adjustments
    df = pd.merge(df, get_cities(target_db)[['CityID','OldCityID']], on='OldCityID', how='left')
    df = pd.merge(df, get_accounts(target_db), on='OldUserID', how='left')
    missing_acc = df['AccountID'].isna()
    if missing_acc.sum():
        log.warning(f"Missing AccountIDs: {missing_acc.sum()}")
        raise IncrementalDependencyError(f"Missing AccountIDs for UserID = {df[missing_acc]['OldUserID'].values.tolist()}. Update Accounts Table.")



    # Amenities Adjustments
    amenities_junc = get_custom(source_db, ['LocationID', 'AmenitiesID'], 'dbo.LocationAmenitiesJunc')
    amenities_junc.drop_duplicates(subset=['LocationID', 'AmenitiesID'], inplace=True)
    amenities_junc.rename(columns={'AmenitiesID':'OldAmenitiesID', 'LocationID':'OldLocationID'}, inplace=True)
    amenities = get_custom(target_db, ['Name', 'AmenitiesID'], 'app.Amenities')
    amenities['Name'] = amenities['Name'].map(lambda x: x.strip() if isinstance(x,str) and x.strip()!='' else None)
    amenities.dropna(subset='Name', inplace=True)
    amenities = pd.merge(amenities, get_custom(target_db, '*', 'app.SyncAmenities'), how='inner', on='AmenitiesID')
    amenities = pd.merge(amenities, amenities_junc, how='inner', on='OldAmenitiesID')
    amenities.drop(columns='OldAmenitiesID', inplace=True)
    amenities = amenities.groupby('OldLocationID')['Name'].agg(list).reset_index(name="AmenitiesJson")

    # Services Adjustements
    services_junc = get_custom(source_db, ['LocationID', 'ServiceID'], 'dbo.LocationServiceJunc')
    services_junc.drop_duplicates(subset=['LocationID', 'ServiceID'], inplace=True)
    services_junc.rename(columns={'ServiceID':'OldServiceID', 'LocationID':'OldLocationID'}, inplace=True)
    services = get_custom(target_db, ['Name', 'ServiceID'], 'app.Services')
    services = pd.merge(services, get_custom(target_db, '*', 'app.SyncServices'), how='inner', on='ServiceID')
    services = pd.merge(services, services_junc, how='right', on='OldServiceID')
    services.drop(columns='OldServiceID', inplace=True)
    services.dropna(subset='Name', inplace=True)
    services = services.groupby('OldLocationID')['Name'].agg(list).reset_index(name="ServicesJson")

    # SocialMedia Adjustements
    social_media = get_custom(source_db, ['LocationID', 'Facebook', 'Twitter', 'Instagram', 'TikTok', 'Snapchat'], 'dbo.Receipt')
    for col in social_media.select_dtypes(include='object').columns:
        social_media[col] = social_media[col].apply(lambda x: x.strip() if isinstance(x,str) and x.strip()!='' else None)
    social_media.dropna(subset=['Facebook', 'Twitter', 'Instagram', 'TikTok', 'Snapchat'], how='all', inplace=True)
    social_media.drop_duplicates(subset=['LocationID', 'Facebook', 'Twitter', 'Instagram', 'TikTok', 'Snapchat'], inplace=True)
    social_media.rename(columns={'LocationID':'OldLocationID'}, inplace=True)
    social_media = social_media.groupby('OldLocationID').apply(lambda x: x.drop(columns="OldLocationID").to_dict(orient="records")).reset_index(name="SocialMediaJson")

    # WorkingHours Adjustements
    workinghours = get_custom(source_db, ['LocationID','Time AS WorkingHours'], 'dbo.LocationWorkingHours')
    workinghours.rename(columns={'LocationID':'OldLocationID'}, inplace=True)
    workinghours.drop_duplicates(subset='OldLocationID', inplace=True)
    workinghours['WorkingHours'] = workinghours['WorkingHours'].map(lambda x: x.replace(' ', ''))
    workinghours['WorkingHours'] = workinghours['WorkingHours'].apply(normalize_ranges)

    # Images Adjustements
    images = get_custom(source_db, ['LocationID', 'Image'], 'dbo.LocationImages')
    images.rename(columns={'LocationID':'OldLocationID'}, inplace=True)
    images['Image'] = images['Image'].map(lambda x: x.strip() if isinstance(x,str) and x.strip()!='' else None)
    images.dropna(subset='Image', inplace=True)
    images = images.groupby('OldLocationID')['Image'].agg(list).reset_index(name="LocationImagesJson")

    df = pd.merge(df, amenities, on='OldLocationID', how='left')
    df = pd.merge(df, services, on='OldLocationID', how='left')
    df = pd.merge(df, social_media, on='OldLocationID', how='left')
    df = pd.merge(df, workinghours, on='OldLocationID', how='left')
    df = pd.merge(df, images, on='OldLocationID', how='left')

    df['LocationImagesJson'] = df['LocationImagesJson'].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, list) else pd.NA) 
    df['SocialMediaJson'] = df['SocialMediaJson'].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, list) else pd.NA) 
    df['ServicesJson'] = df['ServicesJson'].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, list) else pd.NA) 
    df['AmenitiesJson'] = df['AmenitiesJson'].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, list) else pd.NA) 

    df[['WorkingHours', "LocationImagesJson", "SocialMediaJson", "ServicesJson", "AmenitiesJson"]] = df[['WorkingHours', "LocationImagesJson", "SocialMediaJson", "ServicesJson", "AmenitiesJson"]].astype("string")


   # Dropping columns
    df.drop(
        columns=['OldUserID', 'OldCityID', 'CountryID'], 
        inplace=True, errors='ignore')



    df.sort_values(by='OldLocationID', inplace=True)


    log.info(f'Transformation complete. df rows: {len(df)}')
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
            log.info(f'dbo.Locations loaded successfully')                
    except Exception as e:
        log.error(f'Failed to load dbo.Locations: {e}')
        raise

# -------------------- Main --------------------
def main(user_id:int, if_load:bool=True):
    source = source_db_conn()
    target = target_db_conn()

    df = extract(user_id, source)
    if df.empty:
        log.info('No new data to load.')
        return
    df = transform(df, source, target)
    print(df)
    if if_load:
        load(df, target)

# if __name__ == '__main__':
#     main()
