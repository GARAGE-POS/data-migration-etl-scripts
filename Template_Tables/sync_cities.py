import os
import warnings
from dotenv import load_dotenv
from sqlalchemy import create_engine, Engine
from urllib.parse import quote_plus
import pandas as pd
from utils.tools import get_logger

warnings.filterwarnings('ignore')
load_dotenv()

log = get_logger('SyncCities')

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
def extract_old_cities(engine: Engine) -> pd.DataFrame:
    """Extract data."""

    query = f"SELECT * FROM dbo.City"
    df = pd.read_sql_query(query, engine)
    log.info(f'Extracted {len(df)} rows from dbo.City')
    return df

def extract_new_cities(engine: Engine) -> pd.DataFrame:
    """Extract data."""

    query = f"SELECT * FROM app.Cities"
    df = pd.read_sql_query(query, engine)
    log.info(f'Extracted {len(df)} rows from dbo.Cities')
    return df

# -------------------- Transform --------------------
def join(old_data: pd.DataFrame, new_data: pd.DataFrame) -> pd.DataFrame:

    city_map = {
        'Sharja':'Sharjah',
        'Sanaa':"Sana'a",
        'Ha il':"Ha'il",
        'Hail':"Ha'il",
        'Ta if':"Ta'if",
        'Kuwait':'Kuwait City',
        'Salala':'Salalah',
        'Masqat':'Muscat',
        'Quassim':'Qassim',
        'al-Salimiya':'Salmiya',
        'Al-Ahsa':'Al Ahsa'
    }

    old_data = old_data.rename(columns={'ID':'OldCityID', 'Name':'CityName'})

    old_data = old_data[['CityName','OldCityID']]

    old_data['CityName'] = old_data['CityName'].map(lambda x: x.strip())
    old_data['CityName'] = old_data['CityName'].map(lambda x: city_map.get(x) if x in city_map.keys() else x)
    new_data['CityName'] = new_data['CityName'].map(lambda x: x.strip())


    joined_data = pd.merge(new_data, old_data, how='inner', on='CityName')

    return joined_data


# -------------------- Main --------------------
def main(if_load:bool=True):
    source = source_db_conn()
    target = target_db_conn()

    old_cities = extract_old_cities(source)
    new_cities = extract_new_cities(target)

 
    df = join(old_cities, new_cities)


    df['CityID'] = pd.to_numeric(df['CityID'],errors='coerce')
    df['OldCityID'] = pd.to_numeric(df['OldCityID'],errors='coerce')
    df['CountryID'] = pd.to_numeric(df['CountryID'],errors='coerce')

    df = df[['CityID','OldCityID', 'CountryID']]

    df.dropna()

    df.loc[df['OldCityID']==2439, ['CityID','CountryID']] = [179,9]
    df.loc[df['OldCityID']==2441, ['CityID','CountryID']] = [298,15]

    df.drop_duplicates(subset='OldCityID', inplace=True)

    if if_load:
        df.to_sql(
            name='SyncCities',
            con=target,
            schema='app',
            if_exists='append',
            index=False,
        )
        log.info('Cities are Synchronized')

# if __name__ == '__main__':
#     main()
