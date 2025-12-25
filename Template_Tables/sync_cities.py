import os
import warnings
from dotenv import load_dotenv
import logging
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, BIGINT
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
def extract_old_cities(engine: Engine) -> pd.DataFrame:
    """Extract data."""

    query = f"SELECT * FROM dbo.City"
    df = pd.read_sql_query(query, engine)
    logging.info(f'Extracted {len(df)} rows from dbo.City')
    return df

def extract_new_cities(engine: Engine) -> pd.DataFrame:
    """Extract data."""

    query = f"SELECT * FROM app.Cities"
    df = pd.read_sql_query(query, engine)
    logging.info(f'Extracted {len(df)} rows from dbo.Cities')
    return df

# -------------------- Transform --------------------
def join(old_data: pd.DataFrame, new_data: pd.DataFrame) -> pd.DataFrame:

    city_map = {
        'Sharja':'Sharjah',
        'Sanaa':"Sana'a",
        'Ha il':"Ha'il",
        'Ta if':"Ta'if",
        'Kuwait':'Kuwait City',
        'Salala':'Salalah',
        'Masqat':'Muscat'
    }

    old_data = old_data.rename(columns={'ID':'OldCityID', 'Name':'CityName'})

    old_data = old_data[['CityName','OldCityID']]

    old_data['CityName'] = old_data['CityName'].map(lambda x: x.strip())
    old_data['CityName'] = old_data['CityName'].map(lambda x: city_map.get(x) if x in city_map.keys() else x)
    new_data['CityName'] = new_data['CityName'].map(lambda x: x.strip())


    joined_data = pd.merge(new_data, old_data, how='left', on='CityName')

    return joined_data


# -------------------- Main --------------------
def main():
    source = source_db_conn()
    target = target_db_conn()

    old_cities = extract_old_cities(source)
    new_cities = extract_new_cities(target)

    # print(new_cities)

    print()

    df = join(old_cities, new_cities)


    df['CityID'] = pd.to_numeric(df['CityID'],errors='coerce')
    df['OldCityID'] = pd.to_numeric(df['OldCityID'],errors='coerce')


    df = df.dropna()

    df = df[['CityID','OldCityID']]
    print(df)
    # return
    df.to_sql(
        name='SyncCities',
        con=target,
        schema='app',
        if_exists='append',
        index=False,
        dtype={'CityID':BIGINT,'OldCityID':BIGINT}
    )
    logging.info('Cities are Synchronized')

if __name__ == '__main__':
    main()
