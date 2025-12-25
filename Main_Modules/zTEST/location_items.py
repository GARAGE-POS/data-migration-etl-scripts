import os
import sys
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
def extract(account_id: int, v1: Engine, v2: Engine) -> pd.DataFrame:
    """Extract data based on CDC."""


    current_cat_ids = pd.read_sql(f"SELECT CategoryID FROM app.Categories WHERE AccountID={account_id}", v2)

    print(current_cat_ids)


    curr_old_cat_ids = pd.read_sql(f"""
                SELECT s.OldCategoryID, c.CategoryID
                FROM app.synccategories s
                JOIN app.categories c
                    ON s.accountid = c.AccountID
                AND c.Name COLLATE Latin1_General_CS_AS = s.Name COLLATE Latin1_General_CS_AS
                WHERE c.CategoryID IN {str(tuple(current_cat_ids.values.tolist()) + (0,0)).replace('[','').replace(']','')}
                ORDER BY s.OldCategoryID
            """, v2)
    
    old_cat_ids = tuple(curr_old_cat_ids['OldCategoryID'].values.tolist())+(0,0)
    old_cat_ids = str(old_cat_ids).replace('[','').replace(']','')


    df_old = pd.read_sql(f"SELECT CategoryID, LocationID FROM dbo.Category WHERE CategoryID in {old_cat_ids}", v1)
    df_old.rename(columns={
        'LocationID':'OldLocationID',
        'CategoryID':'OldCategoryID'
    }, inplace=True)

    current_loc_ids = pd.read_sql(f"SELECT LocationID, OldLocationID FROM app.Locations WHERE OldLocationID in {str(tuple(df_old['OldLocationID'].values.tolist())+(0,0)).replace('[','').replace(']','')}", v2)


    df_old = pd.merge(df_old, curr_old_cat_ids, on='OldCategoryID', how='left')
    df_old = pd.merge(df_old, current_loc_ids, on='OldLocationID', how='left')


    df = pd.read_sql_query( f"SELECT ItemID, CategoryID, Price, CreatedAt, UpdatedAt, StatusID FROM app.Items WHERE CategoryID IN {str(tuple(current_cat_ids.values.tolist()) + (0,0)).replace('[','').replace(']','')}", v2)
    
    print(df)
    
    df = pd.merge(df, df_old, on='CategoryID', how='left')

    
    logging.info(f'Extracted {len(df)} rows from app.Items')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame, source_db: Engine, target_db: Engine) ->  pd.DataFrame:
    """Clean and transform Category data."""
    # Keep only necessary columns and rename
    df_old = pd.read_sql("SELECT CategoryID, LocationID FROM dbo.Category", source_db)
    df_old.rename(columns={
        'LocationID':'OldLocationID',
        'CategoryID':'OldCategoryID'
    }, inplace=True)

    cat = tuple(df['CategoryID'].values.tolist())+(0,0)
    query = text(f""" 
                SELECT c.CategoryID, s.OldCategoryID
                FROM app.SyncCategories s
                LEFT JOIN app.Categories c
                ON c.Name COLLATE Latin1_General_CS_AS = s.Name COLLATE Latin1_General_CS_AS
                AND c.AccountID = s.AccountID
                WHERE c.CategoryID in {cat}
                ORDER BY s.OldCategoryID
            """)
    current_cat_ids = pd.read_sql(query, target_db)
    current_loc_ids = pd.read_sql(f"SELECT LocationID, OldLocationID FROM app.Locations", target_db)

    df_old = pd.merge(df_old, current_cat_ids, on='OldCategoryID', how='left')
    df_old = pd.merge(df_old, current_loc_ids, on='OldLocationID', how='left')



    df = pd.merge(df, df_old, how='left', on='CategoryID')

    print(df[['ItemID', 'LocationID', 'Price','CategoryID', 'OldCategoryID', 'OldLocationID', 'StatusID', 'UpdatedAt']])
    df.drop(columns=['CategoryID', 'OldCategoryID', 'OldLocationID'], inplace=True)
    logging.info(f'Transformation complete, output rows: {len(df)}')
    return df

# -------------------- Load --------------------
def load(df: pd.DataFrame, engine: Engine):


    try:
        with engine.begin() as conn: 

            # Inserting the Data
            df.to_sql('LocationItems', con=conn, schema='app', if_exists='append', index=False) # type: ignore
            logging.info(f'app.LocationItems loaded successfully')

    except Exception as e:
        logging.error(f'Failed to load app.LocationItems: {e}')
        raise

# -------------------- Main --------------------
def main():
    source = source_db_conn()
    target = target_db_conn()

    account_id = int(sys.argv[1])

    while True:
        df = extract(account_id, source, target)
        if df.empty:
            logging.info('No new data to load.')
            return
        # df = transform(df, source, target)
        print(df)
        # return
        # load(df, target)
        return
    
if __name__ == '__main__':
    main()
