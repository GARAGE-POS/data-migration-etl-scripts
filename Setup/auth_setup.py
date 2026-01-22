import os
import warnings
import logging
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR, DECIMAL
from urllib.parse import quote_plus
import pandas as pd
import numpy as np


logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | AuthSetup | %(message)s")


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
    logging.info(f'Connected to {os.getenv(db_env)} at {os.getenv(server_env)}')
    return engine

def source_db_conn(): return get_engine('AZURE_SERVER','AZURE_DATABASE','AZURE_USERNAME','AZURE_PASSWORD')
def target_db_conn(): return get_engine('STAGE_SERVER','STAGE_DATABASE','STAGE_USERNAME','STAGE_PASSWORD')


# -------------------- Main --------------------
def main():

    target = target_db_conn()


    openidscopes = pd.read_csv('Setup/OpenIddictScopes.csv')


    
    try:
        with target.begin() as conn:  # Transaction-safe
            
            conn.execute(text('''
                    INSERT INTO app.OpenIddictApplications (Id, ApplicationType, ClientId, ClientSecret, ClientType, ConcurrencyToken, ConsentType, DisplayName, DisplayNames, JsonWebKeySet, Permissions, PostLogoutRedirectUris, Properties, RedirectUris, Requirements, Settings)
                    VALUES ('55a6373b-6083-4bb8-a7d0-1116a72e846a', NULL, 'auth-code-client', NULL, 'public', '576ce872-3d71-44b0-a681-d4d94e08e6df', NULL, 'Authorization Code Client', NULL, NULL, :scopes, :redirects, NULL, :redirects, '["ft:pkce"]', NULL);
                '''),
                {'scopes':os.getenv('SCOPES'), 'redirects':os.getenv('REDIRECTS')}
            )
            logging.info(f'OpenIddictApplications inserted successfully.')

            

            openidscopes.to_sql(name='OpenIddictScopes', con=conn, schema='app', if_exists='append', index=False)
            logging.info(f'OpenIddictScopes row {len(openidscopes)} appended.')


    except Exception as e:
        logging.error(f'Failed to Setup the DB: {e}')
        raise    

if __name__ == '__main__':
    main()
