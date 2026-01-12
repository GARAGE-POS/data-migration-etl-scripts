import os
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR, DECIMAL
from urllib.parse import quote_plus
import pandas as pd
from utils.tools import get_logger
from utils.fks_mapper import get_accounts, get_users
from utils.custom_err import IncrementalDependencyError 

log = get_logger('UserRoles')
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
            text("SELECT ISNULL(MaxIndex,0) FROM app.ETLcdc WHERE TableName=:table_name"),
            {"table_name": 'dbo.UserRoles'}
        ).scalar()
    max_id = max_id if not max_id is None else 0
    log.info(f'Current CDC for dbo.UserRoles: {max_id}')

    old_acc_ids = tuple(pd.read_sql_query(f"SELECT top 10 UserID AS OldUserID FROM dbo.Users WHERE UserID > {max_id} AND StatusID = 1 ORDER BY UserID", source_db)['OldUserID'].values.tolist()) + (0,0)

    query = text(f"""
            SELECT  u.UserID AS OldUserID, 
                    su.SubUserID AS OldID, 
                    FormName='Accounts', 
                    rgf.New, 
                    rgf.Remove, 
                    rgf.Edit, 
                    rgf.Access
            FROM Users u
            JOIN SubUsers su ON u.UserID = su.UserID
            JOIN Role_Group rg ON u.UserID = rg.UserID
            JOIN Role_GroupForms rgf ON rg.GroupID = rgf.GroupID
            JOIN Role_Forms rf ON rf.FormID = rgf.FormID
            WHERE su.StatusID = 1 AND u.UserID in {old_acc_ids} AND rf.FormName IN ('Users')
            ORDER BY u.UserID, su.SubUserID
    """)
    df = pd.read_sql(query, source_db)
    log.info(f'Extracted {len(df)} rows from dbo.SubUsers.')


    df = pd.merge(df, get_users(target_db, df['OldID']), on='OldID', how='left')
    missing_users = df['Id'].isna().sum()
    if missing_users:
        log.warning(f'Missing UserIDs: {missing_users}')
        raise IncrementalDependencyError('Update SubUsers in AspNetUsers Table.')
    
    df = pd.merge(df, get_accounts(target_db, df['OldUserID']), on='OldUserID', how='left')
    missing_accs = df['AccountID'].isna().sum()
    if missing_accs:
        log.warning(f'Missing AccountIDs: {missing_accs}')
        raise IncrementalDependencyError('Update Users in Accounts Table.')

    df.drop(columns='OldID', inplace=True)
    return df

# -------------------- Transform --------------------
def transform(df:pd.DataFrame) -> pd.DataFrame:

    df.rename(columns={'Id':'UserID'}, inplace=True)

    roles_table = {
        'Accounts': ['POST /api/v1/accounts', 'DELETE /api/v1/accounts/{AccountID}', 'GET /api/v1/accounts/{AccountID}', 'PUT /api/v1/accounts/{AccountID}'],
        'AppSources': ['POST /api/v1/appsources', 'DELETE  POST /api/v1/appsources/{AppSourceID}', 'GET  POST /api/v1/appsources/{AppSourceID}', 'PUT  POST /api/v1/appsources/{AppSourceID}'],
    }

    type_map = {'New':0, 'Remove': 1, 'Access': 2, 'Edit': 3}

    df = df.melt(
        id_vars=['OldUserID','AccountID', 'UserID', 'FormName'],
        value_vars=['New', 'Remove', 'Edit', 'Access'],
        var_name='ClaimType',
        value_name='ClaimValue'
    ) 



    df = df[df['ClaimValue']==True]


    df['ClaimType'] = df.apply(lambda row: roles_table.get(row['FormName'])[type_map.get(row['ClaimType'])], axis=1) # type: ignore


    return df


# -------------------- Load --------------------
def load(df: pd.DataFrame, engine: Engine):
    dtype_mapping = {col:NVARCHAR(None) for col in df.select_dtypes(include='object').columns}

    max_id = df['OldUserID'].max()

    df.drop(columns='OldUserID', inplace=True)

    try:
        with engine.begin() as conn:  # Transaction-safe

            df.to_sql('AspNetUserClaims', con=conn, schema='app', if_exists='append', index=False, dtype=dtype_mapping) # type: ignore
            log.info(f'app.AspNetUserClaims loaded successfully')

            conn.execute(
                text("""
                    MERGE app.[ETLcdc] AS target
                    USING (SELECT :table_name AS [TableName], :max_index AS [MaxIndex]) AS source
                    ON target.[TableName] = source.[TableName]
                    WHEN MATCHED THEN UPDATE SET target.[MaxIndex] = source.[MaxIndex]
                    WHEN NOT MATCHED THEN INSERT ([TableName],[MaxIndex]) VALUES (source.[TableName],source.[MaxIndex]);
                """),
                {"table_name": f'dbo.UserRoles', "max_index": int(max_id)}
            )
            log.info(f'app.AspNetUserClaims loaded successfully, CDC updated to {max_id}')
    except Exception as e:
        log.error(f'Failed to load app.AspNetUserClaims: {e}')
        raise





def main():
    source = source_db_conn()
    target = target_db_conn()

    df = extract(source, target)
    print(df)
    df = transform(df)
    print()
    print(df)    
    return
    while True:
        df = extract(source, target)
        if df.empty:
            log.info('No new data to load.')
            return
        # return
        df = transform(df, target)
        # print(df)
        # return
        load(df, target)

if __name__ == '__main__':
    main()
