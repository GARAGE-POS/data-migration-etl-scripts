import os
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR, DECIMAL
from urllib.parse import quote_plus
import pandas as pd
from utils.fks_mapper import get_makes
from utils.tools import get_logger

log = get_logger('Models')
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
def extract(source_db: Engine) -> pd.DataFrame:
    """Extract data."""

    invalid_models = ('2', '3', '3', '5', '6', '7', '8', '9', '86', '92', '93', '99', '107', '108', '206', '307', '308', '320', '360', '404', '406', '407', '408', '500', '508', '520', '520', '595', '620', '745', '750', '820', '850', '900', '911', '960', '1616', '3008', '9000', '09-Mar', '09-May', '????? ??? ?? ????????', '03+', '208E', '208E', 'A', 'A', 'Accent', 'Accent', 'Actyon', 'Actyon', 'ACURA', 'ACURA', 'ACURA rafi1', 'Amarok', 'Amarok', 'Arrizo5Pro', 'Arrizo5Pro', 'Arrizo5Pro', 'Arrizo5Pro', 'Arrizo5Pro', 'Arrizo5Pro', 'Arrizo6Pro', 'Arrizo6Pro', 'Arrizo6Pro', 'Arrizo6Pro', 'BOXER', 'Boxer', 'BT 200', 'BT 200', 'BT 200', 'C300', 'C300', 'Camaro ss 1Le', 'CAMARO SS 1LE', 'CANTER', 'CANTER', 'CANTER', 'CLA 250', 'CLA 250', 'compass', 'compass', 'COOLRAY', 'COOLRAY', 'cooper', 'COOPER', 'Corvette', 'Corvette', 'Cougar', 'Cougar', 'CX 5', 'CX 5', 'Defender', 'Defender', 'Defender 90', 'Defender 90', 'Defender 90', 'Defender 90', 'Defender 90', 'Discovery Sport', 'Discovery Sport', 'Discovery Sport', 'Discovery Sport', 'Discovery Sport', 'Discovery Sport', 'Discovery Sport', 'Discovery Sport', 'Discovery Sport', 'Discovery Sport', 'Discovery Sport', 'Explorer', 'Explorer', 'F40', 'f40', 'f40', 'F5', 'F5', 'F7', 'F7', 'Focus', 'Focus', 'Fusion', 'Fusion', 'G500 4x4', 'G500 4x4', 'G63', 'G63', 'GLE 500', 'GLE 500', 'GLE 500', 'GLE 500', 'Grand vitara', 'Grand vitara', 'GS', 'GS', 'GT', 'Gt', 'GT', 'H1', 'H1', 'H5', 'H5', 'H7', 'H7', 'H9', 'H9', 'highlander', 'Highlander', 'HS5', 'HS5', 'K8', 'K8', 'Kicks', 'Kicks', 'L 200', 'L 200', 'L 200', 'L 200', 'Legend', 'Legend', 'Logan', 'LOGAN', 'LS 460', 'LS 460', 'LS 460', 'Malibu', 'Malibu', 'Matrix', 'Matrix', 'mazda 3', 'mazda 3', 'mazda 3', 'MDX', 'MDX', 'Mini cooper', 'Mini Cooper', 'mini cooper s', 'Mini Cooper S', 'Mistral', 'Mistral', 'Omoda C5', 'Omoda C5', 'Pick up', 'Pick up', 'POER', 'Poer', 'Q 8', 'Q 8', 'Range Rover', 'Range Rover', 'Range Rover Evoque', 'Range Rover Evoque', 'Range Rover Evoque', 'Range Rover Evoque', 'Range Rover Evoque', 'Range Rover Evoque', 'Range Rover Evoque', 'Range Rover Evoque', 'Range Rover Evoque', 'Range Rover Evoque', 'Range Rover Evoque', 'Range Rover Evoque', 'Range Rover Evoque', 'Range Rover Evoque', 'Range Rover Evoque', 'Range Rover Velar', 'Range Rover Velar', 'Range Rover Velar', 'Range Rover Velar', 'Range Rover Velar', 'Range Rover Velar', 'Range Rover Velar', 'Range Rover Velar', 'Range Rover Velar', 'RDX', 'RDX', 'REWARD', 'REWARD', 'S 3', 'S 3', 'S2', 'S2', 'S3', 'S3', 'S2', 'Sonata', 'sonata', 'T6', 'T6', 'T90', 'T90', 'Tank 300', 'Tank 300', 'Tank 300', 'Tank 300', 'Tank 300', 'Tank 500', 'Tank 500', 'Terios', 'TERIOS', 'Terracan', 'Terracan', 'Test', 'Test', 'Test', 'Test', 'Test', 'test', 'test', 'test', 'Tiggo 4 Pro', 'Tiggo 4 Pro', 'Tiggo 4 Pro', 'Tiggo 4 Pro', 'Tiggo 4 Pro', 'Tiggo 4 Pro', 'Tiggo 4 Pro', 'Tiggo 7 Pro', 'Tiggo 7 Pro', 'Tiggo 7 Pro', 'Tiggo 7 Pro', 'Tiggo 7 Pro Max', 'Tiggo 7 Pro Max', 'Tiggo 7 Pro Max', 'Tiggo 8 Pro Max', 'Tiggo 8 Pro Max', 'Tiggo 8 Pro Max', 'Trajet', 'Trajet', 'VV@', 'VX', 'waja', 'X35', 'X35', 'X7', 'X7', 'X70S', 'X70S', 'YZF-R1M', 'YZF-R1M', 'Z7', 'Z7', 'ZS', 'ZS')

    query = f"SELECT * FROM dbo.Model WHERE TRIM(Name) NOT IN {invalid_models} AND StatusID=1 ORDER BY ModelID"
    df = pd.read_sql_query(query, source_db)
    log.info(f'Extracted {len(df)} rows from dbo.Model')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame, target_db: Engine) -> pd.DataFrame:
    """Clean and transform Model data."""
    # Keep only necessary columns and rename

    df.drop(
        columns=['RowID','CreatedBy','LastUpdatedBy'], inplace=True
    )
    df = df.rename(columns={
        'ArabicName':'NameAr',
        'ModelID':'OldModelID',
        'MakeID':'OldMakeID',
        'CreatedOn':'CreatedAt',
        'LastUpdatedDate':'UpdatedAt',
        'RecommendedLitres':'RecommendedLiters'
    })

    df['UpdatedAt'] = df['UpdatedAt'].fillna(datetime.now())
    df.loc[df['CreatedAt'].isna(), 'CreatedAt'] = df['UpdatedAt']

    df['RecommendedLiters'] = pd.to_numeric(df['RecommendedLiters'], errors='coerce')

    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) and x.strip() != '' else x)

    df['Year'] = df['Year'].fillna(0)


    # Sync ModelIDs
    df = pd.merge(df, get_makes(target_db), on='OldMakeID', how='left')
    df.drop(columns="OldMakeID", inplace=True)

    df.dropna(subset='MakeID', inplace=True)

    log.info('Transformation complete')
    return df

# -------------------- Load --------------------
def load(df: pd.DataFrame, engine: Engine):

    dtype_mapping = {'Name':NVARCHAR(None), 'NameAr':NVARCHAR(None), 'RecommendedLiters':DECIMAL(18,2), 'ImagePath':NVARCHAR(None)}

    try:
        with engine.begin() as conn:  # Transaction-safe

            conn.execute(text("""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.columns
                    WHERE Name = 'OldModelID'
                    AND Object_ID = Object_ID('app.Models')
                )
                BEGIN
                    ALTER TABLE app.Models
                    ADD OldModelID BIGINT NULL;
                END
            """))
            log.info("Verified/Added OldModelID column.")

            df.to_sql('Models', con=conn, schema='app', if_exists='append', index=False, dtype=dtype_mapping) # type: ignore
            log.info(f'dbo.Model loaded successfully')

    except Exception as e:
        log.error(f'Failed to load dbo.Model: {e}')
        raise

# -------------------- Main --------------------
def main():
    source = source_db_conn()
    target = target_db_conn()

    df = extract(source)
    if df.empty:
        log.info('No data to load.')
        return
    df = transform(df, target)
    # print(df)
    # return
    load(df, target)

if __name__ == '__main__':
    main()
