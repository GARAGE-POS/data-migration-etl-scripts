import os
import warnings
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine, text, Engine, NVARCHAR
from urllib.parse import quote_plus
import pandas as pd
from utils.tools import get_logger

log = get_logger('Countries')
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

    query = f"SELECT * FROM dbo.Country"
    df = pd.read_sql_query(query, source_db)
    log.info(f'Extracted {len(df)} rows from dbo.Country')
    return df

# -------------------- Transform --------------------
def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and transform Countries data."""
    # Keep only necessary columns and rename
    df = df[['Code', 'Name', 'Curr_Code']]
    df = df.rename(columns={
        'Name':'CountryName',
        'Curr_Code':'Currency',
    })


    df['TaxPercentage'] = 20
    df['ConversionRate'] = 1
    df['Currency'] = df['Currency'].fillna('')

    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x,str) else x)

    alpha2_to_alpha3 = {
    "AD": "AND",
    "AF": "AFG",
    "AG": "ATG",
    "AI": "AIA",
    "AL": "ALB",
    "AM": "ARM",
    "AO": "AGO",
    "AQ": "ATA",
    "AR": "ARG",
    "AS": "ASM",
    "AT": "AUT",
    "AU": "AUS",
    "AW": "ABW",
    "AX": "ALA",
    "AZ": "AZE",
    "BA": "BIH",
    "BB": "BRB",
    "BD": "BGD",
    "BE": "BEL",
    "BF": "BFA",
    "BG": "BGR",
    "BH": "BHR",
    "BI": "BDI",
    "BJ": "BEN",
    "BL": "BLM",
    "BM": "BMU",
    "BN": "BRN",
    "BO": "BOL",
    "BQ": "BES",
    "BR": "BRA",
    "BS": "BHS",
    "BT": "BTN",
    "BV": "BVT",
    "BW": "BWA",
    "BY": "BLR",
    "BZ": "BLZ",
    "CA": "CAN",
    "CC": "CCK",
    "CD": "COD",
    "CF": "CAF",
    "CG": "COG",
    "CH": "CHE",
    "CI": "CIV",
    "CK": "COK",
    "CL": "CHL",
    "CM": "CMR",
    "CN": "CHN",
    "CO": "COL",
    "CR": "CRI",
    "CU": "CUB",
    "CV": "CPV",
    "CX": "CXR",
    "CY": "CYP",
    "CZ": "CZE",
    "DE": "DEU",
    "DJ": "DJI",
    "DK": "DNK",
    "DM": "DMA",
    "DO": "DOM",
    "DZ": "DZA",
    "EC": "ECU",
    "EE": "EST",
    "EG": "EGY",
    "EH": "ESH",
    "ER": "ERI",
    "ES": "ESP",
    "ET": "ETH",
    "FI": "FIN",
    "FJ": "FJI",
    "FK": "FLK",
    "FM": "FSM",
    "FO": "FRO",
    "FR": "FRA",
    "GA": "GAB",
    "GB": "GBR",
    "GE": "GEO",
    "GF": "GUF",
    "GG": "GGY",
    "GH": "GHA",
    "GI": "GIB",
    "GL": "GRL",
    "GM": "GMB",
    "GN": "GIN",
    "GP": "GLP",
    "GQ": "GNQ",
    "GR": "GRC",
    "GS": "SGS",
    "GT": "GTM",
    "GU": "GUM",
    "GW": "GNB",
    "GY": "GUY",
    "HK": "HKG",
    "HM": "HMD",
    "HN": "HND",
    "HR": "HRV",
    "HT": "HTI",
    "HU": "HUN",
    "ID": "IDN",
    "IE": "IRL",
    "IL": "ISR",
    "IM": "IMN",
    "IN": "IND",
    "IO": "IOT",
    "IQ": "IRQ",
    "IR": "IRN",
    "IS": "ISL",
    "IT": "ITA",
    "JE": "JEY",
    "JM": "JAM",
    "JO": "JOR",
    "JP": "JPN",
    "KE": "KEN",
    "KG": "KGZ",
    "KH": "KHM",
    "KI": "KIR",
    "KM": "COM",
    "KN": "KNA",
    "KP": "PRK",
    "KR": "KOR",
    "KW": "KWT",
    "KY": "CYM",
    "KZ": "KAZ",
    "LA": "LAO",
    "LB": "LBN",
    "LC": "LCA",
    "LI": "LIE",
    "LK": "LKA",
    "LR": "LBR",
    "LS": "LSO",
    "LT": "LTU",
    "LU": "LUX",
    "LV": "LVA",
    "LY": "LBY",
    "MA": "MAR",
    "MC": "MCO",
    "MD": "MDA",
    "ME": "MNE",
    "MF": "MAF",
    "MG": "MDG",
    "MH": "MHL",
    "MK": "MKD",
    "ML": "MLI",
    "MM": "MMR",
    "MN": "MNG",
    "MO": "MAC",
    "MP": "MNP",
    "MQ": "MTQ",
    "MR": "MRT",
    "MS": "MSR",
    "MT": "MLT",
    "MU": "MUS",
    "MV": "MDV",
    "MW": "MWI",
    "MX": "MEX",
    "MY": "MYS",
    "MZ": "MOZ",
    "NA": "NAM",
    "NC": "NCL",
    "NE": "NER",
    "NF": "NFK",
    "NG": "NGA",
    "NI": "NIC",
    "NL": "NLD",
    "NO": "NOR",
    "NP": "NPL",
    "NR": "NRU",
    "NU": "NIU",
    "NZ": "NZL",
    "OM": "OMN",
    "PA": "PAN",
    "PE": "PER",
    "PF": "PYF",
    "PG": "PNG",
    "PH": "PHL",
    "PK": "PAK",
    "PL": "POL",
    "PM": "SPM",
    "PN": "PCN",
    "PR": "PRI",
    "PS": "PSE",
    "PT": "PRT",
    "PW": "PLW",
    "PY": "PRY",
    "QA": "QAT",
    "RE": "REU",
    "RO": "ROU",
    "RS": "SRB",
    "RU": "RUS",
    "RW": "RWA",
    "SA": "SAU",
    "SB": "SLB",
    "SC": "SYC",
    "SD": "SDN",
    "SE": "SWE",
    "SG": "SGP",
    "SH": "SHN",
    "SI": "SVN",
    "SJ": "SJM",
    "SK": "SVK",
    "SL": "SLE",
    "SM": "SMR",
    "SN": "SEN",
    "SO": "SOM",
    "SR": "SUR",
    "SS": "SSD",
    "ST": "STP",
    "SV": "SLV",
    "SX": "SXM",
    "SY": "SYR",
    "SZ": "SWZ",
    "TC": "TCA",
    "TD": "TCD",
    "TF": "ATF",
    "TG": "TGO",
    "TH": "THA",
    "TJ": "TJK",
    "TK": "TKL",
    "TL": "TLS",
    "TM": "TKM",
    "TN": "TUN",
    "TO": "TON",
    "TR": "TUR",
    "TT": "TTO",
    "TV": "TUV",
    "TW": "TWN",
    "TZ": "TZA",
    "UA": "UKR",
    "UG": "UGA",
    "UM": "UMI",
    "US": "USA",
    "UY": "URY",
    "UZ": "UZB",
    "VA": "VAT",
    "VC": "VCT",
    "VE": "VEN",
    "VG": "VGB",
    "VI": "VIR",
    "VN": "VNM",
    "VU": "VUT",
    "WF": "WLF",
    "WS": "WSM",
    "YE": "YEM",
    "ZA": "ZAF",
    "ZM": "ZMB",
    "ZW": "ZWE",
    }

    df['Code']  = df["Code"].map(lambda x: alpha2_to_alpha3.get(x) if alpha2_to_alpha3.get(x) else x)

    mask = df['Code'].map(lambda x: len(x) == 2)
    df = df[~mask]

    log.info('Transformation complete')
    return df

# -------------------- Load --------------------
def load(df: pd.DataFrame, engine: Engine):

    dtype_mapping = {'Code':NVARCHAR(None), 'Name':NVARCHAR(None), 'NameAr':NVARCHAR(None)}
    # max_id = df['OldCountryID'].max()

    try:
        with engine.begin() as conn:  # Transaction-safe

            conn.execute(text("""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.columns
                    WHERE Name = 'Code'
                    AND Object_ID = Object_ID('app.Countries')
                )
                BEGIN
                    ALTER TABLE app.Countries
                    ADD Code VARCHAR(3) NULL;
                END
            """))
            log.info("Verified/Added Code column.")

            df.to_sql('Countries', con=conn, schema='app', if_exists='append', index=False, dtype=dtype_mapping) # type: ignore
            log.info(f'dbo.Country loaded successfully')

    except Exception as e:
        log.error(f'Failed to load dbo.Country: {e}')
        raise

# -------------------- Main --------------------
def main():
    source = source_db_conn()
    target = target_db_conn()
    df = extract(source, target)
    if df.empty:
        log.info('No new data to load.')
        return
    df = transform(df)
    # return
    load(df, target)

if __name__ == '__main__':
    main()







