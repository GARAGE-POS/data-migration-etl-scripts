import json
import re
import logging
import pandas as pd
from datetime import datetime
from sqlalchemy import Connection, Engine, text

def get_logger(name: str) -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    return logging.getLogger(name)
# %(funcName)s:%(lineno)d | 




def clean_contact(num: str) -> str | None:
    if pd.isna(num): 
        return None
    num = ''.join(filter(lambda x: x in '+1234567890', list(num)))
    if num == '':
        return None
    while num.startswith('0'): 
        num = num[1:]
    if num.startswith('5'): 
        return '+966'+num[:12]
    elif num.startswith('9'): 
        return '+'+num[:14]
    return num[:15]


def parse_date(s):
    formats = [
        '%b %d %Y %I:%M%p',        # May 29 2020 8:39AM
        '%m/%d/%Y %I:%M:%S %p',    # 3/3/2025 1:28:20 PM
    ]
    for fmt in formats:
        try:
            return pd.to_datetime(s, format=fmt)
        except (ValueError, TypeError):
            continue
    return pd.NaT


def fix_order_checkout(row: pd.Series) -> pd.Series:
    a = 1 if row['Subtotal'] else 0
    b = 1 if row['GrandTotal'] else 0
    c = 1 if row['ItemTaxTotal'] else 0
    if a + b + c == 2:
        if a == 0:
            row['Subtotal'] = row['GrandTotal'] - row['ItemTaxTotal'] + row['OrderDiscountTotal']
        elif b == 0:
            row['GrandTotal'] = row['Subtotal'] - row['OrderDiscountTotal'] + row['ItemTaxTotal'] 
        else:
            row['ItemTaxTotal'] = row['GrandTotal'] - row['Subtotal'] + row['OrderDiscountTotal']
        # row['OrderDiscountPercent'] = (row['ItemTaxTotal']/row['GrandTotal']) * 100
    return row




TIME_REGEX = re.compile(r'\b\d{1,2}(:\d{2})?\s*(AM|PM)?\b', re.I)

def normalize_time(t: str) -> str | None:
    t = t.strip().upper()
    
    # Insert space before AM/PM if missing (e.g. 8:30AM -> 8:30 AM)
    t = re.sub(r'(\d)(AM|PM)$', r'\1 \2', t, flags=re.I)
    
    # Reject ambiguous numbers like "9"
    if re.fullmatch(r'\d{1,2}', t):
        return None
    
    # Add minutes if missing (e.g. 9 AM -> 9:00 AM)
    if re.fullmatch(r'\d{1,2}\s*(AM|PM)', t):
        t = t.replace(' ', ':00 ')
    
    try:
        if 'AM' in t or 'PM' in t:
            return datetime.strptime(t, '%I:%M %p').strftime('%H:%M')
        return datetime.strptime(t, '%H:%M').strftime('%H:%M')
    except ValueError:
        return None


def normalize_ranges(text: str) -> str | None:
    if not isinstance(text, str):
        return None
    
    # Replace fancy dashes with normal hyphen
    text = re.sub(r'[–—−]', '-', text)

    # Replace narrow or weird spaces with normal space
    text = re.sub(r'[\u202F\u00A0]', ' ', text)

    times = []
    for match in TIME_REGEX.finditer(text):
        normalized = normalize_time(match.group())
        if normalized:
            times.append(normalized)
    
    if not times:
        return None
    
    return "-".join(times)




def fix_app_sources(text: str) -> str:
    map_ = {
        'snap':'snapchat',
        'insta':'instagram',
        'سناب':'snapchat',
        'friend':'wordofmouth',
        'انست':'instagram',
        'تيكتوك':'tiktok',
        'twitter':'x(twitter)',
    }

    if text.lower().strip() == 'x':
        return 'x(twitter)'
    
    for k, v in map_.items():
        if k in text:
            return v

    return text



def fill_useraccounts(conn: Connection, df: pd.DataFrame):

    df = df[['AccountID', 'UserID']]

    values = ','.join([f'({account_id}, {user_id})' for account_id, user_id in df.values.tolist()])

    conn.execute(text(f''' 
            MERGE app.UserAccounts o
            USING(
                    VALUES {values}
            ) n (AccountID, UserID)
            ON o.AccountID = n.AccountID AND o.UserID = n.UserID
            WHEN NOT MATCHED THEN
                INSERT (AccountID, UserID) VALUES (n.AccountID, n.UserID);
        ''')) 


def filter_subscriptions(text: str) -> bool:

    keywords = ['Garage GO - Software Bundle', 'Garage POS - Starter', 'Garage POS Starter', 'Karage POS', 'Software Bundle']

    for keyword in keywords:
        if keyword in text:
            return True
    
    return False


def get_last_ingested(account_id: int, table: str) -> int:
    with open('last_ingested.json', 'r') as f:
        data = json.load(f)
    return data.get(str(account_id), {}).get(table, 0)


def update_last_ingested(account_id: int, table: str, row_id: int) -> None:
    with open('last_ingested.json', 'r') as f:
        data = json.load(f)

    if not data.get(str(account_id)):
        data[str(account_id)] = {}

    data[str(account_id)][table] = row_id

    with open('last_ingested.json', 'w') as f:
        json.dump(data, f, indent=2)


def time_line(time: str) -> list[dict[str, str]] | None:

    if isinstance(time, str):

        schedule = time.split('-')

        if len(schedule) == 2:
            return [{'opening': schedule[0], 'closing':schedule[1]},{'opening':'', 'closing':''}] 
        
        if len(schedule) == 4:
            return [{'opening': schedule[0], 'closing':schedule[1]},{'opening':schedule[2], 'closing':schedule[3]}] 
