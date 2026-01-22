import os
import re
import logging
import hashlib
import base64
import struct
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



def fill_useraccounts(conn: Connection, account_id: int, user_ids: list):

    values = ','.join([f'({account_id}, {user_id})' for user_id in user_ids])

    conn.execute(text(f''' 
            MERGE app.UserAccounts o
            USING(
                    VALUES {values}
            ) n (AccountID, UserID)
            ON o.AccountID = n.AccountID AND o.UserID = n.UserID
            WHEN NOT MATCHED THEN
                INSERT (AccountID, UserID) VALUES (n.AccountID, n.UserID);
        ''')) 


# TIME_REGEX = re.compile(
#     r'\b\d{1,2}(:\d{2})?\s*(AM|PM)?\b',
#     re.I
# )

# def normalize_time(t: str) -> str | None:
#     """
#     Convert a time string to 24h HH:MM format.
#     Returns None if invalid or ambiguous.
#     """
#     t = t.strip().upper()

#     # Reject ambiguous plain numbers (e.g. "9")
#     if re.fullmatch(r'\d{1,2}', t):
#         return None

#     # Add minutes if missing (e.g. "9 AM" -> "9:00 AM")
#     if re.fullmatch(r'\d{1,2}\s*(AM|PM)', t):
#         t = t.replace(' ', ':00 ')

#     try:
#         # 12-hour format
#         if 'AM' in t or 'PM' in t:
#             return datetime.strptime(t, '%I:%M %p').strftime('%H:%M')

#         # 24-hour format
#         return datetime.strptime(t, '%H:%M').strftime('%H:%M')

#     except ValueError:
#         return None


# def normalize_ranges(text: str) -> str | None:
#     """
#     Extract all valid times from a string and return
#     a normalized 24h range string joined by '-'.
#     """
#     times = []

#     for match in TIME_REGEX.finditer(text):
#         normalized = normalize_time(match.group())
#         if normalized:
#             times.append(normalized)

#     return "-".join(times) if times else None





# TIME_REGEX = re.compile(
#     r'\b\d{1,2}(:\d{2})?\s*(AM|PM)?\b',
#     re.I
# )

# def normalize_time(t):
#     t = t.strip().upper()

#     # Add :00 if missing minutes (e.g. "9 AM")
#     if re.match(r'^\d+\s*(AM|PM)$', t):
#         t = t.replace(' ', ':00 ')

#     # 12-hour format
#     if 'AM' in t or 'PM' in t:
#         return datetime.strptime(t, '%I:%M %p').strftime('%H:%M')

#     # 24-hour format
#     return datetime.strptime(t, '%H:%M').strftime('%H:%M')


# def normalize_ranges(text):


#     matches = TIME_REGEX.finditer(text)


#     # Split on dash, comma, or multiple spaces
#     parts = re.split(r'\s*-\s*|\s*,\s*', text)

#     times = []
#     for part in parts:
#         # Extract time tokens
#         matches = re.findall(r'\d{1,2}(:\d{2})?\s*(AM|PM)?', part, re.I)
#         if matches:
#             times.append(normalize_time(part))

#     return "-".join(times)