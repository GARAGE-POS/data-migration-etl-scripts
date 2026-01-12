import logging
import pandas as pd

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
    # elif a + b == 1:
    #     if a == 0:
    #         row['Subtotal'] = row['DiscountAmount'] + ( row['Total'] / (1 + 0.05) )
    #     else:
    #         row['Total'] = ( row['Subtotal'] - row['DiscountAmount'] ) * (1 + 0.05)
    #     row['Tax'] = row['Total'] - row['Subtotal'] 
    # return row