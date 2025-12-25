import logging
import pandas as pd

def get_logger(name: str):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    return logging.getLogger(name)
# %(funcName)s:%(lineno)d | 




def clean_contact(num: str) -> str | None:
    if pd.isna(num): return None
    num = num.replace(' ','')
    while num.startswith('0'): num = num[1:]
    if num.startswith('5'): return '+966'+num[:12]
    elif num.startswith('9'): return '+'+num[:14]
    return num[:15]