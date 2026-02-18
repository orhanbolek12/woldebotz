
import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd
import logging

logging.basicConfig(level=logging.DEBUG)

tickers = ["CCID", "GS-D", "GS-A", "GOODO"]

def parse_ticker_yf(raw_ticker):
    if '-' in raw_ticker:
        parts = raw_ticker.split('-')
        if len(parts) == 2:
            base, suffix = parts
            return f"{base}-P{suffix}"
    return raw_ticker

for raw in tickers:
    print(f"\n--- Debugging {raw} ---")
    yf_sym = parse_ticker_yf(raw)
    t = yf.Ticker(yf_sym)
    
    # 1. Info/Calendar
    next_ex_date = None
    try:
        ex_ts = t.info.get("exDividendDate")
        if ex_ts:
            next_ex_date = datetime.fromtimestamp(ex_ts)
            print(f"Found via Info: {next_ex_date}")
        else:
            cal = t.calendar
            if cal and 'Ex-Dividend Date' in cal:
                val = cal['Ex-Dividend Date']
                if hasattr(val, 'iloc'): val = val.iloc[0]
                elif isinstance(val, list) and val: val = val[0]
                next_ex_date = datetime.combine(val, datetime.min.time()) if hasattr(val, 'date') else val
                print(f"Found via Calendar: {next_ex_date}")
    except Exception as e:
        print(f"Info/Cal Error: {e}")

    # 2. Estimation Fallback (simulate logic.py)
    if not next_ex_date or (isinstance(next_ex_date, datetime) and next_ex_date.date() < datetime.now().date()):
        print("Falling back to estimation...")
        divs = t.dividends
        if not divs.empty:
            last_ex = divs.index[-1].replace(tzinfo=None)
            print(f"Last Dividend Date: {last_ex}")
            freq = 91
            if len(divs) >= 2:
                freq = (divs.index[-1] - divs.index[-2]).days
                print(f"Detected Frequency: {freq} days")
            
            if freq < 20: 
                print("Frequency too low, defaulting to 91")
                freq = 91
            
            next_ex_date = last_ex + timedelta(days=freq)
            while next_ex_date.date() < datetime.now().date():
                next_ex_date += timedelta(days=freq)
            print(f"Estimated Next Ex-Date: {next_ex_date}")
        else:
            print("No dividends for estimation.")

    if next_ex_date:
        days = (next_ex_date.date() - datetime.now().date()).days
        print(f"Result: {next_ex_date.date()} ({days} days away)")
    else:
        print("Result: None")
