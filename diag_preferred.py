
import yfinance as yf
from datetime import datetime
import pandas as pd

# Tickers the user mentioned as missing or partially found
tickers_to_test = ["CCID", "PSEC-A", "GS-D", "GS-C", "GS-A", "LANDO", "LANDP", "GOODO", "ADC-A", "GOODN"]

def parse_ticker_yf(raw_ticker):
    """
    Standardizes ticker for Yahoo Finance.
    Converts GS-D -> GS-PD, GS-C -> GS-PC, etc.
    """
    # Preferreds: Ticker-P[Series]
    # Handle both Ticker-A and Ticker-PA formats
    if '-' in raw_ticker:
        parts = raw_ticker.split('-')
        t = parts[0]
        s = parts[1]
        if s in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q']:
            return f"{t}-P{s}"
    return raw_ticker

print(f"{'Raw':<8} | {'YF':<8} | {'Ex-Date (Info)':<15} | {'Ex-Date (Cal)':<15} | {'Error'}")
print("-" * 70)

for raw in tickers_to_test:
    yf_sym = parse_ticker_yf(raw)
    error_msg = ""
    ex_date_info = "None"
    ex_date_cal = "None"
    
    try:
        tick = yf.Ticker(yf_sym)
        # Attempt 1: Info (Timestamp)
        ex_ts = tick.info.get("exDividendDate")
        if ex_ts:
            ex_date_info = datetime.fromtimestamp(ex_ts).date().strftime('%Y-%m-%d')
            
        # Attempt 2: Calendar
        cal = tick.calendar
        if cal and 'Ex-Dividend Date' in cal:
            val = cal['Ex-Dividend Date']
            if isinstance(val, (list, pd.Series)) and len(val) > 0:
                ex_date_cal = val[0].strftime('%Y-%m-%d')
            else:
                ex_date_cal = val.strftime('%Y-%m-%d') if hasattr(val, 'strftime') else str(val)
                
    except Exception as e:
        error_msg = str(e)
        
    print(f"{raw:<8} | {yf_sym:<8} | {ex_date_info:<15} | {ex_date_cal:<15} | {error_msg}")
