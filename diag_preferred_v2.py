
import yfinance as yf
from datetime import datetime
import pandas as pd
import logging

tickers_to_test = ["CCID", "PSEC-A", "GS-D", "GS-C", "GS-A", "LANDO", "LANDP", "GOODO", "ADC-A", "GOODN"]

def get_candidates(raw_ticker):
    base, suffix = "", ""
    if '-' in raw_ticker:
        parts = raw_ticker.split('-')
        base, suffix = parts[0], parts[1]
    elif len(raw_ticker) > 3 and raw_ticker[-1].isalpha():
         if len(raw_ticker) == 4:
             base = raw_ticker[:3]
             suffix = raw_ticker[3]
         elif len(raw_ticker) == 5:
             base = raw_ticker[:4]
             suffix = raw_ticker[4]
    
    cands = [raw_ticker]
    if base and suffix:
        cands.extend([
            f"{base}-P{suffix}", 
            f"{base}.PR{suffix}", 
            f"{base}P-{suffix}", 
            f"{base}-{suffix}", 
            f"{base}{suffix}",
            f"{base}-P-{suffix}"
        ])
    return list(dict.fromkeys(cands)) # Unique

print(f"{'Raw':<8} | {'Candidate':<10} | {'Ex-Date':<12} | {'Divs'}")
print("-" * 50)

for raw in tickers_to_test:
    cands = get_candidates(raw)
    found_any = False
    for c in cands:
        try:
            t = yf.Ticker(c)
            ex_date = "None"
            ex_ts = t.info.get("exDividendDate")
            if ex_ts:
                ex_date = datetime.fromtimestamp(ex_ts).date().strftime('%Y-%m-%d')
            
            div_count = len(t.dividends)
            if ex_date != "None" or div_count > 0:
                print(f"{raw:<8} | {c:<10} | {ex_date:<12} | {div_count}")
                found_any = True
        except:
            pass
    if not found_any:
        print(f"{raw:<8} | {'FAILED':<10} | {'None':<12} | 0")
    print("-" * 50)
