
import yfinance as yf
from datetime import datetime
import pandas as pd

tickers = ['CLM', 'CRF', 'GGN', 'GGT', 'GNT', 'GUT', 'JFR', 'JMM', 'JPC', 'JQC', 'KYN', 'MCN', 'NHS', 'NRO', 'OCCI', 'OIA', 'SOR', 'SPMC', 'TSI', 'VVR', 'XFLT']

print(f"Deep debugging {len(tickers)} missing tickers...")
print("-" * 60)

for t in tickers:
    print(f"--- {t} ---")
    try:
        tick = yf.Ticker(t)
        
        # 1. Info
        try:
            ex_ts = tick.info.get("exDividendDate")
            print(f"Info TS: {ex_ts} -> {datetime.fromtimestamp(ex_ts) if ex_ts else 'None'}")
        except Exception as e:
            print(f"Info Error: {e}")
            
        # 2. Calendar
        try:
            cal = tick.calendar
            print(f"Calendar Type: {type(cal)}")
            print(f"Calendar: {cal}")
        except Exception as e:
            print(f"Calendar Error: {e}")
            
        # 3. Dividends
        try:
            divs = tick.dividends
            if not divs.empty:
                last_div = divs.index[-1]
                print(f"Last Dividend: {last_div}")
            else:
                print("No dividends found.")
        except Exception as e:
            print(f"Dividends Error: {e}")

    except Exception as e:
        print(f"General Error: {e}")
    print("\n")
