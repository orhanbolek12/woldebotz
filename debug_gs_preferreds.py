
import yfinance as yf
from datetime import datetime
import pandas as pd

gs_tickers = ["GS-PA", "GS-PD"]

for sym in gs_tickers:
    print(f"\n--- Checking {sym} ---")
    t = yf.Ticker(sym)
    
    # Check Info
    info = t.info
    ex_ts = info.get("exDividendDate")
    if ex_ts:
        print(f"Ex-Date (Info): {datetime.fromtimestamp(ex_ts).date()}")
    else:
        print("Ex-Date (Info): None")
        
    # Check Calendar
    cal = t.calendar
    print(f"Calendar: {cal}")
    
    # Check History (App uses 3mo)
    hist = t.history(period="3mo")
    print(f"History (3mo) Rows: {len(hist)}")
    if not hist.empty:
        print(f"Last Price: {hist['Close'].iloc[-1]}")
    
    # Check Dividends
    divs = t.dividends
    print(f"Dividends found: {len(divs)}")
    if not divs.empty:
        print(f"Last Div Date: {divs.index[-1]}")
