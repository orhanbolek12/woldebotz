
import yfinance as yf
from datetime import datetime
import pandas as pd

tickers = [
    "AVK", "CLM", "CRF", "DTF", "EFR", "EIM", "EOI", "EOS", "EOT", "ETB", "ETG", "ETJ", "ETO", "ETV", 
    "ETW", "ETX", "ETY", "EVG", "EVT", "EXG", "GBAB", "GDV", "GGN", "GGT", "GLU", "GNT", "GOF", "GUG", 
    "GUT", "IIM", "IQI", "JFR", "JGH", "JLS", "JMM", "JOF", "JPC", "JQC", "JRI", "KYN", "MCN", "MMD", 
    "NAC", "NAD", "NAN", "NAZ", "NBB", "NBXG", "NCA", "NDMO", "NEA", "NHS", "NIM", "NKX", "NMAI", 
    "NMCO", "NMI", "NML", "NMS", "NMT", "NMZ", "NNY", "NOM", "NPCT", "NPFD", "NPV", "NQP", "NRK", 
    "NRO", "NUV", "NUW", "NVG", "NXJ", "NXP", "NZF", "OCCI", "OIA", "OPP", "PGZ", "RFM", "RFMZ", 
    "RMI", "RIV", "RMM", "RMMZ", "RSF", "SOR", "SPMC", "TSI", "VBF", "VCV", "VGM", "VKI", "VKQ", 
    "VLT", "VMO", "VPV", "VTN", "VVR", "XFLT"
]

print(f"Checking {len(tickers)} tickers...")
print("Ticker | Ex-Date (Info) | Ex-Date (Calendar) | Info TS")
print("-" * 60)

found_count = 0
today = datetime.now().date()

for t in tickers:
    try:
        tick = yf.Ticker(t)
        
        # Method 1: Info (Timestamp)
        ex_ts = tick.info.get("exDividendDate")
        ex_date_info = "None"
        if ex_ts:
            ex_date_info = datetime.fromtimestamp(ex_ts).date()
            
        # Method 2: Calendar
        cal = tick.calendar
        ex_date_cal = "None"
        if cal and 'Ex-Dividend Date' in cal:
             val = cal['Ex-Dividend Date']
             # Calendar can return a list/series or single value
             if isinstance(val, (list, pd.Series)) and len(val) > 0:
                 ex_date_cal = val[0]
             else:
                 ex_date_cal = val

        print(f"{t:<6} | {ex_date_info} | {ex_date_cal} | {ex_ts}")
        
        if ex_date_info == today or ex_date_cal == today:
            found_count += 1
            
    except Exception as e:
        print(f"{t:<6} | Error: {e}")

print("-" * 60)
print(f"Server Date: {today}")
print(f"Matches for today: {found_count}")
