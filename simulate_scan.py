
import sys
import os
# Mocking logging to see output
import logging
logging.basicConfig(level=logging.DEBUG)

# Add current dir to path
sys.path.append(os.getcwd())
from logic import analyze_dividend_recovery

test_tickers = ["CCID", "GS-D", "GS-A", "GOODO", "GS-C", "GOODN"]

print("--- Simulated Scan Results ---")
for t in test_tickers:
    print(f"\nProcessing {t}...")
    res = analyze_dividend_recovery(t, lookback=3, recovery_window=5)
    if 'error' in res:
        print(f"Error: {res['error']}")
    else:
        print(f"Next Ex: {res.get('next_ex_date')} ({res.get('next_div_days')} days)")
        print(f"Divs found: {len(res.get('dividends', []))}")
