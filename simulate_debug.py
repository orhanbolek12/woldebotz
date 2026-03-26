import yfinance as yf
import pandas as pd
import numpy as np
import logging
from pre_exdiv_logic import fetch_pre_exdiv_momentum
import json
import os

# Setup detailed logging to see drops
import sys
root = logging.getLogger()
root.setLevel(logging.WARNING) # We specifically want warnings for drop reasons
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.WARNING)
formatter = logging.Formatter('%(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)

def get_sector_map():
    if os.path.exists('sector_map.json'):
        with open('sector_map.json', 'r') as f:
            return json.load(f)
    return {}

def test_scan():
    tickers = ['PDI', 'PDO', 'ADX', 'ACV', 'USA', 'ASG', 'AEF']
    sector_map = get_sector_map()
    
    print(f"Testing tickers: {tickers}")
    print(f"Sector map coverage: {sum(1 for t in tickers if t in sector_map)}/{len(tickers)}")
    
    results = fetch_pre_exdiv_momentum(
        tickers=tickers,
        lookahead_days=30,
        min_entry_day=2,
        max_entry_day=10,
        min_score=60,
        min_win_rate=0.40,
        min_hist_alpha=0.001,
        min_volume_daily=10000, # Lower for debug
        show_estimated=True,
        sector_map=sector_map
    )
    
    print("\nScan Summary:")
    print(f"Total Scanned: {results['total_scanned']}")
    print(f"Total Found: {results['total_found']}")
    
    if results['results']:
        for r in results['results']:
            print(f"Result: {r['ticker']} | Score: {r['composite_score']} | Ex-Date: {r['ex_date']}")
    else:
        print("No results found in simulation.")

if __name__ == "__main__":
    test_scan()
