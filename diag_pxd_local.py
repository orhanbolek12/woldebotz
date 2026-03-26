import yfinance as yf
from pre_exdiv_logic import fetch_pre_exdiv_momentum
import json
import os

# Sample tickers from CEF list
tickers = ["ECC", "PDI", "BST", "THQ", "THW"]

# Mock sector map
sector_map = {t: "sector_equity" for t in tickers}

print(f"Running diagnostic scan for: {tickers}")

# Run with defaults from index.html
# entryMin = 2, entryMax = 10, minWinRate = 0.40, minAlpha = 0.001, minVol = 100000, showEstimated = False, minScore = 60
gen = fetch_pre_exdiv_momentum(
    tickers=tickers,
    lookahead_days=30,
    min_entry_day=2,
    max_entry_day=10,
    min_score=60,
    min_win_rate=0.40,
    min_hist_alpha=0.001,
    min_volume_daily=100000,
    show_estimated=False,
    sector_map=sector_map
)

for event in gen:
    if event["type"] == "progress":
        print(f"Progress: {event['msg']}")
    elif event["type"] == "result":
        print(f"FOUND: {event['data']['ticker']} - Score: {event['data']['composite_score']}")
    elif event["type"] == "final":
        print(f"Final results: {len(event['data']['results'])} found out of {len(tickers)} scanned.")

print("\n--- RETRY WITH show_estimated=True ---")

gen = fetch_pre_exdiv_momentum(
    tickers=tickers,
    lookahead_days=30,
    min_entry_day=2,
    max_entry_day=10,
    min_score=60,
    min_win_rate=0.40,
    min_hist_alpha=0.001,
    min_volume_daily=100000,
    show_estimated=True,
    sector_map=sector_map
)

for event in gen:
    if event["type"] == "progress":
        print(f"Progress: {event['msg']}")
    elif event["type"] == "result":
        print(f"FOUND: {event['data']['ticker']} - Score: {event['data']['composite_score']}")
    elif event["type"] == "final":
        print(f"Final results: {len(event['data']['results'])} found out of {len(tickers)} scanned.")
