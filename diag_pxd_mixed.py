import yfinance as yf
from pre_exdiv_logic import fetch_pre_exdiv_momentum
import json

# Mixed tickers: CEFs and regular stocks
tickers = ["ECC", "PDI", "AAPL", "MSFT", "O", "MAIN"]

# Mock sector map
sector_map = {t: "sector_equity" for t in tickers}

print(f"Running diagnostic scan for: {tickers}")

# Current defaults in index.html
params = {
    "lookahead_days": 30,
    "min_entry_day": 2,
    "max_entry_day": 10,
    "min_score": 50,
    "min_win_rate": 0.40,
    "min_hist_alpha": 0.0005,
    "min_volume_daily": 100000,
    "show_estimated": True,
    "sector_map": sector_map
}

gen = fetch_pre_exdiv_momentum(tickers=tickers, **params)

for event in gen:
    if event["type"] == "progress":
        # Only print skip messages or final
        if "Skipped" in event["msg"]:
            print(f"Progress: {event['msg']}")
    elif event["type"] == "result":
        print(f"FOUND: {event['data']['ticker']} - Score: {event['data']['composite_score']} - Ex-Date: {event['data']['ex_date']}")
    elif event["type"] == "final":
        print(f"\nFinal results: {len(event['data']['results'])} found out of {len(tickers)} scanned.")
