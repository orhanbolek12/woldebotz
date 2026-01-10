from logic import fetch_imbalance
import logging

logging.basicConfig(level=logging.DEBUG)

tickers = ["agncz"]
print(f"Testing ticker: {tickers}")

results = fetch_imbalance(tickers, days=20, min_green_bars=1, min_red_bars=1)
print("Results:", results)
