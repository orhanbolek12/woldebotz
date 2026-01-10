from logic import fetch_imbalance
import logging

# Set up logging to console
logging.basicConfig(level=logging.DEBUG)

tickers = ["AGNCM", "AGNCP", "ATLCL", "ATLCP", "ATAGNCL", "BPOLCZ", "BANFP"]

print(f"Testing tickers: {tickers}")

def progress(c, t):
    print(f"Progress: {c}/{t}")

results = fetch_imbalance(tickers, 
                          days=40, 
                          min_green_bars=25, 
                          min_red_bars=25,
                          long_wick_size=0.1, 
                          short_wick_size=0.1,
                          progress_callback=progress)

print("\nResults:")
for r in results:
    print(r)
    
if not results:
    print("No results found.")
