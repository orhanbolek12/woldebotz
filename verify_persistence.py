import json
import os
import time
from datetime import datetime, timedelta, timezone

HISTORY_FILE = 'results_history.json'
IMBALANCE_FILE = 'imbalance_history.json'

def verify_persistence():
    print("Verifying Persistence Logic...")
    
    # Check if files exist
    if not os.path.exists(HISTORY_FILE) or not os.path.exists(IMBALANCE_FILE):
        print("Error: History files missing. Run the app once to generate them.")
        return

    # Verify Prefs History
    with open(HISTORY_FILE, 'r') as f:
        data = json.load(f)
        results = data.get('results', [])
        baseline = data.get('baseline_tickers', [])
        print(f"Prefs: Found {len(results)} results and {len(baseline)} baseline stickers.")
        
        # Check for is_new consistency
        new_count = sum(1 for r in results if r.get('is_new'))
        print(f"Prefs 'New' items: {new_count}")

    # Verify Imbalance History
    with open(IMBALANCE_FILE, 'r') as f:
        data = json.load(f)
        results = data.get('results', [])
        baseline = data.get('baseline_tickers', [])
        print(f"Imbalance: Found {len(results)} results and {len(baseline)} baseline tickers.")
        
        # Check for is_new consistency
        new_count = sum(1 for r in results if r.get('is_new'))
        print(f"Imbalance 'New' items: {new_count}")

if __name__ == "__main__":
    verify_persistence()
