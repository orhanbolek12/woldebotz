"""
Verification script for baseline and new label logic
"""
import json
import os

def verify_baseline_logic():
    print("=== Baseline and 'New' Label Verification ===\n")
    
    # Check Prefs History
    if os.path.exists('results_history.json'):
        with open('results_history.json', 'r') as f:
            prefs_data = json.load(f)
            prefs_results = prefs_data.get('results', [])
            prefs_baseline = prefs_data.get('baseline_tickers', [])
            
            print(f"Smart Watchlist:")
            print(f"  - Total results: {len(prefs_results)}")
            print(f"  - Baseline tickers: {len(prefs_baseline)}")
            print(f"  - Items marked as 'New': {sum(1 for r in prefs_results if r.get('is_new'))}")
            
            if prefs_baseline:
                print(f"  - Sample baseline tickers: {prefs_baseline[:5]}")
            else:
                print(f"  - ⚠️  WARNING: Baseline is empty! All items will show as 'New' on next scan.")
            print()
    
    # Check Imbalance History  
    if os.path.exists('imbalance_history.json'):
        with open('imbalance_history.json', 'r') as f:
            imb_data = json.load(f)
            imb_results = imb_data.get('results', [])
            imb_baseline = imb_data.get('baseline_tickers', [])
            
            print(f"Imbalance AI:")
            print(f"  - Total results: {len(imb_results)}")
            print(f"  - Baseline tickers: {len(imb_baseline)}")
            print(f"  - Items marked as 'New': {sum(1 for r in imb_results if r.get('is_new'))}")
            
            if imb_baseline:
                print(f"  - Sample baseline tickers: {imb_baseline[:5]}")
            else:
                print(f"  - ⚠️  WARNING: Baseline is empty! All items will show as 'New' on next scan.")
            print()
    
    print("\n=== Expected Behavior ===")
    print("After the NEXT scan completes:")
    print("  1. Current results will become the baseline")
    print("  2. Items that appear in both scans will NOT show 'New'")
    print("  3. Only genuinely new items will show 'New'")
    print("\nTo test: Run 'Force Sync', then run it again - repeated items should lose 'New' label.")

if __name__ == '__main__':
    verify_baseline_logic()
