import pandas as pd
import yfinance as yf
import json
import os
import time

SOURCE_CSV = r'C:\Users\orhan\Downloads\PFF_holdings.csv'
MAP_FILE = 'pff_resolution_map.json'

def load_map():
    if os.path.exists(MAP_FILE):
        with open(MAP_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_map(data):
    with open(MAP_FILE, 'w') as f:
        json.dump(data, f, indent=4)

def deep_resolve():
    res_map = load_map()
    df = pd.read_csv(SOURCE_CSV, skiprows=9)
    
    # Filter out cash/money market
    df = df[df['Asset Class'] == 'Equity']
    
    print(f"[*] Starting deep resolution for {len(df)} holdings...")
    
    for i, row in df.iterrows():
        ticker = str(row['Ticker']).strip().upper()
        name = str(row['Name']).strip().upper()
        price = float(str(row['Price']).replace(',', ''))
        weight = float(str(row['Weight (%)']).replace(',', ''))
        
        # Unique key for resolution (Ticker + Weight + Price)
        key = f"{ticker}|{weight:.2f}|{price:.2f}"
        
        if key in res_map:
            continue
            
        print(f"[*] Searching for {name} ({ticker})...")
        try:
            # Try searching by full name
            search = yf.Search(name)
            if search.quotes:
                resolved = search.quotes[0].get('symbol', ticker)
                # Cleanup Yahoo format
                if '-P' in resolved: resolved = resolved.replace('-P', '-')
                if '.PR' in resolved: resolved = resolved.replace('.PR', '-')
                
                print(f"  [+] Resolved: {resolved}")
                res_map[key] = resolved
            else:
                # Try searching by ticker + part of name
                search = yf.Search(f"{ticker} preferred")
                if search.quotes:
                    resolved = search.quotes[0].get('symbol', ticker)
                    if '-P' in resolved: resolved = resolved.replace('-P', '-')
                    res_map[key] = resolved
                else:
                    res_map[key] = ticker # Fallback to base
        except Exception as e:
            print(f"  [!] Error searching {ticker}: {e}")
            res_map[key] = ticker
            
        # Save periodically
        if i % 5 == 0:
            save_map(res_map)
        
        time.sleep(0.5) # Avoid rate limits

    save_map(res_map)
    print("[*] Deep resolution complete.")

if __name__ == "__main__":
    deep_resolve()
