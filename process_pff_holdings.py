import pandas as pd
import os
import json
import yfinance as yf
from difflib import get_close_matches
import time

# Paths
PFF_CSV = r"c:\Users\orhan.bolek\Desktop\PFF_holdings.csv"
MASTER_XLSX = r"c:\Users\orhan.bolek\Desktop\Updated Master List.xlsx"
OUTPUT_CSV = "pff_holdings_tickers.csv"
SECTOR_MAP_FILE = "sector_map.json"
MANUAL_DISCOVERY_FILE = "discovered_tickers.json"

def load_master_list():
    print(f"[*] Loading Master List from {MASTER_XLSX}...")
    try:
        df = pd.read_excel(MASTER_XLSX)
        inactive_keywords = ['REDEEM', 'MATURE', 'DELIST', 'SUSP']
        
        def is_inactive(row):
            for col in ['Current Price', 'Current Yield']:
                val = str(row.get(col, '')).upper()
                if any(kw in val for kw in inactive_keywords):
                    return True
            return False
            
        initial_count = len(df)
        df = df[~df.apply(is_inactive, axis=1)].copy()
        print(f"[*] Filtered out {initial_count - len(df)} inactive tickers.")

        def clean_price(val):
            if pd.isna(val) or str(val).strip() == '': return 0.0
            if isinstance(val, str):
                val = val.replace('$', '').replace(',', '').strip()
            try: return float(val)
            except: return 0.0
        
        df['CleanPrice'] = df['Current Price'].apply(clean_price)
        return df
    except Exception as e:
        print(f"[!] Error loading Master List: {e}")
        return None

def throttled_discover_via_yfinance(base_ticker, target_price):
    """
    EXTREMELY CONSERVATIVE Discovery for unknown tickers.
    Only tried for high-weight items.
    """
    print(f"  [?] Throttled discovery for {base_ticker} at ${target_price}...")
    
    # Try ONLY -PA and -A to minimize requests
    suffixes = ["-PA", "-A"]
    
    best_match = None
    min_diff = 999
    
    for suffix in suffixes:
        symbol = f"{base_ticker}{suffix}"
        try:
            # Add significant delay to protect IP
            time.sleep(10)
            
            # Use Ticker.history for slightly more reliable price in some regions
            t = yf.Ticker(symbol)
            hist = t.history(period='1d')
            if not hist.empty:
                price = round(float(hist['Close'].iloc[-1]), 2)
                diff = abs(price - target_price)
                
                # Broad tolerance for discovery
                tolerance = max(3.0, target_price * 0.15)
                if diff <= tolerance:
                    if diff < min_diff:
                        min_diff = diff
                        # Normalize to -A format
                        letter = suffix[-1]
                        best_match = f"{base_ticker}-{letter}"
                        print(f"    [OK] Discovered {best_match} via Yahoo (Price: {price:.2f})")
        except:
            continue
            
    return best_match

def process():
    print("=" * 60)
    print("IP-SAFE PFF HOLDINGS PROCESSOR")
    print("=" * 60)
    
    master_df = load_master_list()
    if master_df is None: return

    # Load manual discovery map
    manual_map = {}
    if os.path.exists(MANUAL_DISCOVERY_FILE):
        print(f"[*] Loading Manual Discovery Map from {MANUAL_DISCOVERY_FILE}...")
        with open(MANUAL_DISCOVERY_FILE, 'r') as f:
            manual_map = json.load(f)

    # Load PFF data
    header_idx = -1
    with open(PFF_CSV, 'r', encoding='utf-8', errors='replace') as f:
        lines = f.readlines()
        for i, line in enumerate(lines):
            if 'Ticker,Name,Sector' in line:
                header_idx = i
                break
    
    if header_idx == -1: return
    df_pff = pd.read_csv(PFF_CSV, skiprows=header_idx)
    
    initial_holdings = []
    used_tickers = set()

    print(f"[*] Processing {len(df_pff)} rows...")

    for idx, row in df_pff.iterrows():
        raw_t = str(row.get('Ticker', '-')).strip()
        name = str(row.get('Name', 'N/A'))
        if raw_t == '-' or pd.isna(raw_t) or "Ticker" in raw_t or len(raw_t) > 15: continue
        
        # Stricter disclaimer/metadata cleaning
        if any(kw in name.upper() for kw in ["BLACKROCK", "OWNED OR LICENSED", "CONTENT CONTAINED HEREIN"]):
            continue

        try:
            p = float(str(row.get('Price', '0')).replace(',', ''))
            w = float(str(row.get('Weight (%)', '0')).replace(',', ''))
            mv = float(str(row.get('Market Value', '0')).replace(',', ''))
            q = float(str(row.get('Quantity', '0')).replace(',', ''))
        except: continue

        base = raw_t.split('-')[0].strip().upper()
        
        # 1. Manual Map Check (Highest Priority & Safest)
        # Check for exact price match or close match in manual map
        resolved = None
        sector = "Other"
        
        # Try finding in manual map (using formatted key base_price)
        mkey = f"{base}_{p:.2f}"
        if mkey in manual_map:
            resolved = manual_map[mkey]
            # print(f"    [MANUAL] Resolved {mkey} -> {resolved}")
        
        # 2. Master List Match (Second Priority)
        if not resolved:
            candidates = master_df[master_df['Ticker'].apply(lambda t: str(t).startswith(f"{base}-") or str(t) == base)].copy()
            if not candidates.empty:
                candidates = candidates[~candidates['Ticker'].isin(used_tickers)].copy()
                if not candidates.empty:
                    candidates['diff'] = (candidates['CleanPrice'] - p).abs()
                    tol = max(4.0, p * 0.15) if p < 100 else p * 0.10
                    valid = candidates[candidates['diff'] <= tol]
                    if not valid.empty:
                        best = valid.sort_values('diff').iloc[0]
                        resolved = best['Ticker']
                        sector = best['Sector']

        # 3. Throttled Discovery (Third Priority - Only for High Weight > 0.3%)
        if not resolved and w > 0.3:
            resolved = throttled_discover_via_yfinance(base, p)
            if resolved:
                # Add to manual map for future runs to save requests
                manual_map[f"{base}_{p:.2f}"] = resolved
                with open(MANUAL_DISCOVERY_FILE, 'w') as f:
                    json.dump(manual_map, f, indent=4)

        final_res = resolved if resolved else raw_t
        if resolved: used_tickers.add(resolved)

        initial_holdings.append({
            'ticker': final_res, 'name': name, 'p': p, 'w': w, 'mv': mv, 'q': q, 'sector': sector
        })

    # 4. Finalize Output
    sector_map = {}
    if os.path.exists(SECTOR_MAP_FILE):
        with open(SECTOR_MAP_FILE, 'r') as f: sector_map = json.load(f)

    results = []
    for h in initial_holdings:
        ticker = h['ticker']
        sec = sector_map.get(ticker, h['sector'])
        
        results.append({
            'Base Ticker': ticker.split('-')[0],
            'Company Name': h['name'],
            'Preferred Stock': ticker,
            'Last Price': h['p'],
            'Full Name': h['name'],
            'Weight (%)': h['w'],
            'Market Value': h['mv'],
            'Quantity': h['q']
        })
        sector_map[ticker] = sec

    pd.DataFrame(results).sort_values('Weight (%)', ascending=False).to_csv(OUTPUT_CSV, index=False)
    with open(SECTOR_MAP_FILE, 'w') as f: json.dump(sector_map, f, indent=4)
    print(f"[+] Final Export: {len(results)} holdings saved to {OUTPUT_CSV}")

if __name__ == "__main__":
    process()
