import pandas as pd
import os
import json
import yfinance as yf
from difflib import get_close_matches

# Paths
PFF_CSV = r"c:\Users\orhan.bolek\Desktop\PFF_holdings.csv"
MASTER_XLSX = r"c:\Users\orhan.bolek\Desktop\Updated Master List.xlsx"
OUTPUT_CSV = "pff_holdings_tickers.csv"
SECTOR_MAP_FILE = "sector_map.json"
TICKERS_FILE = "tickers.txt"

def load_master_list():
    print(f"[*] Loading Master List from {MASTER_XLSX}...")
    try:
        df = pd.read_excel(MASTER_XLSX)
        
        # 1. Filter out inactive tickers
        # Keywords: Mature, Redeem, Redeemed, Delist, Susp
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

        # 2. Clean price column
        def clean_price(val):
            if pd.isna(val): return 0.0
            if isinstance(val, str):
                val = val.replace('$', '').replace(',', '').strip()
            try: return float(val)
            except: return 0.0
        
        df['CleanPrice'] = df['Current Price'].apply(clean_price)
        return df
    except Exception as e:
        print(f"[!] Error loading Master List: {e}")
        return None

def find_best_ticker_match(name, price, base_ticker, master_df, used_tickers):
    """
    Find the best AVAILABLE ticker match for a given name and price from the Master List.
    """
    # 1. Filter by Base Ticker first (most reliable)
    candidates = master_df[master_df['Ticker'].apply(lambda t: str(t).startswith(f"{base_ticker}-") or str(t) == base_ticker)].copy()
    
    if candidates.empty:
        # Fallback to Issuer name fuzzy match if no ticker match
        issuers = master_df['Issuer'].unique()
        close_matches = get_close_matches(name.upper(), [str(i).upper() for i in issuers], n=1, cutoff=0.6)
        if close_matches:
            matched_issuer = close_matches[0]
            candidates = master_df[master_df['Issuer'].str.upper() == matched_issuer].copy()
    
    if candidates.empty:
        return None, None

    # 2. Exclude already used tickers to prevent duplicates
    candidates = candidates[~candidates['Ticker'].isin(used_tickers)].copy()
    
    if candidates.empty:
        # If all candidates are used, we might have a duplicate in PFF or insufficient Master List data
        # In this case, we return None to avoid incorrect duplicate assignment
        return None, None
    
    # 3. Match by price among remaining candidates
    candidates['diff'] = (candidates['CleanPrice'] - price).abs()
    
    tolerance = max(4.0, price * 0.15) if price < 100 else price * 0.10
    candidates = candidates[candidates['diff'] <= tolerance]
    
    if candidates.empty:
        return None, None
        
    best = candidates.sort_values('diff').iloc[0]
    return best['Ticker'], best['Sector']

def process():
    print("=" * 60)
    print("FIXED PFF HOLDINGS PROCESSOR")
    print("=" * 60)
    
    if not os.path.exists(PFF_CSV):
        print(f"[!] Error: {PFF_CSV} not found.")
        return

    master_df = load_master_list()
    if master_df is None:
        return

    # Load existing sector map
    sector_map = {}
    if os.path.exists(SECTOR_MAP_FILE):
        with open(SECTOR_MAP_FILE, 'r', encoding='utf-8') as f:
            sector_map = json.load(f)

    # Read PFF_holdings.csv
    header_idx = -1
    with open(PFF_CSV, 'r', encoding='utf-8', errors='replace') as f:
        lines = f.readlines()
        for i, line in enumerate(lines):
            if 'Ticker,Name,Sector' in line:
                header_idx = i
                break
    
    if header_idx == -1:
        print("[!] Error: Could not find header row in PFF CSV.")
        return

    df_pff = pd.read_csv(PFF_CSV, skiprows=header_idx)
    
    results = []
    new_sector_mappings = {}
    used_master_tickers = set()

    print(f"[*] Processing {len(df_pff)} rows from {PFF_CSV}...")
    
    for _, row in df_pff.iterrows():
        raw_ticker = str(row.get('Ticker', '-')).strip()
        name = str(row.get('Name', 'N/A'))
        
        # 1. Filter out non-holding rows (e.g., disclaimer text)
        if raw_ticker == '-' or pd.isna(raw_ticker) or "Ticker" in raw_ticker:
            continue
        
        # Stricter check for legal text in ticker or name
        if "CONTENT CONTAINED HEREIN" in name.upper() or "IS OWNED OR LICENSED" in name.upper():
            print(f"[*] Skipping disclaimer row found at: {name[:50]}...")
            continue
        
        if len(raw_ticker) > 20 or len(name) > 300: # Typical disclaimer or corrupted row
            print(f"[*] Skipping suspicious long row: {raw_ticker[:10]}... | {name[:50]}...")
            continue

        try:
            price_str = str(row.get('Price', '0')).replace(',', '')
            price = float(price_str)
            weight = float(str(row.get('Weight (%)', '0')).replace(',', ''))
            market_value = float(str(row.get('Market Value', '0')).replace(',', ''))
            quantity = float(str(row.get('Quantity', '0')).replace(',', ''))
        except:
            price, weight, market_value, quantity = 0.0, 0.0, 0.0, 0.0
            continue

        base_ticker = raw_ticker.split('-')[0].strip().upper()
        
        # Resolve via Master List with Uniqueness check
        resolved_ticker, resolved_sector = find_best_ticker_match(name, price, base_ticker, master_df, used_master_tickers)
        
        if resolved_ticker:
            display_ticker = resolved_ticker
            sector = resolved_sector
            used_master_tickers.add(resolved_ticker)
        else:
            # Fallback Discovery or keep raw
            display_ticker = raw_ticker
            sector = row.get('Sector', 'Other')
            # If raw ticker is in Master List but wasn't matched (maybe due to price), 
            # we should still try to get the sector from Master List if possible
            if display_ticker in master_df['Ticker'].values:
                sector = master_df[master_df['Ticker'] == display_ticker]['Sector'].iloc[0]

        if display_ticker and display_ticker != '-':
            if display_ticker not in sector_map:
                new_sector_mappings[display_ticker] = sector
                sector_map[display_ticker] = sector

        results.append({
            'Base Ticker': display_ticker.split('-')[0] if '-' in display_ticker else display_ticker,
            'Company Name': name,
            'Preferred Stock': display_ticker,
            'Last Price': price,
            'Full Name': name,
            'Weight (%)': weight,
            'Market Value': market_value,
            'Quantity': quantity,
            'Original Name': name
        })

    # Export CSV
    if not results:
        print("[!] No results collected.")
        return

    df_results = pd.DataFrame(results)
    if 'Weight (%)' in df_results.columns:
        df_results.sort_values('Weight (%)', ascending=False, inplace=True)
        
    df_results.to_csv(OUTPUT_CSV, index=False)
    print(f"[+] Exported {len(df_results)} rows to {OUTPUT_CSV}")

    # Update sector_map.json
    if new_sector_mappings:
        print(f"[*] Adding {len(new_sector_mappings)} new sector mappings to {SECTOR_MAP_FILE}")
        with open(SECTOR_MAP_FILE, 'w', encoding='utf-8') as f:
            json.dump(sector_map, f, indent=4)

if __name__ == "__main__":
    process()
