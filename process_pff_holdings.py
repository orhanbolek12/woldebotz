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
        # Columns: [' ', 'Issuer', 'Cum', 'QDI', 'Sector', 'Fix/\nFloat', 'Type', 'Ticker', 'Coupon  Percent', 'Current Price', ...]
        df = pd.read_excel(MASTER_XLSX)
        # Clean price column
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

def find_best_ticker_match(name, price, master_df):
    """
    Find the best ticker match for a given name and price from the Master List.
    """
    # 1. Try to find issuer matches
    issuers = master_df['Issuer'].unique()
    close_matches = get_close_matches(name.upper(), [str(i).upper() for i in issuers], n=1, cutoff=0.6)
    
    if not close_matches:
        # Try a substring match if fuzzy match fails
        candidates = master_df[master_df['Issuer'].str.contains(name.split()[0], na=False, case=False)]
    else:
        matched_issuer = close_matches[0]
        candidates = master_df[master_df['Issuer'].str.upper() == matched_issuer]
    
    if candidates.empty:
        return None, None
    
    # 2. Match by price among candidates
    candidates = candidates.copy()
    candidates['diff'] = (candidates['CleanPrice'] - price).abs()
    best = candidates.sort_values('diff').iloc[0]
    
    return best['Ticker'], best['Sector']

def process():
    print("=" * 60)
    print("PFF HOLDINGS PROCESSOR")
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
        with open(SECTOR_MAP_FILE, 'r') as f:
            sector_map = json.load(f)

    # Read PFF_holdings.csv
    # Find header row dynamically
    try:
        with open(PFF_CSV, 'r', encoding='utf-8', errors='replace') as f:
            lines = f.readlines()
            header_idx = -1
            for i, line in enumerate(lines):
                if 'Ticker,Name,Sector' in line:
                    header_idx = i
                    break
        
        if header_idx == -1:
            print("[!] Error: Could not find header row in PFF CSV.")
            return

        df_pff = pd.read_csv(PFF_CSV, skiprows=header_idx)
        print(f"[*] PFF CSV Columns: {df_pff.columns.tolist()}")
    except Exception as e:
        print(f"[!] Error reading PFF CSV: {e}")
        return

    results = []
    new_sector_mappings = {}

    print(f"[*] Processing {len(df_pff)} rows from {PFF_CSV}...")
    
    for _, row in df_pff.iterrows():
        raw_ticker = str(row.get('Ticker', '-')).strip()
        name = str(row.get('Name', 'N/A'))
        
        if raw_ticker == '-' or pd.isna(raw_ticker) or "Ticker" in raw_ticker:
            continue
            
        try:
            price = float(str(row.get('Price', '0')).replace(',', ''))
            weight = float(str(row.get('Weight (%)', '0')).replace(',', ''))
            market_value = float(str(row.get('Market Value', '0')).replace(',', ''))
            quantity = float(str(row.get('Quantity', '0')).replace(',', ''))
        except:
            price, weight, market_value, quantity = 0.0, 0.0, 0.0, 0.0

        # Special Case: High price (Bonds/Units/Convertibles)
        # Logic from resolve_pff_tickers.py: if pff_price > 32.25, it's not a standard preferred
        if price > 32.25 and not raw_ticker.endswith(('-P', '-L', '-Z')):
             # We still want to keep it but maybe we don't resolve a new ticker for it
             display_ticker = raw_ticker
             sector = row.get('Sector', 'Other')
        else:
            # Resolve via Master List
            resolved_ticker, resolved_sector = find_best_ticker_match(name, price, master_df)
            
            if resolved_ticker:
                display_ticker = resolved_ticker
                sector = resolved_sector
            else:
                # Fallback Discovery logic
                print(f"  [?] Ticker not found for {name} (${price}). Keep raw: {raw_ticker}")
                display_ticker = raw_ticker
                sector = row.get('Sector', 'Other')

        # Update sector map cache if it's a new or missing mapping
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
        print("[!] No results collected. Check PFF CSV parsing.")
        return

    df_results = pd.DataFrame(results)
    print(f"[*] Columns in results: {df_results.columns.tolist()}")
    
    if 'Weight (%)' in df_results.columns:
        df_results.sort_values('Weight (%)', ascending=False, inplace=True)
    else:
        print("[!] Warning: 'Weight (%)' column missing in results dataframe.")
        
    df_results.to_csv(OUTPUT_CSV, index=False)
    print(f"[+] Exported {len(df_results)} rows to {OUTPUT_CSV}")

    # Update sector_map.json if needed
    if new_sector_mappings:
        print(f"[*] Adding {len(new_sector_mappings)} new sector mappings to {SECTOR_MAP_FILE}")
        with open(SECTOR_MAP_FILE, 'w') as f:
            json.dump(sector_map, f, indent=4)

if __name__ == "__main__":
    process()
