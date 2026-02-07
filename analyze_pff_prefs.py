import pandas as pd
import yfinance as yf
import os
from collections import defaultdict
import time

def extract_company_name(name_str):
    """
    Extract clean company name from the holdings name field.
    """
    if not name_str or pd.isna(name_str):
        return "N/A"
    
    # Remove common corporate suffixes
    name = name_str.upper()
    suffixes_to_remove = [
        ' INCORPORATED', ' INC', ' CORPORATION', ' CORP', 
        ' COMPANY', ' CO', ' LIMITED', ' LTD',
        ' UNITS', ' DS REPSTG', ' DS REPRESENTING',
        ' NON-CUMULATIVE PREF', ' PERP STRETCH PRF',
        ' PERP STRIFE PRF', ' CONV PR', ' DRC',
        ' CAPITAL HOLDINGS', ' CAPITAL XIII',
        ' THE'
    ]
    
    for suffix in suffixes_to_remove:
        if name.endswith(suffix):
            name = name[:-len(suffix)]
    
    return name.strip()

def resolve_series_ticker(ticker, name, price, series_map=None):
    """
    Heuristic to resolve a specific series ticker from CSV data.
    """
    if not name or pd.isna(name): return ticker
    name = name.upper()
    
    # 1. Parse Series from Name (e.g. "SERIES D")
    if ' SERIES ' in name:
        series_part = name.split(' SERIES ')[1].strip()
        if series_part and len(series_part) >= 1:
            letter = series_part[0]
            if letter.isalpha():
                return f"{ticker}-{letter}" # Standard format for UI
    
    # 2. Hardcoded / Price-based Mapping for known tricky ones
    # ABR Logic
    if ticker == 'ABR':
        # Arbor Prices as of Feb 2026: F ~22.3, D ~17.5, E ~17.6
        if price > 20: return 'ABR-F'
        if price > 17.52: return 'ABR-E' # E is usually slightly higher than D
        return 'ABR-D'
        
    # 3. Known Mappings (Optional expansion)
    known_names = {
        'CITIGROUP CAPITAL XIII': 'C-PN', # Example
        'WELLS FARGO & COMPANY SERIES L': 'WFC-PL'
    }
    for k, v in known_names.items():
        if k in name: return v

    return ticker

def analyze_pff_holdings(csv_path):
    """
    Redesigned Comprehensive Analysis:
    - Iterates through EVERY row in the iShares CSV.
    - Uses 100% of the Weight (%) and Market Value data.
    - Resolves Series Tickers (A-Z) via name/price heuristics.
    """
    print("=" * 80)
    print("PFF COMPREHENSIVE WEIGHT ANALYZER")
    print("=" * 80)
    print()
    
    # 1. Read Original CSV file (Robustly find header)
    print(f"[*] Reading source: {csv_path}")
    try:
        # First, find the header row
        with open(csv_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            header_idx = 0
            for i, line in enumerate(lines):
                if 'Ticker,Name,Sector' in line:
                    header_idx = i
                    break
        
        df = pd.read_csv(csv_path, skiprows=header_idx)
    except Exception as e:
        # Try Latin-1 if UTF-8 fails (iShares CSVs sometimes are)
        try:
            df = pd.read_csv(csv_path, skiprows=9, encoding='latin1')
        except:
            print(f"[!] Error reading CSV: {e}")
            return {}

    print(f"[+] Processing {len(df)} rows from CSV (Header at row {header_idx})...")
    
    results = {}
    processed_count = 0
    
    for idx, row in df.iterrows():
        raw_ticker = str(row.get('Ticker', '-')).strip()
        name = str(row.get('Name', 'N/A'))
        
        if raw_ticker == '-' or pd.isna(raw_ticker) or "Ticker" in raw_ticker:
            continue
            
        base_ticker = raw_ticker.split('-')[0].strip()
        
        # Parse numeric values correctly
        try:
            # Handle both string and float types
            w_raw = str(row.get('Weight (%)', '0')).replace(',', '')
            mv_raw = str(row.get('Market Value', '0')).replace(',', '')
            p_raw = str(row.get('Price', '0')).replace(',', '')
            
            weight = float(w_raw)
            market_value = float(mv_raw)
            price = float(p_raw)
        except Exception as e:
            weight, market_value, price = 0.0, 0.0, 0.0
            
        # Resolve Series Ticker
        display_ticker = resolve_series_ticker(base_ticker, name, price)
        
        if base_ticker not in results:
            results[base_ticker] = {
                'company_name': extract_company_name(name),
                'preferred_stocks': []
            }
        
        results[base_ticker]['preferred_stocks'].append({
            'ticker': display_ticker,
            'name': name,
            'last_price': price,
            'weight': weight,
            'market_value': market_value,
            'original_name': name
        })
        processed_count += 1

    print(f"[*] Total rows processed: {processed_count}")
    print(f"[*] Total issuers found: {len(results)}")
    
    # Verification Print
    if 'BA' in results:
        ba_data = results['BA']['preferred_stocks'][0]
        print(f"    [VERIFY] BA Weight: {ba_data['weight']}%")
    
    export_results(results)
    return results

def export_results(results, silent=False):
    """
    Export results to a CSV file.
    """
    output_file = "pff_preferred_stocks_analysis.csv"
    
    rows = []
    for base_ticker, data in sorted(results.items()):
        for pref in data['preferred_stocks']:
            rows.append({
                'Base Ticker': base_ticker,
                'Company Name': data['company_name'],
                'Preferred Stock': pref['ticker'],
                'Last Price': pref['last_price'],
                'Full Name': pref['name'],
                'Weight (%)': pref['weight'],
                'Market Value': pref['market_value'],
                'Original Name': pref['original_name']
            })
    
    if not rows:
        print("[!] No rows generated for export.")
        return

    print(f"[*] Preparing to export {len(rows)} rows to {output_file}...")
    df_export = pd.DataFrame(rows)
    # Sort by Weight descending (primary sort)
    df_export.sort_values(by='Weight (%)', ascending=False, inplace=True)
    df_export.to_csv(output_file, index=False)
    
    if not silent:
        print(f"[*] Results exported successfully. File size: {os.path.getsize(output_file)} bytes")
        print()

if __name__ == "__main__":
    # Prefer the file in Downloads if it exists
    home = os.path.expanduser('~')
    downloads_path = os.path.join(home, 'Downloads', 'PFF_holdings.csv')
    temp_path = os.path.join(os.environ.get('TEMP', ''), 'pff_holdings.csv')
    
    csv_path = downloads_path if os.path.exists(downloads_path) else temp_path
    
    if not os.path.exists(csv_path):
        print(f"[!] Error: CSV file not found at {csv_path}")
        print("Please ensure PFF_holdings.csv is in your Downloads folder.")
    else:
        results = analyze_pff_holdings(csv_path)
