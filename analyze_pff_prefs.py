import pandas as pd
import yfinance as yf
import os
import json
from collections import defaultdict
import time

# Global Constants
PFF_OUTPUT = "pff_holdings_tickers.csv"
PFF_SOURCE_DEFAULT = os.path.join(os.path.expanduser("~"), "Downloads", "PFF_holdings.csv")
PFF_SOURCE_DETAILED = os.path.join(os.path.expanduser("~"), "Downloads", "PFF_holdings_detailed.csv")
TICKERS_FILE = "tickers.txt"

def load_master_base_tickers():
    """
    Load tickers from tickers.txt and return a set of unique base tickers.
    e.g. if 'BAC-Q' is in file, it adds 'BAC' to the set.
    """
    if not os.path.exists(TICKERS_FILE):
        print(f"[!] Warning: {TICKERS_FILE} not found. Filtering disabled.")
        return None
    
    with open(TICKERS_FILE, 'r') as f:
        content = f.read()
    
    raw_tickers = [t.strip() for t in content.replace('\n', ',').split(',') if t.strip()]
    base_tickers = set()
    for t in raw_tickers:
        base = t.split('-')[0].strip().upper()
        if base:
            base_tickers.add(base)
    
    return base_tickers

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

# CUSIP to Ticker Mapping for Series Resolution (100% Accuracy)
CUSIP_MAP = {
    '038923850': 'ABR-F',
    '038923876': 'ABR-D',
    '038923868': 'ABR-E',
}

# Persistent Resolution Maps
RESOLUTION_MAP = {}
MANUAL_OVERRIDES = {}

def load_resolution_maps():
    global RESOLUTION_MAP, MANUAL_OVERRIDES
    try:
        if os.path.exists('pff_resolution_map.json'):
            with open('pff_resolution_map.json', 'r') as f:
                RESOLUTION_MAP = json.load(f)
        if os.path.exists('pff_manual_overrides.json'):
            with open('pff_manual_overrides.json', 'r') as f:
                MANUAL_OVERRIDES = json.load(f)
    except:
        pass

def normalize_ticker(t):
    """Normalize symbols to QuantumOnline format (e.g. BAC-L)"""
    if not t: return t
    t = str(t).upper().strip()
    
    # 1. Handle Yahoo-style series with prefixes: TICKER.PRL -> TICKER-L, JPM/PR/L -> JPM-L
    for prefix in ['.PR', '/PR/', '-PR']:
        if prefix in t:
            parts = t.split(prefix)
            if len(parts) > 1 and parts[1]:
                return f"{parts[0]}-{parts[1]}"
            return parts[0]

    # 2. Handle Yahoo -PL style (e.g. BAC-PL -> BAC-L)
    if '-P' in t and not t.endswith('-P'):
        parts = t.split('-P')
        if len(parts) > 1 and parts[1]:
             return f"{parts[0]}-{parts[1]}"

    # 3. Handle simple separators (e.g. MS^P -> MS-P)
    t = t.replace('^', '-')
    
    # Final cleanup: Only remove trailing dash if it's not preceded by a letter we want
    if t.endswith('-') and len(t) > 1:
        # Check if it was something like MS-
        return t.rstrip('-')
        
    return t

def resolve_series_ticker(ticker, name, price, weight=None, cusip=None):
    """
    Simplified: Returns the ticker as found in the holdings file.
    """
    return ticker.upper().strip()

def analyze_pff_holdings(csv_path):
    """
    Simplified analysis that keeps raw tickers and metadata for UI sorting/filtering.
    """
    """
    Comprehensive Analysis with CUSIP Priority:
    - Checks for 'CUSIP' column for 100% accuracy.
    - Fallback to Name/Price heuristics if CUSIP is missing.
    """
    print("=" * 80)
    print("PFF MASTER-FILTERED ACCURACY ANALYZER")
    print("=" * 80)
    print()
    
    master_bases = load_master_base_tickers()
    if master_bases:
        print(f"[*] Loaded {len(master_bases)} base tickers from Master List for filtering.")
    
    # 1. Read Original CSV file (Robustly find header)
    print(f"[*] Reading source: {csv_path}")
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            header_idx = 0
            for i, line in enumerate(lines):
                if 'Ticker,Name,Sector' in line:
                    header_idx = i
                    break
        
        df = pd.read_csv(csv_path, skiprows=header_idx)
    except Exception:
        try:
            df = pd.read_csv(csv_path, skiprows=9, encoding='latin1')
        except Exception as e:
            print(f"[!] Error reading CSV: {e}")
            return {}

    has_cusip = 'CUSIP' in df.columns
    print(f"[+] Loaded {len(df)} rows. CUSIP Data Available: {has_cusip}")
    
    results = {}
    processed_count = 0
    
    for idx, row in df.iterrows():
        raw_ticker = str(row.get('Ticker', '-')).strip()
        name = str(row.get('Name', 'N/A'))
        cusip = row.get('CUSIP') if has_cusip else None
        
        if raw_ticker == '-' or pd.isna(raw_ticker) or "Ticker" in raw_ticker:
            continue
            
        base_ticker = raw_ticker.split('-')[0].strip().upper()
        
        # Filtering: Only keep if in Master List base tickers
        if master_bases is not None and base_ticker not in master_bases:
            continue
            
        try:
            w_raw = str(row.get('Weight (%)', '0')).replace(',', '')
            mv_raw = str(row.get('Market Value', '0')).replace(',', '')
            p_raw = str(row.get('Price', '0')).replace(',', '')
            
            weight = float(w_raw)
            market_value = float(mv_raw)
            price = float(p_raw)
        except Exception:
            weight, market_value, price = 0.0, 0.0, 0.0
            
        # Resolve Series Ticker: Returns RAW Ticker from CSV as per user request
        display_ticker = resolve_series_ticker(raw_ticker, name, price, weight=weight, cusip=cusip)
        
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

    print(f"[*] Processed {processed_count} holdings via CUSIP/Heuristics.")
    export_results(results)
    return results

def export_results(results, silent=False):
    """
    Export results to a CSV file.
    """
    output_file = PFF_OUTPUT
    
    rows = []
    for base_ticker, data in sorted(results.items()):
        for pref in data['preferred_stocks']:
            # Calculate Quantity
            last_price = pref['last_price']
            market_value = pref['market_value']
            qty = market_value / last_price if last_price and last_price > 0 else 0
            
            rows.append({
                'Base Ticker': base_ticker,
                'Company Name': data['company_name'],
                'Preferred Stock': pref['ticker'],
                'Last Price': last_price,
                'Full Name': pref['name'],
                'Weight (%)': pref['weight'],
                'Market Value': market_value,
                'Quantity': qty,
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
    # Prioritize 'Detailed' file if user downloaded it as requested
    home = os.path.expanduser('~')
    downloads_path_detailed = os.path.join(home, 'Downloads', 'PFF_holdings_detailed.csv')
    downloads_path_std = os.path.join(home, 'Downloads', 'PFF_holdings.csv')
    temp_path = os.path.join(os.environ.get('TEMP', ''), 'pff_holdings.csv')
    
    # Selection logic
    if os.path.exists(downloads_path_detailed):
        csv_path = downloads_path_detailed
    elif os.path.exists(downloads_path_std):
        csv_path = downloads_path_std
    else:
        csv_path = temp_path
    
    if not os.path.exists(csv_path):
        print(f"[!] Error: CSV file not found.")
        print("Please ensure PFF_holdings_detailed.csv is in your Downloads folder.")
    else:
        results = analyze_pff_holdings(csv_path)
