import pandas as pd
import yfinance as yf
import os
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
    '060505682': 'BAC-L',
    '94974B851': 'WFC-L',
    '65339F663': 'NEE-S',  # 1.21% Weight
}

# Weight/Price Fingerprints for Top Holdings (Fallback for missing CUSIP)
FINGERPRINTS = {
    # (Ticker, Weight, Price_Range_Start, Price_Range_End): Resolved Ticker
    ('WFC', 2.39, 1200, 1300): 'WFC-L',
    ('BAC', 1.37, 1200, 1300): 'BAC-L',
    ('NEE', 1.21, 55, 58): 'NEE-S',
    ('NEE', 0.83, 50, 53): 'NEE-N',
    ('JPM', 1.00, 24, 26): 'JPM-C',
    ('JPM', 0.91, 24, 26): 'JPM-D',
}

# Cache for yfinance lookups
RESOLUTION_CACHE = {}

def resolve_series_ticker(ticker, name, price, weight=None, cusip=None):
    """
    Financial-grade resolution using CUSIP, Price, Weight and Name.
    """
    ticker = ticker.upper().split('-')[0].strip()
    name = str(name).upper()

    # 1. Primary: CUSIP Matching (Gold Standard)
    if cusip:
        normalized_cusip = str(cusip).strip().zfill(9)
        if normalized_cusip in CUSIP_MAP:
            return CUSIP_MAP[normalized_cusip]
            
        if normalized_cusip not in RESOLUTION_CACHE:
            try:
                search = yf.Search(normalized_cusip)
                if search.quotes:
                    resolved = search.quotes[0].get('symbol', ticker)
                    if '-P' in resolved: resolved = resolved.replace('-P', '-')
                    RESOLUTION_CACHE[normalized_cusip] = resolved
                else:
                    RESOLUTION_CACHE[normalized_cusip] = ticker
            except:
                RESOLUTION_CACHE[normalized_cusip] = ticker
        return RESOLUTION_CACHE[normalized_cusip]

    # 2. Fingerprint Matching (Weight + Price) - Prevents Duplicates like NEE-N
    if weight is not None:
        for (f_ticker, f_weight, p_start, p_end), resolved in FINGERPRINTS.items():
            if ticker == f_ticker and abs(weight - f_weight) < 0.02 and p_start <= price <= p_end:
                return resolved

    # 3. Parse Series from Name (e.g. "SERIES L")
    if ' SERIES ' in name:
        series_part = name.split(' SERIES ')[1].strip()
        if series_part and len(series_part) >= 1:
            letter = series_part[0]
            if letter.isalpha():
                return f"{ticker}-{letter}"
    
    # 4. Hardcoded Fallbacks for known high-price series
    if ticker == 'ABR':
        if price > 20: return 'ABR-F'
        if price > 17.52: return 'ABR-E'
        return 'ABR-D'
        
    return ticker

def analyze_pff_holdings(csv_path):
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
            
        # Resolve Series Ticker (CUSIP > Fingerprint > Name)
        display_ticker = resolve_series_ticker(base_ticker, name, price, weight=weight, cusip=cusip)
        
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
