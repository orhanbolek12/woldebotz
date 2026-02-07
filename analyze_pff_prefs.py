import pandas as pd
import yfinance as yf
import os
from collections import defaultdict
import time

def extract_company_name(name_str):
    """
    Extract clean company name from the holdings name field.
    Examples:
    - "NEXTERA ENERGY UNITS INC" -> "NEXTERA ENERGY"
    - "DTE ENERGY COMPANY" -> "DTE ENERGY"
    - "WELLS FARGO & COMPANY SERIES L" -> "WELLS FARGO"
    """
    if not name_str or pd.isna(name_str):
        return None
    
    # Remove common suffixes
    name = name_str.upper()
    
    # Remove series info
    if ' SERIES ' in name:
        name = name.split(' SERIES ')[0]
    
    # Remove common corporate suffixes
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

def search_preferred_stocks_by_name(company_name, base_ticker):
    """
    Search for preferred stocks using Yahoo Finance search.
    This is more reliable than guessing ticker formats.
    """
    print(f"  Searching for '{company_name}' preferred stocks...")
    
    preferred_stocks = []
    
    # Try to get info from the base ticker's related securities
    try:
        base = yf.Ticker(base_ticker)
        
        # Method 1: Try common preferred stock patterns
        # Format: TICKER-PA, TICKER-PB, etc.
        for letter in 'ABCDEFGHIJ':
            pref_ticker = f"{base_ticker}-P{letter}"
            try:
                t = yf.Ticker(pref_ticker)
                hist = t.history(period="5d")
                if not hist.empty:
                    info = {}
                    try:
                        info = t.info
                        name = info.get('longName', info.get('shortName', 'N/A'))
                    except:
                        name = 'N/A'
                    
                    preferred_stocks.append({
                        'ticker': pref_ticker,
                        'name': name,
                        'last_price': round(hist['Close'].iloc[-1], 2) if not hist.empty else None
                    })
                    print(f"    [+] Found: {pref_ticker}")
            except:
                pass
        
        # Method 2: Try alternative formats (like KKRT for KKR)
        # Some companies use different ticker for preferreds
        alt_formats = [
            f"{base_ticker}T",  # e.g., KKRT
            f"{base_ticker}-S", # e.g., KKR-S
            f"{base_ticker}P",  # e.g., KKRP
        ]
        
        for alt_ticker in alt_formats:
            try:
                t = yf.Ticker(alt_ticker)
                hist = t.history(period="5d")
                if not hist.empty:
                    info = {}
                    try:
                        info = t.info
                        name = info.get('longName', info.get('shortName', 'N/A'))
                        # Check if it's actually related to our company
                        if company_name.split()[0] in name.upper():
                            preferred_stocks.append({
                                'ticker': alt_ticker,
                                'name': name,
                                'last_price': round(hist['Close'].iloc[-1], 2) if not hist.empty else None
                            })
                            print(f"    [+] Found: {alt_ticker}")
                    except:
                        pass
            except:
                pass
        
        time.sleep(0.15)  # Rate limiting
        
    except Exception as e:
        print(f"    [!] Error searching: {e}")
    
    return preferred_stocks

def find_best_match_row(pref_ticker, company_rows):
    """
    Find the best matching row in the original CSV for a found preferred ticker.
    Logic:
    1. Check for Series match (e.g., 'PA' -> 'SERIES A')
    2. Check for exact ticker match (if original had full ticker)
    3. Fallback to row with largest weight (assuming it's the main holding)
    """
    if not company_rows:
        return None
        
    # Extract suffix letter (e.g., 'A' from 'ABC-PA')
    suffix = None
    if '-' in pref_ticker:
        parts = pref_ticker.split('-')
        if len(parts) > 1 and parts[1].startswith('P') and len(parts[1]) == 2:
            suffix = parts[1][1] # 'A'
    
    # 1. Try Series Match
    if suffix:
        series_str = f"SERIES {suffix}"
        for row in company_rows:
            if series_str in str(row['Name']).upper():
                return row
    
    # 2. Try simple fuzzy match of ticker in name
    for row in company_rows:
        if pref_ticker in str(row['Ticker']):
            return row
            
    # 3. Fallback: Largest weight
    # Sort by Weight (descending)
    try:
        sorted_rows = sorted(company_rows, key=lambda x: float(str(x['Weight (%)']).replace(',','')) if pd.notna(x['Weight (%)']) else 0, reverse=True)
        return sorted_rows[0]
    except:
        return company_rows[0]

def analyze_pff_holdings(csv_path):
    """
    Main analysis function to process PFF holdings and find matching preferred stocks.
    Preserves Weight (%) and Market Value from the original CSV.
    """
    print("=" * 80)
    print("PFF ETF PREFERRED STOCK ANALYZER (With Weight Integration)")
    print("=" * 80)
    print()
    
    # Read CSV file
    print(f"[*] Reading CSV file: {csv_path}")
    
    # Skip the header rows (first 9 rows are metadata)
    df = pd.read_csv(csv_path, skiprows=9)
    
    print(f"[+] Loaded {len(df)} holdings")
    print()
    
    # Group rows by Base Ticker
    # Structure: {'ABR': [{'Name':..., 'Weight':..., ...}, ...]}
    company_groups = defaultdict(list)
    company_info_map = {} # {'ABR': 'ARBOR REALTY TRUST'}
    
    for idx, row in df.iterrows():
        ticker = row.get('Ticker')
        name = row.get('Name')
        
        if pd.isna(ticker) or ticker == '-':
            continue
            
        base_ticker = ticker.split('-')[0].strip()
        
        # Add to group
        company_groups[base_ticker].append(row)
        
        # Extract name if not already done
        if base_ticker not in company_info_map:
            clean_name = extract_company_name(name)
            if clean_name:
                company_info_map[base_ticker] = clean_name
    
    print(f"[*] Found {len(company_groups)} unique base tickers")
    print()
    
    # Analyze and Map
    results = {} # Key: Base Ticker
    
    print("[*] Searching for preferred stocks and mapping weights...")
    print("-" * 80)
    
    total_companies = len(company_info_map)
    for i, (base_ticker, company_name) in enumerate(sorted(company_info_map.items()), 1):
        print(f"\n[{i}/{total_companies}] {base_ticker} - {company_name}")
        
        # 1. Search for Preferreds
        preferred_stocks = search_preferred_stocks_by_name(company_name, base_ticker)
        
        if preferred_stocks:
            mapped_prefs = []
            rows = company_groups[base_ticker]
            
            # 2. Map found preferreds to original rows
            for pref in preferred_stocks:
                match = find_best_match_row(pref['ticker'], rows)
                
                weight = 0.0
                market_value = 0.0
                orig_name = "N/A"
                
                if match is not None:
                    try:
                        weight = float(str(match['Weight (%)']).replace(',', '')) if pd.notna(match['Weight (%)']) else 0.0
                        # Market Value might be a string with commas e.g. "641,703,000.06"
                        mv_str = str(match['Market Value'])
                        market_value = float(mv_str.replace(',', '')) if pd.notna(match['Market Value']) else 0.0
                        orig_name = match['Name']
                    except Exception as e:
                        print(f"    [!] Error parsing weight/mv: {e}")
                
                pref['weight'] = weight
                pref['market_value'] = market_value
                pref['original_name'] = orig_name
                mapped_prefs.append(pref)
            
            results[base_ticker] = {
                'company_name': company_name,
                'preferred_stocks': mapped_prefs
            }
            print(f"  [+] Mapped {len(mapped_prefs)} preferred stock(s)")
        else:
            print(f"  [-] No preferred stocks found")
            
    print()
    print("=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)
    print()
    
    # Export
    if results:
        export_results(results)
    else:
        print("[!] No preferred stocks found.")
        
    return results

def export_results(results):
    """
    Export results to a CSV file including Weight and Market Value.
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
    
    df_export = pd.DataFrame(rows)
    # Sort by Weight descending
    df_export.sort_values(by='Weight (%)', ascending=False, inplace=True)
    
    df_export.to_csv(output_file, index=False)
    
    print(f"[*] Results exported to: {output_file}")
    print()

if __name__ == "__main__":
    # Get the CSV file path
    csv_path = os.path.join(os.environ['TEMP'], 'pff_holdings.csv')
    
    if not os.path.exists(csv_path):
        print(f"[!] Error: CSV file not found at {csv_path}")
        print("Please download the PFF holdings CSV first.")
    else:
        results = analyze_pff_holdings(csv_path)
