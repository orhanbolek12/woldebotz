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
        
        # Method 1: Try common preferred stock ticker formats
        # Series A-J
        for letter in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J']:
            # Try multiple formats Yahoo Finance uses
            suffixes = [f"-P{letter}", f"^{letter}", f"-{letter}", f"p{letter}"]
            
            for suffix in suffixes:
                pref_ticker = f"{base_ticker}{suffix}"
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
                        break # Found for this letter, move to next letter
                except:
                    pass
        
        # Method 2: Manual Overrides for specific companies (if search still fails or for speed)
        manual_map = {
            'ABR': ['ABR^D', 'ABR^E', 'ABR^F']
        }
        
        if base_ticker in manual_map and not preferred_stocks:
            print(f"    [*] Using manual ticker overrides for {base_ticker}")
            for pt in manual_map[base_ticker]:
                try:
                    t = yf.Ticker(pt)
                    hist = t.history(period="5d")
                    if not hist.empty:
                        preferred_stocks.append({
                            'ticker': pt,
                            'name': f"{company_name} Preferred",
                            'last_price': round(hist['Close'].iloc[-1], 2)
                        })
                        print(f"    [+] Added: {pt}")
                except:
                    pass

        # Method 3: Try alternative formats (like KKRT for KKR)
        alt_formats = [
            f"{base_ticker}T",  # e.g., KKRT
            f"{base_ticker}-S", # e.g., KKR-S
            f"{base_ticker}P",  # e.g., KKRP
        ]
        
        for alt_ticker in alt_formats:
            # Avoid duplicates
            if any(p['ticker'] == alt_ticker for p in preferred_stocks): continue
            
            try:
                t = yf.Ticker(alt_ticker)
                hist = t.history(period="5d")
                if not hist.empty:
                    info = {}
                    try:
                        info = t.info
                        name = info.get('longName', info.get('shortName', 'N/A'))
                        # Check if it's actually related to our company
                        if company_name.split()[0].upper() in name.upper():
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

def find_best_match_row(pref_ticker, company_rows, claimed_indices, pref_price=None):
    """
    Find the best matching row in the original CSV for a found preferred ticker.
    Logic:
    1. Try Series Match (e.g. 'Series A')
    2. Try Price Match (if price is provided within 5% tolerance)
    3. Try exact ticker match
    4. Fallback to largest unclaimed row
    """
    if not company_rows:
        return None, None
        
    # Extract suffix letter (e.g., 'A' from 'ABC-PA')
    suffix = None
    if '-' in pref_ticker:
        parts = pref_ticker.split('-')
        if len(parts) > 1 and parts[1].startswith('P') and len(parts[1]) == 2:
            suffix = parts[1][1] 
    elif '^' in pref_ticker:
        suffix = pref_ticker.split('^')[1]
    
    # 1. Try Series Match on Unclaimed Rows
    if suffix:
        series_str = f"SERIES {suffix}"
        for idx, row in enumerate(company_rows):
            if idx in claimed_indices: continue
            if series_str in str(row['Name']).upper():
                return row, idx
    
    # 2. Try Price Match (High priority if multiple rows exist)
    if pref_price and len(company_rows) > 1:
        best_price_match = None
        min_diff = float('inf')
        
        for idx, row in enumerate(company_rows):
            if idx in claimed_indices: continue
            try:
                row_price = float(str(row['Price']).replace(',', ''))
                diff = abs(row_price - pref_price)
                # If within 5% tolerance
                if diff < (pref_price * 0.05) and diff < min_diff:
                    min_diff = diff
                    best_price_match = (row, idx)
            except:
                continue
        
        if best_price_match:
            return best_price_match
    
    # 3. Try simple fuzzy match of ticker in name
    for idx, row in enumerate(company_rows):
        if idx in claimed_indices: continue
        if pref_ticker in str(row['Ticker']):
            return row, idx
            
    # 4. Fallback: Any Unclaimed Row
    for idx, row in enumerate(company_rows):
        if idx in claimed_indices: continue
        return row, idx
        
    return None, None

def load_scraped_holdings():
    """
    Load holdings from scraper outputs (e.g. arbor_holdings.csv) to get accurate CUSIP/Weight data.
    Returns a dictionary keyed by CUSIP.
    """
    scraped_data = {}
    
    # List of scraped files to check
    files = ['arbor_holdings.csv']
    
    for f in files:
        if os.path.exists(f):
            print(f"[*] Loading scraped data from {f}...")
            try:
                # Force CUSIP to string to prevent dropping leading zeros
                df = pd.read_csv(f, dtype={'CUSIP': str})
                for _, row in df.iterrows():
                    raw_cusip = str(row.get('CUSIP', '')).strip()
                    if raw_cusip:
                        # Normalize CUSIP: Ensure it's 9 digits if it looks like one
                        if len(raw_cusip) == 8:
                            raw_cusip = '0' + raw_cusip
                        scraped_data[raw_cusip] = row
                print(f"    [+] Loaded {len(scraped_data)} CUSIP entries")
            except Exception as e:
                print(f"    [!] Error loading {f}: {e}")
                
    return scraped_data

def get_cusip_mappings():
    """
    Returns a hardcoded map of CUSIP -> Series Ticker
    Based on manual verification/search results.
    """
    return {
        '038923876': 'ABR^D', # Series D
        '038923868': 'ABR^E', # Series E
        '038923850': 'ABR^F', # Series F
        # Add others as found/needed
    }

def load_previous_analysis():
    """
    Load previous analysis results from CSV to avoid restarting from scratch.
    """
    output_file = "pff_preferred_stocks_analysis.csv"
    results = {}
    if os.path.exists(output_file):
        print(f"[*] Loading previous analysis from {output_file}...")
        try:
            df = pd.read_csv(output_file)
            for _, row in df.iterrows():
                base_ticker = row['Base Ticker']
                if base_ticker not in results:
                    results[base_ticker] = {
                        'company_name': row['Company Name'],
                        'preferred_stocks': []
                    }
                
                # Normalize ticker immediately upon loading: Replace ^ with -
                normalized_ticker = str(row['Preferred Stock']).replace('^', '-')
                
                results[base_ticker]['preferred_stocks'].append({
                    'ticker': normalized_ticker,
                    'name': row['Full Name'],
                    'last_price': row['Last Price'],
                    'weight': row['Weight (%)'],
                    'market_value': row['Market Value'],
                    'original_name': row['Original Name']
                })
            print(f"    [+] Loaded results for {len(results)} companies")
        except Exception as e:
            print(f"    [!] Error loading previous analysis: {e}")
    return results

def analyze_pff_holdings(csv_path):
    """
    Main analysis function to process PFF holdings and find matching preferred stocks.
    Prioritizes CUSIP-based mapping from scraped data, then falls back to name matching.
    """
    print("=" * 80)
    print("PFF ETF PREFERRED STOCK ANALYZER (With CUSIP Integration)")
    print("=" * 80)
    print()
    
    # 1. Load Scraped Data & Mappings
    scraped_holdings = load_scraped_holdings()
    cusip_map = get_cusip_mappings()
    
    # 2. Load Previous Results (Persistence)
    results = load_previous_analysis()
    
    # 3. Read Original CSV file (still needed for other companies)
    print(f"[*] Reading main CSV file: {csv_path}")
    df = pd.read_csv(csv_path, skiprows=9)
    print(f"[+] Loaded {len(df)} holdings from main file")
    print()
    
    # Group rows by Base Ticker
    company_groups = defaultdict(list)
    company_info_map = {} 
    
    for idx, row in df.iterrows():
        ticker = row.get('Ticker')
        name = row.get('Name')
        
        if pd.isna(ticker) or ticker == '-':
            continue
            
        base_ticker = ticker.split('-')[0].strip()
        company_groups[base_ticker].append(row)
        
        if base_ticker not in company_info_map:
            clean_name = extract_company_name(name)
            if clean_name:
                company_info_map[base_ticker] = clean_name
    
    print(f"[*] Found {len(company_groups)} unique base tickers")
    print()
    
    print("[*] Analysis via CUSIPs and Smart Mapping...")
    print("-" * 80)
    
    total_companies = len(company_info_map)
    for i, (base_ticker, company_name) in enumerate(sorted(company_info_map.items()), 1):
        # SKIP if already analyzed in previous run
        if base_ticker in results:
            continue

        is_focus = base_ticker == 'ABR'
        if is_focus or i % 10 == 0:
            print(f"\n[{i}/{total_companies}] {base_ticker} - {company_name}")
        
        # 1. Search for Preferreds (Yahoo)
        preferred_stocks = search_preferred_stocks_by_name(company_name, base_ticker)
        
        # 2. Add Preferreds from Scraped Data (if not found by Yahoo)
        for cusip, row in scraped_holdings.items():
            mapped_ticker = cusip_map.get(cusip)
            if mapped_ticker and mapped_ticker.startswith(base_ticker):
                if not any(p['ticker'] == mapped_ticker for p in preferred_stocks):
                    if is_focus:
                        print(f"    [+] Adding {mapped_ticker} from scraped data (Yahoo failed/skipped)")
                    
                    price = 0.0
                    try:
                        mv = float(str(row.get('Market Value', '0')).replace('$', '').replace(',', ''))
                        shares = float(str(row.get('Shares', '0')).replace(',', ''))
                        if shares > 0:
                            price = round(mv / shares, 2)
                    except:
                        pass
                    
                    preferred_stocks.append({
                        'ticker': mapped_ticker,
                        'name': f"{company_name} Preferred",
                        'last_price': price
                    })
        
        if preferred_stocks:
            mapped_prefs = []
            rows = company_groups[base_ticker]
            claimed_indices = set()
            
            preferred_stocks.sort(key=lambda x: x['ticker'])
            
            for pref in preferred_stocks:
                # STRATEGY 1: CUSIP Mapping
                mapped_via_cusip = False
                
                # Check mapping for this ticker (handles both -P and ^ formats)
                target_cusips = [c for c, t in cusip_map.items() if t == pref['ticker']]
                
                for cusip in target_cusips:
                    if cusip in scraped_holdings:
                        scraped_row = scraped_holdings[cusip]
                        if is_focus:
                            print(f"    [+] CUSIP MATCH {cusip} -> {pref['ticker']}")
                        
                        try:
                            w_val = scraped_row.get('Weight (%)', 0)
                            if isinstance(w_val, str):
                                w_val = float(w_val.replace(',', ''))
                            pref['weight'] = float(w_val)
                            
                            mv_val = scraped_row.get('Market Value', 0)
                            if isinstance(mv_val, str):
                                mv_val = mv_val.replace('$', '').replace(',', '')
                            pref['market_value'] = float(mv_val)
                            
                            pref['original_name'] = scraped_row.get('Name')
                            mapped_via_cusip = True
                        except Exception as e:
                            if is_focus:
                                print(f"    [!] Error parsing scraped data for {cusip}: {e}")
                        break
                
                if mapped_via_cusip:
                    mapped_prefs.append(pref)
                    continue

                # STRATEGY 2: Fallback to CSV Matching
                match, match_idx = find_best_match_row(pref['ticker'], rows, claimed_indices, pref_price=pref['last_price'])
                
                weight = 0.0
                market_value = 0.0
                orig_name = "N/A"
                
                if match is not None:
                    claimed_indices.add(match_idx)
                    try:
                        weight = float(str(match['Weight (%)']).replace(',', '')) if pd.notna(match['Weight (%)']) else 0.0
                        mv_str = str(match['Market Value'])
                        market_value = float(mv_str.replace(',', '')) if pd.notna(match['Market Value']) else 0.0
                        orig_name = match['Name']
                    except Exception as e:
                        pass
                
                pref['weight'] = weight
                pref['market_value'] = market_value
                pref['original_name'] = orig_name
                mapped_prefs.append(pref)
            
            results[base_ticker] = {
                'company_name': company_name,
                'preferred_stocks': mapped_prefs
            }
            if is_focus:
                print(f"  [+] Mapped {len(mapped_prefs)} preferred stock(s)")
        
        # Incremental Export
        if is_focus or i % 10 == 0:
            export_results(results, silent=True)
            
    print()
    print("=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)
    print()
    
    if results:
        export_results(results)
    else:
        print("[!] No preferred stocks found.")
        
    return results

def export_results(results, silent=False):
    """
    Export results to a CSV file including Weight and Market Value.
    Normalizes ticker symbols (e.g. ABR^D -> ABR-D)
    """
    output_file = "pff_preferred_stocks_analysis.csv"
    
    rows = []
    for base_ticker, data in sorted(results.items()):
        for pref in data['preferred_stocks']:
            # Normalize ticker: Replace ^ with - for display
            display_ticker = pref['ticker'].replace('^', '-')
            
            rows.append({
                'Base Ticker': base_ticker,
                'Company Name': data['company_name'],
                'Preferred Stock': display_ticker,
                'Last Price': pref['last_price'],
                'Full Name': pref['name'],
                'Weight (%)': pref['weight'],
                'Market Value': pref['market_value'],
                'Original Name': pref['original_name']
            })
    
    if not rows:
        return

    df_export = pd.DataFrame(rows)
    df_export.sort_values(by='Weight (%)', ascending=False, inplace=True)
    df_export.to_csv(output_file, index=False)
    
    if not silent:
        print(f"[*] Results exported to: {output_file}")
        print()

if __name__ == "__main__":
    # Prefer the file in Downloads if it exists
    downloads_path = os.path.join(os.path.expanduser('~'), 'Downloads', 'PFF_holdings.csv')
    temp_path = os.path.join(os.environ['TEMP'], 'pff_holdings.csv')
    
    csv_path = downloads_path if os.path.exists(downloads_path) else temp_path
    
    if not os.path.exists(csv_path):
        print(f"[!] Error: CSV file not found at {csv_path}")
        print("Please download the PFF holdings CSV first.")
    else:
        results = analyze_pff_holdings(csv_path)
