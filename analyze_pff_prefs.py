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

def analyze_pff_holdings(csv_path):
    """
    Main analysis function to process PFF holdings and find preferred stocks.
    """
    print("=" * 80)
    print("PFF ETF PREFERRED STOCK ANALYZER (Company Name Based)")
    print("=" * 80)
    print()
    
    # Read CSV file
    print(f"[*] Reading CSV file: {csv_path}")
    
    # Skip the header rows (first 9 rows are metadata)
    df = pd.read_csv(csv_path, skiprows=9)
    
    print(f"[+] Loaded {len(df)} holdings")
    print()
    
    # Extract unique companies
    companies = {}
    for idx, row in df.iterrows():
        ticker = row.get('Ticker')
        name = row.get('Name')
        
        if pd.isna(ticker) or ticker == '-':
            continue
        
        base_ticker = ticker.split('-')[0].strip()
        company_name = extract_company_name(name)
        
        if company_name and base_ticker:
            # Store unique companies
            if base_ticker not in companies:
                companies[base_ticker] = {
                    'name': company_name,
                    'original_name': name
                }
    
    print(f"[*] Found {len(companies)} unique companies")
    print()
    
    # Analyze each company for preferred stocks
    results = {}
    
    print("[*] Searching for preferred stock variants...")
    print("-" * 80)
    
    for i, (base_ticker, company_info) in enumerate(sorted(companies.items()), 1):
        print(f"\n[{i}/{len(companies)}] {base_ticker} - {company_info['name']}")
        
        preferred_stocks = search_preferred_stocks_by_name(
            company_info['name'], 
            base_ticker
        )
        
        if preferred_stocks:
            results[base_ticker] = {
                'company_name': company_info['name'],
                'preferred_stocks': preferred_stocks
            }
            print(f"  [+] Total found: {len(preferred_stocks)} preferred stock(s)")
        else:
            print(f"  [-] No preferred stocks found")
    
    print()
    print("=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)
    print()
    
    # Display results summary
    if results:
        print(f"[SUMMARY] {len(results)} companies have preferred stocks")
        print()
        print("-" * 80)
        
        total_prefs = 0
        for base_ticker, data in sorted(results.items()):
            prefs = data['preferred_stocks']
            total_prefs += len(prefs)
            print(f"\n[{base_ticker}] {data['company_name']}")
            print(f"  ({len(prefs)} preferred stock{'s' if len(prefs) > 1 else ''})")
            for pref in prefs:
                price_str = f"${pref['last_price']}" if pref['last_price'] else "N/A"
                print(f"   - {pref['ticker']:10s} - {price_str:>8s}")
        
        print()
        print("-" * 80)
        print(f"[TOTAL] {total_prefs} preferred stocks found across {len(results)} companies")
        print()
        
        # Export to CSV
        export_results(results)
    else:
        print("[!] No preferred stocks found in the analysis")
    
    return results

def export_results(results):
    """
    Export results to a CSV file for easy reference.
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
                'Full Name': pref['name']
            })
    
    df_export = pd.DataFrame(rows)
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
