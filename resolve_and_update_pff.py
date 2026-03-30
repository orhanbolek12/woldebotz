import pandas as pd
import yfinance as yf
import os
import json
import re

# Source: User's Desktop
HOLDINGS_FILE = r'C:\Users\orhan.bolek\Desktop\notepadpffholdings.txt'
# Target: Dashboard Data Source
TARGET_CSV = r'c:\Users\orhan.bolek\Desktop\woldebotz\pff_holdings_tickers.csv'
# Backup/Audit Report
REPORT_FILE = r'C:\Users\orhan.bolek\Desktop\pff_resolution_mapping.txt'

# Resolution Rules for Yahoo Finance
# BA-A (User) -> BA-PA (Yahoo)
# WFC-L (User) -> WFC-PL (Yahoo)
# Some are direct symbols: SOMN, STRC

CUSIP_MAP = {
    '097023204': ('BA-A', 'BA-PA'),
    '68389X204': ('ORCL-D', 'ORCL-PD'),
    '949746804': ('WFC-L', 'WFC-PL'),
    '012653200': ('ALB-A', 'ALB-PA'),
    '594972853': ('MSTR-A', 'STRC'), 
    '173080201': ('C-N', 'C-PN'),
    '060505682': ('BAC-L', 'BAC-PL'),
    '65339F663': ('NEE-S', 'NEE-PS'),
    '842587842': ('SOMN', 'SOMN'),
    '48251W500': ('KKR-C', 'KKR-PC'),
    '42824C208': ('HPE-C', 'HPE-PC'),
}

# Load the local resolution map (450+ entries)
EXISTING_MAP = {}
MAP_PATH = r'c:\Users\orhan.bolek\Desktop\woldebotz\pff_resolution_map.json'
if os.path.exists(MAP_PATH):
    try:
        with open(MAP_PATH, 'r', encoding='utf-8') as f:
            EXISTING_MAP = json.load(f)
        print(f"[*] Loaded {len(EXISTING_MAP)} mappings from {MAP_PATH}")
    except:
        print("[!] Failed to load existing map.")

def get_yahoo_ticker(pref_ticker):
    """
    Standard conversion: BA-A -> BA-PA
    Exceptions: STRC, SOMN, STRF
    """
    if pref_ticker in ['STRC', 'SOMN', 'STRF']:
        return pref_ticker
    if '-' in pref_ticker:
        parts = pref_ticker.split('-')
        base = parts[0]
        # Most are series: BA-A -> BA-PA
        if len(parts[1]) == 1:
            return f"{base}-P{parts[1]}"
        # If it's something like "BAC-L", Yahoo uses "BAC-PL"
        return f"{base}-P{parts[1]}"
    return pref_ticker

def resolve_ticker(base_ticker, name, cusip):
    """
    1. CUSIP Priority
    2. Exact Existing Map (rare but if we have the combo)
    3. Heuristic / Series match
    """
    if cusip in CUSIP_MAP:
        return CUSIP_MAP[cusip]
    
    # 2. Heuristic for name pattern "SERIES X" or "SER X" or "SER X"
    norm_name = name.upper()
    
    # Existing Map fallback (Check if any key in map contains our base ticker and the series from name)
    match = re.search(r'SERIES\s+([A-Z])', norm_name)
    if not match:
        match = re.search(r'SER\s+([A-Z])', norm_name)
    
    if match:
        series = match.group(1)
        res_ticker = f"{base_ticker}-{series}"
        return res_ticker, get_yahoo_ticker(res_ticker)

    # 3. Handle specific labels like "CAPITAL XIII" -> C-N
    if "CAPITAL XIII" in norm_name: return 'C-N', 'C-PN'
    if "SOMN" in norm_name: return 'SOMN', 'SOMN'
    if "STRATEGY INC" in norm_name: return 'MSTR-A', 'STRC'
    
    # If we have the mapping in the existing map under the base ticker
    # (Since keys are Ticker|Weight|Price, we look for matches starting with Ticker|)
    for key, val in EXISTING_MAP.items():
        if key.startswith(f"{base_ticker}|"):
             if val and val != base_ticker:
                  # Found a series-specific ticker in the old map!
                  return val, get_yahoo_ticker(val)

    return base_ticker, base_ticker

def fetch_prices(tickers):
    """
    Fetch current prices from Yahoo Finance in batches.
    """
    print(f"[*] Fetching prices for {len(tickers)} tickers...")
    data = yf.download(tickers, period='1d', group_by='ticker', threads=True)
    prices = {}
    for t in tickers:
        try:
            if t in data.columns.levels[0]:
                val = data[t]['Close'].iloc[-1]
                if not pd.isna(val) and val > 0:
                    prices[t] = round(float(val), 2)
            else:
                # If only one ticker was requested, yf.download format is different
                val = data['Close'].iloc[-1]
                if not pd.isna(val):
                    prices[t] = round(float(val), 2)
        except:
             prices[t] = 0.0
    return prices

def process_holdings():
    # 1. Parse user's file
    try:
        # Use a more robust reading approach for notepad dumps
        with open(HOLDINGS_FILE, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        if not lines:
            print("[!] File is empty.")
            return

        # Header is often space-separated while data is tab-separated
        # We'll skip the first line and use our own header based on the data
        header_cols = ['Ticker', 'Name', 'Sector', 'Asset_Class', 'Mrkt_Value', 'Weight%', 'Notion_Value', 'Quantity', 'CUSIP', 'ISIN', 'SEDOL', 'ACCRUAL_DATE']
        
        # Parse data rows (skipping header)
        data_rows = []
        for line in lines[1:]:
            parts = line.strip('\n').split('\t')
            if len(parts) >= 9:
                data_rows.append(parts[:len(header_cols)])
        
        df = pd.DataFrame(data_rows, columns=header_cols[:len(data_rows[0]) if data_rows else 0])
    except Exception as e:
        print(f"Error parsing file: {e}")
        return
    
    rows = []
    yahoo_tickers_to_fetch = set()
    mappings = []

    for idx, row in df.iterrows():
        raw_ticker = str(row.get('Ticker', '-')).strip()
        name = str(row.get('Name', 'N/A'))
        cusip = str(row.get('CUSIP', '')).strip()
        
        if raw_ticker == '-' or pd.isna(raw_ticker) or "Ticker" in raw_ticker:
            continue
            
        # Extract Weight%, Mrkt_Value, and Quantity
        try:
            weight_str = str(row.get('Weight%', '0')).replace('%', '').strip()
            weight = float(weight_str)
            
            mv_str = str(row.get('Mrkt_Value', '0')).replace('$', '').replace(',', '').strip()
            market_value = float(mv_str)
            
            qty_str = str(row.get('Quantity', '0')).replace(',', '').strip()
            quantity = float(qty_str)
        except:
            weight, market_value, quantity = 0.0, 0.0, 0.0
            
        user_ticker, yahoo_ticker = resolve_ticker(raw_ticker, name, cusip)
        
        rows.append({
            'Base Ticker': raw_ticker,
            'Company Name': name,
            'Preferred Stock': user_ticker, # BA-A
            'Yahoo Ticker': yahoo_ticker,   # BA-PA
            'Weight (%)': weight,
            'Quantity': quantity,
            'Market Value': market_value,
            'CUSIP': cusip,
            'Name': name
        })
        
        if yahoo_ticker:
            yahoo_tickers_to_fetch.add(yahoo_ticker)
        
        mappings.append(f"{cusip} | {name[:30]:<30} | {raw_ticker:<8} -> {user_ticker:<8} | Yahoo: {yahoo_ticker}")

    # 2. Fetch prices (chunked to avoid API limits)
    all_prices = {}
    chunk_size = 50
    tickers_list = list(yahoo_tickers_to_fetch)
    for i in range(0, len(tickers_list), chunk_size):
        chunk = tickers_list[i : i + chunk_size]
        all_prices.update(fetch_prices(chunk))

    # 3. Compile final PFF Data
    final_rows = []
    for r in rows:
        price = all_prices.get(r['Yahoo Ticker'], 0.0)
        
        # If Yahoo price is 0, try the base ticker (maybe it's a common or the symbol is the same)
        if price == 0 and r['Base Ticker'] != r['Yahoo Ticker']:
            price = all_prices.get(r['Base Ticker'], 0.0)

        final_rows.append({
            'Base Ticker': r['Base Ticker'],
            'Company Name': r['Company Name'],
            'Preferred Stock': r['Preferred Stock'], # BA-A
            'Last Price': price,
            'Full Name': r['Name'],
            'Weight (%)': r['Weight (%)'],
            'Market Value': r.get('Market Value', 0),
            'Quantity': r.get('Quantity', 0),
            'Original Name': r['Name']
        })

    # 4. Save to Dashboard Data Source
    if not final_rows:
        print("[!] No rows to save.")
        return
        
    df_out = pd.DataFrame(final_rows)
    print(f"[*] Generated DataFrame with columns: {df_out.columns.tolist()}")
    
    # Use the exact column name for sorting
    sort_col = 'Weight (%)'
    if sort_col in df_out.columns:
        df_out.sort_values(sort_col, ascending=False, inplace=True)
    else:
        print(f"[!] Warning: {sort_col} not found in {df_out.columns.tolist()}")

    df_out.to_csv(TARGET_CSV, index=False)
    print(f"[+] Dashboard data overwritten: {TARGET_CSV}")

    # 5. Save Report
    with open(REPORT_FILE, 'w', encoding='utf-8') as f:
        f.write("\n".join(mappings))
    print(f"[+] Audit report saved: {REPORT_FILE}")

if __name__ == "__main__":
    process_holdings()
