import pandas as pd
import glob
import os

PFF_DATA = "pff_holdings_tickers.csv"
OUTPUT_FILE = "pff_holdings_tickers.csv" # Overwrite existing

def get_master_list_path():
    files = glob.glob(r'C:\Users\orhan\Downloads\*2025 Master List*xlsx')
    if not files:
        raise FileNotFoundError("Master List Excel file not found in Downloads.")
    return files[0]

def clean_price(val):
    if pd.isna(val):
        return 0.0
    if isinstance(val, str):
        val = val.replace('$', '').replace(',', '').strip()
    try:
        return float(val)
    except:
        return 0.0

def resolve():
    print(f"[*] Loading PFF data from {PFF_DATA}...")
    df_pff = pd.read_csv(PFF_DATA)
    
    master_path = get_master_list_path()
    print(f"[*] Loading Master List from {master_path}...")
    df_master = pd.read_excel(master_path)
    
    # Pre-process master list
    # The columns identified were: 'Ticker', 'Current Price', 'Issuer'
    df_master = df_master[['Ticker', 'Current Price', 'Issuer']].copy()
    df_master['CleanPrice'] = df_master['Current Price'].apply(clean_price)
    
    print("[*] Starting ticker resolution...")
    resolved_count = 0
    
    for idx, row in df_pff.iterrows():
        base_ticker = str(row['Base Ticker']).strip().upper()
        pff_price = clean_price(row['Last Price'])
        
        # Find candidates in Master List where Ticker starts with base_ticker
        # E.g. if base is JPM, match JPM-A, JPM-D etc.
        # We also handle exactly JPM if it exists as such.
        candidates = df_master[df_master['Ticker'].str.startswith(base_ticker, na=False)].copy()
        
        # Filter candidates to ensure it's a prefix match or exact match followed by dash/nothing
        # e.g. 'JPM' should not match 'JPMORGAN'
        candidates = candidates[candidates['Ticker'].apply(lambda t: t == base_ticker or t.startswith(f"{base_ticker}-"))]
        
        if not candidates.empty:
            # Find the one with minimum price difference
            candidates['diff'] = (candidates['CleanPrice'] - pff_price).abs()
            best_match = candidates.sort_values('diff').iloc[0]
            
            # Update PFF row
            df_pff.at[idx, 'Preferred Stock'] = best_match['Ticker']
            df_pff.at[idx, 'Company Name'] = best_match['Issuer']
            resolved_count += 1
            
            # (Optional) Log significant matches for debugging
            if base_ticker == 'JPM':
                print(f"  [DEBUG] JPM match: PFF Price {pff_price} -> Resolved {best_match['Ticker']} (Price {best_match['CleanPrice']})")

    print(f"[+] Resolution complete. Updated {resolved_count} out of {len(df_pff)} rows.")
    
    # Save results
    df_pff.to_csv(OUTPUT_FILE, index=False)
    print(f"[*] Saved updated results to {OUTPUT_FILE}")

if __name__ == "__main__":
    try:
        resolve()
    except Exception as e:
        print(f"[!] Error: {e}")
