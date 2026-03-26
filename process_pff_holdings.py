import pandas as pd
import os
import json

# Paths
PFF_CSV = r"c:\Users\orhan.bolek\Desktop\PFF_holdings.csv"
MASTER_XLSX = r"c:\Users\orhan.bolek\Desktop\Updated Master List.xlsx"
OUTPUT_CSV = "pff_holdings_tickers.csv"
SECTOR_MAP_FILE = "sector_map.json"

# ============================================================================
# NON-STANDARD TICKER MAPPING
# The Master List uses non-standard ticker symbols for some companies.
# This maps PFF base tickers to ALL possible ML ticker prefixes.
# ============================================================================
NON_STANDARD_PREFIX_MAP = {
    # Southern Company: PFF uses "SO", ML uses "SOCGP", "SOJC", "SOJD", "SOJE", "SOJF"
    'SO':     ['SOCGP', 'SOJC', 'SOJD', 'SOJE', 'SOJF', 'SO-'],
    # T-Mobile: PFF uses "TMUS", ML uses "TMUSI", "TMUSL", "TMUSZ"
    'TMUS':   ['TMUSI', 'TMUSL', 'TMUSZ'],
    # KKR: ML uses "KKRT", "KKRS"
    'KKR':    ['KKRT', 'KKRS', 'KKR-'],
    # Apollo: ML uses "APOS"
    'APO':    ['APOS', 'APO-'],
    # TPG: ML uses "TPGXL"
    'TPG':    ['TPGXL', 'TPG-'],
    # Xcel Energy: ML uses "XELLL"
    'XEL':    ['XELLL', 'XEL-'],
    # SRE: ML uses "SREA"
    'SRE':    ['SREA', 'SRE-'],
    # CMS Energy: ML uses "CMSA", "CMSC", "CMSD", "CMS-C"
    'CMS':    ['CMSA', 'CMSC', 'CMSD', 'CMS-'],
    # Webster Financial: ML uses "WTFCN"
    'WTFC':   ['WTFCN', 'WTFC-'],
    # AT&T: ML uses "T-A", "T-C", "TBB"
    'T':      ['T-', 'TBB'],
    # CenturyLink/Lumen: ML uses "CTBB", "CTDD"
    'CTL':    ['CTBB', 'CTDD', 'CTL-'],
    # Telephone & Data Systems: ML uses "TDS-U", "TDS-V", "UZD", "UZE", "UZF"
    'TDS':    ['TDS-', 'UZD', 'UZE', 'UZF'],
    # Fifth Third Bancorp: ML uses "FITBM", "FITBI", "FITBP", "FITBO"
    'FITB':   ['FITBM', 'FITBI', 'FITBP', 'FITBO', 'FITB-'],
    # AGNC: ML uses "AGNCN", "AGNCO", "AGNCP"
    'AGNC':   ['AGNCN', 'AGNCO', 'AGNCP', 'AGNC-'],
    # Valley National: ML uses "VLYPO", "VLYPN", "VLYPP"
    'VLY':    ['VLYPN', 'VLYPO', 'VLYPP', 'VLY-'],
    # ConnectOne: ML uses "CNOBP"
    'CNOB':   ['CNOBP', 'CNOB-'],
    # Dime Community: ML uses "DCOMP"
    'DCOM':   ['DCOMP', 'DCOM-'],
    # Huntington Bancshares: ML uses "HBANM", "HBANP"
    'HBANZ':  ['HBANM', 'HBANN', 'HBANO', 'HBANP', 'HBAN-'],
    'HBAN':   ['HBANM', 'HBANN', 'HBANO', 'HBANP', 'HBAN-'],
    # Gladstone Investment: ML uses "GAINL", "GAINZ"
    'GAIN':   ['GAINL', 'GAINZ', 'GAIN-'],
    # Gladstone Land: ML uses "LANDM", "LANDO", "LANDP"
    'LAND':   ['LANDM', 'LANDO', 'LANDP', 'LAND-'],
    # Trinity Capital: ML uses "TRINZ", "TRINI"
    'TRIN':   ['TRINZ', 'TRINI', 'TRINL', 'TRIN-'],
    # Old National: ML uses "ONBPP", "ONBPO"
    'ONB':    ['ONBPP', 'ONBPO', 'ONB-'],
    # Merchants Bancorp: ML uses "MBINM", "MBINN"
    'MBIN':   ['MBINM', 'MBINN', 'MBINO', 'MBINP', 'MBIN-'],
    # Atlanticus: ML uses "ATLCP"
    'ATLC':   ['ATLCP', 'ATLC-'],
    # American Financial Group: ML uses "AFGB", "AFGC", "AFGD", "AFGE"
    'AFG':    ['AFGE', 'AFGD', 'AFGB', 'AFGC', 'AFG-'],
    # NY Mortgage Trust (ADAM = NYMT preferred): ML uses "NYMTM", "NYMTN", "NYMTZ"
    'ADAM':   ['NYMTM', 'NYMTN', 'NYMTZ', 'NYMT-'],
    # Popular: ML uses "BPOPM"
    'BPOP':   ['BPOPM', 'BPOP-'],
    # B. Riley: ML uses "RILYN", "RILYZ"
    'RILY':   ['RILYN', 'RILYZ', 'RILYM', 'RILYO', 'RILYP', 'RILY-'],
    # Saratoga Investment: ML uses "SARM", "SARN"
    'SAR':    ['SARM', 'SARN', 'SAR-'],
    # Runway Growth: ML uses "RWAYL", "RWAYZ"
    'RWAY':   ['RWAYL', 'RWAYZ', 'RWAY-'],
    # Federal Agric Mortgage: ML uses "AGMPP"
    'AGMPP':  ['AGMPP'],
    'FAMCA':  ['AGMPP', 'AGM-', 'AGMA', 'AGMB'],
    # Midland States: ML uses "MSBIN", "MSBIP"
    'MSBI':   ['MSBIP', 'MSBIN', 'MSBI-'],
    # Selective Insurance: ML uses "SIGIP"
    'SIGI':   ['SIGIP', 'SIGI-'],
    # Kemper: ML uses "KMPB"
    'KMPR':   ['KMPB', 'KMPR-'],
    # New Mountain Finance: ML uses "NMFCZ"
    'NMFC':   ['NMFCZ', 'NMFC-'],
    # MFA Financial: ML uses "MFAN"
    'MFA':    ['MFAN', 'MFA-'],
    # Chimera: ML uses "CIMP", "CIMN", "CIMO", "CIM-A/B/C/D"
    'CIM':    ['CIMP', 'CIMN', 'CIMO', 'CIM-'],
    # Ready Capital
    'RC':     ['RC-'],
    # Redwood Trust
    'RWT':    ['RWT-'],
    # Two Harbors
    'TWO':    ['TWO-', 'TWOD'],
    # Armada Hoffler
    'AHH':    ['AHH-'],
    # Truist: ML uses "TFC-R", "TFC-I"
    'TFC':    ['TFC-'],
    # FTAI Aviation: ML uses "FTAIN", "FTAIM"
    'FTAI':   ['FTAIN', 'FTAIM', 'FTAI-'],
    # QVC: ML uses "QVCD", "QVCC"
    'QVCN':   ['QVCD', 'QVCC', 'QVC-'],
    # NEE: ML uses "NEE-N"
    'NEE':    ['NEE-'],
    # Duke Energy: ML uses "DUK-A", "DUKB"
    'DUK':    ['DUK-', 'DUKA', 'DUKB'],
    # Stanley Black & Decker: ML uses "SWKHL"
    'SWK':    ['SWKHL', 'SWK-'],
    # Brookfield Property: ML uses "BPYPN", "BPYPO", "BPYPP"
    'BPY':    ['BPYPN', 'BPYPO', 'BPYPP', 'BPY-'],
    # Brookfield Infrastructure: ML uses "BIPH", "BIPI"
    'BIP':    ['BIPH', 'BIPI', 'BIP-'],
    # Hancock Whitney: ML uses "HWC-D"
    'HWC':    ['HWC-'],
    # PMT
    'PMT':    ['PMTA', 'PMTD', 'PMT-'],
    # Pebblebrook
    'PEB':    ['PEBP', 'PEB-'],
    # CION Investment
    'CICC':   ['CICC-'],
    # BEP/BNCN (Brookfield Renewable/Finance)
    'BEPUCN': ['BEP-'],
    'BNCN':   ['BN-'],
    'BRBRPH': ['BRB-'],
    # WBS
    'WBS':    ['WBS-'],
    # REG
    'REG':    ['REG-'],
    # LXP
    'LXP':    ['LXP-'],
    # BAC
    'BAC':    ['BAC-'],
    # WFC
    'WFC':    ['WFC-'],
    # USB
    'USB':    ['USB-'],
    # MET
    'MET':    ['MET-'],
    # STT
    'STT':    ['STT-'],
    # NYCB
    'NYCB':   ['NYCB-'],
    # Entergy: ML may not have, but try
    'ETR':    ['ETRA', 'ETRB', 'ETR-'],
}

def load_master_list():
    print(f"[*] Loading Master List from {MASTER_XLSX}...")
    df = pd.read_excel(MASTER_XLSX)
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

    def clean_price(val):
        if pd.isna(val) or str(val).strip() == '': return 0.0
        if isinstance(val, str):
            val = val.replace('$', '').replace(',', '').strip()
        try: return float(val)
        except: return 0.0
    
    df['CleanPrice'] = df['Current Price'].apply(clean_price)
    return df

def find_ml_candidates(ml_df, base_ticker, used_set):
    """Find all ML candidates for a given PFF base ticker, using both standard and non-standard prefixes."""
    
    # Get all possible prefixes for this base ticker
    prefixes = [base_ticker + '-']  # Standard: e.g., "MS-"
    if base_ticker in NON_STANDARD_PREFIX_MAP:
        prefixes = NON_STANDARD_PREFIX_MAP[base_ticker]  # Override with comprehensive list
    
    def matches_any_prefix(ticker):
        t = str(ticker)
        for pfx in prefixes:
            if pfx.endswith('-'):
                if t.startswith(pfx) or t == pfx[:-1]:
                    return True
            else:
                if t == pfx:
                    return True
        return False
    
    candidates = ml_df[ml_df['Ticker'].apply(matches_any_prefix)].copy()
    candidates = candidates[~candidates['Ticker'].isin(used_set)]
    return candidates

def process():
    print("=" * 60)
    print("COMPREHENSIVE MASTER-LIST-BASED PFF HOLDINGS PROCESSOR")
    print("=" * 60)
    
    ml_df = load_master_list()

    # Load PFF data
    header_idx = -1
    with open(PFF_CSV, 'r', encoding='utf-8', errors='replace') as f:
        lines = f.readlines()
        for i, line in enumerate(lines):
            if 'Ticker,Name,Sector' in line:
                header_idx = i
                break
    
    if header_idx == -1:
        print("[!] Could not find header row in PFF CSV.")
        return
        
    df_pff = pd.read_csv(PFF_CSV, skiprows=header_idx)
    
    used_tickers = set()
    holdings = []
    match_count = 0
    miss_count = 0

    # Sort by weight descending to prioritize high-weight items
    pff_items = []
    for _, row in df_pff.iterrows():
        raw_t = str(row.get('Ticker', '-')).strip()
        name = str(row.get('Name', 'N/A'))
        if raw_t == '-' or pd.isna(raw_t) or "Ticker" in raw_t or len(raw_t) > 15: continue
        if any(kw in name.upper() for kw in ['BLACKROCK', 'OWNED OR LICENSED', 'CONTENT CONTAINED']): continue
        try:
            p = float(str(row.get('Price', '0')).replace(',', ''))
            w = float(str(row.get('Weight (%)', '0')).replace(',', ''))
            mv = float(str(row.get('Market Value', '0')).replace(',', ''))
            q = float(str(row.get('Quantity', '0')).replace(',', ''))
        except: continue
        pff_items.append({'raw': raw_t, 'name': name, 'p': p, 'w': w, 'mv': mv, 'q': q, 'row': row})
    
    # Sort by weight descending
    pff_items.sort(key=lambda x: x['w'], reverse=True)
    
    print(f"[*] Processing {len(pff_items)} valid PFF holdings...")

    for item in pff_items:
        raw_t = item['raw']
        name = item['name']
        p = item['p']
        w = item['w']
        mv = item['mv']
        q = item['q']
        
        base = raw_t.split('-')[0].strip().upper()
        
        # Find ML candidates
        candidates = find_ml_candidates(ml_df, base, used_tickers)
        
        resolved = None
        sector = "Other"
        
        if not candidates.empty:
            candidates['diff'] = (candidates['CleanPrice'] - p).abs()
            tol = max(4.0, p * 0.15) if p < 100 else p * 0.10
            valid = candidates[candidates['diff'] <= tol]
            if not valid.empty:
                best = valid.sort_values('diff').iloc[0]
                resolved = str(best['Ticker'])
                sector = str(best.get('Sector', 'Other'))
                used_tickers.add(resolved)
                match_count += 1

        if not resolved:
            resolved = raw_t
            miss_count += 1

        holdings.append({
            'Base Ticker': resolved.split('-')[0] if '-' in resolved else base,
            'Company Name': name,
            'Preferred Stock': resolved,
            'Last Price': p,
            'Full Name': name,
            'Weight (%)': w,
            'Market Value': mv,
            'Quantity': q,
            'Sector': sector
        })

    # Export
    df_out = pd.DataFrame(holdings).sort_values('Weight (%)', ascending=False)
    df_out.to_csv(OUTPUT_CSV, index=False)
    
    # Update sector map
    sector_map = {}
    if os.path.exists(SECTOR_MAP_FILE):
        with open(SECTOR_MAP_FILE, 'r') as f:
            sector_map = json.load(f)
    for h in holdings:
        sector_map[h['Preferred Stock']] = h['Sector']
    with open(SECTOR_MAP_FILE, 'w') as f:
        json.dump(sector_map, f, indent=4)
    
    print(f"\n[+] Results: {match_count} matched, {miss_count} unresolved")
    print(f"[+] Exported {len(holdings)} holdings to {OUTPUT_CSV}")
    
    # List unresolved for review
    if miss_count > 0:
        print(f"\n[!] Unresolved tickers ({miss_count}):")
        for h in holdings:
            if h['Preferred Stock'] == h.get('raw', h['Base Ticker']):
                pass
        unres = [h for h in holdings if not any(c in h['Preferred Stock'] for c in ['-']) and h['Preferred Stock'] != 'XTSLA']
        # Just show items where resolved == raw ticker (not a dash ticker)
        for h in sorted(holdings, key=lambda x: -x['Weight (%)']):
            raw_base = h['Base Ticker']
            res = h['Preferred Stock']
            if res == raw_base or (len(res) <= 5 and '-' not in res and res not in ['XTSLA', 'USD']):
                print(f"  {res:12s} ${h['Last Price']:8.2f}  w={h['Weight (%)']:.2f}%  {h['Company Name'][:50]}")

if __name__ == "__main__":
    process()
