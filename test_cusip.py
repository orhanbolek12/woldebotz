import requests
import re
import json
import time

# Load OpenFIGI results
with open("pff_figi_results.json", "r", encoding="utf-8") as f:
    holdings = json.load(f)

print(f"Total holdings: {len(holdings)}")

# Strategy: For preferred stocks, Yahoo Finance uses format: BASE-PA, BASE-PB etc.
# User wants: BASE-A, BASE-B format
# We need to figure out the series letter for each holding

# OpenFIGI gives us tickers like:
#   "WFC 7.5 PERP L" -> series L -> WFC-L (Yahoo: WFC-PL)
#   "BA 6 10/15/27"  -> no series letter -> need to find it
#   "JPM 4.75 PERP DD" -> series DD -> JPM-DD (Yahoo: JPM-PDD)

# For those without series letters, let's try Yahoo Finance search API
# to find the correct preferred ticker

def find_yahoo_preferred_ticker(base_ticker, cusip, name_hint=""):
    """Search Yahoo Finance for the correct preferred ticker."""
    # Try searching by ticker
    try:
        url = f"https://query1.finance.yahoo.com/v1/finance/search?q={base_ticker}+preferred&quotesCount=20&newsCount=0"
        headers = {'User-Agent': 'Mozilla/5.0'}
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            quotes = data.get('quotes', [])
            preferred_tickers = []
            for q in quotes:
                symbol = q.get('symbol', '')
                qname = q.get('longname', '') or q.get('shortname', '')
                qtype = q.get('quoteType', '')
                # Look for preferred tickers matching base
                if symbol.startswith(base_ticker + '-P') or symbol.startswith(base_ticker + '-'):
                    preferred_tickers.append({
                        'symbol': symbol,
                        'name': qname,
                        'type': qtype,
                    })
            return preferred_tickers
    except Exception as e:
        pass
    return []


def try_yahoo_price(ticker):
    """Quick check if a Yahoo ticker is valid by getting its price."""
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=1d"
        headers = {'User-Agent': 'Mozilla/5.0'}
        resp = requests.get(url, headers=headers, timeout=8)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('chart', {}).get('result'):
                meta = data['chart']['result'][0].get('meta', {})
                price = meta.get('regularMarketPrice')
                if price and price > 0:
                    return price
    except:
        pass
    return None


# For holdings that already have series, verify on Yahoo
# For holdings without series, try to find the correct ticker

# Common preferred ticker patterns on Yahoo Finance:
# BASE-PA, BASE-PB, BASE-PC, ... BASE-PZ
# BASE-PRA, BASE-PRB (some older style)
# Some special: BEPC-PA, etc.

# Also some tickers use different names on Yahoo:
# FAMCA -> on Yahoo it might be under a different base

results = []

for i, h in enumerate(holdings):
    base = h.get('base_ticker', h['ticker'])
    series = h.get('series', '')
    figi_ticker = h.get('figi_ticker', '')
    cusip = h['cusip']
    name = h['name']
    weight = h['weight']
    
    yahoo_ticker = None
    user_ticker = None
    price = None
    
    if series:
        # We have a series letter, construct Yahoo ticker
        yahoo_ticker = f"{base}-P{series}"
        user_ticker = f"{base}-{series}"
        
        # Verify it works on Yahoo
        price = try_yahoo_price(yahoo_ticker)
        
        if not price:
            # Maybe Yahoo doesn't use -P prefix for this one
            price = try_yahoo_price(f"{base}-{series}")
            if price:
                yahoo_ticker = f"{base}-{series}"
    else:
        # No series - need to figure out from OpenFIGI description
        # Some patterns in figi_ticker:
        # "BA 6 10/15/27" - convertible/mandatory, may trade as BA-PA on Yahoo
        # "ORCL 6.5 01/15/29 D" - the D could be a series
        # "COF F PERP N" - wait, N is the series
        
        # Try to extract more from the figi description
        parts = figi_ticker.split() if figi_ticker else []
        
        # Check if there's a letter or code after the date/PERP
        # Some have format: "BASE RATE PERP" (no series - truly perpetual preferred)
        
        # Try common Yahoo patterns: base-PA, base-PB, etc.
        # Search Yahoo for this base ticker's preferreds
        yahoo_results = find_yahoo_preferred_ticker(base, cusip, name)
        
        if yahoo_results:
            # Try to match by checking prices or just take the first valid one
            for yr in yahoo_results:
                p = try_yahoo_price(yr['symbol'])
                if p:
                    yahoo_ticker = yr['symbol']
                    # Convert Yahoo format (BASE-PA) to user format (BASE-A)
                    if '-P' in yahoo_ticker:
                        user_ticker = yahoo_ticker.replace('-P', '-')
                    else:
                        user_ticker = yahoo_ticker
                    price = p
                    break
        
        if not yahoo_ticker:
            # Last resort: try the original ticker from holdings file
            # Some tickers in the file might already be special tickers
            orig = h['ticker']
            # Try common patterns
            for suffix in ['', '-PA', '-PB', '-PC', '-PD', '-PE']:
                test = orig + suffix if suffix else orig
                p = try_yahoo_price(test)
                if p:
                    yahoo_ticker = test
                    user_ticker = test.replace('-P', '-') if '-P' in test else test
                    price = p
                    break
    
    if not user_ticker:
        user_ticker = h.get('resolved', h['ticker'])
    if not yahoo_ticker:
        yahoo_ticker = user_ticker
    
    results.append({
        'user_ticker': user_ticker,
        'yahoo_ticker': yahoo_ticker,
        'original_ticker': h['ticker'],
        'name': name,
        'weight': weight,
        'cusip': cusip,
        'price': price,
        'figi_ticker': figi_ticker,
        'series': series,
    })
    
    status = f"${price}" if price else "NO PRICE"
    print(f"[{i+1}/{len(holdings)}] {h['ticker']:<10} -> {user_ticker:<15} (Yahoo: {yahoo_ticker:<15}) {status}")
    
    # Rate limit Yahoo
    if i % 5 == 0 and i > 0:
        time.sleep(0.5)

# Save results
with open("pff_resolved_full.json", "w", encoding="utf-8") as f:
    json.dump(results, f, indent=2)

# Stats
priced = sum(1 for r in results if r['price'])
print(f"\nTotal: {len(results)}")
print(f"With price: {priced}")
print(f"Without price: {len(results) - priced}")

# Show unresolved
unresolved = [r for r in results if not r['price']]
if unresolved:
    print(f"\nUnresolved ({len(unresolved)}):")
    for r in unresolved:
        print(f"  {r['original_ticker']:<10} {r['cusip']:<15} {r['figi_ticker']:<30} {r['name'][:40]}")
