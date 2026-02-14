import yfinance as yf
import pandas as pd
import logging
import time
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta


# MANUAL OVERRIDES for missing Yahoo Info
# Format: Ticker (YF format): 'YYYY-MM-DD'
DIVIDEND_OVERRIDES = {
    'GS-PC': '2026-01-26',
    'GS-PD': '2026-01-26',
    'GS-PA': '2026-01-26'
}

# USER-DEFINED MAPPINGS (User Ticker -> Yahoo Ticker)
TICKER_MAPPINGS = {
    'PCG-I': 'PCG-PI',
    'WRB-F': 'WRB-PF',
    'CNO-A': 'CNO-PA',
    'ETI-': 'ETI-P',
    'NEE-N': 'NEE-PN',
    'PBI-B': 'PBI-PB',
    'WRB-E': 'WRB-PE',
    'WRB-H': 'WRB-PH',
    'F-D': 'F-PD',
    'WRB-G': 'WRB-PG',
    'ALL-B': 'ALL-PB',
    'NEE-U': 'NEE-PU',
    'F-C': 'F-PC',
    'GL-D': 'GL-PD'
}

logging.basicConfig(filename='debug.log', level=logging.DEBUG)

def parse_ticker_yf(raw_ticker):
    """
    Converts user format (e.g., ABR-D) to Yahoo Finance format (ABR-PD).
    Rule 1: Check Manual Mappings
    Rule 2: SYMBOL-SUFFIX -> SYMBOL-PSUFFIX
    Also handles common preferred suffixes like GOODO -> GOOD-PO
    """
    if raw_ticker in TICKER_MAPPINGS:
        return TICKER_MAPPINGS[raw_ticker]

    if '-' in raw_ticker:
        parts = raw_ticker.split('-')
        if len(parts) == 2:
            base, suffix = parts
            # If suffix is just a series letter, it needs -P prefix for YF
            if len(suffix) == 1:
                return f"{base}-P{suffix}"
            return f"{base}-{suffix}"
    
    # HYPHENLESS TICKERS (GOODO, GOODN, CCID, etc.)
    # These often work directly in YF. If they fail, resolve_ticker_yf will try variations.
    return raw_ticker

def fetch_dividends_fallback(raw_ticker):
    """
    Scrapes dividend history from StockAnalysis.com or DividendInvestor.com
    Returns a pandas Series indexed by Date.
    """
    logging.info(f"Attempting scraping fallback for {raw_ticker}")
    
    # Try StockAnalysis first
    # Many preferreds like BUSEP, CCIA, MFICL are here
    formats = [raw_ticker.lower(), raw_ticker.lower().replace('-', '')]
    for fmt in formats:
        url = f"https://stockanalysis.com/stocks/{fmt}/dividend/"
        try:
            r = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
            if r.status_code == 200:
                soup = BeautifulSoup(r.text, 'html.parser')
                table = soup.find('table')
                if table:
                    rows = table.find_all('tr')
                    data = {}
                    for row in rows[1:]:
                        cols = row.find_all('td')
                        if len(cols) >= 2:
                            # StockAnalysis structure: Ex-Date (1st or 2nd col depending on table)
                            # Actually it's often: Amount, Ex-Date (on some views)
                            # or Ex-Date, Amount. Let's be smart.
                            # Usually 1st or 2nd col is date.
                            text1 = cols[0].get_text(strip=True)
                            text2 = cols[1].get_text(strip=True)
                            
                            # Try to find which one is the amount
                            amt = 0.0
                            date_val = None
                            
                            for txt in [text1, text2]:
                                if '$' in txt or (txt.replace('.', '').isdigit() and '.' in txt):
                                    try: 
                                        amt = float(txt.replace('$', '').replace(',', ''))
                                    except: pass
                                else:
                                    try:
                                        date_val = pd.to_datetime(txt)
                                    except: pass
                            
                            if date_val and amt > 0:
                                data[date_val] = amt
                    
                    if data:
                        logging.info(f"Successfully scraped {len(data)} dividends from StockAnalysis for {raw_ticker}")
                        return pd.Series(data).sort_index()
        except Exception as e:
            logging.error(f"StockAnalysis scrape failed for {raw_ticker} ({url}): {e}")

    # Fallback to DividendInvestor (Good for others)
    # URL: https://www.dividendinvestor.com/dividend-history-detail/ticker/
    url = f"https://www.dividendinvestor.com/dividend-history-detail/{raw_ticker.lower().replace('-', '')}/"
    try:
        r = requests.get(url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'}, timeout=10)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, 'html.parser')
            table = soup.find('table', {'id': 'dividends'}) or soup.find('table')
            if table:
                rows = table.find_all('tr')
                data = {}
                for row in rows:
                    if 'detail' not in row.get('class', []): continue
                    cols = row.find_all(['td', 'th'])
                    if len(cols) >= 5:
                        # DivInvestor structure usually: Decl, Ex, Rec, Pay, Type, Amount
                        # Ex is often at index 1, Amount at index 5
                        try:
                            # Prefer 'desktop' span if exists
                            ex_date_td = cols[1]
                            desktop_span = ex_date_td.find('span', class_='desktop')
                            date_str = desktop_span.get_text(strip=True) if desktop_span else ex_date_td.get_text(strip=True)
                            
                            amt_td = cols[-1] # Usually last column
                            desktop_amt = amt_td.find('span', class_='desktop')
                            amt_str = desktop_amt.get_text(strip=True) if desktop_amt else amt_td.get_text(strip=True)
                            
                            if date_str and date_str != 'N/A' and amt_str:
                                date_val = pd.to_datetime(date_str)
                                amt = float(amt_str.replace('$', '').replace(',', ''))
                                if date_val and amt > 0:
                                    data[date_val] = amt
                        except: pass
                if data:
                    logging.info(f"Successfully scraped {len(data)} dividends from DividendInvestor for {raw_ticker}")
                    return pd.Series(data).sort_index()
    except Exception as e:
        logging.error(f"DividendInvestor scrape failed for {raw_ticker}: {e}")

    return pd.Series(dtype=float)

def resolve_ticker_yf(raw_ticker):
    """
    Attempts to find a valid Yahoo Finance ticker by trying several common formats.
    """
    logging.debug(f"Resolving ticker for {raw_ticker}")
    
    standard = parse_ticker_yf(raw_ticker)
    candidates = [standard, raw_ticker]
    
    base, suffix = "", ""
    if '-' in raw_ticker:
        parts = raw_ticker.split('-')
        base, suffix = parts[0], parts[1]
    elif len(raw_ticker) > 3 and raw_ticker[-1].isalpha():
         if len(raw_ticker) == 4:
             base = raw_ticker[:3]
             suffix = raw_ticker[3]
         elif len(raw_ticker) == 5:
             base = raw_ticker[:4]
             suffix = raw_ticker[4]
             
    if base and suffix:
        variations = [f"{base}-P{suffix}", f"{base}.PR{suffix}", f"{base}P-{suffix}", f"{base}-{suffix}", f"{base}{suffix}"]
        for v in variations:
            if v not in candidates: candidates.append(v)
                
    overrides = {
        "PCG-I": ["PCG-PI", "PCG.PRI"],
        "WRB-F": ["WRB-PF", "WRB.PRF"],
    }
    if raw_ticker in overrides:
        candidates.extend(overrides[raw_ticker])
        
    for cand in candidates:
        try:
            t = yf.Ticker(cand)
            hist = t.history(period="5d")
            if not hist.empty:
                return cand
        except:
            pass
    return None

def fetch_and_process(tickers, progress_callback=None):
    results = []
    total = len(tickers)
    
    for i, raw_ticker in enumerate(tickers):
        yf_ticker = parse_ticker_yf(raw_ticker)
        logging.debug(f"Processing {raw_ticker} -> {yf_ticker}")
        
        try:
            ticker = yf.Ticker(yf_ticker)
            df = ticker.history(period="3mo", auto_adjust=True)
            
            if df.empty:
                resolved = resolve_ticker_yf(raw_ticker)
                if resolved:
                     yf_ticker = resolved
                     df = yf.Ticker(resolved).history(period="3mo", auto_adjust=True)
                
            if df.empty:
                logging.error(f"Failed to fetch {yf_ticker} (Empty)")
                continue

            daily_spreads = df['High'] - df['Low']
            avg_daily_spread = daily_spreads.mean()
            high_max = df['High'].max()
            low_min = df['Low'].min()
            
            if pd.isna(high_max) or pd.isna(low_min):
                continue
                
            total_range = high_max - low_min

            if total_range <= 1.00 and avg_daily_spread >= 0.10:
                tv_link_symbol = parse_ticker_tv(raw_ticker)
                pattern = None
                red_bars = df[df['Close'] < df['Open']]

                if not pattern and len(red_bars) >= 12:
                    wick_check = (red_bars['High'] - red_bars['Open']) <= 0.051
                    if wick_check.all():
                        pattern = "Short"

                results.append({
                    'ticker': raw_ticker,
                    'yf_symbol': yf_ticker,
                    'tv_symbol': tv_link_symbol, 
                    'spread': round(total_range, 2),
                    'min': round(low_min, 2),
                    'max': round(high_max, 2),
                    'current': round(df['Close'].iloc[-1], 2),
                    'avg_daily_spread': round(avg_daily_spread, 3),
                    'pattern': pattern
                })
        except Exception as e:
            logging.error(f"Error processing {raw_ticker}: {e}")
        
        if progress_callback:
            if progress_callback(i + 1, total) == 'STOP': break
            
    return results

def fetch_imbalance(tickers, days=30, min_count=20, max_wick=0.12, progress_callback=None):
    results = []
    total = len(tickers)
    for i, raw_ticker in enumerate(tickers):
        if progress_callback:
            if progress_callback(i, total) == 'STOP': return results
        yf_ticker = parse_ticker_yf(raw_ticker)
        tv_symbol = parse_ticker_tv(raw_ticker)
        try:
            ticker_obj = yf.Ticker(yf_ticker)
            df = ticker_obj.history(period="6mo", interval="1d", auto_adjust=True)
            if df.empty:
                 resolved = resolve_ticker_yf(raw_ticker)
                 if resolved:
                     yf_ticker = resolved
                     df = yf.Ticker(resolved).history(period="6mo", interval="1d", auto_adjust=True)
            df = df.dropna(how='all')
            if df.empty or len(df) < days: continue
            df_slice = df.tail(days).copy()
            if 'Close' not in df_slice.columns or 'Open' not in df_slice.columns: continue
            is_green = df_slice['Close'] > df_slice['Open']
            green_wicks = df_slice['Open'] - df_slice['Low']
            green_wick_ok = green_wicks <= (max_wick + 0.00001)
            valid_green = df_slice[is_green & green_wick_ok]
            is_red = df_slice['Open'] > df_slice['Close']
            red_wicks = df_slice['High'] - df_slice['Open']
            red_wick_ok = red_wicks <= (max_wick + 0.00001)
            valid_red = df_slice[is_red & red_wick_ok]
            patterns_found = []
            if len(valid_green) >= min_count:
                patterns_found.append({'type': 'Long', 'count': len(valid_green), 'avg_wick': round(green_wicks[is_green & green_wick_ok].mean(), 4)})
            if len(valid_red) >= min_count:
                patterns_found.append({'type': 'Short', 'count': len(valid_red), 'avg_wick': round(red_wicks[is_red & red_wick_ok].mean(), 4)})
            for p in patterns_found:
                results.append({'ticker': raw_ticker, 'yf_symbol': yf_ticker, 'tv_symbol': tv_symbol, 'type': p['type'], 'match_count': p['count'], 'avg_diff': p['avg_wick'], 'total_days': days, 'target_color': 'Green' if p['type'] == 'Long' else 'Red'})
        except Exception as e:
            logging.error(f"Error processing {raw_ticker}: {e}")
    if progress_callback: progress_callback(total, total)
    return results

def fetch_range_ai(tickers, days=90, max_points=1.0, max_percent=5.0, filter_point=True, filter_percent=True, progress_callback=None):
    results = []
    total = len(tickers)
    for i, raw_ticker in enumerate(tickers):
        if progress_callback:
            if progress_callback(i, total) == 'STOP': return results
        yf_ticker = parse_ticker_yf(raw_ticker)
        tv_symbol = parse_ticker_tv(raw_ticker)
        try:
            ticker_obj = yf.Ticker(yf_ticker)
            df = ticker_obj.history(period="1y", interval="1d", auto_adjust=True)
            if df.empty:
                 resolved = resolve_ticker_yf(raw_ticker)
                 if resolved:
                     yf_ticker = resolved
                     df = yf.Ticker(resolved).history(period="1y", interval="1d", auto_adjust=True)
            df = df.dropna(how='all')
            if df.empty or len(df) < days: continue
            
            # Slice for the analysis period
            df_slice = df.tail(days).copy()
            if df_slice.empty: continue
            
            low_min = df_slice['Low'].min()
            high_max = df_slice['High'].max()
            current_price = df_slice['Close'].iloc[-1]
            
            if pd.isna(low_min) or pd.isna(high_max): continue
            
            point_range = high_max - low_min
            percent_range = (point_range / low_min) * 100 if low_min > 0 else 0
            
            # Dynamic Filtering Logic
            passes_point = point_range <= max_points
            passes_percent = percent_range <= max_percent
            
            keep = False
            if filter_point and filter_percent:
                keep = passes_point and passes_percent
            elif filter_point:
                keep = passes_point
            elif filter_percent:
                keep = passes_percent
            else:
                keep = True # If no filter selected, show all (or could be False)
            
            if keep:
                # Zone Logic (User requested 10 cent buffer)
                # Low Zone: [Low, Low + 0.10]
                # High Zone: [High - 0.10, High]
                low_zone_limit = low_min + 0.10
                high_zone_limit = high_max - 0.10
                
                signal = "Neutral"
                # If Price is near Low (<= Low + 0.10) -> Buy
                if current_price <= low_zone_limit:
                    signal = "Buy"
                # If Price is near High (>= High - 0.10) -> Sell
                elif current_price >= high_zone_limit:
                    signal = "Sell"
                    
                # Duration Analysis
                # Calculate average days for Low->High and High->Low transitions
                # We iterate through the daily bars in the slice
                transitions_lh = [] # Days taken to go from Low Zone to High Zone
                transitions_hl = [] # Days taken to go from High Zone to Low Zone
                
                last_state = None # 'low', 'high', 'mid'
                last_zone_date = None
                
                for date, row in df_slice.iterrows():
                    l, h = row['Low'], row['High']
                    
                    # Determine current state for this bar
                    # We look if the bar TOUCHED the zones
                    touched_low = l <= low_zone_limit
                    touched_high = h >= high_zone_limit
                    
                    current_state = None
                    if touched_low and touched_high:
                        # Touched both in one day? Rare but possible. 
                        # Treat as neutral or skip effectively for cycle count to avoid noise, 
                        # or simpler: check close. Let's stick to touches using precedence or simple state machine.
                        # If we were High, and now touched Low, it's a completion.
                        # For simplicity, let's prioritize the one that completes a cycle if pending.
                        if last_state == 'high': current_state = 'low'
                        elif last_state == 'low': current_state = 'high'
                        else: current_state = 'mid' # Indeterminate
                    elif touched_low:
                        current_state = 'low'
                    elif touched_high:
                        current_state = 'high'
                    else:
                        current_state = 'mid'
                        
                    if current_state in ['low', 'high']:
                        if last_state and current_state != last_state:
                            if last_zone_date:
                                days = (date - last_zone_date).days
                                if last_state == 'low' and current_state == 'high':
                                    transitions_lh.append(days)
                                elif last_state == 'high' and current_state == 'low':
                                    transitions_hl.append(days)
                        
                        # Update state if we are in a zone
                        if current_state != last_state:
                            last_state = current_state
                            last_zone_date = date
                            
                avg_days_lh = round(sum(transitions_lh) / len(transitions_lh), 1) if transitions_lh else 0
                avg_days_hl = round(sum(transitions_hl) / len(transitions_hl), 1) if transitions_hl else 0

                # Sanitize NaN values for JSON compatibility
                def sanitize_float(val):
                    return val if pd.notna(val) else None

                results.append({
                    'ticker': raw_ticker, 
                    'yf_symbol': yf_ticker, 
                    'tv_symbol': tv_symbol, 
                    'min': sanitize_float(round(low_min, 2)), 
                    'max': sanitize_float(round(high_max, 2)), 
                    'current': sanitize_float(round(current_price, 2)), 
                    'point_range': sanitize_float(round(point_range, 2)), 
                    'percent_range': sanitize_float(round(percent_range, 2)), 
                    'days': days,
                    'signal': signal,
                    'avg_days_low_to_high': avg_days_lh,
                    'avg_days_high_to_low': avg_days_hl
                })
        except Exception as e:
            logging.error(f"Error in Range AI for {raw_ticker}: {e}")
    if progress_callback: progress_callback(total, total)
    return results

def analyze_dividend_recovery(raw_ticker, lookback=3, recovery_window=5):
    yf_ticker = parse_ticker_yf(raw_ticker)
    logging.debug(f"Dividend Recovery: {raw_ticker} -> {yf_ticker}")
    # Retry wrapper for robustness
    max_retries = 3
    for attempt in range(max_retries):
        try:
            ticker = yf.Ticker(yf_ticker)
            dividends = ticker.dividends
            
            # If dividends found, we break the retry loop and proceed
            # If strictly empty, it might just be empty, not an error.
            # But if it crashes, we retry.
            break
        except Exception as e:
            if attempt == max_retries - 1:
                logging.error(f"Failed to fetch data for {raw_ticker} after retries: {e}")
                tv_symbol = parse_ticker_tv(raw_ticker)
                return {'ticker': raw_ticker, 'tv_symbol': tv_symbol, 'error': str(e), 'dividends': [], 'current_price': None, 'days_since_last_div': None}
            time.sleep(1 * (attempt + 1))

    try:
        if dividends.empty:
            resolved = resolve_ticker_yf(raw_ticker)
            if resolved:
                 yf_ticker = resolved
                 ticker = yf.Ticker(resolved)
                 dividends = ticker.dividends
        
        # FINAL FALLBACK: Scraping
        if dividends.empty:
            dividends = fetch_dividends_fallback(raw_ticker)

        if dividends.empty:
            tv_symbol = parse_ticker_tv(raw_ticker)
            return {'ticker': raw_ticker, 'tv_symbol': tv_symbol, 'error': 'No dividend history found', 'dividends': [], 'current_price': None, 'days_since_last_div': None}
        
        recent_divs = dividends.tail(lookback)
        
        # Fetch History with Retry
        hist = pd.DataFrame()
        for attempt in range(max_retries):
            try:
                hist = ticker.history(period="2y", auto_adjust=False)
                if not hist.empty: break
            except:
                time.sleep(1)
        
        if hist.empty:
            # If price history failed too, try the resolved ticker
            resolved = resolve_ticker_yf(raw_ticker)
            if resolved:
                 hist = yf.Ticker(resolved).history(period="2y", auto_adjust=False)
        
        if hist.empty:
            tv_symbol = parse_ticker_tv(raw_ticker)
            return {'ticker': raw_ticker, 'tv_symbol': tv_symbol, 'error': 'No price history found', 'dividends': [], 'current_price': None, 'days_since_last_div': None}
        
        current_price = hist['Close'].iloc[-1]
        # Handle NaN values to prevent JSON serialization issues
        if pd.isna(current_price):
            current_price = None
        dividend_analysis = []
        for ex_date, amount in recent_divs.items():
            # Standardize ex_date to be naive if history index is naive, or match timezone
            if hist.index.tz is not None and ex_date.tzinfo is None:
                ex_date = ex_date.tz_localize(hist.index.tz)
            elif hist.index.tz is None and ex_date.tzinfo is not None:
                ex_date = ex_date.tz_localize(None)
            
            ex_date_str = ex_date.strftime('%Y-%m-%d')
            pre_div_close = None
            for i in range(5):
                check_date = ex_date - timedelta(days=i+1)
                if check_date in hist.index:
                    pre_div_close = hist.loc[check_date, 'Close']
                    break
            # Check for NaN in pre_div_close
            if pre_div_close is None or pd.isna(pre_div_close):
                dividend_analysis.append({'ex_date': ex_date_str, 'amount': round(amount, 3), 'error': 'Price data missing', 'recovered': False, 'recovery_days': 9999, 'current_distance': 0, 'window_recv_pct': 0})
                continue
            recovered = False
            recovery_days = None
            current_distance = 0
            future_dates = hist[hist.index >= ex_date]
            if not future_dates.empty:
                for date, row in future_dates.iterrows():
                    days_elapsed = (date.date() - ex_date.date()).days
                    high_val = row['High']
                    if pd.notna(high_val) and high_val >= pre_div_close:
                        recovered = True
                        recovery_days = days_elapsed
                        break
                if not recovered:
                    latest_close = future_dates['Close'].iloc[-1]
                    if pd.notna(latest_close):
                        current_distance = round(pre_div_close - latest_close, 2)
                    else:
                        current_distance = 0
                    recovery_days = (datetime.now().date() - ex_date.date()).days
            future_dates_window = future_dates.head(recovery_window)
            window_recv_pct = 0.0
            if not future_dates_window.empty and amount > 0:
                max_high_window = future_dates_window['High'].max()
                if pd.notna(max_high_window):
                    theoretical_base = pre_div_close - amount
                    recovered_amt = max_high_window - theoretical_base
                    window_recv_pct = round((recovered_amt / amount) * 100, 1)
                    # Handle NaN in window_recv_pct
                    if pd.isna(window_recv_pct):
                        window_recv_pct = 0.0
            
            # === PRE-DIVIDEND 14-DAY ANALYSIS ===
            pre_div_14d_analysis = None
            try:
                # Get 14 trading days before ex_date
                pre_div_dates = hist[hist.index < ex_date].tail(14)
                if len(pre_div_dates) >= 3:
                    lowest_price = pre_div_dates['Low'].min()
                    highest_price = pre_div_dates['High'].max()
                    
                    # Find which day had the lowest price
                    lowest_day = None
                    highest_day = None
                    if pd.notna(lowest_price):
                        lowest_idx = pre_div_dates['Low'].idxmin()
                        lowest_day = -(len(pre_div_dates) - pre_div_dates.index.get_loc(lowest_idx))
                    if pd.notna(highest_price):
                        highest_idx = pre_div_dates['High'].idxmax()
                        highest_day = -(len(pre_div_dates) - pre_div_dates.index.get_loc(highest_idx))
                    
                    # Get close prices for 14-day change calculation
                    close_on_ex_minus_1 = pre_div_dates['Close'].iloc[-1] if len(pre_div_dates) >= 1 else None
                    close_on_ex_minus_14 = pre_div_dates['Close'].iloc[0] if len(pre_div_dates) >= 14 else pre_div_dates['Close'].iloc[0]
                    
                    # Calculate price change percentage and dollar amount
                    price_change_pct = 0.0
                    price_change_usd = 0.0
                    if pd.notna(close_on_ex_minus_1) and pd.notna(close_on_ex_minus_14) and close_on_ex_minus_14 > 0:
                        price_change_pct = round(((close_on_ex_minus_1 - close_on_ex_minus_14) / close_on_ex_minus_14) * 100, 2)
                        price_change_usd = round(close_on_ex_minus_1 - close_on_ex_minus_14, 2)
                    
                    # Pump Detection: Compare first 3 days avg vs last 3 days avg
                    pump_detected = False
                    pump_start_day = None
                    if len(pre_div_dates) >= 6:
                        first_3_avg = pre_div_dates['Close'].iloc[:3].mean()
                        last_3_avg = pre_div_dates['Close'].iloc[-3:].mean()
                        if pd.notna(first_3_avg) and pd.notna(last_3_avg) and first_3_avg > 0:
                            pump_pct = ((last_3_avg - first_3_avg) / first_3_avg) * 100
                            if pump_pct >= 1.0:  # 1% or more increase
                                pump_detected = True
                                # Find pump start: first day where close > previous close
                                for i in range(1, len(pre_div_dates)):
                                    if pre_div_dates['Close'].iloc[i] > pre_div_dates['Close'].iloc[i-1]:
                                        pump_start_day = -(len(pre_div_dates) - i)
                                        break
                    
                    pre_div_14d_analysis = {
                        'lowest_price': round(lowest_price, 2) if pd.notna(lowest_price) else None,
                        'lowest_day': lowest_day,
                        'highest_price': round(highest_price, 2) if pd.notna(highest_price) else None,
                        'highest_day': highest_day,
                        'close_ex_m1': round(close_on_ex_minus_1, 2) if pd.notna(close_on_ex_minus_1) else None,
                        'close_ex_m14': round(close_on_ex_minus_14, 2) if pd.notna(close_on_ex_minus_14) else None,
                        'price_change_pct': price_change_pct if pd.notna(price_change_pct) else 0.0,
                        'price_change_usd': price_change_usd if pd.notna(price_change_usd) else 0.0,
                        'pump_detected': pump_detected,
                        'pump_start_day': pump_start_day,
                        'avg_120d_volume': 0,
                        'avg_14d_volume': 0,
                        'volume_spike_detected': False,
                        'volume_spike_pct': 0
                    }
                    
                    # Volume Analysis
                    try:
                        # 120 day average volume leading up to ex_date
                        history_120d = hist[hist.index < ex_date].tail(120)
                        if not history_120d.empty and 'Volume' in history_120d.columns:
                            avg_120d = history_120d['Volume'].mean()
                            
                            # 14 day average volume before ex_date (same as pre_div_dates)
                            if 'Volume' in pre_div_dates.columns:
                                avg_14d = pre_div_dates['Volume'].mean()
                                
                                vol_spike = False
                                spike_pct = 0
                                if avg_120d > 0 and avg_14d > 0:
                                    if avg_14d >= avg_120d * 1.10: # 10% or more increase
                                        vol_spike = True
                                        spike_pct = round(((avg_14d - avg_120d) / avg_120d) * 100, 1)
                                
                                pre_div_14d_analysis.update({
                                    'avg_120d_volume': int(avg_120d),
                                    'avg_14d_volume': int(avg_14d),
                                    'volume_spike_detected': vol_spike,
                                    'volume_spike_pct': spike_pct
                                })
                    except Exception as ve:
                        logging.error(f"Volume analysis error: {ve}")
            except Exception as e:
                logging.error(f"Pre-div 14d analysis error for {raw_ticker}: {e}")
                pre_div_14d_analysis = None
            
            dividend_analysis.append({
                'ex_date': ex_date_str, 
                'amount': round(amount, 3), 
                'pre_div_close': round(pre_div_close, 2), 
                'recovered': recovered, 
                'recovery_days': recovery_days, 
                'current_distance': 0 if recovered else current_distance, 
                'window_recv_pct': window_recv_pct,
                'pre_div_14d': pre_div_14d_analysis
            })
        last_div_date = None
        days_since_last = None
        if not recent_divs.empty:
            last_div_date = recent_divs.index[-1]
            days_since_last = (datetime.now().date() - last_div_date.date()).days
        
        # Next Dividend Logic
        next_div_days = None
        next_ex_date = None
        
        # 1. CHECK MANUAL OVERRIDES FIRST
        if yf_ticker in DIVIDEND_OVERRIDES:
            try:
                next_ex_date = datetime.strptime(DIVIDEND_OVERRIDES[yf_ticker], '%Y-%m-%d')
                logging.info(f"Using Manual Override for {yf_ticker}: {next_ex_date}")
            except Exception as e:
                logging.error(f"Invalid override format for {yf_ticker}: {e}")
        
        if not next_ex_date:
            try:
                # INFO FETCH WITH RETRY
                ex_div_ts = None
                cal = None
            
                for attempt in range(max_retries):
                    try:
                        ex_div_ts = ticker.info.get("exDividendDate")
                        if ex_div_ts: break
                        
                        cal = ticker.calendar
                        if cal: break
                    except:
                        time.sleep(1)
    
                if ex_div_ts:
                    next_ex_date = datetime.fromtimestamp(ex_div_ts)
                elif cal and 'Ex-Dividend Date' in cal:
                    val = cal['Ex-Dividend Date']
                    # Helper for Calendar List/Series
                    if hasattr(val, 'iloc'): # Series
                        val = val.iloc[0]
                    elif isinstance(val, list) and val:
                        val = val[0]
                        
                    if hasattr(val, 'date'): # Could be date or datetime
                        next_ex_date = datetime.combine(val, datetime.min.time()) if not isinstance(val, datetime) else val
                    elif isinstance(val, (str, datetime)): # Direct value
                         next_ex_date = val
            except Exception as e:
                logging.error(f"Next Div Logic Error {raw_ticker}: {e}")
        
        # Estimation Fallback
        if not next_ex_date or (isinstance(next_ex_date, datetime) and next_ex_date.date() < datetime.now().date()):
            if not dividends.empty:
                last_ex = dividends.index[-1].replace(tzinfo=None)
                
                # Smarter Frequency Detection
                offsets = []
                if len(dividends) >= 2:
                    # Calculate gaps between recent dividends to find periodicity
                    for j in range(1, min(5, len(dividends))):
                        gap = (dividends.index[-j] - dividends.index[-(j+1)]).days
                        offsets.append(gap)
                
                # Median gap
                freq = 91 
                if offsets:
                    offsets.sort()
                    median_gap = offsets[len(offsets)//2]
                    # Classify: Monthly (~30), Quarterly (~91), Semi-Annual (~182), Annual (~365)
                    if 25 <= median_gap <= 35: freq = 30
                    elif 80 <= median_gap <= 100: freq = 91
                    elif 170 <= median_gap <= 195: freq = 182
                    else: freq = median_gap
                
                if freq < 20: freq = 30 # Default to monthly if very low
                
                # Roll forward until we find a date in the future
                next_ex_date = last_ex + timedelta(days=freq)
                while next_ex_date.date() < datetime.now().date():
                    next_ex_date += timedelta(days=freq)
        
        if next_ex_date:
            next_div_days = (next_ex_date.date() - datetime.now().date()).days

        # Calculate 30-day average volume
        avg_volume_30d = None
        try:
            if not hist.empty and 'Volume' in hist.columns:
                last_30_days = hist.tail(30)
                if not last_30_days.empty:
                    avg_vol = last_30_days['Volume'].mean()
                    if pd.notna(avg_vol):
                        avg_volume_30d = int(avg_vol)
        except Exception as e:
            logging.error(f"Error calculating 30d avg volume for {raw_ticker}: {e}")

        tv_symbol = parse_ticker_tv(raw_ticker)
        return {
            'ticker': raw_ticker, 
            'tv_symbol': tv_symbol, 
            'dividends': dividend_analysis, 
            'current_price': round(current_price, 2) if current_price is not None else None, 
            'days_since_last_div': days_since_last,
            'next_div_days': next_div_days,
            'next_ex_date': next_ex_date.strftime('%Y-%m-%d') if next_ex_date else None,
            'avg_volume_30d': avg_volume_30d
        }
    except Exception as e:
        logging.error(f"Error in Dividend Recovery for {raw_ticker}: {e}")
        tv_symbol = parse_ticker_tv(raw_ticker)
        return {'ticker': raw_ticker, 'tv_symbol': tv_symbol, 'error': str(e), 'dividends': [], 'current_price': None, 'days_since_last_div': None}

def fetch_rebalance_patterns(tickers, months_back=12, progress_callback=None):
    results = []
    total = len(tickers)
    for i, raw_ticker in enumerate(tickers):
        if progress_callback:
            if progress_callback(i, total) == 'STOP': return results
        
        yf_ticker = parse_ticker_yf(raw_ticker)
        tv_symbol = parse_ticker_tv(raw_ticker)
        
        try:
            ticker_obj = yf.Ticker(yf_ticker)
            # Fetch 2 years for context and rolling averages
            df = ticker_obj.history(period="2y", interval="1d", auto_adjust=True)
            if df.empty:
                resolved = resolve_ticker_yf(raw_ticker)
                if resolved:
                    yf_ticker = resolved
                    ticker_obj = yf.Ticker(resolved)
                    df = ticker_obj.history(period="2y", interval="1d", auto_adjust=True)
            
            df = df.dropna(how='all')
            if df.empty or len(df) < 100: continue # Need enough for 90d avg
            
            # 90-day rolling volume average
            df['AvgVol90'] = df['Volume'].rolling(window=90).mean()
            
            # Get dividends
            dividends = ticker_obj.dividends
            
            # Identify month-end rebalance days
            df['Month'] = df.index.month
            df['Year'] = df.index.year
            reb_indices = df.groupby(['Year', 'Month']).tail(1).index
            
            events = []
            for reb_day in reb_indices:
                try:
                    idx = df.index.get_loc(reb_day)
                    if idx < 90 or idx + 3 >= len(df): continue
                    
                    # 1. Bar Color Consistency (3/4 Rule)
                    # Window: [idx-3, idx-2, idx-1, idx]
                    colors = []
                    for w_idx in range(idx-3, idx+1):
                        bar = df.iloc[w_idx]
                        if bar['Close'] >= bar['Open']:
                            colors.append('G')
                        else:
                            colors.append('R')
                    
                    green_count = colors.count('G')
                    red_count = colors.count('R')
                    
                    if max(green_count, red_count) < 3:
                        continue # Skip if 3/4 rule not met
                    
                    # 2. Volume Breakdown
                    avg_vol_90 = df.iloc[idx]['AvgVol90']
                    pre_vols = df.iloc[idx-3:idx]['Volume']
                    pre_vol_avg = pre_vols.mean()
                    reb_vol = df.iloc[idx]['Volume']
                    post_vols = df.iloc[idx+1:idx+4]['Volume']
                    post_vol_avg = post_vols.mean()
                    
                    # 3. Dividend Intersection
                    start_date = df.index[idx-3]
                    end_date = df.index[idx+3]
                    window_divs = []
                    for div_date, amount in dividends.items():
                        if start_date <= div_date <= end_date:
                            try:
                                d_idx = df.index.get_loc(div_date)
                                d_vol = df.iloc[d_idx]['Volume']
                            except:
                                d_vol = 0
                            window_divs.append({
                                'date': div_date.strftime('%Y-%m-%d'),
                                'amount': float(amount),
                                'volume': int(d_vol)
                            })
                    
                    # 4. Close-to-Close Dollar Differences & Reba Metrics
                    p_minus_3_close = df.iloc[idx-3]['Close']
                    p_reb_close = df.iloc[idx]['Close']
                    p_plus_3_close = df.iloc[idx+3]['Close']
                    
                    # Reba specific: Body (C-O) and Range (H-L)
                    reb_bar = df.iloc[idx]
                    reba_body_diff = reb_bar['Close'] - reb_bar['Open']
                    reba_range_diff = reb_bar['High'] - reb_bar['Low']
                    reba_color = 'Green' if reba_body_diff >= 0 else 'Red'
                    
                    diff_pre = p_reb_close - p_minus_3_close
                    diff_post = p_plus_3_close - p_reb_close
                    
                    # Trend description remains for metadata
                    perf_pre_pct = ((p_reb_close - p_minus_3_close) / p_minus_3_close) * 100 if p_minus_3_close > 0 else 0
                    perf_post_pct = ((p_plus_3_close - p_reb_close) / p_reb_close) * 100 if p_reb_close > 0 else 0
                    trend = "Flat"
                    if perf_pre_pct < -0.5 and perf_post_pct > 0.5: trend = "Sell-off then Recovery"
                    elif perf_pre_pct > 0.5 and perf_post_pct < -0.5: trend = "Pump then Dump"
                    elif perf_pre_pct < -1: trend = "Strong Selling"
                    elif perf_post_pct > 1: trend = "Strong Buying"
                    
                    events.append({
                        'date': reb_day.strftime('%Y-%m-%d'),
                        'pre_3_diff': round(diff_pre, 3),
                        'pre_3_pct': round(perf_pre_pct, 4),
                        'post_3_diff': round(diff_post, 3),
                        'post_3_pct': round(perf_post_pct, 4),
                        'reba_body_diff': round(reba_body_diff, 3),
                        'reba_body_pct': round((reba_body_diff / reb_bar['Open'] * 100), 4) if reb_bar['Open'] > 0 else 0,
                        'reba_range_diff': round(reba_range_diff, 3),
                        'reba_color': reba_color,
                        'avg_vol_90': round(avg_vol_90, 0),
                        'pre_vol_avg': round(pre_vol_avg, 0),
                        'reb_vol': round(reb_vol, 0),
                        'post_vol_avg': round(post_vol_avg, 0),
                        'dividends': window_divs,
                        'trend': trend,
                        'dominant_color': 'Green' if green_count >= 3 else 'Red'
                    })
                except Exception as ex:
                    logging.error(f"Error processing reb_day {reb_day} for {raw_ticker}: {ex}")
                    continue
            
            recent_events = events[-months_back:]
            if not recent_events: continue
            
            pre_diffs = [e['pre_3_diff'] for e in recent_events]
            pre_pcts = [e['pre_3_pct'] for e in recent_events]
            post_diffs = [e['post_3_diff'] for e in recent_events]
            post_pcts = [e['post_3_pct'] for e in recent_events]
            reba_body_diffs = [e['reba_body_diff'] for e in recent_events]
            reba_body_pcts = [e['reba_body_pct'] for e in recent_events]
            reba_range_diffs = [e['reba_range_diff'] for e in recent_events]
            reba_colors = [e['reba_color'] for e in recent_events]
            
            green_reba = reba_colors.count('Green')
            red_reba = reba_colors.count('Red')
            reba_dominant = 'Green' if green_reba >= red_reba else 'Red'

            results.append({
                'ticker': raw_ticker,
                'yf_symbol': yf_ticker,
                'tv_symbol': tv_symbol,
                'avg_pre_3_diff': round(sum(pre_diffs) / len(pre_diffs), 3),
                'pre_3_min': round(min(pre_diffs), 3),
                'pre_3_max': round(max(pre_diffs), 3),
                'pre_3_std': round(pd.Series(pre_diffs).std(), 3) if len(pre_diffs) > 1 else 0,
                'pre_3_std_pct': round(pd.Series(pre_pcts).std(), 3) if len(pre_pcts) > 1 else 0,
                'pre_3_pos': sum(1 for d in pre_diffs if d > 0),
                'pre_3_neg': sum(1 for d in pre_diffs if d < 0),
                
                'avg_post_3_diff': round(sum(post_diffs) / len(post_diffs), 3),
                'post_3_min': round(min(post_diffs), 3),
                'post_3_max': round(max(post_diffs), 3),
                'post_3_std': round(pd.Series(post_diffs).std(), 3) if len(post_diffs) > 1 else 0,
                'post_3_std_pct': round(pd.Series(post_pcts).std(), 3) if len(post_pcts) > 1 else 0,
                'post_3_pos': sum(1 for d in post_diffs if d > 0),
                'post_3_neg': sum(1 for d in post_diffs if d < 0),
                
                'avg_reba_body_diff': round(sum(reba_body_diffs) / len(reba_body_diffs), 3),
                'reba_body_min': round(min(reba_body_diffs), 3),
                'reba_body_max': round(max(reba_body_diffs), 3),
                'reba_body_std': round(pd.Series(reba_body_diffs).std(), 3) if len(reba_body_diffs) > 1 else 0,
                'reba_body_std_pct': round(pd.Series(reba_body_pcts).std(), 3) if len(reba_body_pcts) > 1 else 0,
                'reba_body_pos': sum(1 for d in reba_body_diffs if d > 0),
                'reba_body_neg': sum(1 for d in reba_body_diffs if d < 0),
                
                'avg_reba_range_diff': round(sum(reba_range_diffs) / len(reba_range_diffs), 3),
                'reba_dominant_color': reba_dominant,
                'avg_vol_90': round(sum([e['avg_vol_90'] for e in recent_events]) / len(recent_events), 0),
                'avg_pre_vol': round(sum([e['pre_vol_avg'] for e in recent_events]) / len(recent_events), 0),
                'avg_reb_vol': round(sum([e['reb_vol'] for e in recent_events]) / len(recent_events), 0),
                'avg_post_vol': round(sum([e['post_vol_avg'] for e in recent_events]) / len(recent_events), 0),
                'sample_size': len(recent_events),
                'details': recent_events
            })
            
        except Exception as e:
            logging.error(f"Error in Rebalance Patterns for {raw_ticker}: {e}")
            
    if progress_callback: progress_callback(total, total)
    return results

def parse_ticker_tv(raw_ticker):
    """
    Converts user format (e.g., ABR-D) to TradingView format (ABR/PD).
    Rule: SYMBOL-SUFFIX -> SYMBOL/PSUFFIX
    """
    if '-' in raw_ticker:
        parts = raw_ticker.split('-')
        if len(parts) == 2:
            base, suffix = parts
            # TradingView standard for preferreds: SYMBOL/P + SUFFIX
            return f"{base}/P{suffix}"
    return raw_ticker
