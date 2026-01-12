import yfinance as yf
import pandas as pd
import logging
import time
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

logging.basicConfig(filename='debug.log', level=logging.DEBUG)

def parse_ticker_yf(raw_ticker):
    """
    Converts user format (e.g., ABR-D) to Yahoo Finance format (ABR-PD).
    Rule: SYMBOL-SUFFIX -> SYMBOL-PSUFFIX
    """
    if '-' in raw_ticker:
        parts = raw_ticker.split('-')
        if len(parts) == 2:
            base, suffix = parts
            return f"{base}-P{suffix}"
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

def fetch_range_ai(tickers, days=90, max_points=1.0, max_percent=5.0, progress_callback=None):
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
            df_slice = df.tail(days).copy()
            if df_slice.empty: continue
            low_min = df_slice['Low'].min()
            high_max = df_slice['High'].max()
            current_price = df_slice['Close'].iloc[-1]
            if pd.isna(low_min) or pd.isna(high_max): continue
            point_range = high_max - low_min
            percent_range = (point_range / low_min) * 100 if low_min > 0 else 0
            if point_range <= max_points and percent_range <= max_percent:
                results.append({'ticker': raw_ticker, 'yf_symbol': yf_ticker, 'tv_symbol': tv_symbol, 'min': round(low_min, 2), 'max': round(high_max, 2), 'current': round(current_price, 2), 'point_range': round(point_range, 2), 'percent_range': round(percent_range, 2), 'days': days})
        except Exception as e:
            logging.error(f"Error in Range AI for {raw_ticker}: {e}")
    if progress_callback: progress_callback(total, total)
    return results

def analyze_dividend_recovery(raw_ticker, lookback=3, recovery_window=5):
    yf_ticker = parse_ticker_yf(raw_ticker)
    logging.debug(f"Dividend Recovery: {raw_ticker} -> {yf_ticker}")
    try:
        ticker = yf.Ticker(yf_ticker)
        dividends = ticker.dividends
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
        # Fetching enough price history for the dividends we found
        # If we got scraped dividends, let's make sure we have enough history
        hist = ticker.history(period="2y", auto_adjust=False)
        if hist.empty:
            # If price history failed too, try the resolved ticker
            resolved = resolve_ticker_yf(raw_ticker)
            if resolved:
                hist = yf.Ticker(resolved).history(period="2y", auto_adjust=False)
        
        if hist.empty:
            tv_symbol = parse_ticker_tv(raw_ticker)
            return {'ticker': raw_ticker, 'tv_symbol': tv_symbol, 'error': 'No price history found', 'dividends': [], 'current_price': None, 'days_since_last_div': None}
        
        current_price = hist['Close'].iloc[-1]
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
            if pre_div_close is None:
                dividend_analysis.append({'ex_date': ex_date_str, 'amount': round(amount, 3), 'error': 'Price data missing', 'recovered': False, 'recovery_days': 9999, 'current_distance': 0, 'window_recv_pct': 0})
                continue
            recovered = False
            recovery_days = None
            future_dates = hist[hist.index >= ex_date]
            if not future_dates.empty:
                for date, row in future_dates.iterrows():
                    days_elapsed = (date.date() - ex_date.date()).days
                    if row['High'] >= pre_div_close:
                        recovered = True
                        recovery_days = days_elapsed
                        break
                if not recovered:
                    latest_close = future_dates['Close'].iloc[-1]
                    current_distance = round(pre_div_close - latest_close, 2)
                    recovery_days = (datetime.now().date() - ex_date.date()).days
            future_dates_window = future_dates.head(recovery_window)
            window_recv_pct = 0.0
            if not future_dates_window.empty and amount > 0:
                max_high_window = future_dates_window['High'].max()
                theoretical_base = pre_div_close - amount
                recovered_amt = max_high_window - theoretical_base
                window_recv_pct = round((recovered_amt / amount) * 100, 1)
            dividend_analysis.append({'ex_date': ex_date_str, 'amount': round(amount, 3), 'pre_div_close': round(pre_div_close, 2), 'recovered': recovered, 'recovery_days': recovery_days, 'current_distance': 0 if recovered else current_distance, 'window_recv_pct': window_recv_pct})
        
        days_since_last = None
        if not recent_divs.empty:
            last_div_date = recent_divs.index[-1]
            days_since_last = (datetime.now().date() - last_div_date.date()).days
        
        tv_symbol = parse_ticker_tv(raw_ticker)
        return {'ticker': raw_ticker, 'tv_symbol': tv_symbol, 'dividends': dividend_analysis, 'current_price': round(current_price, 2), 'days_since_last_div': days_since_last}
    except Exception as e:
        logging.error(f"Error in Dividend Recovery for {raw_ticker}: {e}")
        tv_symbol = parse_ticker_tv(raw_ticker)
        return {'ticker': raw_ticker, 'tv_symbol': tv_symbol, 'error': str(e), 'dividends': [], 'current_price': None, 'days_since_last_div': None}

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
