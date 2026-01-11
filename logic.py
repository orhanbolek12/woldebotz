import yfinance as yf
import pandas as pd
import logging
import time
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
            # Yahoo format for preferreds: ABR-PD, PCG-PG
            return f"{base}-P{suffix}"
    return raw_ticker

def fetch_and_process(tickers, progress_callback=None):
    results = []
    total = len(tickers)
    
    for i, raw_ticker in enumerate(tickers):
        yf_ticker = parse_ticker_yf(raw_ticker)
        logging.debug(f"Processing {raw_ticker} -> {yf_ticker}")
        
        try:
            # Fetch 90 days (approx 3 months)
            ticker = yf.Ticker(yf_ticker)
            df = ticker.history(period="3mo", auto_adjust=True)
            
            if df.empty:
                # Try raw ticker
                if yf_ticker != raw_ticker:
                     logging.debug(f"Converted fetch failed, trying original {raw_ticker}")
                     df = yf.Ticker(raw_ticker).history(period="3mo", auto_adjust=True)
                
            if df.empty:
                logging.error(f"Failed to fetch {yf_ticker} (Empty)")
                continue

            # Filter 1: Daily Spread Average >= 0.10
            daily_spreads = df['High'] - df['Low']
            avg_daily_spread = daily_spreads.mean()

            # Filter 2: Total Range (Max High - Min Low) <= 1.00
            high_max = df['High'].max()
            low_min = df['Low'].min()
            
            if pd.isna(high_max) or pd.isna(low_min):
                logging.error(f"NaN values for {yf_ticker}")
                continue
                
            total_range = high_max - low_min
            logging.debug(f"{yf_ticker}: AvgSpread {avg_daily_spread:.3f}, Range {total_range:.3f}")

            # Criteria: Range <= 1.00 AND AvgSpread >= 0.10
            if total_range <= 1.00 and avg_daily_spread >= 0.10:
                logging.info(f"{yf_ticker} PASSED")
                # TradingView Link Logic
                tv_link_symbol = parse_ticker_tv(raw_ticker)

                # Pattern detection logic
                pattern = None
                # Red bars are where Close < Open
                red_bars = df[df['Close'] < df['Open']]

                # Short Pattern Check
                if not pattern and len(red_bars) >= 12:
                    # Check wick constraint: High - Open <= 0.05
                    # This checks if the upper wick is small for red bars
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
            else:
                logging.info(f"{yf_ticker} FAILED (Range {total_range:.2f} > 1.00 OR AvgSpread {avg_daily_spread:.3f} < 0.10)")
                
        except Exception as e:
            logging.error(f"Error processing {raw_ticker}: {e}")
        
        if progress_callback:
            if progress_callback(i + 1, total) == 'STOP':
                logging.info(f"Stop signal received. Aborting fetch_and_process.")
                break
            
    return results

def parse_ticker_tv(raw_ticker):
    """
    Helpers for logic.py independent of app
    User: ABR-D -> TV: ABR/PD (often)
    """
    if '-' in raw_ticker:
        parts = raw_ticker.split('-')
        if len(parts) == 2:
            base, suffix = parts
            return f"{base}/P{suffix}"
    return raw_ticker

def fetch_imbalance(tickers, 
                   days=30, 
                   min_count=20,
                   max_wick=0.12,
                   progress_callback=None):
    """
    Automated Pattern Detection Logic:
    Analyzes tickers to find both Green (Long) and Red (Short) patterns.
    
    Wick Rules:
    - Green Bars (Close > Open): Wick = Open - Low
    - Red Bars (Open > Close): Wick = High - Open
    
    A ticker is returned if it has >= min_count occurrences of either pattern 
    within the last 'days' with an average wick <= max_wick.
    """
    results = []
    total = len(tickers)
    
    for i, raw_ticker in enumerate(tickers):
        # Progress check
        if progress_callback:
            if progress_callback(i, total) == 'STOP':
                logging.info(f"Stop signal received at {raw_ticker}.")
                return results

        yf_ticker = parse_ticker_yf(raw_ticker)
        tv_symbol = parse_ticker_tv(raw_ticker)
        
        try:
            # Fetch data 
            ticker_obj = yf.Ticker(yf_ticker)
            df = ticker_obj.history(period="6mo", interval="1d", auto_adjust=True)
            
            # Fallback
            if df.empty and yf_ticker != raw_ticker:
                 df = yf.Ticker(raw_ticker).history(period="6mo", interval="1d", auto_adjust=True)

            # Clean
            df = df.dropna(how='all')
            if df.empty or len(df) < days:
                continue

            # Slice the requested days
            df_slice = df.tail(days).copy()
            
            if 'Close' not in df_slice.columns or 'Open' not in df_slice.columns:
                continue
            
            # 1. GREEN (Long) Detection
            # Pattern: Close > Open, Wick: (Open - Low)
            is_green = df_slice['Close'] > df_slice['Open']
            green_wicks = df_slice['Open'] - df_slice['Low']
            green_wick_ok = green_wicks <= (max_wick + 0.00001)
            valid_green = df_slice[is_green & green_wick_ok]
            
            # 2. RED (Short) Detection
            # Pattern: Open > Close, Wick: (High - Open)
            is_red = df_slice['Open'] > df_slice['Close']
            red_wicks = df_slice['High'] - df_slice['Open']
            red_wick_ok = red_wicks <= (max_wick + 0.00001)
            valid_red = df_slice[is_red & red_wick_ok]

            # Check counts
            # If both hit, we add both (rare) or prioritize. User said "son 30 günde 20 tane yeşil bar varsa... eğer 20 tane kırmızı bar varsa..."
            # Let's add all matching patterns.
            
            patterns_found = []
            if len(valid_green) >= min_count:
                patterns_found.append({
                    'type': 'Long',
                    'count': len(valid_green),
                    'avg_wick': round(green_wicks[is_green & green_wick_ok].mean(), 4)
                })
            
            if len(valid_red) >= min_count:
                patterns_found.append({
                    'type': 'Short',
                    'count': len(valid_red),
                    'avg_wick': round(red_wicks[is_red & red_wick_ok].mean(), 4)
                })

            for p in patterns_found:
                results.append({
                    'ticker': raw_ticker,
                    'yf_symbol': yf_ticker,
                    'tv_symbol': tv_symbol,
                    'type': p['type'],
                    'match_count': p['count'],
                    'avg_diff': p['avg_wick'],
                    'total_days': days,
                    'target_color': 'Green' if p['type'] == 'Long' else 'Red'
                })

        except Exception as e:
            logging.error(f"Error processing {raw_ticker}: {e}")
            continue
            
    # Final progress update
    if progress_callback: progress_callback(total, total)
    
    return results
def fetch_range_ai(tickers, 
                   days=90, 
                   max_points=1.0, 
                   max_percent=5.0,
                   progress_callback=None):
    """
    Range AI Logic (Replaces Custom Analysis):
    Analyzes tickers to find those that have stayed within a specific range
    over the last 'days' (dividend adjusted).
    
    Criteria:
    1. (Max - Min) <= max_points
    2. (Max - Min) / Min * 100 <= max_percent
    """
    results = []
    total = len(tickers)
    
    for i, raw_ticker in enumerate(tickers):
        # Progress check
        if progress_callback:
            if progress_callback(i, total) == 'STOP':
                logging.info(f"Stop signal received at {raw_ticker}.")
                return results

        yf_ticker = parse_ticker_yf(raw_ticker)
        tv_symbol = parse_ticker_tv(raw_ticker)
        
        try:
            # Fetch data (use 1y to be safe for any requested 'days')
            ticker_obj = yf.Ticker(yf_ticker)
            df = ticker_obj.history(period="1y", interval="1d", auto_adjust=True)
            
            # Fallback
            if df.empty and yf_ticker != raw_ticker:
                 df = yf.Ticker(raw_ticker).history(period="1y", interval="1d", auto_adjust=True)

            # Clean
            df = df.dropna(how='all')
            if df.empty or len(df) < days:
                continue

            # Slice the requested days
            df_slice = df.tail(days).copy()
            
            if df_slice.empty:
                continue
                
            low_min = df_slice['Low'].min()
            high_max = df_slice['High'].max()
            current_price = df_slice['Close'].iloc[-1]
            
            if pd.isna(low_min) or pd.isna(high_max):
                continue
                
            point_range = high_max - low_min
            percent_range = (point_range / low_min) * 100 if low_min > 0 else 0
            
            # Criteria Check
            if point_range <= max_points and percent_range <= max_percent:
                results.append({
                    'ticker': raw_ticker,
                    'yf_symbol': yf_ticker,
                    'tv_symbol': tv_symbol,
                    'min': round(low_min, 2),
                    'max': round(high_max, 2),
                    'current': round(current_price, 2),
                    'point_range': round(point_range, 2),
                    'percent_range': round(percent_range, 2),
                    'days': days
                })

        except Exception as e:
            logging.error(f"Error in Range AI for {raw_ticker}: {e}")
            continue
            
    # Final progress update
    if progress_callback: progress_callback(total, total)
    
    return results


def analyze_dividend_recovery(raw_ticker, lookback=3, recovery_window=5):
    """
    Analyze dividend recovery for a preferred stock.
    
    Args:
        raw_ticker: Stock symbol (e.g., 'BAC-Q')
        lookback: Number of recent dividends to analyze (3 or 5)
        recovery_window: Days window to calculate recovery percentage (default 5)
    """
    yf_ticker = parse_ticker_yf(raw_ticker)
    logging.debug(f"Dividend Recovery: {raw_ticker} -> {yf_ticker}")
    
    try:
        ticker = yf.Ticker(yf_ticker)
        
        # Fetch dividend history (last 2 years to ensure we get enough data)
        dividends = ticker.dividends
        
        if dividends.empty:
            # Try raw ticker
            if yf_ticker != raw_ticker:
                logging.debug(f"Converted fetch failed, trying original {raw_ticker}")
                ticker = yf.Ticker(raw_ticker)
                dividends = ticker.dividends
        
        if dividends.empty or len(dividends) == 0:
            return {
                'ticker': raw_ticker,
                'tv_symbol': yf_ticker,
                'error': 'No dividend history found',
                'dividends': [],
                'current_price': None,
                'days_since_last_div': None
            }
        
        # Get last N dividends
        recent_divs = dividends.tail(lookback)
        
        # Fetch historical price data (2 years to cover all dividends + recovery periods)
        # We need raw prices (auto_adjust=False) to check if price returned to actual pre-div level
        hist = ticker.history(period="2y", auto_adjust=False)
        
        if hist.empty:
            return {
                'ticker': raw_ticker,
                'tv_symbol': yf_ticker,
                'error': 'No price history found',
                'dividends': [],
                'current_price': None,
                'days_since_last_div': None
            }
        
        current_price = hist['Close'].iloc[-1] if not hist.empty else None
        
        dividend_analysis = []
        
        for ex_date, amount in recent_divs.items():
            ex_date_str = ex_date.strftime('%Y-%m-%d')
            
            # Get close price on day BEFORE ex-dividend date
            pre_div_date = ex_date - timedelta(days=1)
            
            # Find the actual trading day before ex-date
            pre_div_close = None
            for i in range(5):  # Look back up to 5 days for a valid trading day
                check_date = ex_date - timedelta(days=i+1)
                if check_date in hist.index:
                    pre_div_close = hist.loc[check_date, 'Close']
                    break
            
            if pre_div_close is None:
                logging.warning(f"Could not find pre-dividend close for {raw_ticker} on {ex_date_str}")
                dividend_analysis.append({
                    'ex_date': ex_date_str,
                    'amount': round(amount, 3),
                    'error': 'Price data missing',
                    'recovered': False,
                    'recovery_days': 9999,
                    'current_distance': 0,
                    'window_recv_pct': 0
                })
                continue
            
            # Track prices after ex-dividend date to find recovery
            recovered = False
            recovery_days = None
            current_distance = None
            
            # Get all dates starting from ex-date (Day 1)
            future_dates = hist[hist.index >= ex_date]
            
            if not future_dates.empty:
                for date, row in future_dates.iterrows():
                    # Calculate calendar days elapsed
                    days_elapsed = (date.date() - ex_date.date()).days
                    
                    # User Rule: Check if 'High' reached the pre-div close, not just Close
                    if row['High'] >= pre_div_close:
                        recovered = True
                        recovery_days = days_elapsed
                        break
                
                # If not recovered, calculate current distance
                if not recovered:
                    latest_close = future_dates['Close'].iloc[-1]
                    current_distance = round(pre_div_close - latest_close, 2)
                    recovery_days = (datetime.now().date() - ex_date.date()).days  # Days since dividend (calendar)
            
            # Calculate Recovery Percentage for custom window (e.g. 5 days)
            # (Max_High_In_Window - (Pre_Div_Close - Amount)) / Amount
            future_dates_window = future_dates.head(recovery_window)
            window_recv_pct = 0.0
            if not future_dates_window.empty and amount > 0:
                max_high_window = future_dates_window['High'].max()
                theoretical_base = pre_div_close - amount
                recovered_amt = max_high_window - theoretical_base
                window_recv_pct = round((recovered_amt / amount) * 100, 1)

            dividend_analysis.append({
                'ex_date': ex_date_str,
                'amount': round(amount, 3),
                'pre_div_close': round(pre_div_close, 2),
                'recovered': recovered,
                'recovery_days': recovery_days,
                'current_distance': current_distance,
                'window_recv_pct': window_recv_pct
            })
        
        # Calculate days since last dividend
        last_div_date = recent_divs.index[-1]
        days_since_last = (datetime.now().date() - last_div_date.date()).days
        
        return {
            'ticker': raw_ticker,
            'tv_symbol': yf_ticker,
            'dividends': dividend_analysis,
            'current_price': round(current_price, 2) if current_price else None,
            'days_since_last_div': days_since_last
        }
        
    except Exception as e:
        logging.error(f"Error in Dividend Recovery for {raw_ticker}: {e}")
        return {
            'ticker': raw_ticker,
            'tv_symbol': yf_ticker,
            'error': str(e),
            'dividends': [],
            'current_price': None,
            'days_since_last_div': None
        }
