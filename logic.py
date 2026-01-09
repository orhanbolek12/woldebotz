import yfinance as yf
import pandas as pd
import logging
import time

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
                   days=20, 
                   min_green_bars=12, 
                   min_red_bars=12,
                   long_wick_size=0.05,
                   short_wick_size=0.05,
                   progress_callback=None):
    """
    Analyzes tickers for Imbalance patterns (Long/Short).
    SEQUENTIAL PROCESSING: Fetches one by one to ensure reliability and correct data.
    """
    results = []
    total = len(tickers)
    
    for i, raw_ticker in enumerate(tickers):
        # Progress check at start of loop
        if progress_callback:
            if progress_callback(i, total) == 'STOP':
                logging.info(f"Stop signal received at {raw_ticker}.")
                return results

        yf_ticker = parse_ticker_yf(raw_ticker)
        tv_symbol = parse_ticker_tv(raw_ticker)
        
        try:
            # Fetch data using Ticker().history()
            # auto_adjust=True ensures we get split/dividend adjusted prices (comparable to TV adj close)
            ticker_obj = yf.Ticker(yf_ticker)
            df = ticker_obj.history(period="3mo", interval="1d", auto_adjust=True)
            
            # If empty, try raw ticker as fallback
            if df.empty and yf_ticker != raw_ticker:
                 df = yf.Ticker(raw_ticker).history(period="3mo", interval="1d", auto_adjust=True)

            # Clean and validate
            df = df.dropna(how='all')
            if df.empty or len(df) < 15:
                continue

            # Slice the requested days
            df_slice = df.tail(days).copy()
            
            if 'Close' not in df_slice.columns or 'Open' not in df_slice.columns:
                continue
            
            # Pattern Logic
            is_green = df_slice['Close'] > df_slice['Open']
            # Wick check: (Open - Low) <= long_wick_size
            long_wick_ok = (df_slice['Open'] - df_slice['Low']) <= (long_wick_size + 0.00001)
            valid_green_bars = df_slice[is_green & long_wick_ok]
            
            is_red = df_slice['Close'] < df_slice['Open']
            short_wick_ok = (df_slice['High'] - df_slice['Open']) <= (short_wick_size + 0.00001)
            valid_red_bars = df_slice[is_red & short_wick_ok]
            
            pattern = None
            if len(valid_green_bars) >= min_green_bars:
                pattern = "Long"
            elif len(valid_red_bars) >= min_red_bars:
                pattern = "Short"
            
            if pattern:
                results.append({
                    'ticker': raw_ticker,
                    'type': pattern,
                    'green_count': len(valid_green_bars),
                    'red_count': len(valid_red_bars),
                    'tv_symbol': tv_symbol,
                    'yf_symbol': yf_symbol
                })

        except Exception as e:
            logging.error(f"Error processing {raw_ticker}: {e}")
            continue
            
    # Final progress update
    if progress_callback: progress_callback(total, total)
    
    return results
