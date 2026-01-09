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
    Optimized to use parallel downloading.
    """
    results = []
    total = len(tickers)
    
    # Map raw tickers to YF tickers
    # Store mapping to retrieve original ticker later
    ticker_map = {parse_ticker_yf(t): t for t in tickers}
    yf_tickers = list(ticker_map.keys())
    
    if not yf_tickers:
        return []

    try:
        # Batch download for speed
        # use 'threads=True' for parallel fetching
        logging.info(f"Starting batch download for {len(yf_tickers)} tickers...")
        
        # We fetch slightly more data than 'days' to ensure we have enough valid trading days
        # '3mo' is safe buffer for 20-40 day analysis
        data = yf.download(yf_tickers, period="3mo", interval="1d", group_by='ticker', threads=True, progress=False)
        
        logging.info("Batch download complete. Processing data...")
        
        # Handle single ticker case (data structure is different)
        if len(yf_tickers) == 1:
            # Reformat to match multi-ticker structure for consistent loop
            # Create a MultiIndex-like structure or just process directly
            single_ticker = yf_tickers[0]
            # If it's a single ticker, 'data' is just the DataFrame for that ticker
            # We wrap it in a dict for the loop below
            data_dict = {single_ticker: data}
        else:
            # Multi-ticker: data columns are (Ticker, Feature)
            # Use stack(level=0) isn't always best.
            # Best is to iterate through the unique level 0 columns
            data_dict = {}
            # Accessing by column level 0
            # Note: yfinance 0.2+ might behave differently, but typical group_by='ticker' gives top level ticker
            for t in yf_tickers:
                try:
                    # xs is safe way to slice MultiIndex
                    df_t = data.xs(t, axis=1, level=0, drop_level=True)
                    data_dict[t] = df_t
                except KeyError:
                    logging.warning(f"No data found for {t} in batch download")
                    continue

        # Process each ticker
        for i, (yf_symbol, df) in enumerate(data_dict.items()):
            
            # Progress update
            if progress_callback:
                if progress_callback(i + 1, total) == 'STOP':
                    logging.info(f"Stop signal received. Aborting fetch_imbalance.")
                    break
            
            raw_ticker = ticker_map.get(yf_symbol, yf_symbol)
            tv_symbol = parse_ticker_tv(raw_ticker)
            
            try:
                # Clean and validate
                df = df.dropna(how='all') # Drop days where this ticker had no data
                
                if df.empty or len(df) < 15: 
                    continue
                    
                # Slice the requested days
                df_slice = df.tail(days).copy()
                
                # Check for required columns (sometimes download fails partially)
                if 'Close' not in df_slice.columns or 'Open' not in df_slice.columns:
                    continue

                green_bars = df_slice[df_slice['Close'] > df_slice['Open']]
                red_bars = df_slice[df_slice['Close'] < df_slice['Open']]
                
                pattern = None
                
                # Long Pattern Check
                if len(green_bars) >= min_green_bars:
                    wick_check = (green_bars['Open'] - green_bars['Low']) <= long_wick_size + 0.001
                    if wick_check.all():
                        pattern = "Long"
                
                # Short Pattern Check
                if not pattern and len(red_bars) >= min_red_bars:
                    wick_check = (red_bars['High'] - red_bars['Open']) <= short_wick_size + 0.001
                    if wick_check.all():
                        pattern = "Short"
                
                if pattern:
                    results.append({
                        'ticker': raw_ticker,
                        'type': pattern,
                        'green_count': len(green_bars),
                        'red_count': len(red_bars),
                        'tv_symbol': tv_symbol,
                        'yf_symbol': yf_symbol
                    })
                    
            except Exception as e:
                logging.error(f"Error processing {raw_ticker}: {e}")
                
    except Exception as e:
        logging.error(f"Critical error in batch fetch: {e}")
        # Fallback to serial if batch fails completely?
        # For now, just logging.

    return results
