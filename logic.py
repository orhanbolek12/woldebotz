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
    Optimized to use parallel downloading in CHUNKS to allow progress updates.
    """
    results = []
    total = len(tickers)
    
    # Map raw tickers to YF tickers
    ticker_map = {parse_ticker_yf(t): t for t in tickers}
    all_yf_tickers = list(ticker_map.keys())
    
    if not all_yf_tickers:
        return []

    CHUNK_SIZE = 10
    processed_count = 0

    for i in range(0, len(all_yf_tickers), CHUNK_SIZE):
        # Check Stop before starting a new heavy download
        if progress_callback:
            if progress_callback(processed_count, total) == 'STOP':
                logging.info(f"Stop signal received before chunk {i}.")
                return results

        chunk = all_yf_tickers[i:i + CHUNK_SIZE]
        
        try:
            # Batch download for this chunk
            # 'threads=True' for parallel fetching within the chunk
            # Reduced chunk size means this blocks for much less time
            data = yf.download(chunk, period="3mo", interval="1d", group_by='ticker', threads=True, progress=False)
            
            # Prepare data dict for processing
            data_dict = {}
            if len(chunk) == 1:
                data_dict = {chunk[0]: data}
            else:
                for t in chunk:
                    try:
                        if isinstance(data.columns, pd.MultiIndex):
                            try:
                                df_t = data.xs(t, axis=1, level=0, drop_level=True)
                                data_dict[t] = df_t
                            except KeyError:
                                logging.warning(f"No data for {t} in chunk")
                        else:
                             logging.warning(f"Unexpected data structure for {t}")
                    except Exception as e:
                         logging.warning(f"Error extracing data for {t}: {e}")

            # Process each ticker
            for yf_symbol in chunk:
                processed_count += 1
                
                # Check Stop inside the loop for maximum responsiveness
                if progress_callback:
                    if progress_callback(processed_count, total) == 'STOP':
                        return results

                if yf_symbol not in data_dict:
                    continue

                df = data_dict[yf_symbol]
                raw_ticker = ticker_map.get(yf_symbol, yf_symbol)
                tv_symbol = parse_ticker_tv(raw_ticker)
                
                try:
                    # Clean and validate
                    df = df.dropna(how='all') 
                    
                    if df.empty or len(df) < 15: 
                        continue
                        
                    # Slice the requested days
                    df_slice = df.tail(days).copy()
                    
                    if 'Close' not in df_slice.columns or 'Open' not in df_slice.columns:
                        continue
                    
                    # Vectorized checks for Pattern Logic
                    is_green = df_slice['Close'] > df_slice['Open']
                    # Ensure minimal wick size logic is applied correctly to green bars
                    # (Open - Low) must be <= long_wick_size
                    long_wick_ok = (df_slice['Open'] - df_slice['Low']) <= (long_wick_size + 0.00001)
                    valid_green_bars = df_slice[is_green & long_wick_ok]
                    
                    is_red = df_slice['Close'] < df_slice['Open']
                    # (High - Open) must be <= short_wick_size
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

        except Exception as e:
            logging.error(f"Error in chunk fetch: {e}")
            # IMPORTANT: If chunk fails, we must still increment progress
            # so the bar moves and we don't get stuck forever
            processed_count += len(chunk) 
            if progress_callback:
                progress_callback(processed_count, total)
            continue

    return results
