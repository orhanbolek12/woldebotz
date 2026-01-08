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
            progress_callback(i + 1, total)
            
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

def fetch_imbalance(tickers, progress_callback=None):
    """
    Analyzes tickers for Imbalance patterns (Long/Short).
    Long: >= 12 green bars in last 20, for green bars Open-Low <= 0.05
    Short: >= 12 red bars in last 20, for red bars High-Open <= 0.05
    """
    results = []
    total = len(tickers)
    
    for i, raw_ticker in enumerate(tickers):
        if progress_callback:
            progress_callback(i + 1, total)
            
        yf_symbol = parse_ticker_yf(raw_ticker)
        tv_symbol = parse_ticker_tv(raw_ticker)
        
        try:
            ticker_obj = yf.Ticker(yf_symbol)
            df = ticker_obj.history(period="1mo", interval="1d", auto_adjust=True)
            
            if df.empty or len(df) < 15: 
                continue
                
            df = df.tail(20).copy()
            
            green_bars = df[df['Close'] > df['Open']]
            red_bars = df[df['Close'] < df['Open']]
            
            pattern = None
            
            # Long Pattern Check: >= 12 green, wicks <= 0.05
            if len(green_bars) >= 12:
                # Open - Low should be small for green bars
                wick_check = (green_bars['Open'] - green_bars['Low']) <= 0.051
                if wick_check.all():
                    pattern = "Long"
            
            # Short Pattern Check: >= 12 red, wicks <= 0.05
            if not pattern and len(red_bars) >= 12:
                # High - Open should be small for red bars
                wick_check = (red_bars['High'] - red_bars['Open']) <= 0.051
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
            logging.error(f"Error processing imbalance for {raw_ticker}: {e}")
            
    return results
