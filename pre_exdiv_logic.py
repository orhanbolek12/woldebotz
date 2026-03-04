import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from logic import parse_ticker_yf, parse_ticker_tv, fetch_history_with_fallback, TICKER_MAPPINGS, MANUAL_DIVIDEND_HISTORY
from dateutil.relativedelta import relativedelta

# Constants from prompt
SECTOR_ETFS = {
    "senior_loans":     ["BKLN", "SRLN"],
    "investment_grade": ["VCIT", "VCSH"],
    "high_yield":       ["HYG"],
    "municipal":        ["MUB", "VTEB"],
    "convertible":      ["ICVT", "CWB"],
    "preferred_stocks": ["PFF", "FPE"],
    "real_estate":      ["VNQ"],
    "emerging_markets": ["IEMG", "VWO"],
    "asia_equity":      ["VPL"],
    "global_income":    ["AGG", "AOR"],
    "us_equity":        ["SPY", "QQQ"],
    "sector_equity":    ["SPY", "QQQ"],
}

BENCHMARKS = ["SPY", "TLT", "HYG", "^VIX"]

def sanitize_val(val, decimals=2):
    if pd.isna(val) or val is None or np.isinf(val): return 0
    return round(float(val), decimals)

# Core scoring function skeleton
def fetch_pre_exdiv_momentum(
    tickers, 
    lookahead_days=30, 
    min_entry_day=2, 
    max_entry_day=10, 
    min_score=60, 
    min_win_rate=0.40, 
    min_hist_alpha=0.001, 
    min_volume_daily=100000, 
    show_estimated=False,
    sector_map=None,
    progress_callback=None
):
    """
    Main entry point for Pre Ex-Div Momentum analysis.
    Sector map should map ticker -> sector_key (e.g. 'PDI' -> 'senior_loans')
    """
    if sector_map is None:
        sector_map = {}
        
    results = []
    total_tickers = len(tickers)
    
    # --- 1. Fetch Benchmarks and Sector ETFs ---
    logging.info("Fetching Benchmarks and Sector ETFs for Pre-ExDiv...")
    etf_data = {}
    
    all_etfs = set(BENCHMARKS)
    for etf_list in SECTOR_ETFS.values():
        all_etfs.update(etf_list)
        
    for etf_sym in all_etfs:
        try:
            t = yf.Ticker(etf_sym)
            df = fetch_history_with_fallback(t, period="3mo", interval="1d", auto_adjust=True)
            if not df.empty:
                # Calculate 10d and 21d returns
                current_price = df['Close'].iloc[-1]
                price_10d = df['Close'].iloc[-10] if len(df) >= 10 else df['Close'].iloc[0]
                price_21d = df['Close'].iloc[-21] if len(df) >= 21 else df['Close'].iloc[0]
                
                etf_data[etf_sym] = {
                    'ret_10d': (current_price - price_10d) / price_10d,
                    'ret_21d': (current_price - price_21d) / price_21d,
                    'df': df
                }
        except Exception as e:
            logging.error(f"Failed to fetch ETF {etf_sym}: {e}")

    spy_ret_21d = etf_data.get('SPY', {}).get('ret_21d', 0)
    
    sector_snapshot = {}
    
    # --- 2. Iterate over CEF Tickers ---
    now = datetime.now()
    cutoff_date = (now + timedelta(days=lookahead_days)).date()
    
    for i, raw_ticker in enumerate(tickers):
        if progress_callback:
            if progress_callback(i, total_tickers) == 'STOP': break
            
        yf_ticker = parse_ticker_yf(raw_ticker)
        tv_symbol = parse_ticker_tv(raw_ticker)
        sector_key = sector_map.get(raw_ticker.upper(), 'sector_equity') # Default to broad equity
        
        try:
            ticker_obj = yf.Ticker(yf_ticker)
            # Fetch 2y to ensure enough history for historical ex-div backtesting
            df = fetch_history_with_fallback(ticker_obj, period="2y", interval="1d", auto_adjust=True)
            if df.empty: continue
            
            df = df.dropna(how='all')
            if len(df) < 60: continue # Need at least some history
            
            # --- 3. Pull Dividends & Find Upcoming Ex-Date ---
            dividends = ticker_obj.dividends
            if raw_ticker.upper() in MANUAL_DIVIDEND_HISTORY:
                manual_data = {pd.to_datetime(d): a for d, a in MANUAL_DIVIDEND_HISTORY[raw_ticker.upper()]}
                dividends = pd.Series(manual_data).sort_index()
            elif yf_ticker.upper() in MANUAL_DIVIDEND_HISTORY:
                manual_data = {pd.to_datetime(d): a for d, a in MANUAL_DIVIDEND_HISTORY[yf_ticker.upper()]}
                dividends = pd.Series(manual_data).sort_index()
                
            if dividends.empty: continue
            
            # Find next ex-date
            # Check if there's a declared dividend in the future
            future_divs = dividends[dividends.index.date >= now.date()]
            
            target_ex_date = None
            is_declared = False
            div_amount = 0.0
            
            if not future_divs.empty:
                # Get the next immediate dividend
                next_div_ts = future_divs.index[0]
                target_ex_date = next_div_ts.date()
                div_amount = future_divs.iloc[0]
                is_declared = True
            elif show_estimated:
                # Estimate next based on last dividend
                last_div_ts = dividends.index[-1]
                last_amount = dividends.iloc[-1]
                
                # Check frequency (e.g. monthly)
                if len(dividends) >= 2:
                    diff_days = (last_div_ts - dividends.index[-2]).days
                    if 20 <= diff_days <= 40: # Monthly
                        est_date = last_div_ts + relativedelta(months=1)
                        if est_date.date() >= now.date():
                            target_ex_date = est_date.date()
                            div_amount = last_amount
                            is_declared = False
            
            if not target_ex_date: continue
            if target_ex_date > cutoff_date: continue
            
            days_to_ex = (target_ex_date - now.date()).days
            
            # --- 4. Fundamental Scoring (45%) ---
            fund_score = 0
            
            # 4.1 Discount Z-Score (simplified for now as we don't have direct NAV history via YF, use standard 50 if naive)
            # Would need an external NAV source, substituting with price VS 52w moving average proxy for now
            price_ma200 = df['Close'].rolling(200).mean().iloc[-1]
            if not pd.isna(price_ma200) and price_ma200 > 0:
                discount_proxy = (price_ma200 - df['Close'].iloc[-1]) / price_ma200
                discount_score = max(0, min(100, 50 + (discount_proxy * 500))) # ±10% roughly covers 0-100
            else:
                discount_score = 50
                
            # 4.2 Distribution Quality (stability over last 3 divs)
            dist_score = 50
            if len(dividends) >= 3:
                recent_divs = dividends.iloc[-3:].values
                if recent_divs[2] >= recent_divs[1] >= recent_divs[0]: dist_score = 90
                elif recent_divs[2] < recent_divs[1]: dist_score = 40
                
            # 4.3 Yield Attractiveness
            current_yield = (div_amount * 12) / df['Close'].iloc[-1] if df['Close'].iloc[-1] > 0 else 0
            yield_score = max(0, min(100, current_yield * 1000)) # 10% yield -> 100 score
            
            # 4.4 Liquidity (Daily $ Vol)
            avg_vol = df['Volume'].tail(20).mean()
            dollar_vol = (avg_vol * df['Close'].iloc[-1]) / 1000 # in thousands
            if dollar_vol < min_volume_daily / 1000: continue
            liq_score = min(100, (dollar_vol / 2000) * 100) # 2M = 100 score
            
            # 4.5 Sector Momentum
            sec_etf = SECTOR_ETFS.get(sector_key, ['SPY'])[0]
            sec_data = etf_data.get(sec_etf, {'ret_10d': 0, 'ret_21d': 0})
            sec_score = 50 + (sec_data['ret_21d'] * 1000)
            sec_score = max(0, min(100, sec_score))
            
            fund_score = (discount_score * 0.25) + (dist_score * 0.25) + (yield_score * 0.15) + (liq_score * 0.20) + (sec_score * 0.15)
            
            # --- 5. Technical Scoring (55%) ---
            tech_score = 0
            current_price = df['Close'].iloc[-1]
            
            # 5.1 Momentum (10/21d)
            price_10 = df['Close'].iloc[-10] if len(df) >= 10 else df['Close'].iloc[0]
            price_21 = df['Close'].iloc[-21] if len(df) >= 21 else df['Close'].iloc[0]
            mom_ret = (current_price - price_21) / price_21
            mom_score = max(0, min(100, 50 + (mom_ret * 500)))
            
            # 5.2 Trend (SMA20, Golden Cross)
            sma20 = df['Close'].rolling(20).mean().iloc[-1]
            sma50 = df['Close'].rolling(50).mean().iloc[-1]
            trend_score = 50
            if current_price > sma20: trend_score += 25
            if current_price > sma50: trend_score += 25
            
            # 5.3 Volatility (ATR)
            # Calculate ATR proxy (simplified 14d)
            high_low = df['High'] - df['Low']
            atr_14 = high_low.tail(14).mean()
            atr_pct = atr_14 / current_price if current_price > 0 else 0
            # Lower ATR = higher score
            vol_score = max(0, min(100, 100 - (atr_pct * 2000)))
            
            # 5.4 Volume Pattern (Accumulation)
            vol_last_5 = df['Volume'].tail(5).mean()
            vol_last_20 = df['Volume'].tail(20).mean()
            is_accumulating = vol_last_5 > vol_last_20 and current_price > sma20
            vol_pattern_score = 80 if is_accumulating else 40
            
            # 5.5 Price Position (52w range)
            high_52 = df['High'].tail(252).max()
            low_52 = df['Low'].tail(252).min()
            pos_range = high_52 - low_52
            pos_rank = (current_price - low_52) / pos_range if pos_range > 0 else 0.5
            pos_score = pos_rank * 100
            
            # 5.6 Pre-ExDiv History (Backtest)
            historical_ex_dates = dividends[dividends.index.date < now.date()].index
            entry_returns = []
            spy_alphas = []
            win_count = 0
            
            best_n = min_entry_day
            best_n_ret = -999
            
            # We will test entries from T-MAX to T-MIN
            # For brevity in this loop, we calculate an average run-up across all valid N
            for ex in historical_ex_dates:
                # Find index of ex-date
                try:
                    # Get nearest valid trading day before or on ex-date
                    ex_idx = df.index.get_indexer([ex], method='pad')[0]
                    if ex_idx == -1: continue
                    ex_price = df['Close'].iloc[ex_idx]
                    
                    # Test multiple N days prior
                    for n in range(min_entry_day, max_entry_day + 1):
                        entry_idx = max(0, ex_idx - n)
                        entry_price = df['Close'].iloc[entry_idx]
                        if entry_price > 0:
                            ret = (ex_price - entry_price) / entry_price
                            entry_returns.append((n, ret))
                except: pass
                
            pre_exdiv_score = 50
            hist_avg_alpha = 0
            hist_win_rate = 0
            optimal_n = min_entry_day
            avg_ret = 0
            
            if entry_returns:
                df_rets = pd.DataFrame(entry_returns, columns=['n', 'ret'])
                grp = df_rets.groupby('n')['ret'].mean()
                optimal_n = grp.idxmax()
                best_n_ret = grp.max()
                
                # Filter for optimal N to get win rate
                opt_rets = df_rets[df_rets['n'] == optimal_n]['ret']
                win_count = sum(opt_rets > 0)
                hist_win_rate = win_count / len(opt_rets) if len(opt_rets) > 0 else 0
                avg_ret = opt_rets.mean()
                
                # Approximate Alpha (just raw ret - approx SPY daily drift)
                hist_avg_alpha = avg_ret - 0.0005 # very naive benchmark removal wrapper
                
                pre_exdiv_score = max(0, min(100, 50 + (hist_avg_alpha * 2000)))
                
            # Skip if doesn't meet historical requirements
            if hist_win_rate < min_win_rate: continue
            if hist_avg_alpha < min_hist_alpha: continue
            
            # 5.7 RSI
            delta = df['Close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            rsi_14 = 100 - (100 / (1 + rs)).iloc[-1]
            if pd.isna(rsi_14): rsi_14 = 50
            
            # Oversold = higher score (up to a point)
            if 30 <= rsi_14 <= 50: rsi_score = 90
            elif 50 < rsi_14 <= 70: rsi_score = 60
            else: rsi_score = 30
            
            tech_score = (mom_score * 0.20) + (trend_score * 0.15) + (vol_score * 0.15) + \
                         (vol_pattern_score * 0.10) + (pos_score * 0.10) + (pre_exdiv_score * 0.20) + (rsi_score * 0.10)
                         
            # --- 6. Composite & Grade ---
            composite_score = (fund_score * 0.45) + (tech_score * 0.55)
            
            if composite_score < min_score: continue
            
            if composite_score >= 75:
                stars = "★★★"
                label = "STRONG"
                suggested_entry_day = max(optimal_n, 8)
            elif composite_score >= 60:
                stars = "★★☆"
                label = "DECENT"
                suggested_entry_day = max(optimal_n, 5)
            else:
                stars = "★☆☆"
                label = "WEAK"
                suggested_entry_day = max(optimal_n, 3)
                
            # Risk Reward
            stop_loss_pct = atr_pct * 1.5
            reward_target_pct = hist_avg_alpha * 0.80
            risk_reward = reward_target_pct / stop_loss_pct if stop_loss_pct > 0 else 0
            
            results.append({
                "ticker": raw_ticker,
                "sector": sector_key,
                "composite_score": sanitize_val(composite_score, 1),
                "fundamental_score": sanitize_val(fund_score, 1),
                "technical_score": sanitize_val(tech_score, 1),
                "stars": stars,
                "label": label,
                "ex_date": target_ex_date.strftime("%Y-%m-%d"),
                "days_to_ex": days_to_ex,
                "declared": is_declared,
                "div_amount": sanitize_val(div_amount, 4),
                "current_price": sanitize_val(current_price, 2),
                "suggested_entry_day": suggested_entry_day,
                "days_to_entry": days_to_ex - suggested_entry_day,
                "stop_loss_pct": sanitize_val(stop_loss_pct * 100, 2),
                "reward_target_pct": sanitize_val(reward_target_pct * 100, 2),
                "risk_reward": sanitize_val(risk_reward, 2),
                "hist_avg_alpha": sanitize_val(hist_avg_alpha * 100, 2),
                "hist_win_rate": sanitize_val(hist_win_rate * 100, 1),
                "avg_dollar_vol": int(dollar_vol * 1000),
                "optimal_n": int(optimal_n),
                "rsi_14": int(rsi_14),
                "is_accumulating": bool(is_accumulating),
                "golden_cross": bool(sma20 > sma50),
                "above_sma20": bool(current_price > sma20),
                "atr_pct": sanitize_val(atr_pct, 4),
                "score_components": {
                    "discount_score": int(discount_score),
                    "distribution_score": int(dist_score),
                    "yield_score": int(yield_score),
                    "liquidity_score": int(liq_score),
                    "sector_score": int(sec_score),
                    "momentum_score": int(mom_score),
                    "trend_score": int(trend_score),
                    "volatility_score": int(vol_score),
                    "volume_pattern_score": int(vol_pattern_score),
                    "price_position_score": int(pos_score),
                    "pre_exdiv_score": int(pre_exdiv_score),
                    "rsi_score": int(rsi_score)
                }
            })
            
        except Exception as e:
            logging.error(f"Error processing {raw_ticker} for Pre-ExDiv: {e}")
            
    # Compile Sector Snapshot
    for sec_key, etf_list in SECTOR_ETFS.items():
        primary_etf = etf_list[0] if etf_list else 'SPY'
        if primary_etf in etf_data:
            ret10 = etf_data[primary_etf]['ret_10d']
            ret21 = etf_data[primary_etf]['ret_21d']
            
            n_upc = sum(1 for r in results if r['sector'] == sec_key)
            n_str = sum(1 for r in results if r['sector'] == sec_key and r['label'] == 'STRONG')
            n_dec = sum(1 for r in results if r['sector'] == sec_key and r['label'] == 'DECENT')
            
            sector_snapshot[sec_key] = {
                'sector': sec_key,
                'ret_10d': sanitize_val(ret10 * 100, 2),
                'ret_21d': sanitize_val(ret21 * 100, 2),
                'vs_spy_21d': sanitize_val((ret21 - spy_ret_21d) * 100, 2),
                'n_upcoming': n_upc,
                'n_strong': n_str,
                'n_decent': n_dec
            }
            
    # Compile Today's Actions
    enter_now = []
    entering_soon = []
    
    for r in results:
        dto_ex = r['days_to_ex']
        sug_entry = r['suggested_entry_day']
        
        if r['label'] in ['STRONG', 'DECENT']:
            if 2 <= dto_ex <= sug_entry:
                enter_now.append(r['ticker'])
            elif dto_ex - sug_entry <= 2 and dto_ex > sug_entry:
                entering_soon.append(r['ticker'])
                
    # Sort results by composite score descending
    results.sort(key=lambda x: x['composite_score'], reverse=True)

    return {
        "scan_time": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "total_scanned": total_tickers,
        "total_found": len(results),
        "results": results,
        "sector_snapshot": list(sector_snapshot.values()),
        "todays_actions": {
            "enter_now": [],
            "entering_soon": []
        }
    }
