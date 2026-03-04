import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import time
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

# -----------------------------------------------------------------------
# Download settings to avoid yfinance RateLimitError
# -----------------------------------------------------------------------
FETCH_CHUNK_SIZE = 15       # lowered from 20 to be safer
FETCH_CHUNK_SLEEP = 3.0      # increased from 2.0 to be safer


def sanitize_val(val, decimals=2):
    if pd.isna(val) or val is None or np.isinf(val):
        return 0
    return round(float(val), decimals)


def to_py(val):
    """Convert any numpy scalar to a native Python type (int/float/bool)."""
    if isinstance(val, (np.integer,)):
        return int(val)
    if isinstance(val, (np.floating,)):
        return float(val)
    if isinstance(val, (np.bool_,)):
        return bool(val)
    return val


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
    if sector_map is None:
        sector_map = {}

    results = []
    total_tickers = len(tickers)
    now = datetime.now()
    cutoff_date = (now + timedelta(days=lookahead_days)).date()

    # 1. Prepare benchmark and ETF data first
    yield {"type": "progress", "pct": 5, "msg": "Fetching benchmarks and sector ETFs..."}
    benchmark_symbols = list(set(BENCHMARKS))
    for etf_list in SECTOR_ETFS.values():
        benchmark_symbols.extend(etf_list)
    benchmark_symbols = list(set(benchmark_symbols))

    bench_data = pd.DataFrame()
    for i in range(0, len(benchmark_symbols), FETCH_CHUNK_SIZE):
        chunk = benchmark_symbols[i:i + FETCH_CHUNK_SIZE]
        try:
            chunk_df = yf.download(chunk, period="2y", interval="1d", group_by="ticker", actions=False, progress=False)
            if not chunk_df.empty:
                if bench_data.empty: bench_data = chunk_df
                else: bench_data = pd.concat([bench_data, chunk_df], axis=1)
        except Exception as e:
            logging.error(f"[PRE-EXDIV] Benchmark chunk download failed: {e}")
        time.sleep(FETCH_CHUNK_SLEEP)

    etf_summary = {}
    def _extract_df(data, sym):
        try:
            if data is None or data.empty: return pd.DataFrame()
            if sym in data.columns and not isinstance(data.columns, pd.MultiIndex):
                return data[[sym]].dropna(how="all")
            if isinstance(data.columns, pd.MultiIndex):
                if sym in data.columns.get_level_values(0): return data[sym].dropna(how="all")
                if sym in data.columns.get_level_values(1): return data.xs(sym, axis=1, level=1).dropna(how="all")
            if "Close" in data.columns: return data.dropna(how="all")
            return pd.DataFrame()
        except: return pd.DataFrame()

    for sym in benchmark_symbols:
        df_bench = _extract_df(bench_data, sym)
        if not df_bench.empty and "Close" in df_bench.columns:
            cp = df_bench["Close"].iloc[-1]
            p10 = df_bench["Close"].iloc[-10] if len(df_bench) >= 10 else df_bench["Close"].iloc[0]
            p21 = df_bench["Close"].iloc[-21] if len(df_bench) >= 21 else df_bench["Close"].iloc[0]
            etf_summary[sym] = {
                "ret_10d": (cp - p10) / p10 if p10 else 0,
                "ret_21d": (cp - p21) / p21 if p21 else 0,
                "df": df_bench
            }

    spy_ret_21d = etf_summary.get("SPY", {}).get("ret_21d", 0)

    # 2. Process tickers in chunks
    yf_target_map = {t: parse_ticker_yf(t) for t in tickers}
    target_tickers = list(tickers)

    processed_count = 0
    for i in range(0, total_tickers, FETCH_CHUNK_SIZE):
        chunk = target_tickers[i:i + FETCH_CHUNK_SIZE]
        chunk_yf = [yf_target_map[t] for t in chunk]
        
        logging.info(f"[PRE-EXDIV] Fetching chunk {i//FETCH_CHUNK_SIZE + 1}: {chunk}")
        pct = 10 + int((i / total_tickers) * 85)
        yield {"type": "progress", "pct": pct, "msg": f"Analyzing chunk {i//FETCH_CHUNK_SIZE + 1}: {', '.join(chunk[:3])}..."}
        
        try:
            chunk_data = yf.download(chunk_yf, period="2y", interval="1d", group_by="ticker", actions=True, progress=False)
        except Exception as e:
            logging.warning(f"[PRE-EXDIV] Chunk download failed: {e}")
            chunk_data = pd.DataFrame()

        for raw_ticker in chunk:
            processed_count += 1
            yf_sym = yf_target_map[raw_ticker]
            sector_key = sector_map.get(raw_ticker.upper(), "sector_equity")

            try:
                df = _extract_df(chunk_data, yf_sym)
                if df.empty or len(df) < 60:
                    try:
                        t = yf.Ticker(yf_sym)
                        df = fetch_history_with_fallback(t, period="2y")
                    except: pass
                
                if df.empty or len(df) < 60:
                    yield {"type": "progress", "pct": pct, "msg": f"Skipped {raw_ticker}: No price data"}
                    continue

                divs = pd.Series(dtype="float64")
                if 'Dividends' in df.columns: divs = df['Dividends'][df['Dividends'] > 0]
                if divs.empty:
                    try: divs = yf.Ticker(yf_sym).dividends
                    except: pass

                if not divs.empty:
                    divs.index = divs.index.tz_localize(None) if divs.index.tz is None else divs.index.tz_convert(None)
                    divs = divs.sort_index()

                for key in (raw_ticker.upper(), yf_sym.upper()):
                    if key in MANUAL_DIVIDEND_HISTORY:
                        manual_data = {pd.to_datetime(d): a for d, a in MANUAL_DIVIDEND_HISTORY[key]}
                        divs = pd.Series(manual_data).sort_index()
                        break

                if divs.empty:
                    yield {"type": "progress", "pct": pct, "msg": f"Skipped {raw_ticker}: No dividends found"}
                    continue

                future_divs = divs[divs.index.date >= now.date()]
                target_ex_date, is_declared, div_amount = None, False, 0.0

                if not future_divs.empty:
                    target_ex_date = future_divs.index[0].date()
                    div_amount = future_divs.iloc[0]
                    is_declared = True
                elif show_estimated:
                    last_div_ts, last_amount = divs.index[-1], divs.iloc[-1]
                    if len(divs) >= 2:
                        diff = (last_div_ts - divs.index[-2]).days
                        if 20 <= diff <= 45: est = last_div_ts + relativedelta(months=1)
                        elif 70 <= diff <= 110: est = last_div_ts + relativedelta(months=3)
                        else: est = last_div_ts + timedelta(days=int((divs.index[-1]-divs.index[0]).days/(len(divs)-1)))
                        if est.date() >= now.date():
                            target_ex_date = est.date()
                            div_amount = last_amount
                            is_declared = False

                if not target_ex_date or target_ex_date > cutoff_date:
                    yield {"type": "progress", "pct": pct, "msg": f"Skipped {raw_ticker}: No ex-date in range (Found: {target_ex_date})"}
                    continue

                days_to_ex = (target_ex_date - now.date()).days
                current_price = df["Close"].iloc[-1]
                price_ma200 = df["Close"].rolling(200).mean().iloc[-1]
                discount_score = max(0, min(100, 50 + (((price_ma200 - current_price) / price_ma200 * 500) if price_ma200 else 0)))
                
                dist_score = 50
                if len(divs) >= 3:
                    vals = divs.iloc[-3:].values
                    if vals[2] >= vals[1] >= vals[0]: dist_score = 90
                    elif vals[2] < vals[1]: dist_score = 40

                yield_score = max(0, min(100, (div_amount * 12 / current_price * 1000) if current_price else 0))
                avg_vol = df["Volume"].tail(20).mean()
                dollar_vol = (avg_vol * current_price) / 1000
                if dollar_vol < min_volume_daily / 1000:
                    yield {"type": "progress", "pct": pct, "msg": f"Skipped {raw_ticker}: Vol low (${dollar_vol:.0f}K)"}
                    continue
                liq_score = min(100, (dollar_vol / 2000) * 100)

                sec_etf = SECTOR_ETFS.get(sector_key, ["SPY"])[0]
                sec_data = etf_summary.get(sec_etf, {"ret_21d": 0})
                sec_score = max(0, min(100, 50 + (sec_data["ret_21d"] * 1000)))

                fund_score = discount_score*0.25 + dist_score*0.25 + yield_score*0.15 + liq_score*0.20 + sec_score*0.15

                p21 = df["Close"].iloc[-21] if len(df) >= 21 else df["Close"].iloc[0]
                mom_score = max(0, min(100, 50 + ((current_price - p21) / p21 * 500)))
                sma20 = df["Close"].rolling(20).mean().iloc[-1]
                sma50 = df["Close"].rolling(50).mean().iloc[-1]
                trend_score = 50 + (25 if current_price > sma20 else 0) + (25 if current_price > sma50 else 0)
                atr_14 = (df["High"] - df["Low"]).tail(14).mean()
                atr_pct = atr_14 / current_price if current_price else 0
                vol_score = max(0, min(100, 100 - (atr_pct * 2000)))
                is_accumulating = (df["Volume"].tail(5).mean() > df["Volume"].tail(20).mean()) and (current_price > sma20)
                vol_pattern_score = 80 if is_accumulating else 40
                h52, l52 = df["High"].tail(252).max(), df["Low"].tail(252).min()
                pos_score = ((current_price - l52) / (h52 - l52) * 100) if (h52 - l52) else 50

                hist_ex = divs[divs.index.date < now.date()].index
                rets = []
                for ex in hist_ex:
                    try:
                        idx = df.index.get_indexer([ex], method="pad")[0]
                        if idx <= 0: continue
                        xp = df["Close"].iloc[idx]
                        for n in range(min_entry_day, max_entry_day + 1):
                            e_idx = max(0, idx - n)
                            ep = df["Close"].iloc[e_idx]
                            if ep > 0: rets.append((n, (xp - ep) / ep))
                    except: pass
                
                pre_exdiv_score, hist_avg_alpha, hist_win_rate, optimal_n = 50, 0.0, 0.0, min_entry_day
                if rets:
                    rd = pd.DataFrame(rets, columns=["n", "ret"])
                    grp = rd.groupby("n")["ret"].mean()
                    optimal_n = grp.idxmax()
                    opt_vals = rd[rd["n"] == optimal_n]["ret"]
                    hist_win_rate = (opt_vals > 0).sum() / len(opt_vals) if not opt_vals.empty else 0
                    hist_avg_alpha = opt_vals.mean() - 0.0005
                    pre_exdiv_score = max(0, min(100, 50 + (hist_avg_alpha * 2000)))

                if hist_win_rate < min_win_rate or hist_avg_alpha < min_hist_alpha:
                    yield {"type": "progress", "pct": pct, "msg": f"Skipped {raw_ticker}: Backtest (WR:{hist_win_rate*100:.0f}% < {min_win_rate*100:.0f}%, Alpha:{hist_avg_alpha*100:.2f}% < {min_hist_alpha*100:.2f}%)"}
                    continue

                d = df["Close"].diff()
                g, l = (d.where(d > 0, 0)).rolling(14).mean(), (-d.where(d < 0, 0)).rolling(14).mean()
                rs = g / l
                rsi = (100 - (100 / (1 + rs))).iloc[-1]
                if pd.isna(rsi): rsi = 50
                rsi_score = 90 if 30 <= rsi <= 50 else (60 if 50 < rsi <= 70 else 30)

                tech_score = mom_score*0.20 + trend_score*0.15 + vol_score*0.15 + vol_pattern_score*0.10 + pos_score*0.10 + pre_exdiv_score*0.20 + rsi_score*0.10
                comp_score = fund_score * 0.45 + tech_score * 0.55

                if comp_score < min_score:
                    yield {"type": "progress", "pct": pct, "msg": f"Skipped {raw_ticker}: Low score ({comp_score:.1f} < {min_score})"}
                    continue

                stars = "★★★" if comp_score >= 75 else ("★★☆" if comp_score >= 60 else "★☆☆")
                label = "STRONG" if comp_score >= 75 else ("DECENT" if comp_score >= 60 else "WEAK")
                s_entry = max(optimal_n, 8 if comp_score >= 75 else (5 if comp_score >= 60 else 3))
                
                ticker_res = {
                    "ticker": raw_ticker, "sector": sector_key, "composite_score": sanitize_val(comp_score, 1),
                    "fundamental_score": sanitize_val(fund_score, 1), "technical_score": sanitize_val(tech_score, 1),
                    "stars": stars, "label": label, "ex_date": target_ex_date.strftime("%Y-%m-%d"),
                    "days_to_ex": to_py(days_to_ex), "declared": bool(is_declared), "div_amount": sanitize_val(div_amount, 4),
                    "current_price": sanitize_val(current_price, 2), "suggested_entry_day": to_py(int(s_entry)),
                    "days_to_entry": to_py(int(days_to_ex) - int(s_entry)), "stop_loss_pct": sanitize_val(atr_pct * 150, 2),
                    "reward_target_pct": sanitize_val(hist_avg_alpha * 80, 2), "risk_reward": sanitize_val((hist_avg_alpha * 0.8) / (atr_pct * 1.5) if atr_pct else 0, 2),
                    "hist_avg_alpha": sanitize_val(hist_avg_alpha * 100, 2), "hist_win_rate": sanitize_val(hist_win_rate * 100, 1),
                    "avg_dollar_vol": to_py(int(dollar_vol * 1000)), "optimal_n": to_py(int(optimal_n)), "rsi_14": to_py(int(rsi)),
                    "is_accumulating": bool(is_accumulating), "golden_cross": bool(sma20 > sma50), "above_sma20": bool(current_price > sma20), "atr_pct": sanitize_val(atr_pct, 4),
                    "score_components": {
                        "discount_score": to_py(int(discount_score)), "distribution_score": to_py(int(dist_score)), "yield_score": to_py(int(yield_score)),
                        "liquidity_score": to_py(int(liq_score)), "sector_score": to_py(int(sec_score)), "momentum_score": to_py(int(mom_score)),
                        "trend_score": to_py(int(trend_score)), "volatility_score": to_py(int(vol_score)), "volume_pattern_score": to_py(int(vol_pattern_score)),
                        "price_position_score": to_py(int(pos_score)), "pre_exdiv_score": to_py(int(pre_exdiv_score)), "rsi_score": to_py(int(rsi_score))
                    }
                }
                results.append(ticker_res)
                yield {"type": "result", "data": ticker_res}

            except Exception as e:
                logging.error(f"[PRE-EXDIV] Error processing {raw_ticker}: {e}")

        # Pause to prevent rate limit
        if i + FETCH_CHUNK_SIZE < total_tickers:
            time.sleep(FETCH_CHUNK_SLEEP)

    # 3. Finalize
    yield {"type": "progress", "pct": 100, "msg": "Finalizing analysis..."}
    final_data = _finalize_results(results, etf_summary, spy_ret_21d, total_tickers)
    yield {"type": "final", "data": final_data}


def _finalize_results(results, etf_summary, spy_ret_21d, total_tickers):
    # Sector Snapshot
    sector_snapshot = {}
    for sec_key, etf_list in SECTOR_ETFS.items():
        primary = etf_list[0] if etf_list else "SPY"
        if primary in etf_summary:
            s_data = etf_summary[primary]
            sector_snapshot[sec_key] = {
                "sector": sec_key, "ret_10d": sanitize_val(s_data["ret_10d"] * 100, 2),
                "ret_21d": sanitize_val(s_data["ret_21d"] * 100, 2), "vs_spy_21d": sanitize_val((s_data["ret_21d"] - spy_ret_21d) * 100, 2),
                "n_upcoming": sum(1 for r in results if r["sector"] == sec_key),
                "n_strong": sum(1 for r in results if r["sector"] == sec_key and r["label"] == "STRONG"),
                "n_decent": sum(1 for r in results if r["sector"] == sec_key and r["label"] == "DECENT")
            }

    # Actions
    enter_now, entering_soon = [], []
    for r in results:
        if r["label"] in ("STRONG", "DECENT"):
            if 2 <= r["days_to_ex"] <= r["suggested_entry_day"]: enter_now.append(r["ticker"])
            elif 0 <= (r["days_to_ex"] - r["suggested_entry_day"]) <= 2: entering_soon.append(r["ticker"])

    results.sort(key=lambda x: x["composite_score"], reverse=True)
    return {
        "scan_time": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "total_scanned": total_tickers, "total_found": len(results),
        "results": results, "sector_snapshot": list(sector_snapshot.values()),
        "todays_actions": {"enter_now": enter_now, "entering_soon": entering_soon}
    }

