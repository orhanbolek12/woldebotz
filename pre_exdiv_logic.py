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
# Dividend batch size — fetching too many at once triggers yfinance errors
# -----------------------------------------------------------------------
DIVIDEND_BATCH_SIZE = 20   # number of tickers per batch
DIVIDEND_BATCH_SLEEP = 1.0  # seconds to sleep between dividend batches


def sanitize_val(val, decimals=2):
    if pd.isna(val) or val is None or np.isinf(val):
        return 0
    return round(float(val), decimals)


def _fetch_dividends_batched(ticker_list):
    """
    Fetch dividend history for a list of tickers in small batches via
    yf.Ticker().dividends so we never hit rate-limit / missing-data issues.

    Returns a dict: { yf_ticker_str -> pd.Series (sorted by date) }
    """
    dividends_map = {}
    total = len(ticker_list)

    for batch_start in range(0, total, DIVIDEND_BATCH_SIZE):
        batch = ticker_list[batch_start: batch_start + DIVIDEND_BATCH_SIZE]
        for sym in batch:
            try:
                t = yf.Ticker(sym)
                divs = t.dividends
                if divs is not None and not divs.empty:
                    # Normalise timezone → naive date index
                    divs.index = divs.index.tz_localize(None) if divs.index.tz is None else divs.index.tz_convert(None)
                    dividends_map[sym] = divs.sort_index()
                else:
                    dividends_map[sym] = pd.Series(dtype='float64')
            except Exception as e:
                logging.warning(f"[DIVIDEND BATCH] Failed to fetch dividends for {sym}: {e}")
                dividends_map[sym] = pd.Series(dtype='float64')

        # Polite pause between batches
        if batch_start + DIVIDEND_BATCH_SIZE < total:
            time.sleep(DIVIDEND_BATCH_SLEEP)

    return dividends_map


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

    Strategy:
      Phase 1 – single yf.download() for OHLCV of all symbols (fast, concurrent)
      Phase 2 – batched yf.Ticker().dividends for every target ticker (reliable)
    """
    if sector_map is None:
        sector_map = {}

    results = []
    total_tickers = len(tickers)

    # ------------------------------------------------------------------ #
    # Phase 1: Batch-download OHLCV for benchmarks, ETFs, and all targets #
    # ------------------------------------------------------------------ #
    logging.info("[PRE-EXDIV] Phase 1: batch OHLCV download…")

    all_symbols_set = set(BENCHMARKS)
    for etf_list in SECTOR_ETFS.values():
        all_symbols_set.update(etf_list)

    yf_target_map = {t: parse_ticker_yf(t) for t in tickers}
    all_symbols_set.update(yf_target_map.values())

    symbol_list = list(all_symbols_set)

    # actions=False here — we fetch dividends separately (Phase 2)
    try:
        batch_data = yf.download(
            symbol_list,
            period="2y",
            interval="1d",
            group_by="ticker",
            actions=False,
            threads=True,
        )
        logging.info(f"[PRE-EXDIV] Phase 1 complete. Shape: {batch_data.shape}")
    except Exception as e:
        logging.error(f"[PRE-EXDIV] Batch OHLCV download failed: {e}")
        batch_data = pd.DataFrame()

    # ------------------------------------------------------------------ #
    # Phase 2: Fetch dividends in small batches                           #
    # ------------------------------------------------------------------ #
    logging.info("[PRE-EXDIV] Phase 2: batched dividend fetch…")
    target_yf_list = list(set(yf_target_map.values()))
    dividends_map = _fetch_dividends_batched(target_yf_list)
    logging.info(f"[PRE-EXDIV] Phase 2 complete. Dividend data for {len(dividends_map)} tickers.")

    # ------------------------------------------------------------------ #
    # Helper: extract OHLCV DataFrame for a symbol from batch_data        #
    # ------------------------------------------------------------------ #
    def _get_ohlcv(sym):
        if batch_data.empty:
            return pd.DataFrame()
        is_multi = isinstance(batch_data.columns, pd.MultiIndex)
        try:
            if is_multi:
                # columns are (field, ticker)
                if sym in batch_data.columns.get_level_values(1):
                    df = batch_data.xs(sym, axis=1, level=1).dropna(how="all")
                    return df
                # also try level 0 (when group_by='ticker')
                if sym in batch_data.columns.get_level_values(0):
                    df = batch_data[sym].dropna(how="all")
                    return df
                return pd.DataFrame()
            else:
                # single-ticker download
                return batch_data.dropna(how="all")
        except Exception:
            return pd.DataFrame()

    # ------------------------------------------------------------------ #
    # Build ETF / benchmark lookup                                         #
    # ------------------------------------------------------------------ #
    etf_data = {}
    for sym in all_symbols_set:
        try:
            df_sym = _get_ohlcv(sym)
            if df_sym.empty:
                continue
            close_col = "Close" if "Close" in df_sym.columns else df_sym.columns[0]
            cp = df_sym[close_col].iloc[-1]
            p10 = df_sym[close_col].iloc[-10] if len(df_sym) >= 10 else df_sym[close_col].iloc[0]
            p21 = df_sym[close_col].iloc[-21] if len(df_sym) >= 21 else df_sym[close_col].iloc[0]
            etf_data[sym] = {
                "ret_10d": (cp - p10) / p10 if p10 else 0,
                "ret_21d": (cp - p21) / p21 if p21 else 0,
                "df": df_sym,
            }
        except Exception as e:
            logging.error(f"[PRE-EXDIV] ETF data error for {sym}: {e}")

    spy_ret_21d = etf_data.get("SPY", {}).get("ret_21d", 0)

    now = datetime.now()
    cutoff_date = (now + timedelta(days=lookahead_days)).date()

    # ------------------------------------------------------------------ #
    # Phase 3: Score each CEF ticker                                       #
    # ------------------------------------------------------------------ #
    for i, raw_ticker in enumerate(tickers):
        if progress_callback and progress_callback(i, total_tickers) == "STOP":
            break

        yf_ticker  = yf_target_map[raw_ticker]
        sector_key = sector_map.get(raw_ticker.upper(), "sector_equity")

        try:
            # ---------- OHLCV ----------
            df = _get_ohlcv(yf_ticker)

            if df.empty or len(df) < 60:
                logging.warning(
                    f"[PRE-EXDIV DEBUG] {raw_ticker} dropped: df is "
                    f"{'empty' if df.empty else 'too short (' + str(len(df)) + ')'}"
                )
                continue

            # ---------- Dividends ----------
            dividends = dividends_map.get(yf_ticker, pd.Series(dtype="float64"))

            # Override with manual history if available
            for key in (raw_ticker.upper(), yf_ticker.upper()):
                if key in MANUAL_DIVIDEND_HISTORY:
                    manual_data = {pd.to_datetime(d): a for d, a in MANUAL_DIVIDEND_HISTORY[key]}
                    dividends = pd.Series(manual_data).sort_index()
                    break

            if dividends.empty:
                logging.warning(
                    f"[PRE-EXDIV DEBUG] {raw_ticker} dropped: No dividend history found."
                )
                continue

            # ---------- Find upcoming ex-date ----------
            future_divs = dividends[dividends.index.date >= now.date()]

            target_ex_date = None
            is_declared    = False
            div_amount     = 0.0

            if not future_divs.empty:
                next_div_ts    = future_divs.index[0]
                target_ex_date = next_div_ts.date()
                div_amount     = future_divs.iloc[0]
                is_declared    = True
            elif show_estimated:
                last_div_ts  = dividends.index[-1]
                last_amount  = dividends.iloc[-1]

                if len(dividends) >= 2:
                    diff_days = (last_div_ts - dividends.index[-2]).days

                    if 20 <= diff_days <= 45:      # Monthly
                        est_date = last_div_ts + relativedelta(months=1)
                    elif 70 <= diff_days <= 110:   # Quarterly
                        est_date = last_div_ts + relativedelta(months=3)
                    elif 10 <= diff_days <= 20:    # Bi-weekly
                        est_date = last_div_ts + timedelta(days=14)
                    else:
                        avg_diff = (dividends.index[-1] - dividends.index[0]).days / (len(dividends) - 1)
                        est_date = last_div_ts + timedelta(days=int(avg_diff))

                    if est_date and est_date.date() >= now.date():
                        target_ex_date = est_date.date()
                        div_amount     = last_amount
                        is_declared    = False

            if not target_ex_date:
                logging.warning(f"[PRE-EXDIV DEBUG] {raw_ticker} dropped: No upcoming ex-date found.")
                continue
            if target_ex_date > cutoff_date:
                logging.warning(
                    f"[PRE-EXDIV DEBUG] {raw_ticker} dropped: ex-date {target_ex_date} > cutoff {cutoff_date}."
                )
                continue

            days_to_ex = (target_ex_date - now.date()).days

            # ---------- 4. Fundamental Scoring (45%) ----------
            close_col  = "Close"
            current_price = df[close_col].iloc[-1]

            price_ma200 = df[close_col].rolling(200).mean().iloc[-1]
            if not pd.isna(price_ma200) and price_ma200 > 0:
                discount_proxy = (price_ma200 - current_price) / price_ma200
                discount_score = max(0, min(100, 50 + (discount_proxy * 500)))
            else:
                discount_score = 50

            dist_score = 50
            if len(dividends) >= 3:
                rd = dividends.iloc[-3:].values
                if rd[2] >= rd[1] >= rd[0]: dist_score = 90
                elif rd[2] < rd[1]:         dist_score = 40

            current_yield = (div_amount * 12) / current_price if current_price > 0 else 0
            yield_score   = max(0, min(100, current_yield * 1000))

            avg_vol    = df["Volume"].tail(20).mean()
            dollar_vol = (avg_vol * current_price) / 1000  # thousands
            if dollar_vol < min_volume_daily / 1000:
                logging.warning(
                    f"[PRE-EXDIV DEBUG] {raw_ticker} dropped: "
                    f"Dollar vol {dollar_vol:.0f}k < required {min_volume_daily/1000:.0f}k."
                )
                continue
            liq_score = min(100, (dollar_vol / 2000) * 100)

            sec_etf  = SECTOR_ETFS.get(sector_key, ["SPY"])[0]
            sec_data = etf_data.get(sec_etf, {"ret_10d": 0, "ret_21d": 0})
            sec_score = max(0, min(100, 50 + (sec_data["ret_21d"] * 1000)))

            fund_score = (
                discount_score * 0.25
                + dist_score   * 0.25
                + yield_score  * 0.15
                + liq_score    * 0.20
                + sec_score    * 0.15
            )

            # ---------- 5. Technical Scoring (55%) ----------
            price_10  = df[close_col].iloc[-10] if len(df) >= 10 else df[close_col].iloc[0]
            price_21  = df[close_col].iloc[-21] if len(df) >= 21 else df[close_col].iloc[0]
            mom_ret   = (current_price - price_21) / price_21
            mom_score = max(0, min(100, 50 + (mom_ret * 500)))

            sma20 = df[close_col].rolling(20).mean().iloc[-1]
            sma50 = df[close_col].rolling(50).mean().iloc[-1]
            trend_score = 50
            if current_price > sma20: trend_score += 25
            if current_price > sma50: trend_score += 25

            high_low = df["High"] - df["Low"]
            atr_14   = high_low.tail(14).mean()
            atr_pct  = atr_14 / current_price if current_price > 0 else 0
            vol_score = max(0, min(100, 100 - (atr_pct * 2000)))

            vol_last_5  = df["Volume"].tail(5).mean()
            vol_last_20 = df["Volume"].tail(20).mean()
            is_accumulating    = vol_last_5 > vol_last_20 and current_price > sma20
            vol_pattern_score  = 80 if is_accumulating else 40

            high_52  = df["High"].tail(252).max()
            low_52   = df["Low"].tail(252).min()
            pos_range = high_52 - low_52
            pos_rank  = (current_price - low_52) / pos_range if pos_range > 0 else 0.5
            pos_score = pos_rank * 100

            # Pre-ExDiv backtest
            historical_ex_dates = dividends[dividends.index.date < now.date()].index
            entry_returns = []

            for ex in historical_ex_dates:
                try:
                    ex_idx = df.index.get_indexer([ex], method="pad")[0]
                    if ex_idx == -1:
                        continue
                    ex_price = df[close_col].iloc[ex_idx]
                    for n in range(min_entry_day, max_entry_day + 1):
                        entry_idx   = max(0, ex_idx - n)
                        entry_price = df[close_col].iloc[entry_idx]
                        if entry_price > 0:
                            ret = (ex_price - entry_price) / entry_price
                            entry_returns.append((n, ret))
                except Exception:
                    pass

            pre_exdiv_score  = 50
            hist_avg_alpha   = 0.0
            hist_win_rate    = 0.0
            optimal_n        = min_entry_day
            avg_ret          = 0.0

            if entry_returns:
                df_rets   = pd.DataFrame(entry_returns, columns=["n", "ret"])
                grp       = df_rets.groupby("n")["ret"].mean()
                optimal_n = grp.idxmax()

                opt_rets      = df_rets[df_rets["n"] == optimal_n]["ret"]
                win_count     = (opt_rets > 0).sum()
                hist_win_rate = win_count / len(opt_rets) if len(opt_rets) > 0 else 0
                avg_ret       = opt_rets.mean()
                hist_avg_alpha = avg_ret - 0.0005

                pre_exdiv_score = max(0, min(100, 50 + (hist_avg_alpha * 2000)))

            if hist_win_rate < min_win_rate:
                logging.warning(
                    f"[PRE-EXDIV DEBUG] {raw_ticker} dropped: Win rate {hist_win_rate:.2f} < min {min_win_rate}."
                )
                continue
            if hist_avg_alpha < min_hist_alpha:
                logging.warning(
                    f"[PRE-EXDIV DEBUG] {raw_ticker} dropped: Hist Alpha {hist_avg_alpha:.4f} < min {min_hist_alpha}."
                )
                continue

            # RSI-14
            delta  = df[close_col].diff()
            gain   = (delta.where(delta > 0, 0)).rolling(14).mean()
            loss   = (-delta.where(delta < 0, 0)).rolling(14).mean()
            rs     = gain / loss
            rsi_14 = (100 - (100 / (1 + rs))).iloc[-1]
            if pd.isna(rsi_14):
                rsi_14 = 50

            if   30 <= rsi_14 <= 50: rsi_score = 90
            elif 50 <  rsi_14 <= 70: rsi_score = 60
            else:                    rsi_score = 30

            tech_score = (
                mom_score          * 0.20
                + trend_score      * 0.15
                + vol_score        * 0.15
                + vol_pattern_score* 0.10
                + pos_score        * 0.10
                + pre_exdiv_score  * 0.20
                + rsi_score        * 0.10
            )

            composite_score = fund_score * 0.45 + tech_score * 0.55

            if composite_score < min_score:
                logging.warning(
                    f"[PRE-EXDIV DEBUG] {raw_ticker} dropped: Score {composite_score:.1f} < min {min_score}."
                )
                continue

            if composite_score >= 75:
                stars = "★★★"; label = "STRONG"; suggested_entry_day = max(optimal_n, 8)
            elif composite_score >= 60:
                stars = "★★☆"; label = "DECENT"; suggested_entry_day = max(optimal_n, 5)
            else:
                stars = "★☆☆"; label = "WEAK";   suggested_entry_day = max(optimal_n, 3)

            stop_loss_pct     = atr_pct * 1.5
            reward_target_pct = hist_avg_alpha * 0.80
            risk_reward       = reward_target_pct / stop_loss_pct if stop_loss_pct > 0 else 0

            results.append({
                "ticker":           raw_ticker,
                "sector":           sector_key,
                "composite_score":  sanitize_val(composite_score, 1),
                "fundamental_score":sanitize_val(fund_score, 1),
                "technical_score":  sanitize_val(tech_score, 1),
                "stars":            stars,
                "label":            label,
                "ex_date":          target_ex_date.strftime("%Y-%m-%d"),
                "days_to_ex":       days_to_ex,
                "declared":         is_declared,
                "div_amount":       sanitize_val(div_amount, 4),
                "current_price":    sanitize_val(current_price, 2),
                "suggested_entry_day": suggested_entry_day,
                "days_to_entry":    days_to_ex - suggested_entry_day,
                "stop_loss_pct":    sanitize_val(stop_loss_pct * 100, 2),
                "reward_target_pct":sanitize_val(reward_target_pct * 100, 2),
                "risk_reward":      sanitize_val(risk_reward, 2),
                "hist_avg_alpha":   sanitize_val(hist_avg_alpha * 100, 2),
                "hist_win_rate":    sanitize_val(hist_win_rate * 100, 1),
                "avg_dollar_vol":   int(dollar_vol * 1000),
                "optimal_n":        int(optimal_n),
                "rsi_14":           int(rsi_14),
                "is_accumulating":  bool(is_accumulating),
                "golden_cross":     bool(sma20 > sma50),
                "above_sma20":      bool(current_price > sma20),
                "atr_pct":          sanitize_val(atr_pct, 4),
                "score_components": {
                    "discount_score":       int(discount_score),
                    "distribution_score":   int(dist_score),
                    "yield_score":          int(yield_score),
                    "liquidity_score":      int(liq_score),
                    "sector_score":         int(sec_score),
                    "momentum_score":       int(mom_score),
                    "trend_score":          int(trend_score),
                    "volatility_score":     int(vol_score),
                    "volume_pattern_score": int(vol_pattern_score),
                    "price_position_score": int(pos_score),
                    "pre_exdiv_score":      int(pre_exdiv_score),
                    "rsi_score":            int(rsi_score),
                },
            })

        except Exception as e:
            logging.error(f"[PRE-EXDIV] Error processing {raw_ticker}: {e}")

    # ------------------------------------------------------------------ #
    # Sector Snapshot                                                       #
    # ------------------------------------------------------------------ #
    sector_snapshot = {}
    for sec_key, etf_list in SECTOR_ETFS.items():
        primary_etf = etf_list[0] if etf_list else "SPY"
        if primary_etf in etf_data:
            ret10 = etf_data[primary_etf]["ret_10d"]
            ret21 = etf_data[primary_etf]["ret_21d"]
            sector_snapshot[sec_key] = {
                "sector":     sec_key,
                "ret_10d":    sanitize_val(ret10 * 100, 2),
                "ret_21d":    sanitize_val(ret21 * 100, 2),
                "vs_spy_21d": sanitize_val((ret21 - spy_ret_21d) * 100, 2),
                "n_upcoming": sum(1 for r in results if r["sector"] == sec_key),
                "n_strong":   sum(1 for r in results if r["sector"] == sec_key and r["label"] == "STRONG"),
                "n_decent":   sum(1 for r in results if r["sector"] == sec_key and r["label"] == "DECENT"),
            }

    # ------------------------------------------------------------------ #
    # Today's Actions                                                       #
    # ------------------------------------------------------------------ #
    enter_now     = []
    entering_soon = []
    for r in results:
        dto_ex    = r["days_to_ex"]
        sug_entry = r["suggested_entry_day"]
        if r["label"] in ("STRONG", "DECENT"):
            if 2 <= dto_ex <= sug_entry:
                enter_now.append(r["ticker"])
            elif dto_ex - sug_entry <= 2 and dto_ex > sug_entry:
                entering_soon.append(r["ticker"])

    results.sort(key=lambda x: x["composite_score"], reverse=True)

    return {
        "scan_time":    datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "total_scanned":total_tickers,
        "total_found":  len(results),
        "results":      results,
        "sector_snapshot": list(sector_snapshot.values()),
        "todays_actions": {
            "enter_now":    enter_now,
            "entering_soon":entering_soon,
        },
    }
