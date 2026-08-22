#!/usr/bin/env python3
"""
get_stock_data.py
Download historical OHLCV data for many tickers via yfinance.

Modes for existing outputs:
  (default) skip existing (and don't even re-download them)
  --overwrite  -> always rewrite, full history
  --append     -> merge new rows into existing file (dedupe by Date index);
                  only re-fetches a small trailing window per ticker instead
                  of the full history (see --append-lookback-days)

Reliability:
  - Uses a curl_cffi browser-impersonating session (falls back to yfinance's
    default session if curl_cffi isn't installed) since Yahoo increasingly
    rate-limits/blocks plain requests-based clients.
  - All threads share a token-bucket rate limiter (--rate-limit) so total
    request rate to Yahoo stays bounded regardless of --threads.
  - Retries use exponential backoff with jitter; rate-limit errors get a
    longer backoff than other transient errors.

Examples:
  python get_stock_data.py -i tickers.list -s 2019-01-01 -I 1d --include-today
  python get_stock_data.py -i tickers.list --append
"""

import argparse
import concurrent.futures as cf
import datetime as dt
import os
import random
import re
import sys
import threading
import time
from typing import Dict, List, Optional, Tuple

import pandas as pd

try:
    import yfinance as yf
except ImportError:
    sys.stderr.write(
        "Requires 'yfinance' (and pandas, pyarrow for parquet).\n"
        "Install in your venv:\n  pip install -r requirements.txt\n"
    )
    sys.exit(1)

try:
    from yfinance.exceptions import YFRateLimitError
except ImportError:  # older yfinance without a dedicated rate-limit exception
    class YFRateLimitError(Exception):
        pass

_log_file_handle = None  # set in main() when --log-file is given

def log(msg: str) -> None:
    print(msg)
    if _log_file_handle:
        ts = dt.datetime.now().isoformat(timespec="seconds")
        _log_file_handle.write(f"{ts} {msg}\n")
        _log_file_handle.flush()

# ---------- parsing helpers ----------
_SPLIT_RE = re.compile(r"[,;\s]+")
_ALLOWED_RE = re.compile(r"^[A-Za-z0-9\-\.\^=]+$")

def _normalize_token(tok: str) -> Optional[str]:
    if not tok:
        return None
    t = tok.strip()
    if not t or t.startswith("#"):
        return None
    if t.startswith("$"):
        t = t[1:]
    t = t.upper()
    if not _ALLOWED_RE.match(t):
        return None
    return t

def read_tickers(args: argparse.Namespace) -> List[str]:
    tokens: List[str] = []
    if args.input_file:
        with open(args.input_file, "r", encoding="utf-8") as f:
            for line in f:
                for part in _SPLIT_RE.split(line.strip()):
                    nt = _normalize_token(part)
                    if nt:
                        tokens.append(nt)
    for p in (args.tickers or []):
        for part in _SPLIT_RE.split(p):
            nt = _normalize_token(part)
            if nt:
                tokens.append(nt)
    seen, out = set(), []
    for t in tokens:
        if t not in seen:
            seen.add(t)
            out.append(t)
    if not out:
        sys.stderr.write("No tickers provided. Use positionals or -i FILE.\n")
        sys.exit(2)
    return out

# ---------- IO helpers ----------
def save_frame(df: pd.DataFrame, path: str, fmt: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if fmt == "csv":
        df.to_csv(path, index=True)
    else:
        df.to_parquet(path, index=True)

def load_existing(path: str, fmt: str) -> Optional[pd.DataFrame]:
    if not os.path.exists(path):
        return None
    try:
        if fmt == "csv":
            df = pd.read_csv(path, index_col=0, parse_dates=True)
        else:
            df = pd.read_parquet(path)
            # ensure Date index if it saved index as a column
            if "Date" in df.columns and not isinstance(df.index, pd.DatetimeIndex):
                df = df.set_index(pd.to_datetime(df["Date"])).drop(columns=["Date"])
            if not isinstance(df.index, pd.DatetimeIndex):
                # try coercion if the index isn't datetime
                df.index = pd.to_datetime(df.index, errors="coerce")
        return df.sort_index()
    except Exception as e:
        log(f"[warn] Failed to read existing file {path}: {e}")
        return None

def _adjust_close(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "Adj Close" not in df.columns or "Close" not in df.columns:
        return df
    factor = df["Adj Close"] / df["Close"]
    # Close of 0/NaN (halted, delisted, bad data) divides out to inf/-inf/NaN;
    # neutralize those to a no-op factor instead of poisoning OHLC with inf.
    factor = factor.replace([float("inf"), float("-inf")], 1.0).fillna(1.0)
    for col in ["Open", "High", "Low", "Close"]:
        if col in df.columns:
            df[col] = (df[col] * factor).astype(float)
    return df

def align_and_merge(existing: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    """
    Merge on index (dates). Keep union of columns.
    Prefer NEW data on overlapping dates (right-biased combine).
    """
    # Make sure datetime index
    if not isinstance(new.index, pd.DatetimeIndex):
        new.index = pd.to_datetime(new.index, errors="coerce")
    if existing is not None and not isinstance(existing.index, pd.DatetimeIndex):
        existing.index = pd.to_datetime(existing.index, errors="coerce")

    # Align columns (union)
    all_cols = sorted(set((existing.columns if existing is not None else [])) | set(new.columns))
    if existing is None:
        merged = new[all_cols].copy()
    else:
        e = existing.reindex(columns=all_cols)
        n = new.reindex(columns=all_cols)
        # where new is notna, take new; else keep existing
        merged = e.combine_first(n)  # existing first…
        merged.update(n)             # …then overwrite with new where present
    # Drop duplicate index entries by keeping the last occurrence
    merged = merged[~merged.index.duplicated(keep="last")]
    return merged.sort_index()

# ---------- networking / rate limiting ----------
_thread_local = threading.local()
_session_warned = False

def _get_session():
    """Thread-local curl_cffi session (impersonates a real browser).
    Falls back to yfinance's default session if curl_cffi isn't installed.
    Yahoo has gotten aggressive about rate-limiting/blocking plain
    requests-based clients, so curl_cffi is strongly recommended
    (it's in requirements.txt)."""
    global _session_warned
    sess = getattr(_thread_local, "session", "unset")
    if sess != "unset":
        return sess
    try:
        from curl_cffi import requests as cffi_requests
        sess = cffi_requests.Session(impersonate="chrome")
    except ImportError:
        if not _session_warned:
            log("[warn] curl_cffi not installed; using yfinance's default session "
                "(more likely to be rate-limited by Yahoo). pip install curl_cffi")
            _session_warned = True
        sess = None
    _thread_local.session = sess
    return sess

class RateLimiter:
    """Thread-safe token-bucket limiter shared across all download threads,
    so the *total* request rate to Yahoo stays bounded regardless of how
    many --threads are running concurrently."""
    def __init__(self, rate_per_sec: float):
        self.min_interval = 1.0 / rate_per_sec if rate_per_sec > 0 else 0.0
        self._lock = threading.Lock()
        self._next_slot = time.monotonic()

    def wait(self) -> None:
        if self.min_interval <= 0:
            return
        with self._lock:
            now = time.monotonic()
            slot = max(now, self._next_slot)
            self._next_slot = slot + self.min_interval
        delay = slot - now
        if delay > 0:
            time.sleep(delay)

# ---------- downloader ----------
def fetch_one(ticker: str, start: str, end: str, interval: str, adjust: bool,
              retries: int, sleep_sec: float,
              limiter: RateLimiter) -> Tuple[str, Optional[pd.DataFrame], Optional[str]]:
    last_err = None
    for attempt in range(1, retries + 1):
        limiter.wait()
        try:
            t = yf.Ticker(ticker, session=_get_session())
            df = t.history(start=start, end=end, interval=interval,
                           auto_adjust=False, actions=True, raise_errors=False)
            if df is None or df.empty:
                limiter.wait()
                df = t.history(period="max", interval=interval,
                               auto_adjust=False, actions=True, raise_errors=False)
                if df is None or df.empty:
                    return ticker, None, f"No data returned (interval={interval})."
            if adjust:
                df = _adjust_close(df)
            cols = [c for c in ["Open", "High", "Low", "Close", "Adj Close", "Volume", "Dividends", "Stock Splits"]
                    if c in df.columns]
            if cols:
                df = df[cols]
            return ticker, df, None
        except YFRateLimitError as e:
            last_err = f"Rate limited: {e}"
            if attempt < retries:
                backoff = max(sleep_sec, 1.0) * (2 ** attempt) + random.uniform(0, sleep_sec)
                log(f"[warn] {ticker}: rate limited, backing off {backoff:.1f}s (attempt {attempt}/{retries})")
                time.sleep(backoff)
            else:
                return ticker, None, last_err
        except Exception as e:
            last_err = str(e)
            if attempt < retries:
                backoff = sleep_sec * attempt + random.uniform(0, sleep_sec * 0.5)
                time.sleep(backoff)
            else:
                return ticker, None, last_err
    return ticker, None, last_err

# ---------- interval range sanity check ----------
# Approximate limits Yahoo enforces on intraday history (days of lookback).
_INTRADAY_LIMIT_DAYS = {
    "1m": 7, "2m": 60, "5m": 60, "15m": 60, "30m": 60, "90m": 60,
    "60m": 730, "1h": 730,
}

def warn_intraday_range(interval: str, start: str, end: str) -> None:
    limit = _INTRADAY_LIMIT_DAYS.get(interval)
    if not limit:
        return
    try:
        span = (dt.date.fromisoformat(end) - dt.date.fromisoformat(start)).days
    except Exception:
        return
    if span > limit:
        log(f"[warn] interval={interval}: Yahoo typically only serves ~{limit} days of history "
            f"for this interval; requested range is {span} days and will likely come back truncated.")

# ---------- args ----------
def parse_args() -> argparse.Namespace:
    today = dt.date.today().isoformat()
    p = argparse.ArgumentParser(description="Download historical OHLCV for tickers via yfinance.")
    p.add_argument("tickers", nargs="*", help="Tickers (space-separated).")
    p.add_argument("-i", "--input-file", help="Text file with one or many tickers per line.")
    p.add_argument("-s", "--start", default="2000-01-01", help="Start YYYY-MM-DD. Default: 2000-01-01")
    p.add_argument("-e", "--end", default=today, help=f"End YYYY-MM-DD (exclusive on Yahoo). Default: {today}")
    p.add_argument("-I", "--interval", default="1d",
                   choices=["1m","2m","5m","15m","30m","60m","90m","1h","1d","5d","1wk","1mo","3mo"],
                   help="Interval. Default: 1d")
    p.add_argument("-o", "--outdir", default="history", help="Output directory. Default: ./history")
    p.add_argument("-f", "--format", default="csv", choices=["csv", "parquet"], help="Output format.")
    p.add_argument("--adjust", action="store_true", help="Rescale OHLC so Close equals Adj Close.")
    p.add_argument("--threads", type=int, default=min(8, os.cpu_count() or 4), help="Concurrency.")
    p.add_argument("--retries", type=int, default=3, help="Retry count.")
    p.add_argument("--sleep", type=float, default=0.5, help="Base seconds for retry backoff.")
    p.add_argument("--rate-limit", type=float, default=3.0,
                   help="Max requests/sec to Yahoo, shared across all threads. 0 = unlimited. Default: 3.0")
    p.add_argument("--show-empty", action="store_true", help="Save files even if dataframe is empty.")
    p.add_argument("--include-today", action="store_true",
                   help="Shift 'end' by +1 day so today’s bar is included (Yahoo end is exclusive).")
    p.add_argument("--print-tickers", action="store_true", help="Print parsed tickers and exit.")
    p.add_argument("--validate", action="store_true",
                   help="Preflight: write valid_tickers.txt / invalid_tickers.txt and use only valid unless --exit-after-validate.")
    p.add_argument("--exit-after-validate", action="store_true", help="Stop after validation.")
    p.add_argument("--log-file", help="Also write timestamped log lines to this file (appends).")
    # mutually exclusive write behavior
    group = p.add_mutually_exclusive_group()
    group.add_argument("--overwrite", action="store_true", help="Overwrite existing outputs.")
    group.add_argument("--append", action="store_true", help="Append/merge new rows into existing outputs.")
    p.add_argument("--append-lookback-days", type=int, default=5,
                   help="With --append, re-fetch only from (last saved date - N days) instead of "
                        "the full --start, to catch late corrections. Default: 5")
    return p.parse_args()

def maybe_warn_market_not_closed(include_today: bool):
    if not include_today:
        return
    try:
        from zoneinfo import ZoneInfo
        from datetime import datetime, timedelta
        now = datetime.now(ZoneInfo("America/New_York"))
        close_dt = now.replace(hour=16, minute=0, second=0, microsecond=0)
        if now < (close_dt + timedelta(minutes=15)):
            log("[warn] Market may not be closed yet in America/New_York; today's bar may be incomplete.")
    except Exception:
        pass

# ---------- main ----------
def main():
    global _log_file_handle
    args = parse_args()

    if args.log_file:
        _log_file_handle = open(args.log_file, "a", encoding="utf-8")

    try:
        _run(args)
    finally:
        if _log_file_handle:
            _log_file_handle.close()

def _run(args: argparse.Namespace):
    if args.include_today:
        try:
            y, m, d = map(int, args.end.split("-"))
            end_dt = dt.date(y, m, d) + dt.timedelta(days=1)
        except Exception:
            end_dt = dt.date.today() + dt.timedelta(days=1)
        args.end = end_dt.isoformat()
        maybe_warn_market_not_closed(include_today=True)

    tickers = read_tickers(args)
    if args.print_tickers:
        print("Parsed tickers:", " ".join(tickers))
        return

    limiter = RateLimiter(args.rate_limit)

    # validation
    if args.validate:
        log(f"[info] validating {len(tickers)} tickers ...")
        valid, invalid = [], []
        # sequential (not threaded) to keep well under the rate limit while
        # still sharing the same limiter/session as the main download pass
        for t in tickers:
            limiter.wait()
            try:
                dfv = yf.Ticker(t, session=_get_session()).history(
                    period="5d", interval="1d", auto_adjust=False, actions=False, raise_errors=False)
                if dfv is not None and not dfv.empty:
                    valid.append(t)
                else:
                    invalid.append((t, "empty"))
            except Exception as e:
                invalid.append((t, str(e)))
        with open("valid_tickers.txt", "w", encoding="utf-8") as f:
            f.write("\n".join(sorted(valid)) + ("\n" if valid else ""))
        with open("invalid_tickers.txt", "w", encoding="utf-8") as f:
            for t, msg in sorted(invalid):
                f.write(f"{t}\t{msg}\n")
        log(f"[info] wrote valid_tickers.txt ({len(valid)}) and invalid_tickers.txt ({len(invalid)})")
        if args.exit_after_validate:
            return
        tickers = sorted(valid)
        if not tickers:
            log("[err] no valid tickers to download after validation.")
            return

    warn_intraday_range(args.interval, args.start, args.end)

    # Decide per-ticker what to do *before* downloading anything:
    #  - default mode: if the output already exists, don't fetch it at all.
    #  - --append: if the output exists, only re-fetch a small trailing
    #    window (last saved date - lookback) instead of the full --start,
    #    and reuse the already-loaded existing frame for the merge later.
    #  - --overwrite: always fetch the full requested range.
    fetch_plan: Dict[str, Tuple[str, Optional[pd.DataFrame]]] = {}
    tickers_to_fetch: List[str] = []
    skipped: List[str] = []

    for t in tickers:
        out_path = os.path.join(args.outdir, f"{t}.{args.format}")
        exists = os.path.exists(out_path)

        if exists and not args.overwrite and not args.append:
            skipped.append(t)
            continue

        eff_start = args.start
        df_old = None
        if exists and args.append:
            df_old = load_existing(out_path, args.format)
            if df_old is not None and not df_old.empty:
                last_date = df_old.index.max()
                if pd.notna(last_date):
                    buffered = (last_date.date() - dt.timedelta(days=args.append_lookback_days)).isoformat()
                    if buffered > args.start:
                        eff_start = buffered

        fetch_plan[t] = (eff_start, df_old)
        tickers_to_fetch.append(t)

    for t in skipped:
        log(f"[skip] {t}: output file already exists -> {os.path.join(args.outdir, f'{t}.{args.format}')}")

    log(f"[info] downloading {len(tickers_to_fetch)} tickers @ {args.interval} "
        f"({len(skipped)} skipped, already exist; rate-limited to {args.rate_limit or 'unlimited'} req/s)")

    results = []
    with cf.ThreadPoolExecutor(max_workers=max(1, args.threads)) as ex:
        futures = [
            ex.submit(fetch_one, t, fetch_plan[t][0], args.end, args.interval,
                      args.adjust, args.retries, args.sleep, limiter)
            for t in tickers_to_fetch
        ]
        for fut in cf.as_completed(futures):
            results.append(fut.result())

    successes, failures = 0, []
    os.makedirs(args.outdir, exist_ok=True)

    for ticker, df_new, err in sorted(results, key=lambda x: x[0]):
        out_path = os.path.join(args.outdir, f"{ticker}.{args.format}")

        if df_new is None or (not args.show_empty and df_new.empty):
            msg = err or "Empty dataframe"
            log(f"[err]  {ticker}: {msg}")
            failures.append((ticker, msg))
            continue

        if args.overwrite and os.path.exists(out_path):
            save_frame(df_new, out_path, args.format)
            log(f"[ok]  {ticker}: {len(df_new):>5} rows -> {out_path} (overwrote)")
            successes += 1
        elif args.append and os.path.exists(out_path):
            df_old = fetch_plan.get(ticker, (None, None))[1]
            merged = align_and_merge(df_old, df_new) if df_old is not None else df_new
            save_frame(merged, out_path, args.format)
            added = len(merged) - (0 if df_old is None else len(df_old))
            log(f"[ok]  {ticker}: {len(merged):>5} rows -> {out_path} (appended {max(0, added)} new rows)")
            successes += 1
        else:
            save_frame(df_new, out_path, args.format)
            log(f"[ok]  {ticker}: {len(df_new):>5} rows -> {out_path}")
            successes += 1

    log(f"\nDone. {successes} succeeded, {len(failures)} failed, {len(skipped)} skipped.")
    if failures:
        log("Failed tickers:")
        for t, m in failures:
            log(f"  - {t}: {m}")

if __name__ == "__main__":
    main()
