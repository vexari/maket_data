#!/usr/bin/env python3
"""
get_stock_data.py
Download historical OHLCV data for many tickers via yfinance, or via
Interactive Brokers (--source ibkr) as an alternative data source.

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

IBKR source (--source ibkr):
  Pulls bars from a running TWS or IB Gateway instance instead of Yahoo, via
  the ib_async package (pip install ib_async). Requires TWS/IB Gateway to be
  running, logged in, with API access enabled (File > Global Configuration >
  API > Settings > Enable ActiveX and Socket Clients) and market data
  permissions for the exchanges you're requesting. See --ib-host/--ib-port/
  --ib-client-id. Explicit mappings resolve index aliases; all resolved
  contracts require one unambiguous non-zero conId before collection.

Examples:
  python get_stock_data.py -i tickers.list -s 2019-01-01 -I 1d --include-today
  python get_stock_data.py -i tickers.list --append
  python get_stock_data.py -i tickers.list --source ibkr --ib-port 7496
"""

import argparse
import asyncio
import concurrent.futures as cf
import datetime as dt
import hashlib
import json
import logging
import os
import random
import re
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

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

try:
    from ib_async import IB, Contract, RequestError, StartupFetchNONE
    from ib_async.client import Client
    from ib_async.objects import BarDataList
    from ib_async.wrapper import Wrapper
    import ib_async.util as ib_util
except ImportError:
    IB = None  # only required when --source ibkr is used
    Contract = Any
    RequestError = Exception
    StartupFetchNONE = None
    Client = Any
    BarDataList = Any
    Wrapper = Any
    ib_util = None

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
                line = line.strip()
                if not line or line.startswith("#"):
                    continue  # whole-line comment
                for part in _SPLIT_RE.split(line):
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
_METADATA_SCHEMA_VERSION = 1

def _metadata_path(path: str) -> str:
    return f"{path}.meta.json"

def _sha256_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()

def _write_frame(df: pd.DataFrame, path: str, fmt: str) -> None:
    if fmt == "csv":
        df.to_csv(path, index=True)
    else:
        df.to_parquet(path, index=True)

def save_frame_with_metadata(
    df: pd.DataFrame, path: str, fmt: str, metadata: Dict[str, Any]
) -> None:
    """Replace data then metadata; interrupted replacement is detected by SHA."""
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    data_fd, data_tmp = tempfile.mkstemp(prefix=".market-data-", dir=directory)
    meta_fd, meta_tmp = tempfile.mkstemp(prefix=".market-meta-", dir=directory)
    os.close(data_fd)
    os.close(meta_fd)
    try:
        _write_frame(df, data_tmp, fmt)
        complete = dict(metadata)
        complete["data_sha256"] = _sha256_file(data_tmp)
        with open(meta_tmp, "w", encoding="utf-8") as handle:
            json.dump(complete, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(data_tmp, path)
        os.replace(meta_tmp, _metadata_path(path))
    finally:
        for temporary in (data_tmp, meta_tmp):
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass

def load_and_validate_metadata(
    path: str,
    *,
    source: str,
    ticker: str,
    interval: str,
    adjusted: bool,
    ib_use_rth: bool,
    what_to_show: Optional[str] = None,
) -> Dict[str, Any]:
    sidecar = _metadata_path(path)
    if not os.path.exists(sidecar):
        raise ValueError(
            f"legacy output has no trusted provenance: {sidecar}; "
            "use --overwrite once to establish metadata"
        )
    try:
        with open(sidecar, "r", encoding="utf-8") as handle:
            metadata = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid provenance metadata for {path}: {exc}") from exc
    expected = {
        "schema_version": _METADATA_SCHEMA_VERSION,
        "source": source,
        "requested_ticker": ticker,
        "interval": interval,
        "adjusted": adjusted,
    }
    for key, value in expected.items():
        if metadata.get(key) != value:
            raise ValueError(
                f"provenance mismatch for {path}: {key} is "
                f"{metadata.get(key)!r}, expected {value!r}"
            )
    actual_sha = _sha256_file(path)
    if metadata.get("data_sha256") != actual_sha:
        raise ValueError(f"provenance SHA-256 mismatch for {path}")
    if source == "ibkr":
        ibkr = metadata.get("ibkr")
        if not isinstance(ibkr, dict):
            raise ValueError(f"IBKR provenance block missing for {path}")
        if ibkr.get("useRTH") is not ib_use_rth:
            raise ValueError(f"IBKR useRTH provenance mismatch for {path}")
        if ibkr.get("whatToShow") != what_to_show:
            raise ValueError(f"IBKR whatToShow provenance mismatch for {path}")
        if not isinstance(ibkr.get("conId"), int) or ibkr["conId"] == 0:
            raise ValueError(f"IBKR provenance has invalid conId for {path}")
    return metadata

def _base_metadata(args: argparse.Namespace, ticker: str) -> Dict[str, Any]:
    return {
        "schema_version": _METADATA_SCHEMA_VERSION,
        "source": args.source,
        "requested_ticker": ticker,
        "interval": args.interval,
        "adjusted": args.adjust,
        "requested_start": args.start,
        "requested_end": args.end,
        "fetched_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "data_sha256": "",
        "time_semantics": _time_semantics(args.source, args.interval),
        "ibkr": None,
    }

def output_path(args: argparse.Namespace, ticker: str) -> str:
    directory = os.path.join(args.outdir, "ibkr") if args.source == "ibkr" else args.outdir
    return os.path.join(directory, f"{ticker}.{args.format}")

def validate_ib_append_identity(
    existing_metadata: Dict[str, Any], fetched_metadata: Dict[str, Any]
) -> None:
    existing = existing_metadata["ibkr"]["conId"]
    resolved = fetched_metadata["ibkr"]["conId"]
    if existing != resolved:
        raise ValueError(
            f"IBKR conId provenance mismatch: existing={existing}, resolved={resolved}"
        )

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
            if not df.empty:
                # A ticker code can coincidentally match an unrelated (often
                # delisted/thin) instrument on another exchange. That doesn't
                # raise an error -- Yahoo just returns stale data -- so flag
                # it here: if the latest bar is far older than requested,
                # this is very likely the wrong symbol (needs a suffix like
                # .TA/.KS, or a hyphenated share class like BRK-B).
                try:
                    last_dt = df.index.max()
                    end_ts = pd.Timestamp(end)
                    if pd.notna(last_dt) and (end_ts - last_dt.tz_localize(None)).days > 30:
                        log(f"[warn] {ticker}: latest bar is {last_dt.date()}, well before "
                            f"requested end {end} — likely the wrong symbol (delisted, or "
                            f"matching an unrelated ticker on another exchange).")
                except Exception:
                    pass
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

# ---------- IBKR (Interactive Brokers) source ----------
# Alternative to yfinance: pulls bars from a running TWS/IB Gateway instance
# over its local API socket via ib_async. This is your own broker connection
# (not scraping Yahoo), so it isn't subject to Yahoo's rate-limiting, and it
# can reach some symbols Yahoo has no data for -- but it needs TWS/IB Gateway
# running and logged in, with API access enabled and market data permissions
# for the relevant exchanges.

# yfinance-style interval -> IB barSizeSetting. A few yfinance intervals have
# no clean IB equivalent and are simply unsupported here (90m, 5d, 3mo).
_IB_BAR_SIZE = {
    "1m": "1 min", "2m": "2 mins", "5m": "5 mins", "15m": "15 mins",
    "30m": "30 mins", "60m": "1 hour", "1h": "1 hour",
    "1d": "1 day", "1wk": "1 week", "1mo": "1 month",
}

# Operational page sizes, deliberately conservative and locally bounded. They
# are not represented as official IBKR limits.
_IB_CHUNK_DURATION = {
    "1 min": "1 D", "2 mins": "2 D", "5 mins": "5 D", "15 mins": "10 D",
    "30 mins": "20 D", "1 hour": "30 D",
    "1 day": "1 Y", "1 week": "5 Y", "1 month": "10 Y",
}

_IB_WINDOW_DELTA = {
    "1 min": dt.timedelta(days=1), "2 mins": dt.timedelta(days=2),
    "5 mins": dt.timedelta(days=5), "15 mins": dt.timedelta(days=10),
    "30 mins": dt.timedelta(days=20), "1 hour": dt.timedelta(days=30),
    "1 day": dt.timedelta(days=366), "1 week": dt.timedelta(days=5 * 366),
    "1 month": dt.timedelta(days=10 * 366),
}

_IB_BAR_DELTA = {
    "1 min": dt.timedelta(minutes=1), "2 mins": dt.timedelta(minutes=2),
    "5 mins": dt.timedelta(minutes=5), "15 mins": dt.timedelta(minutes=15),
    "30 mins": dt.timedelta(minutes=30), "1 hour": dt.timedelta(hours=1),
    "1 day": dt.timedelta(days=1), "1 week": dt.timedelta(days=7),
    "1 month": dt.timedelta(days=28),
}

# Yahoo-style suffix -> (IB exchange, currency). Bare symbols (no suffix)
# are treated as US stocks/ETFs on IB's SMART router. Extend this for other
# exchanges as needed -- it only covers what this tool's own ticker lists use
# plus a few common ones.
_IB_SUFFIX_MAP = {
    ".TA": ("TASE", "ILS"), ".KS": ("KSE", "KRW"), ".KQ": ("KOSDAQ", "KRW"),
    ".L": ("LSE", "GBP"), ".DE": ("IBIS", "EUR"), ".PA": ("SBF", "EUR"),
    ".HK": ("SEHK", "HKD"), ".TO": ("TSE", "CAD"), ".AX": ("ASX", "AUD"),
}

_IB_PAGE_CAP = 200
_RETRYABLE_IB_CODES = {1100, 1101, 1102, 2103, 2105, 2107, 2108}
_PERMANENT_IB_CODES = {200, 321, 322, 354, 366, 420}

@dataclass(frozen=True)
class IBFailure:
    category: str
    message: str
    retryable: bool

def classify_ib_error(exc: BaseException) -> IBFailure:
    code = getattr(exc, "code", None)
    message = str(getattr(exc, "message", "") or exc)
    sanitized = re.sub(r"\s+", " ", message).strip()[:300]
    sanitized = re.sub(
        r"(?i)\b(account|acct)\s*[:=]?\s*[A-Z0-9-]+",
        r"\1=<redacted>",
        sanitized,
    )
    sanitized = re.sub(
        r"(?i)\b(?:DU|DF|U|F)[A-Z0-9_-]{4,}\b", "<account-id-redacted>", sanitized
    )
    lowered = sanitized.lower()
    if isinstance(exc, (TimeoutError, ConnectionError, OSError)):
        category = "RETRYABLE_TIMEOUT" if isinstance(exc, TimeoutError) else "RETRYABLE_CONNECTION"
        return IBFailure(category, sanitized or type(exc).__name__, True)
    if code == 354 or "not subscribed" in lowered or "permission" in lowered:
        return IBFailure("PERMANENT_NO_ENTITLEMENT", sanitized, False)
    if code == 162 and ("pacing" in lowered or "farm" in lowered or "temporar" in lowered):
        return IBFailure("RETRYABLE_PACING", sanitized, True)
    if code in _RETRYABLE_IB_CODES:
        return IBFailure("RETRYABLE_CONNECTION", sanitized, True)
    if code == 162:
        return IBFailure("PERMANENT_NO_HISTORICAL_DATA", sanitized, False)
    if code in _PERMANENT_IB_CODES:
        return IBFailure("PERMANENT_REQUEST", sanitized, False)
    return IBFailure("PERMANENT_REQUEST", sanitized or type(exc).__name__, False)

def _call_ib_with_retries(
    call, retries: int, sleep_sec: float, *, sleep_fn=None,
    retry_connection: bool = False,
):
    sleep_fn = sleep_fn or time.sleep
    for attempt in range(retries + 1):
        try:
            return call()
        except Exception as exc:
            failure = classify_ib_error(exc)
            connection_failure = failure.category == "RETRYABLE_CONNECTION"
            if (
                not failure.retryable
                or (connection_failure and not retry_connection)
                or attempt >= retries
            ):
                raise RuntimeError(f"{failure.category}: {failure.message}") from exc
            sleep_fn(sleep_sec * (2 ** attempt))
    raise AssertionError("bounded retry loop exhausted unexpectedly")

def load_ib_contract_map(path: Optional[str]) -> Dict[str, Dict[str, Any]]:
    selected = Path(path) if path else Path(__file__).with_name("ib_contracts.json")
    if not selected.exists():
        return {}
    with selected.open("r", encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise ValueError(f"IBKR contract map must be a JSON object: {selected}")
    return {str(key).upper(): value for key, value in raw.items()}

def _ib_candidate_for(ticker: str, contract_map: Dict[str, Dict[str, Any]]) -> "Contract":
    mapped = contract_map.get(ticker.upper())
    if mapped is not None:
        required = {"symbol", "secType", "exchange", "currency"}
        if not isinstance(mapped, dict) or not required.issubset(mapped):
            raise ValueError(f"incomplete explicit IBKR contract mapping for {ticker}")
        return Contract(**mapped)
    if ticker.startswith("^"):
        raise ValueError(
            f"unknown index alias {ticker!r}; add an explicit entry to the IBKR contract map"
        )
    if ticker.endswith("-USD"):
        return Contract(symbol=ticker[:-4], secType="CRYPTO", exchange="PAXOS", currency="USD")
    for suffix, (exchange, currency) in _IB_SUFFIX_MAP.items():
        if ticker.endswith(suffix):
            return Contract(
                symbol=ticker[: -len(suffix)], secType="STK",
                exchange=exchange, currency=currency,
            )
    return Contract(symbol=ticker, secType="STK", exchange="SMART", currency="USD")

def resolve_ib_contract(
    ib: "IB", ticker: str, contract_map: Dict[str, Dict[str, Any]],
    retries: int = 0, sleep_sec: float = 0.5,
) -> Tuple["Contract", Any]:
    """Resolve exactly one contract and enforce the requested identity."""
    candidate = _ib_candidate_for(ticker, contract_map)
    details = _call_ib_with_retries(
        lambda: ib.reqContractDetails(candidate), retries, sleep_sec,
        sleep_fn=getattr(ib, "sleep", None),
    )
    if len(details) != 1:
        raise ValueError(
            f"IBKR_AMBIGUOUS_CONTRACT: {ticker!r} resolved to {len(details)} contracts"
        )
    detail = details[0]
    resolved = detail.contract
    if not getattr(resolved, "conId", 0):
        raise ValueError(f"IBKR_INVALID_CONTRACT: {ticker!r} resolved with conId=0")
    if resolved.secType != candidate.secType:
        raise ValueError(
            f"IBKR_IDENTITY_MISMATCH: expected secType={candidate.secType}, "
            f"resolved secType={resolved.secType}"
        )
    if candidate.currency and resolved.currency != candidate.currency:
        raise ValueError(
            f"IBKR_IDENTITY_MISMATCH: expected currency={candidate.currency}, "
            f"resolved currency={resolved.currency}"
        )
    if candidate.exchange and candidate.exchange != "SMART":
        exchanges = {resolved.exchange, getattr(resolved, "primaryExchange", "")}
        if candidate.exchange not in exchanges:
            raise ValueError(
                f"IBKR_IDENTITY_MISMATCH: expected exchange={candidate.exchange}, "
                f"resolved exchange={resolved.exchange}, "
                f"primaryExchange={getattr(resolved, 'primaryExchange', '')}"
            )
    expected_primary = getattr(candidate, "primaryExchange", "")
    if expected_primary and getattr(resolved, "primaryExchange", "") != expected_primary:
        raise ValueError(
            f"IBKR_IDENTITY_MISMATCH: expected primaryExchange={expected_primary}, "
            f"resolved primaryExchange={getattr(resolved, 'primaryExchange', '')}"
        )
    return resolved, detail

def _format_ib_validation_identity(resolved: "Contract", detail: Any) -> str:
    return (
        f"symbol={resolved.symbol} localSymbol={getattr(resolved, 'localSymbol', '')} "
        f"conId={resolved.conId} secType={resolved.secType} "
        f"exchange={resolved.exchange} "
        f"primaryExchange={getattr(resolved, 'primaryExchange', '')} "
        f"currency={resolved.currency} longName={getattr(detail, 'longName', '')}"
    )

def _is_calendar_interval(interval: str) -> bool:
    return interval in {"1d", "1wk", "1mo"}

def _time_semantics(source: str, interval: str) -> str:
    calendar_intervals = (
        {"1d", "1wk", "1mo"} if source == "ibkr"
        else {"1d", "5d", "1wk", "1mo", "3mo"}
    )
    return "calendar_date" if interval in calendar_intervals else "utc_instant"

def _ib_timezone(detail: Any) -> ZoneInfo:
    timezone_id = getattr(detail, "timeZoneId", "") or "UTC"
    try:
        return ZoneInfo(timezone_id)
    except Exception as exc:
        raise ValueError(f"IBKR_INVALID_TIMEZONE: unsupported timeZoneId={timezone_id!r}") from exc

def _utc_timestamp(value: Any) -> pd.Timestamp:
    if isinstance(value, (int, float)):
        return pd.to_datetime(value, unit="s", utc=True)
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        raise ValueError("IBKR_INVALID_TIME: intraday bar timestamp is timezone-naive")
    return timestamp.tz_convert("UTC")

def _calendar_value(value: Any, timezone: ZoneInfo) -> dt.date:
    if isinstance(value, dt.date) and not isinstance(value, dt.datetime):
        return value
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_convert(timezone)
    return timestamp.date()

def _ib_metadata(
    ticker: str, interval: str, adjust: bool, start: str, end: str,
    resolved: "Contract", detail: Any, what_to_show: str, use_rth: bool,
    provider_head: Any = None,
) -> Dict[str, Any]:
    return {
        "schema_version": _METADATA_SCHEMA_VERSION,
        "source": "ibkr",
        "requested_ticker": ticker,
        "interval": interval,
        "adjusted": adjust,
        "requested_start": start,
        "requested_end": end,
        "fetched_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "data_sha256": "",
        "time_semantics": _time_semantics("ibkr", interval),
        "coverage": {
            "status": "complete",
            "provider_head": _coverage_value(provider_head),
            "actual_start": None,
            "actual_end": None,
        },
        "ibkr": {
            "conId": resolved.conId,
            "symbol": resolved.symbol,
            "localSymbol": getattr(resolved, "localSymbol", ""),
            "secType": resolved.secType,
            "exchange": resolved.exchange,
            "primaryExchange": getattr(resolved, "primaryExchange", ""),
            "currency": resolved.currency,
            "timeZoneId": getattr(detail, "timeZoneId", "") or "",
            "barSizeSetting": _IB_BAR_SIZE[interval],
            "whatToShow": what_to_show,
            "useRTH": use_rth,
        },
    }

def _coverage_value(value: Any, calendar: bool = False) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, dt.date) and not isinstance(value, dt.datetime):
        return value.isoformat()
    timestamp = pd.Timestamp(value)
    return timestamp.date().isoformat() if calendar else timestamp.isoformat()

def _update_ib_coverage(
    metadata: Dict[str, Any], frame: pd.DataFrame, requested_start: str,
    existing_metadata: Optional[Dict[str, Any]] = None,
) -> None:
    coverage = dict(metadata.get("coverage") or {})
    existing_coverage = (existing_metadata or {}).get("coverage") or {}
    provider_heads = [
        value for value in (
            coverage.get("provider_head"), existing_coverage.get("provider_head")
        ) if value
    ]
    provider_head = min(provider_heads) if provider_heads else None
    if provider_head:
        if metadata.get("time_semantics") == "calendar_date":
            limited = dt.date.fromisoformat(str(provider_head)[:10]) > dt.date.fromisoformat(requested_start)
        else:
            timezone_id = metadata["ibkr"].get("timeZoneId") or "UTC"
            requested = pd.Timestamp(
                dt.datetime.combine(dt.date.fromisoformat(requested_start), dt.time.min, ZoneInfo(timezone_id))
            ).tz_convert("UTC")
            limited = pd.Timestamp(provider_head) > requested
    else:
        limited = False
    coverage.update({
        "status": "provider_limited" if limited else "complete",
        "provider_head": provider_head,
        "actual_start": _coverage_value(
            frame.index.min(), metadata.get("time_semantics") == "calendar_date"
        ),
        "actual_end": _coverage_value(
            frame.index.max(), metadata.get("time_semantics") == "calendar_date"
        ),
    })
    metadata["coverage"] = coverage

def _enforce_full_history(metadata: Dict[str, Any], required: bool) -> None:
    if required and metadata.get("coverage", {}).get("status") == "provider_limited":
        raise ValueError("PROVIDER_LIMITED: --require-full-history prevents dataset write")

def _append_dataset_requested_start(
    existing_metadata: Dict[str, Any], cli_requested_start: str,
) -> str:
    existing_start = existing_metadata.get("requested_start")
    if not isinstance(existing_start, str):
        raise ValueError("existing provenance has no valid requested_start")
    existing_date = dt.date.fromisoformat(existing_start)
    cli_date = dt.date.fromisoformat(cli_requested_start)
    if cli_date < existing_date:
        raise ValueError(
            "append --start is earlier than the existing dataset requested_start "
            f"({cli_requested_start} < {existing_start}); use --overwrite to create "
            "a new historical coverage contract"
        )
    return existing_start

def _finalize_dataset_metadata(
    metadata: Dict[str, Any], frame: pd.DataFrame, dataset_requested_start: str,
    requested_end: str, existing_metadata: Optional[Dict[str, Any]] = None,
) -> None:
    metadata["requested_start"] = dataset_requested_start
    metadata["requested_end"] = requested_end
    if metadata.get("source") == "ibkr":
        _update_ib_coverage(
            metadata, frame, dataset_requested_start, existing_metadata
        )

def _calendar_request_end(cursor: dt.date, timezone: ZoneInfo) -> dt.datetime:
    return dt.datetime.combine(cursor, dt.time(23, 59, 59), timezone)

def fetch_one_ibkr(
    ib: "IB", ticker: str, start: str, end: str, interval: str,
    adjust: bool, limiter: "RateLimiter", contract_map: Dict[str, Dict[str, Any]],
    use_rth: bool, retries: int, sleep_sec: float,
    expected_con_id: Optional[int] = None,
) -> Tuple[str, Optional[pd.DataFrame], Optional[str], Optional[Dict[str, Any]]]:
    bar_size = _IB_BAR_SIZE.get(interval)
    if bar_size is None:
        return ticker, None, f"PERMANENT_UNSUPPORTED_INTERVAL: interval={interval}", None
    try:
        limiter.wait()
        contract, details = resolve_ib_contract(
            ib, ticker, contract_map, retries=retries, sleep_sec=sleep_sec
        )
    except Exception as e:
        return ticker, None, str(e), None
    if expected_con_id is not None and contract.conId != expected_con_id:
        return ticker, None, (
            f"PROVENANCE_FAILURE: IBKR conId mismatch: "
            f"existing={expected_con_id}, resolved={contract.conId}"
        ), None

    calendar = _is_calendar_interval(interval)
    timezone = _ib_timezone(details)
    start_date = dt.date.fromisoformat(start)
    end_date = dt.date.fromisoformat(end)
    start_utc = pd.Timestamp(dt.datetime.combine(start_date, dt.time.min, timezone)).tz_convert("UTC")
    end_exclusive_utc = pd.Timestamp(
        dt.datetime.combine(end_date + dt.timedelta(days=1), dt.time.min, timezone)
    ).tz_convert("UTC")
    chunk = _IB_CHUNK_DURATION.get(bar_size, "1 Y")
    what_to_show = "ADJUSTED_LAST" if adjust else "TRADES"
    format_date = 1 if calendar else 2

    try:
        limiter.wait()
        head_value = _call_ib_with_retries(
            lambda: ib.reqHeadTimeStamp(
                contract, whatToShow=what_to_show, useRTH=use_rth, timeout=30
            ),
            retries, sleep_sec, sleep_fn=getattr(ib, "sleep", None),
        )
        known_head = (
            _calendar_value(head_value, timezone)
            if calendar else _utc_timestamp(head_value)
        )
    except Exception as exc:
        return ticker, None, str(exc), None

    target_start = max(start_date, known_head) if calendar else max(start_utc, known_head)

    all_bars, seen_dates = [], set()
    cursor = end_date if calendar else end_exclusive_utc.to_pydatetime()
    reached_start = False
    for _ in range(_IB_PAGE_CAP):
        limiter.wait()
        try:
            bars = _call_ib_with_retries(
                lambda: ib.reqHistoricalData(
                    contract, endDateTime=(
                        _calendar_request_end(cursor, timezone) if calendar else cursor
                    ), durationStr=chunk,
                    barSizeSetting=bar_size, whatToShow=what_to_show,
                    useRTH=use_rth, formatDate=format_date, timeout=60,
                ), retries, sleep_sec, sleep_fn=getattr(ib, "sleep", None),
            )
        except Exception as e:
            return ticker, None, str(e), None
        new_bars = [b for b in bars if b.date not in seen_dates]
        if not new_bars:
            cursor_value = cursor if calendar else _utc_timestamp(cursor)
            if cursor_value <= known_head:
                reached_start = True
                break
            cursor = cursor - _IB_WINDOW_DELTA[bar_size]
            continue
        seen_dates.update(b.date for b in new_bars)
        all_bars.extend(new_bars)
        if calendar:
            earliest = min(_calendar_value(b.date, timezone) for b in new_bars)
            reached_start = earliest <= target_start
            cursor = earliest - _IB_BAR_DELTA[bar_size]
        else:
            try:
                earliest_ts = min(_utc_timestamp(b.date) for b in new_bars)
            except ValueError as exc:
                return ticker, None, str(exc), None
            reached_start = earliest_ts <= target_start
            cursor = (earliest_ts - _IB_BAR_DELTA[bar_size]).to_pydatetime()
        if reached_start:
            break

    if not reached_start:
        return ticker, None, (
            f"PERMANENT_PAGE_CAP: requested start {start} was not reached "
            f"within {_IB_PAGE_CAP} historical pages"
        ), None

    if not all_bars:
        return ticker, None, "PERMANENT_NO_HISTORICAL_DATA: empty confirmed response", None

    raw_dates = [b.date for b in all_bars]
    if calendar:
        index = pd.DatetimeIndex([pd.Timestamp(value).date() for value in raw_dates])
    else:
        try:
            index = pd.DatetimeIndex([_utc_timestamp(value) for value in raw_dates])
        except ValueError as exc:
            return ticker, None, str(exc), None
    df = pd.DataFrame({
        "Open": [b.open for b in all_bars],
        "High": [b.high for b in all_bars],
        "Low": [b.low for b in all_bars],
        "Close": [b.close for b in all_bars],
        "Volume": [b.volume for b in all_bars],
    }, index=index)
    df.index.name = "Date"
    df = df[~df.index.duplicated(keep="last")].sort_index()
    if calendar:
        df = df[(df.index.date >= start_date) & (df.index.date <= end_date)]
    else:
        df = df[(df.index >= start_utc) & (df.index < end_exclusive_utc)]
    if df.empty:
        return ticker, None, "PERMANENT_NO_HISTORICAL_DATA: no bars in requested range", None
    metadata = _ib_metadata(
        ticker, interval, adjust, start, end, contract, details, what_to_show, use_rth,
        known_head,
    )
    _update_ib_coverage(metadata, df, start)
    return ticker, df, None, metadata

class _MarketDataWrapper(Wrapper):
    """Allow market-data responses and discard unsolicited sensitive state."""

    def managedAccounts(self, _accounts_list: str) -> None:
        return None

    # TWS can send these without a collector request (for example when this
    # client is accidentally configured as Master Client ID). Never retain or
    # emit their payloads.
    def updateAccountValue(self, *args) -> None: return None
    def updateAccountTime(self, *args) -> None: return None
    def accountSummary(self, *args) -> None: return None
    def accountSummaryEnd(self, *args) -> None: return None
    def updatePortfolio(self, *args) -> None: return None
    def position(self, *args) -> None: return None
    def positionEnd(self, *args) -> None: return None
    def positionMulti(self, *args) -> None: return None
    def positionMultiEnd(self, *args) -> None: return None
    def accountUpdateMulti(self, *args) -> None: return None
    def accountUpdateMultiEnd(self, *args) -> None: return None
    def accountDownloadEnd(self, *args) -> None: return None
    def openOrder(self, *args) -> None: return None
    def openOrderEnd(self, *args) -> None: return None
    def orderStatus(self, *args) -> None: return None
    def orderBound(self, *args) -> None: return None
    def completedOrder(self, *args) -> None: return None
    def completedOrdersEnd(self, *args) -> None: return None
    def execDetails(self, *args) -> None: return None
    def execDetailsEnd(self, *args) -> None: return None
    def commissionReport(self, *args) -> None: return None
    def pnl(self, *args) -> None: return None
    def pnlSingle(self, *args) -> None: return None

    def contractDetails(self, reqId: int, contractDetails: Any) -> None:
        results = self._results.get(reqId)
        if results is not None:
            results.append(contractDetails)

    def contractDetailsEnd(self, reqId: int) -> None:
        if reqId in self._futures:
            self._endReq(reqId)

    def error(
        self, reqId: int, errorCode: int, errorString: str,
        _advancedOrderRejectJson: str = "",
    ) -> None:
        # Do not call Wrapper.error: it logs the raw broker payload. Only active
        # allowlisted requests receive a RequestError, and system connectivity
        # state is handled without emitting the raw string.
        if reqId in self._futures:
            self._results.pop(reqId, None)
            self._endReq(reqId, RequestError(reqId, errorCode, errorString), False)
        self.ib._onError(reqId, errorCode, errorString, self._reqId2Contract.get(reqId))


class _MarketDataIB(IB):
    """IB request machinery with a market-data-only connection lifecycle."""

    def __init__(self):
        super().__init__()
        self.wrapper = _MarketDataWrapper(self)
        self.client = Client(self.wrapper)
        self.client.apiEnd += self.disconnectedEvent

    async def connectAsync(
        self, host: str, port: int, clientId: int, timeout: float = 10
    ) -> None:
        if int(clientId) == 0:
            raise ValueError("IBKR clientId must be non-zero")
        self.wrapper.clientId = int(clientId)
        try:
            await self.client.connectAsync(host, port, int(clientId), timeout)
            if not self.client.isReady():
                raise ConnectionError("Socket connection broken while connecting")
            self.client._accounts.clear()
            self._assert_market_data_only_state()
            self.connectedEvent.emit()
        except BaseException:
            self.disconnect()
            raise

    def _assert_market_data_only_state(self) -> None:
        sensitive = {
            "wrapper.accounts": self.wrapper.accounts,
            "client._accounts": self.client._accounts,
            "wrapper.accountValues": self.wrapper.accountValues,
            "wrapper.acctSummary": self.wrapper.acctSummary,
            "wrapper.portfolio": self.wrapper.portfolio,
            "wrapper.positions": self.wrapper.positions,
            "wrapper.trades": self.wrapper.trades,
            "wrapper.fills": self.wrapper.fills,
            "wrapper.reqId2PnL": self.wrapper.reqId2PnL,
            "wrapper.reqId2PnlSingle": self.wrapper.reqId2PnlSingle,
        }
        retained = [name for name, value in sensitive.items() if value]
        if retained:
            raise RuntimeError(
                "IBKR_MARKET_DATA_BOUNDARY: sensitive dependency state is not empty: "
                + ", ".join(retained)
            )

    def _cleanup_request(self, req_id: int) -> None:
        self.wrapper._futures.pop(req_id, None)
        self.wrapper._results.pop(req_id, None)
        self.wrapper._reqId2Contract.pop(req_id, None)

    def _fail_active_requests(self, message: str) -> None:
        for req_id in list(self.wrapper._futures):
            self.wrapper._results.pop(req_id, None)
            self.wrapper._endReq(req_id, ConnectionError(message), False)

    def _onError(self, _req_id, error_code, _error_string, _contract) -> None:
        if error_code == 1100:
            self._fail_active_requests("IB/TWS upstream connectivity lost (1100)")
        elif error_code == 1101:
            self._fail_active_requests("IB/TWS connectivity restored; requests lost (1101)")
        # 1102 means data maintained. Deliberately do not inherit IB._onError's
        # account-summary resubscription and do not fail active market requests.

    async def reqContractDetailsStrictAsync(
        self, contract: "Contract", *, timeout: float,
    ) -> Any:
        req_id = self.client.getReqId()
        future = self.wrapper.startReq(req_id, contract)
        self.client.reqContractDetails(req_id, contract)
        try:
            return await asyncio.wait_for(future, timeout)
        finally:
            self._cleanup_request(req_id)

    async def reqHistoricalDataStrictAsync(
        self, contract: "Contract", *, endDateTime: Any, durationStr: str,
        barSizeSetting: str, whatToShow: str, useRTH: bool, formatDate: int,
        timeout: float,
    ) -> Any:
        req_id = self.client.getReqId()
        bars = BarDataList()
        bars.reqId = req_id
        bars.contract = contract
        bars.endDateTime = endDateTime
        bars.durationStr = durationStr
        bars.barSizeSetting = barSizeSetting
        bars.whatToShow = whatToShow
        bars.useRTH = useRTH
        bars.formatDate = formatDate
        bars.keepUpToDate = False
        bars.chartOptions = []
        future = self.wrapper.startReq(req_id, contract, container=bars)
        self.client.reqHistoricalData(
            req_id, contract, ib_util.formatIBDatetime(endDateTime), durationStr,
            barSizeSetting, whatToShow, useRTH, formatDate, False, [],
        )
        try:
            await asyncio.wait_for(future, timeout)
        except asyncio.TimeoutError:
            self.client.cancelHistoricalData(req_id)
            raise
        finally:
            self._cleanup_request(req_id)
        return bars

    async def reqHeadTimeStampStrictAsync(
        self, contract: "Contract", *, whatToShow: str, useRTH: bool,
        timeout: float,
    ) -> Any:
        req_id = self.client.getReqId()
        future = self.wrapper.startReq(req_id, contract)
        self.client.reqHeadTimeStamp(req_id, contract, whatToShow, useRTH, 2)
        try:
            return await asyncio.wait_for(future, timeout)
        finally:
            self.client.cancelHeadTimeStamp(req_id)
            self._cleanup_request(req_id)


class MarketDataIBAdapter:
    """Narrow facade: connection, contract details, head time, history only."""

    readonly = True
    startup_fetch = StartupFetchNONE

    def __init__(self, engine: Optional[_MarketDataIB] = None):
        self._engine = engine or _MarketDataIB()
        self._engine.RaiseRequestErrors = True
        self.host = ""
        self.port = 0
        self.client_id = 0

    def connect(self, host: str, port: int, client_id: int, timeout: float = 10):
        if client_id == 0:
            raise ValueError("IBKR clientId must be non-zero")
        self.host, self.port, self.client_id = host, port, client_id
        self._engine._run(
            self._engine.connectAsync(host, port, clientId=client_id, timeout=timeout)
        )
        self._engine._assert_market_data_only_state()
        return self

    def disconnect(self) -> None:
        self._engine.disconnect()

    def isConnected(self) -> bool:
        return self._engine.isConnected()

    def sleep(self, seconds: float) -> None:
        self._engine.sleep(seconds)

    def reqContractDetails(self, contract: "Contract", timeout: float = 30) -> Any:
        return self._engine._run(
            self._engine.reqContractDetailsStrictAsync(contract, timeout=timeout)
        )

    def reqHeadTimeStamp(
        self, contract: "Contract", whatToShow: str, useRTH: bool, timeout: float = 30
    ) -> Any:
        return self._engine._run(
            self._engine.reqHeadTimeStampStrictAsync(
                contract, whatToShow=whatToShow, useRTH=useRTH, timeout=timeout
            )
        )

    def reqHistoricalData(self, contract: "Contract", **kwargs) -> Any:
        return self._engine._run(
            self._engine.reqHistoricalDataStrictAsync(contract, **kwargs)
        )


def _clamp_ib_async_logging() -> None:
    for logger_name in ("ib_async.client", "ib_async.wrapper", "ib_async.ib"):
        dependency_logger = logging.getLogger(logger_name)
        dependency_logger.disabled = True
        dependency_logger.propagate = False


def ib_connect(host: str, port: int, client_id: int) -> MarketDataIBAdapter:
    _clamp_ib_async_logging()
    return MarketDataIBAdapter().connect(host, port, client_id, timeout=10)

def ib_connect_with_retries(
    host: str, port: int, client_id: int, retries: int, sleep_sec: float
) -> "IB":
    return _call_ib_with_retries(
        lambda: ib_connect(host, port, client_id), retries, sleep_sec,
        retry_connection=True,
    )

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
    p.add_argument("--source", choices=["yfinance", "ibkr"], default="yfinance",
                   help="Market data backend. 'ibkr' pulls from a running TWS/IB Gateway "
                        "instead of Yahoo -- see --ib-* options. Default: yfinance")
    p.add_argument("--ib-host", default="127.0.0.1", help="TWS/IB Gateway host. Default: 127.0.0.1")
    p.add_argument("--ib-port", type=int, default=7496,
                   help="TWS/IB Gateway API port. Default: 7496 (TWS live). "
                        "Common values: 7497 TWS paper, 4001 IB Gateway live, 4002 IB Gateway paper.")
    p.add_argument("--ib-client-id", type=int, default=42,
                   help="IB API client id. Must be unique among concurrent API connections. Default: 42")
    p.add_argument("--ib-contract-map",
                   help="Explicit IBKR contract mapping JSON. Default: repo ib_contracts.json")
    p.add_argument("--ib-use-rth", action=argparse.BooleanOptionalAction, default=True,
                   help="IBKR regular-hours policy. Default: true; use --no-ib-use-rth for extended hours.")
    p.add_argument("--ib-retries", type=int, default=2,
                   help="Bounded retries after an IBKR retryable failure. Default: 2")
    p.add_argument("--require-full-history", action="store_true",
                   help="IBKR: fail without writing when provider history starts after --start.")
    p.add_argument("--ib-rate-limit", type=float, default=2.0,
                   help="Max historical-data requests/sec to IBKR (separate from --rate-limit, "
                        "which only applies to yfinance). Default: 2.0")
    p.add_argument("--adjust", action="store_true",
                   help="yfinance: rescale OHLC so Close equals Adj Close. "
                        "ibkr: request split/dividend-adjusted bars (ADJUSTED_LAST) instead of raw TRADES.")
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
    if args.source == "ibkr" and args.include_today:
        log("[err] --include-today is unsupported for --source ibkr; provide an explicit --end date")
        raise SystemExit(2)
    if args.source == "yfinance" and args.include_today:
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

    ib = None
    if args.source == "ibkr":
        if IB is None:
            log("[err] --source ibkr requires the 'ib_async' package: pip install ib_async")
            return
        try:
            if args.ib_client_id == 0:
                raise ValueError("--ib-client-id must be non-zero")
            ib = ib_connect_with_retries(
                args.ib_host, args.ib_port, args.ib_client_id,
                args.ib_retries, args.sleep,
            )
        except Exception as e:
            log(f"[err] Could not connect to TWS/IB Gateway at {args.ib_host}:{args.ib_port} -- {e}")
            log("      Make sure TWS or IB Gateway is running, logged in, and API access is enabled "
                "(File > Global Configuration > API > Settings > Enable ActiveX and Socket Clients).")
            return
        log(f"[info] connected to IBKR at {args.ib_host}:{args.ib_port} (clientId={args.ib_client_id})")
        limiter = RateLimiter(args.ib_rate_limit)  # IB's own pacing is unrelated to Yahoo's

    try:
        _run_with_source(args, tickers, limiter, ib)
    finally:
        if ib is not None:
            ib.disconnect()

def _run_with_source(args: argparse.Namespace, tickers: List[str],
                      limiter: "RateLimiter", ib: Optional["IB"]) -> None:
    contract_map = load_ib_contract_map(args.ib_contract_map) if args.source == "ibkr" else {}
    # validation
    if args.validate:
        log(f"[info] validating {len(tickers)} tickers ...")
        valid, invalid = [], []
        if args.source == "ibkr":
            for t in tickers:
                limiter.wait()
                try:
                    resolved, details = resolve_ib_contract(
                        ib, t, contract_map, args.ib_retries, args.sleep
                    )
                    identity = _format_ib_validation_identity(resolved, details)
                    valid.append((t, identity, resolved.exchange or ""))
                except Exception as e:
                    invalid.append((t, str(e)))
        else:
            # sequential (not threaded) to keep well under the rate limit while
            # still sharing the same limiter/session as the main download pass
            for t in tickers:
                limiter.wait()
                try:
                    tk = yf.Ticker(t, session=_get_session())
                    dfv = tk.history(period="5d", interval="1d", auto_adjust=False,
                                      actions=False, raise_errors=False)
                    if dfv is not None and not dfv.empty:
                        # Surface the resolved company/exchange so a ticker that
                        # coincidentally matches the WRONG instrument (e.g. a
                        # bare TASE mnemonic hitting an unrelated US micro-cap)
                        # is visible here, before it silently pollutes a download.
                        name, exch = "", ""
                        try:
                            info = tk.get_info()
                            name = info.get("shortName") or info.get("longName") or ""
                            exch = info.get("exchange") or info.get("fullExchangeName") or ""
                        except Exception:
                            pass
                        valid.append((t, name, exch))
                    else:
                        invalid.append((t, "empty"))
                except Exception as e:
                    invalid.append((t, str(e)))
        with open("valid_tickers.txt", "w", encoding="utf-8") as f:
            for t, name, exch in sorted(valid):
                f.write(f"{t}\t{name}\t{exch}\n" if (name or exch) else f"{t}\n")
        with open("invalid_tickers.txt", "w", encoding="utf-8") as f:
            for t, msg in sorted(invalid):
                f.write(f"{t}\t{msg}\n")
        log(f"[info] wrote valid_tickers.txt ({len(valid)}) and invalid_tickers.txt ({len(invalid)}) "
            f"-- check the name/exchange columns in valid_tickers.txt for symbols that "
            f"resolved to an unexpected company (wrong-exchange collision).")
        if args.exit_after_validate:
            return
        tickers = sorted(t for t, _, _ in valid)
        if not tickers:
            log("[err] no valid tickers to download after validation.")
            return

    if args.source == "yfinance":
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
    provenance: Dict[str, Dict[str, Any]] = {}
    dataset_requested_starts: Dict[str, str] = {}
    what_to_show = "ADJUSTED_LAST" if args.adjust else "TRADES"

    for t in tickers:
        out_path = output_path(args, t)
        exists = os.path.exists(out_path)

        if exists and not args.overwrite and not args.append:
            if args.source == "ibkr" or os.path.exists(_metadata_path(out_path)):
                try:
                    load_and_validate_metadata(
                        out_path, source=args.source, ticker=t, interval=args.interval,
                        adjusted=args.adjust, ib_use_rth=args.ib_use_rth,
                        what_to_show=what_to_show if args.source == "ibkr" else None,
                    )
                except ValueError as exc:
                    raise RuntimeError(f"PROVENANCE_FAILURE: {t}: {exc}") from exc
            skipped.append(t)
            continue

        eff_start = args.start
        dataset_requested_starts[t] = args.start
        df_old = None
        if exists and args.append:
            try:
                provenance[t] = load_and_validate_metadata(
                    out_path, source=args.source, ticker=t, interval=args.interval,
                    adjusted=args.adjust, ib_use_rth=args.ib_use_rth,
                    what_to_show=what_to_show if args.source == "ibkr" else None,
                )
            except ValueError as exc:
                raise RuntimeError(f"PROVENANCE_FAILURE: {t}: {exc}") from exc
            try:
                dataset_requested_starts[t] = _append_dataset_requested_start(
                    provenance[t], args.start
                )
            except (TypeError, ValueError) as exc:
                raise RuntimeError(f"PROVENANCE_FAILURE: {t}: {exc}") from exc
            df_old = load_existing(out_path, args.format)
            if df_old is None:
                raise RuntimeError(
                    f"PROVENANCE_FAILURE: {t}: existing data could not be read; refusing append"
                )
            if not df_old.empty:
                last_date = df_old.index.max()
                if pd.notna(last_date):
                    buffered = (last_date.date() - dt.timedelta(days=args.append_lookback_days)).isoformat()
                    if buffered > args.start:
                        eff_start = buffered

        fetch_plan[t] = (eff_start, df_old)
        tickers_to_fetch.append(t)

    for t in skipped:
        log(f"[skip] {t}: output file already exists -> {output_path(args, t)}")

    log(f"[info] downloading {len(tickers_to_fetch)} tickers via {args.source} @ {args.interval} "
        f"({len(skipped)} skipped, already exist)")

    results = []
    if args.source == "ibkr":
        # IB uses one shared socket connection (clientId) -- pace requests
        # sequentially through it rather than hammering it from a thread
        # pool the way the independent yfinance HTTP sessions can be.
        for t in tickers_to_fetch:
            expected_con_id = (
                provenance[t]["ibkr"]["conId"] if t in provenance else None
            )
            result = fetch_one_ibkr(
                ib, t, fetch_plan[t][0], args.end, args.interval, args.adjust,
                limiter, contract_map, args.ib_use_rth, args.ib_retries, args.sleep,
                expected_con_id,
            )
            if result[2] and result[2].startswith("RETRYABLE_CONNECTION:"):
                raise RuntimeError(
                    f"{t}: {result[2]}; connection closed, rerun after TWS/IB Gateway recovery"
                )
            results.append(result)
    else:
        with cf.ThreadPoolExecutor(max_workers=max(1, args.threads)) as ex:
            futures = [
                ex.submit(fetch_one, t, fetch_plan[t][0], args.end, args.interval,
                          args.adjust, args.retries, args.sleep, limiter)
                for t in tickers_to_fetch
            ]
            for fut in cf.as_completed(futures):
                ticker, frame, error = fut.result()
                results.append((ticker, frame, error, None))

    successes, failures = 0, []
    os.makedirs(args.outdir, exist_ok=True)

    for ticker, df_new, err, fetched_metadata in sorted(results, key=lambda x: x[0]):
        out_path = output_path(args, ticker)

        if df_new is None or (not args.show_empty and df_new.empty):
            msg = err or "Empty dataframe"
            if msg.startswith("PROVENANCE_FAILURE:"):
                raise RuntimeError(f"{ticker}: {msg}")
            log(f"[err]  {ticker}: {msg}")
            failures.append((ticker, msg))
            continue

        metadata = fetched_metadata or _base_metadata(args, ticker)
        if args.append and args.source == "ibkr" and ticker in provenance:
            try:
                validate_ib_append_identity(provenance[ticker], metadata)
            except ValueError as exc:
                raise RuntimeError(f"PROVENANCE_FAILURE: {ticker}: {exc}") from exc

        output_frame = df_new
        write_suffix = ""
        if args.overwrite and os.path.exists(out_path):
            write_suffix = " (overwrote)"
        elif args.append and os.path.exists(out_path):
            df_old = fetch_plan.get(ticker, (None, None))[1]
            output_frame = align_and_merge(df_old, df_new) if df_old is not None else df_new
            added = len(output_frame) - (0 if df_old is None else len(df_old))
            write_suffix = f" (appended {max(0, added)} new rows)"

        _finalize_dataset_metadata(
            metadata, output_frame, dataset_requested_starts[ticker], args.end,
            provenance.get(ticker),
        )
        if args.source == "ibkr":
            coverage = metadata["coverage"]
            if coverage["status"] == "provider_limited":
                message = (
                    f"{ticker}: provider history begins at {coverage['provider_head']}, "
                    f"after dataset requested start {metadata['requested_start']}"
                )
                try:
                    _enforce_full_history(
                        metadata, getattr(args, "require_full_history", False)
                    )
                except ValueError:
                    log(f"[err]  {message}; --require-full-history prevents write")
                    failures.append((ticker, "PROVIDER_LIMITED"))
                    continue
                log(f"[warn] {message}; saving explicitly provider-limited dataset")

        save_frame_with_metadata(output_frame, out_path, args.format, metadata)
        log(f"[ok]  {ticker}: {len(output_frame):>5} rows -> {out_path}{write_suffix}")
        successes += 1

    log(f"\nDone. {successes} succeeded, {len(failures)} failed, {len(skipped)} skipped.")
    if failures:
        log("Failed tickers:")
        for t, m in failures:
            log(f"  - {t}: {m}")

if __name__ == "__main__":
    main()
