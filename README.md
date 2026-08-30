# get_stock_data

A flexible Python tool to download **historical OHLCV stock/ETF/index data**, from [Yahoo Finance](https://finance.yahoo.com/) via [yfinance](https://github.com/ranaroussi/yfinance) (default) or from **Interactive Brokers** via [ib_async](https://github.com/ib-api-reloaded/ib_async) (`--source ibkr`).

- Supports **multiple tickers** from command line or file  
- Safe handling of existing files: **skip**, **overwrite**, or **append/merge**  
- Robust ticker parsing (`AAPL`, `$TSLA`, `^GSPC`, `RY.TO`, `EURUSD=X`, etc.)  
- Includes **today’s bar** with `--include-today` (Yahoo `end` is exclusive)  
- Parallel downloads with retries and a shared **rate limiter** (`--rate-limit`) to avoid Yahoo throttling/blocks  
- **`--source ibkr`**: pull bars from your own TWS/IB Gateway connection instead of Yahoo  
- **Incremental `--append`**: only re-fetches a small trailing window per ticker instead of full history  
- Skip mode doesn't even download tickers whose output already exists  
- Export as **CSV** or **Parquet**  
- Preflight **validation** (`--validate`) to separate valid/invalid tickers, showing each ticker's resolved company name and exchange  
- Optional `--log-file` for timestamped logs (handy for cron)  

---

## Installation

Clone this repository and install dependencies into a virtual environment:

```bash
git clone https://github.com/yourname/get_stock_data.git
cd get_stock_data

python3 -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt
```

`requirements.txt` includes [`curl_cffi`](https://github.com/lexiforest/curl_cffi), a browser-impersonating
HTTP client. It's strongly recommended: Yahoo has gotten aggressive about rate-limiting/blocking
plain `requests`-based clients, and the script uses `curl_cffi` automatically when available,
falling back to yfinance's default session (with a warning) if it's missing.

---

## Usage

### Basic example

```bash
python get_stock_data.py AAPL MSFT NVDA -s 2019-01-01 -I 1d --include-today
```

This downloads daily candles for Apple, Microsoft, and Nvidia from Jan 2019 until **today (inclusive)** and saves them as CSV files under `./history/`.

### From a file

```bash
# tickers.list can contain tickers separated by spaces, commas, or semicolons
python get_stock_data.py -i tickers.list -s 2015-01-01 -e 2020-12-31 -o data/ -f parquet
```

### Skip / Overwrite / Append behavior

- **Default**: skip if output file exists — and doesn't even download it, saving a request.
- **Overwrite**: replace file completely, re-fetching the full `--start`..`--end` range  
  ```bash
  python get_stock_data.py -i tickers.list --overwrite
  ```
- **Append**: merge new data with existing file (dedup by date index). Instead of
  re-downloading full history, it only fetches from `(last saved date - --append-lookback-days)`
  onward per ticker — much lighter on Yahoo for daily cron use. The lookback (default 5 days)
  re-fetches a small trailing buffer so any late corrections/adjustments Yahoo publishes get picked up.  
  ```bash
  python get_stock_data.py -i tickers.list --append
  python get_stock_data.py -i tickers.list --append --append-lookback-days 10
  ```

### Rate limiting

All threads share a token-bucket limiter capping total requests/sec to Yahoo, regardless of
`--threads`:

```bash
python get_stock_data.py -i tickers.list --rate-limit 3   # default: 3 req/s, 0 = unlimited
```
If you hit persistent rate-limit errors, lower `--rate-limit` and/or `--threads` rather than
raising `--retries`.

### Alternative source: Interactive Brokers

> **IBKR mode is historical MARKET DATA ONLY. The collector exposes no
> application order-execution path and runs behind TWS/IB Gateway Read-Only
> API enforcement. It is not approved as production-validated.**

`--source ibkr` pulls bars from a running **TWS** or **IB Gateway** instance over its local API
socket (via `ib_async`) instead of scraping Yahoo. This is your own broker connection, so it isn't
subject to Yahoo's rate-limiting, and it can reach some symbols/exchanges Yahoo has no data for.

**Requirements:**
- TWS or IB Gateway running and **logged in**
- API access enabled: *File → Global Configuration → API → Settings → Enable ActiveX and Socket Clients*
- TWS / IB Gateway **Read-Only API** setting enabled. The client also connects
  through a narrow market-data facade whose lifecycle performs no account,
  portfolio, position, order, execution, or PnL request. This code-side defense
  does not replace the gateway setting.
- Prefer localhost or a tightly controlled trusted IP.
- A dedicated, non-zero client ID that **must not** be configured as the
  TWS/IB Gateway Master Client ID.
- Market data subscriptions for whatever exchanges you're requesting

```bash
python get_stock_data.py -i tickers.list --source ibkr --ib-port 7496
```

- `--ib-host` : TWS/IB Gateway host. Default: `127.0.0.1`
- `--ib-port` : API port. Default: `7496` (TWS live). Other common ports: `7497` TWS paper,
  `4001` IB Gateway live, `4002` IB Gateway paper
- `--ib-client-id` : API client id — must be unique among concurrent API connections. Default: `42`
- `--ib-contract-map` : explicit contract mapping JSON. Defaults to the
  committed `ib_contracts.json`. Unknown `^` index aliases fail instead of
  guessing an exchange.
- `--ib-use-rth` / `--no-ib-use-rth`: `true` (default) requests Regular
  Trading Hours only; `false` includes available extended-hours data.
- `--ib-retries`: bounded retry count after retryable IBKR failures. Default: `2`.
- `--require-full-history`: refuse to write an IBKR dataset when the provider's
  earliest available timestamp is later than the requested start.
- `--ib-rate-limit` : max historical-data requests/sec over the IB connection (separate from
  `--rate-limit`, which only applies to yfinance). Default: `2.0`
- `--adjust` : with `--source ibkr`, requests split/dividend-adjusted bars (`ADJUSTED_LAST`)
  instead of raw `TRADES`, rather than yfinance's post-hoc rescaling

Tickers remain user-facing aliases. IBKR resolution requires exactly one
contract detail, validates the mapped security type/currency/exchange, and then
uses `conId` as canonical identity. SPX, VIX, `^GSPC`, and `^VIX` have explicit
index mappings. Unknown `^` aliases fail; no index silently uses the stock
`SMART` route. IBKR output has `Open/High/Low/Close/Volume` only — no `Adj
Close`/`Dividends`/`Stock Splits` columns (those are yfinance-specific).

Because IB uses a single persistent API connection rather than independent HTTP requests, `--source
ibkr` downloads sequentially through that one connection (not via `--threads`) and pages long date
ranges backward in chunks automatically.

IBKR and Yahoo datasets never share a path and are never automatically mixed or
used as fallbacks:

```
history/AAPL.csv                 # yfinance
history/AAPL.csv.meta.json
history/ibkr/AAPL.csv            # IBKR
history/ibkr/AAPL.csv.meta.json
```

Every new or overwritten dataset has a mandatory provenance sidecar containing
the data SHA-256, query semantics, and (for IBKR) resolved contract identity and
coverage status. `coverage.status=provider_limited` records that IBKR's head
timestamp is later than the requested start, while `actual_start` and
`actual_end` describe the complete saved dataset. Such data is saved with a
warning unless `--require-full-history` is set.
Append fails on missing/invalid metadata, changed source/ticker/interval/
adjustment/RTH/what-to-show, changed `conId`, or a data hash mismatch. A legacy
Yahoo file may still be skipped normally, but cannot be appended until one
`--overwrite` establishes trusted metadata. There is no automatic source
fallback or automatic contract migration. Append preserves the existing
dataset-level `requested_start` even when the command uses a later start for
its trailing fetch window, so provider-limited history cannot become falsely
complete. An append start earlier than the trusted dataset baseline fails and
requires `--overwrite` to establish a new historical coverage contract.

Daily/weekly/monthly IBKR bars retain calendar-date semantics. Intraday IBKR
requests use `formatDate=2`, retain timezone-aware UTC instants, and interpret
date boundaries in the resolved instrument timezone. `--include-today` remains
Yahoo-specific and fails clearly with IBKR.

Live validation is still required before trusting this pipeline. A later,
separate read-only TWS validation must compare resolved identity and historical
bars for representative US, TASE, and SPX instruments; no account values belong
in validation output.

TWS necessarily sends managed-account identifiers during the initial API
protocol handshake. The collector cannot prevent those bytes from arriving,
but its client state and wrapper immediately clear/discard them after handshake
readiness: identifiers are not cached beyond that readiness boundary,
used by application logic, printed, logged, persisted, or included in
validation/provenance output. Unexpected account, portfolio, position, order,
execution, commission, and PnL callbacks are dropped without caching or event
emission. Before connecting, the collector disables output and propagation for
`ib_async.client`, `ib_async.wrapper`, and `ib_async.ib`; errors are surfaced
only through the collector's sanitized categories. Connectivity-restored code
1102 does not trigger account-summary resubscription.

Historical requests use collector-owned futures and timeouts. A timeout
cancels the exact IBKR request and is classified `RETRYABLE_TIMEOUT`; it is not
treated as an empty/no-history response. Connection loss fails closed as
`RETRYABLE_CONNECTION` for operator action rather than retrying against a dead
object. Pacing/timeout backoff while connected uses the library event-loop-safe
sleep mechanism. Head timestamps provide a known earliest-history boundary so
empty weekend/holiday pages move backward instead of terminating pagination.

### Validation

Check which tickers are valid before download:

```bash
python get_stock_data.py -i tickers.list --validate --exit-after-validate
```

This creates two files:
- `valid_tickers.txt`
- `invalid_tickers.txt`

### Other options

- `--print-tickers` : print parsed tickers and exit  
- `--source {yfinance,ibkr}` : market data backend, see [Alternative source: Interactive Brokers](#alternative-source-interactive-brokers). Default: `yfinance`  
- `--adjust` : rescale OHLC so Close = Adjusted Close (yfinance) / request adjusted bars (ibkr)  
- `--threads 8` : set concurrency  
- `--rate-limit 3` : max requests/sec to Yahoo, shared across all threads (0 = unlimited)  
- `--format parquet` : save in Parquet instead of CSV  
- `--include-today` : ensure today’s daily bar is included  
- `--show-empty` : write empty files if no rows were returned  
- `--log-file PATH` : also write timestamped log lines to a file (appends)  
- `--append-lookback-days N` : with `--append`, how far back before the last saved date to re-fetch (default 5)  

---

## Output

Each ticker is saved as a separate file:

```
history/
  AAPL.csv
  AAPL.csv.meta.json
  MSFT.csv
  MSFT.csv.meta.json
  NVDA.csv
  NVDA.csv.meta.json
  ibkr/
    AAPL.csv
    AAPL.csv.meta.json
```

Columns (when available):
- Open  
- High  
- Low  
- Close  
- Adj Close  
- Volume  
- Dividends  
- Stock Splits  

---

## Example: append new data daily

You can run the script in a cron job with `--append` to maintain up-to-date files:

```bash
python get_stock_data.py -i tickers.list -s 2000-01-01 -I 1d --include-today --append --log-file cron.log
```

This will:
- Read existing files from `history/`
- Fetch only new rows since the last saved date per ticker (not the full history)
- Merge and deduplicate by date  

---

## Notes

- Yahoo may lag a few minutes after market close before publishing today’s bar.  
- Use `--include-today` only after the market is closed; otherwise you may capture an incomplete candle (script will warn).  
- Tickers that don’t exist on Yahoo (e.g., delisted or wrong symbol) will be logged as failed.  
- Yahoo `1d`, `5d`, `1wk`, `1mo`, and `3mo` provenance uses
  `calendar_date`: the index is a source calendar/period label, not a UTC
  midnight instant.

---

## License

MIT License. See [LICENSE](LICENSE).
