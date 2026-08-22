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

`--source ibkr` pulls bars from a running **TWS** or **IB Gateway** instance over its local API
socket (via `ib_async`) instead of scraping Yahoo. This is your own broker connection, so it isn't
subject to Yahoo's rate-limiting, and it can reach some symbols/exchanges Yahoo has no data for.

**Requirements:**
- TWS or IB Gateway running and **logged in**
- API access enabled: *File → Global Configuration → API → Settings → Enable ActiveX and Socket Clients*
- Market data subscriptions for whatever exchanges you're requesting

```bash
python get_stock_data.py -i tickers.list --source ibkr --ib-port 7496
```

- `--ib-host` : TWS/IB Gateway host. Default: `127.0.0.1`
- `--ib-port` : API port. Default: `7496` (TWS live). Other common ports: `7497` TWS paper,
  `4001` IB Gateway live, `4002` IB Gateway paper
- `--ib-client-id` : API client id — must be unique among concurrent API connections. Default: `42`
- `--ib-rate-limit` : max historical-data requests/sec over the IB connection (separate from
  `--rate-limit`, which only applies to yfinance). Default: `2.0`
- `--adjust` : with `--source ibkr`, requests split/dividend-adjusted bars (`ADJUSTED_LAST`)
  instead of raw `TRADES`, rather than yfinance's post-hoc rescaling

Tickers use this tool's normal Yahoo-style format and get translated to IB contracts on a
best-effort basis: `.TA` → TASE, `.KS`/`.KQ` → Korea, `.L`/`.DE`/`.PA`/`.HK`/`.TO`/`.AX` → their
respective exchanges, `BTC-USD`-style pairs → IB crypto contracts, a bare symbol → a US stock/ETF
on IB's SMART router. **Indices are the main exception** — Yahoo's `^GSPC`/`^VIX`-style symbols
don't match IB's own index symbols (`SPX`, `VIX`), so those may need to be entered directly in IB's
symbol form. IBKR output has `Open/High/Low/Close/Volume` only — no `Adj Close`/`Dividends`/`Stock
Splits` columns (those are yfinance-specific).

Because IB uses a single persistent API connection rather than independent HTTP requests, `--source
ibkr` downloads sequentially through that one connection (not via `--threads`) and pages long date
ranges backward in chunks automatically.

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
  MSFT.csv
  NVDA.csv
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

---

## License

MIT License. See [LICENSE](LICENSE).
