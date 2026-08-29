import argparse
import datetime as dt
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock
from zoneinfo import ZoneInfo

import pandas as pd

import get_stock_data as app


class FakeIB:
    def __init__(self, details=None, bars=None, historical_effects=None):
        self.details = details or []
        self.bars = bars or []
        self.historical_effects = list(historical_effects or [])
        self.candidates = []
        self.historical_calls = []

    def reqContractDetails(self, candidate):
        self.candidates.append(candidate)
        return self.details

    def reqHistoricalData(self, contract, **kwargs):
        self.historical_calls.append((contract, kwargs))
        if self.historical_effects:
            effect = self.historical_effects.pop(0)
            if isinstance(effect, BaseException):
                raise effect
            return effect
        return self.bars


def resolved_detail(
    *, con_id=101, symbol="AAPL", sec_type="STK", exchange="SMART",
    primary="NASDAQ", currency="USD", timezone="US/Eastern", long_name="Apple",
):
    contract = app.Contract(
        conId=con_id,
        symbol=symbol,
        localSymbol=symbol,
        secType=sec_type,
        exchange=exchange,
        primaryExchange=primary,
        currency=currency,
    )
    return SimpleNamespace(contract=contract, timeZoneId=timezone, longName=long_name)


def bar(date):
    return SimpleNamespace(date=date, open=1.0, high=2.0, low=0.5, close=1.5, volume=10)


class PathAndProvenanceTests(unittest.TestCase):
    def args(self, source="yfinance", outdir="history"):
        return argparse.Namespace(
            source=source, outdir=outdir, format="csv", interval="1d",
            adjust=False, start="2025-01-01", end="2025-01-02",
        )

    def test_existing_yfinance_path_regression(self):
        self.assertEqual(app.output_path(self.args(), "AAPL"), "history/AAPL.csv")

    def test_ibkr_path_is_isolated(self):
        self.assertEqual(
            app.output_path(self.args("ibkr"), "AAPL"), "history/ibkr/AAPL.csv"
        )

    def _dataset(self, root, source="yfinance", **changes):
        args = self.args(source, root)
        path = app.output_path(args, "AAPL")
        metadata = app._base_metadata(args, "AAPL")
        if source == "ibkr":
            metadata["ibkr"] = {
                "conId": 101, "symbol": "AAPL", "localSymbol": "AAPL",
                "secType": "STK", "exchange": "SMART", "primaryExchange": "NASDAQ",
                "currency": "USD", "timeZoneId": "US/Eastern",
                "barSizeSetting": "1 day", "whatToShow": "TRADES", "useRTH": True,
            }
        metadata.update(changes)
        frame = pd.DataFrame({"Close": [1.0]}, index=pd.to_datetime(["2025-01-01"]))
        app.save_frame_with_metadata(frame, path, "csv", metadata)
        return path

    def test_source_mismatch_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            path = self._dataset(root)
            with self.assertRaisesRegex(ValueError, "source"):
                app.load_and_validate_metadata(
                    path, source="ibkr", ticker="AAPL", interval="1d", adjusted=False,
                    ib_use_rth=True, what_to_show="TRADES",
                )

    def test_legacy_append_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "AAPL.csv")
            Path(path).write_text("Date,Close\n2025-01-01,1\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "--overwrite"):
                app.load_and_validate_metadata(
                    path, source="yfinance", ticker="AAPL", interval="1d",
                    adjusted=False, ib_use_rth=True,
                )

    def test_interval_and_adjustment_mismatch_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            path = self._dataset(root)
            with self.assertRaisesRegex(ValueError, "interval"):
                app.load_and_validate_metadata(
                    path, source="yfinance", ticker="AAPL", interval="1h",
                    adjusted=False, ib_use_rth=True,
                )
            with self.assertRaisesRegex(ValueError, "adjusted"):
                app.load_and_validate_metadata(
                    path, source="yfinance", ticker="AAPL", interval="1d",
                    adjusted=True, ib_use_rth=True,
                )

    def test_rth_and_sha_mismatch_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            path = self._dataset(root, "ibkr")
            with self.assertRaisesRegex(ValueError, "useRTH"):
                app.load_and_validate_metadata(
                    path, source="ibkr", ticker="AAPL", interval="1d",
                    adjusted=False, ib_use_rth=False, what_to_show="TRADES",
                )
            Path(path).write_text("tampered", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "SHA-256"):
                app.load_and_validate_metadata(
                    path, source="ibkr", ticker="AAPL", interval="1d",
                    adjusted=False, ib_use_rth=True, what_to_show="TRADES",
                )

    def test_con_id_mismatch_is_detectable(self):
        with tempfile.TemporaryDirectory() as root:
            path = self._dataset(root, "ibkr")
            metadata = app.load_and_validate_metadata(
                path, source="ibkr", ticker="AAPL", interval="1d", adjusted=False,
                ib_use_rth=True, what_to_show="TRADES",
            )
            changed = json.loads(json.dumps(metadata))
            changed["ibkr"]["conId"] = 202
            with self.assertRaisesRegex(ValueError, "conId"):
                app.validate_ib_append_identity(metadata, changed)


class ContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mapping = app.load_ib_contract_map(None)

    def test_index_mappings(self):
        expected = {
            "SPX": ("SPX", "IND"), "VIX": ("VIX", "IND"),
            "^GSPC": ("SPX", "IND"), "^VIX": ("VIX", "IND"),
        }
        for alias, identity in expected.items():
            candidate = app._ib_candidate_for(alias, self.mapping)
            self.assertEqual((candidate.symbol, candidate.secType), identity)

    def test_unknown_index_fails_and_never_uses_stock_route(self):
        with self.assertRaisesRegex(ValueError, "explicit entry"):
            app._ib_candidate_for("^UNKNOWN", self.mapping)

    def test_ambiguous_contract_rejected(self):
        ib = FakeIB([resolved_detail(), resolved_detail(con_id=102)])
        with self.assertRaisesRegex(ValueError, "AMBIGUOUS"):
            app.resolve_ib_contract(ib, "AAPL", self.mapping)

    def test_wrong_security_type_rejected(self):
        ib = FakeIB([resolved_detail(sec_type="IND")])
        with self.assertRaisesRegex(ValueError, "secType"):
            app.resolve_ib_contract(ib, "AAPL", self.mapping)

    def test_con_id_mismatch_stops_before_historical_request(self):
        ib = FakeIB([resolved_detail(con_id=202)], [bar(dt.date(2025, 1, 2))])
        result = app.fetch_one_ibkr(
            ib, "AAPL", "2025-01-02", "2025-01-02", "1d", False,
            app.RateLimiter(0), self.mapping, True, 0, 0, expected_con_id=101,
        )
        self.assertIn("PROVENANCE_FAILURE", result[2])
        self.assertEqual(ib.historical_calls, [])

    def test_ibkr_metadata_contains_full_query_and_identity_contract(self):
        detail = resolved_detail()
        metadata = app._ib_metadata(
            "AAPL", "1d", False, "2025-01-01", "2025-01-02",
            detail.contract, detail, "TRADES", True,
        )
        self.assertEqual(
            set(metadata),
            {
                "schema_version", "source", "requested_ticker", "interval",
                "adjusted", "requested_start", "requested_end", "fetched_at_utc",
                "data_sha256", "time_semantics", "ibkr",
            },
        )
        self.assertEqual(
            set(metadata["ibkr"]),
            {
                "conId", "symbol", "localSymbol", "secType", "exchange",
                "primaryExchange", "currency", "timeZoneId", "barSizeSetting",
                "whatToShow", "useRTH",
            },
        )


class TimeAndRequestTests(unittest.TestCase):
    def _fetch(self, interval, timezone, dates, ticker="AAPL"):
        detail = resolved_detail(timezone=timezone)
        ib = FakeIB([detail], [bar(value) for value in dates])
        result = app.fetch_one_ibkr(
            ib, ticker, "2025-01-02", "2025-01-02", interval, False,
            app.RateLimiter(0), {}, True, 0, 0,
        )
        self.assertIsNone(result[2], result[2])
        return result, ib

    def test_us_and_tase_daily_calendar_semantics(self):
        for timezone in ("US/Eastern", "Israel"):
            (ticker, frame, error, metadata), ib = self._fetch(
                "1d", timezone, [dt.date(2025, 1, 2)]
            )
            self.assertIsNone(frame.index.tz)
            self.assertEqual(metadata["time_semantics"], "calendar_date")
            self.assertEqual(metadata["ibkr"]["timeZoneId"], timezone)
            self.assertEqual(ib.historical_calls[0][1]["formatDate"], 1)

    def test_us_and_tase_intraday_are_utc_aware(self):
        for timezone in ("US/Eastern", "Israel"):
            instant = dt.datetime(
                2025, 1, 2, 0, tzinfo=ZoneInfo(timezone)
            ).astimezone(dt.timezone.utc)
            (ticker, frame, error, metadata), ib = self._fetch("1h", timezone, [instant])
            self.assertIsNotNone(frame.index.tz)
            self.assertEqual(str(frame.index.tz), "UTC")
            self.assertEqual(metadata["time_semantics"], "utc_instant")
            self.assertEqual(ib.historical_calls[0][1]["formatDate"], 2)
            self.assertTrue(ib.historical_calls[0][1]["useRTH"])

    def test_naive_intraday_is_rejected(self):
        detail = resolved_detail()
        ib = FakeIB([detail], [bar(dt.datetime(2025, 1, 2, 15))])
        fetched = app.fetch_one_ibkr(
            ib, "AAPL", "2025-01-02", "2025-01-02", "1h", False,
            app.RateLimiter(0), {}, True, 0, 0,
        )
        self.assertIn("timezone-naive", fetched[2])


class RetryAndConnectionTests(unittest.TestCase):
    def test_retry_classification(self):
        self.assertTrue(app.classify_ib_error(TimeoutError("bounded timeout")).retryable)
        permanent = app.classify_ib_error(app.RequestError(1, 354, "not subscribed"))
        self.assertFalse(permanent.retryable)
        self.assertEqual(permanent.category, "PERMANENT_NO_ENTITLEMENT")

    def test_bounded_retry_count(self):
        calls = []
        def fail():
            calls.append(1)
            raise TimeoutError("temporary")
        with self.assertRaisesRegex(RuntimeError, "RETRYABLE"):
            app._call_ib_with_retries(fail, retries=2, sleep_sec=0)
        self.assertEqual(len(calls), 3)

    def test_connection_is_readonly_and_minimal(self):
        instance = SimpleNamespace(RaiseRequestErrors=False)
        instance.connect = mock.Mock()
        with mock.patch.object(app, "IB", return_value=instance):
            self.assertIs(app.ib_connect("127.0.0.1", 7497, 42), instance)
        self.assertTrue(instance.RaiseRequestErrors)
        instance.connect.assert_called_once_with(
            "127.0.0.1", 7497, clientId=42, timeout=10, readonly=True,
            fetchFields=app.StartupFetchNONE,
        )
        with self.assertRaisesRegex(ValueError, "non-zero"):
            app.ib_connect("127.0.0.1", 7497, 0)

    def test_connection_retries_keep_the_same_identity(self):
        connected = object()
        with mock.patch.object(
            app, "ib_connect",
            side_effect=[ConnectionError("temporary"), connected],
        ) as connect, mock.patch.object(app.time, "sleep"):
            result = app.ib_connect_with_retries("127.0.0.1", 7497, 42, 2, 0)
        self.assertIs(result, connected)
        self.assertEqual(connect.call_args_list, [
            mock.call("127.0.0.1", 7497, 42),
            mock.call("127.0.0.1", 7497, 42),
        ])

    def test_include_today_is_source_specific(self):
        args = argparse.Namespace(source="ibkr", include_today=True)
        with mock.patch.object(app, "log") as logger:
            with self.assertRaisesRegex(SystemExit, "2"):
                app._run(args)
        self.assertIn("unsupported", logger.call_args.args[0])


class StaticSafetyBoundaryTests(unittest.TestCase):
    def test_application_references_no_trading_api(self):
        source = Path(app.__file__).read_text(encoding="utf-8")
        forbidden = (
            "placeOrder", "cancelOrder", "modifyOrder", "exerciseOptions",
            "whatIfOrder", "reqPositions", "reqAccountUpdates",
            "reqExecutions", "reqOpenOrders", "reqCompletedOrders",
        )
        for api_name in forbidden:
            self.assertNotIn(api_name, source)


if __name__ == "__main__":
    unittest.main()
