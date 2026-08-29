import argparse
import asyncio
import datetime as dt
import io
import json
import logging
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
    def __init__(self, details=None, bars=None, historical_effects=None, head=None):
        self.details = details or []
        self.bars = bars or []
        self.historical_effects = list(historical_effects or [])
        self.candidates = []
        self.historical_calls = []
        self.head = head
        self.sleep_calls = []

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

    def reqHeadTimeStamp(self, contract, **kwargs):
        if self.head is not None:
            return self.head
        dates = [item.date for item in self.bars]
        return min(dates) if dates else dt.datetime(2025, 1, 2, tzinfo=dt.timezone.utc)

    def sleep(self, seconds):
        self.sleep_calls.append(seconds)


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

    def test_yfinance_period_intervals_use_calendar_labels(self):
        for interval in ("1d", "5d", "1wk", "1mo", "3mo"):
            args = self.args()
            args.interval = interval
            self.assertEqual(
                app._base_metadata(args, "AAPL")["time_semantics"],
                "calendar_date",
            )
        args = self.args()
        args.interval = "1h"
        self.assertEqual(
            app._base_metadata(args, "AAPL")["time_semantics"], "utc_instant"
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


class PaginationTests(unittest.TestCase):
    mapping = app.load_ib_contract_map(None)
    friday = dt.datetime(2025, 1, 3, 5, tzinfo=dt.timezone.utc)
    monday = dt.datetime(2025, 1, 6, 5, tzinfo=dt.timezone.utc)

    def _fetch(self, effects, start="2025-01-03", head=None):
        ib = FakeIB(
            [resolved_detail()], historical_effects=effects,
            head=head or self.friday,
        )
        result = app.fetch_one_ibkr(
            ib, "AAPL", start, "2025-01-06", "1m", False,
            app.RateLimiter(0), self.mapping, True, 0, 0,
        )
        return result, ib

    def test_one_minute_paging_crosses_weekend_and_duplicate_boundary(self):
        result, ib = self._fetch([
            [bar(self.monday)], [bar(self.monday)], [], [bar(self.friday)],
        ])
        self.assertIsNone(result[2], result[2])
        self.assertEqual(len(result[1]), 2)
        self.assertEqual(len(ib.historical_calls), 4)

    def test_multi_day_holiday_gap_is_crossed(self):
        prior = dt.datetime(2024, 12, 31, 5, tzinfo=dt.timezone.utc)
        ib = FakeIB(
            [resolved_detail()],
            historical_effects=[
                [bar(self.monday)], [bar(self.monday)], [], [], [], [bar(prior)],
            ],
            head=prior,
        )
        result = app.fetch_one_ibkr(
            ib, "AAPL", "2024-12-31", "2025-01-06", "1m", False,
            app.RateLimiter(0), self.mapping, True, 0, 0,
        )
        self.assertIsNone(result[2], result[2])
        self.assertEqual(len(result[1]), 2)

    def test_known_earliest_history_boundary_terminates_cleanly(self):
        result, _ = self._fetch(
            [[bar(self.monday)], [], [], [bar(self.friday)]],
            start="2020-01-01", head=self.friday,
        )
        self.assertIsNone(result[2], result[2])
        self.assertEqual(result[1].index.min(), pd.Timestamp(self.friday))

    def test_page_cap_fails_closed(self):
        ancient = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        with mock.patch.object(app, "_IB_PAGE_CAP", 2):
            result, _ = self._fetch([[], []], start="2025-01-03", head=ancient)
        self.assertIn("PERMANENT_PAGE_CAP", result[2])


class HistoricalFailureBoundaryTests(unittest.TestCase):
    mapping = app.load_ib_contract_map(None)
    instant = dt.datetime(2025, 1, 2, 5, tzinfo=dt.timezone.utc)

    def _failure(self, effect):
        ib = FakeIB(
            [resolved_detail()], historical_effects=[effect], head=self.instant
        )
        return app.fetch_one_ibkr(
            ib, "AAPL", "2025-01-02", "2025-01-02", "1m", False,
            app.RateLimiter(0), self.mapping, True, 0, 0,
        )

    def test_timeout_is_retryable_not_no_data(self):
        result = self._failure(TimeoutError("owned request timeout"))
        self.assertIn("RETRYABLE_TIMEOUT", result[2])
        self.assertNotIn("NO_HISTORICAL_DATA", result[2])

    def test_deterministic_empty_is_no_historical_data(self):
        result = self._failure([])
        self.assertIn("PERMANENT_NO_HISTORICAL_DATA", result[2])

    def test_entitlement_is_permanent_entitlement(self):
        result = self._failure(
            app.RequestError(1, 162, "No market data permissions for request")
        )
        self.assertIn("PERMANENT_NO_ENTITLEMENT", result[2])

    def test_invalid_request_is_permanent_request(self):
        result = self._failure(app.RequestError(1, 200, "invalid contract"))
        self.assertIn("PERMANENT_REQUEST", result[2])


class RetryAndConnectionTests(unittest.TestCase):
    def test_retry_classification(self):
        timeout = app.classify_ib_error(TimeoutError("bounded timeout"))
        self.assertEqual(timeout.category, "RETRYABLE_TIMEOUT")
        permanent = app.classify_ib_error(app.RequestError(1, 354, "not subscribed"))
        self.assertFalse(permanent.retryable)
        self.assertEqual(permanent.category, "PERMANENT_NO_ENTITLEMENT")
        permission_162 = app.classify_ib_error(
            app.RequestError(1, 162, "No market data permissions")
        )
        self.assertEqual(permission_162.category, "PERMANENT_NO_ENTITLEMENT")
        no_data = app.classify_ib_error(
            app.RequestError(1, 162, "HMDS query returned no data")
        )
        self.assertEqual(no_data.category, "PERMANENT_NO_HISTORICAL_DATA")
        invalid = app.classify_ib_error(app.RequestError(1, 200, "invalid contract"))
        self.assertEqual(invalid.category, "PERMANENT_REQUEST")

    def test_bounded_retry_count(self):
        calls = []
        def fail():
            calls.append(1)
            raise TimeoutError("temporary")
        with self.assertRaisesRegex(RuntimeError, "RETRYABLE"):
            app._call_ib_with_retries(fail, retries=2, sleep_sec=0)
        self.assertEqual(len(calls), 3)

    def test_market_data_connect_lifecycle_invokes_no_forbidden_requests(self):
        engine = app._MarketDataIB()
        client = SimpleNamespace(
            connectAsync=mock.AsyncMock(),
            isReady=mock.Mock(return_value=True),
            isConnected=mock.Mock(return_value=False),
            disconnect=mock.Mock(),
        )
        engine.client = client
        forbidden = (
            "reqPositionsAsync", "reqAccountUpdatesAsync",
            "reqAccountUpdatesMultiAsync", "reqAccountSummaryAsync",
            "reqExecutionsAsync", "reqOpenOrdersAsync", "reqAllOpenOrders",
            "reqCompletedOrdersAsync", "reqPnLAsync", "reqPnLSingleAsync",
        )
        spies = {}
        for name in forbidden:
            spies[name] = mock.Mock()
            setattr(engine, name, spies[name])
        asyncio.run(engine.connectAsync("127.0.0.1", 7497, 42, 10))
        client.connectAsync.assert_awaited_once_with("127.0.0.1", 7497, 42, 10)
        for spy in spies.values():
            spy.assert_not_called()

        adapter = app.MarketDataIBAdapter(engine)
        self.assertTrue(adapter.readonly)
        self.assertEqual(adapter.startup_fetch, app.StartupFetchNONE)
        for name in forbidden:
            self.assertFalse(hasattr(adapter, name))

    def test_error_1102_does_not_request_account_summary(self):
        engine = app._MarketDataIB()
        summary = mock.Mock()
        engine.reqAccountSummaryAsync = summary
        engine._onError(-1, 1102, "restored", None)
        summary.assert_not_called()

    def test_managed_account_ids_are_discarded_and_never_form_output(self):
        engine = app._MarketDataIB()
        self.assertEqual(engine.wrapper.accounts, [])
        with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout, \
             mock.patch("sys.stderr", new_callable=io.StringIO) as stderr, \
             mock.patch.object(logging.Logger, "_log") as logger:
            engine.wrapper.managedAccounts("DU_SENSITIVE_123")
        self.assertEqual(engine.wrapper.accounts, [])
        self.assertEqual(stdout.getvalue(), "")
        self.assertEqual(stderr.getvalue(), "")
        logger.assert_not_called()
        detail = resolved_detail()
        identity = app._format_ib_validation_identity(detail.contract, detail)
        metadata = app._ib_metadata(
            "AAPL", "1d", False, "2025-01-01", "2025-01-02",
            detail.contract, detail, "TRADES", True,
        )
        self.assertNotIn("DU_SENSITIVE_123", identity)
        self.assertNotIn("DU_SENSITIVE_123", json.dumps(metadata))

    def test_dependency_logging_is_clamped(self):
        app._clamp_ib_async_logging()
        for name in ("ib_async.client", "ib_async.wrapper", "ib_async.ib"):
            self.assertGreaterEqual(logging.getLogger(name).level, logging.WARNING)

    def test_historical_timeout_cancels_request_and_removes_future(self):
        async def scenario():
            engine = app._MarketDataIB()
            engine.client = SimpleNamespace(
                getReqId=mock.Mock(return_value=77),
                reqHistoricalData=mock.Mock(),
                cancelHistoricalData=mock.Mock(),
                isConnected=mock.Mock(return_value=False),
            )
            with self.assertRaises(asyncio.TimeoutError):
                await engine.reqHistoricalDataStrictAsync(
                    app.Contract(conId=101), endDateTime="", durationStr="1 D",
                    barSizeSetting="1 min", whatToShow="TRADES", useRTH=True,
                    formatDate=2, timeout=0.001,
                )
            engine.client.cancelHistoricalData.assert_called_once_with(77)
            self.assertNotIn(77, engine.wrapper._futures)
        asyncio.run(scenario())

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

    def test_connected_request_backoff_uses_adapter_sleep(self):
        calls = []
        sleeps = []
        def request():
            calls.append(1)
            if len(calls) < 2:
                raise TimeoutError("temporary")
            return "ok"
        result = app._call_ib_with_retries(
            request, 2, 0.25, sleep_fn=sleeps.append
        )
        self.assertEqual(result, "ok")
        self.assertEqual(sleeps, [0.25])

    def test_connection_loss_fails_closed_without_same_object_retry(self):
        calls = []
        def request():
            calls.append(1)
            raise ConnectionError("socket lost")
        with self.assertRaisesRegex(RuntimeError, "RETRYABLE_CONNECTION"):
            app._call_ib_with_retries(request, 2, 0, sleep_fn=lambda _: None)
        self.assertEqual(len(calls), 1)

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
            "reqAccountSummary", "reqExecutions", "reqOpenOrders",
            "reqAllOpenOrders", "reqCompletedOrders", "reqPnL", "reqPnLSingle",
        )
        for api_name in forbidden:
            self.assertNotIn(api_name, source)


if __name__ == "__main__":
    unittest.main()
