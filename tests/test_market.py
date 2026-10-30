import asyncio
import hashlib
import json
import threading
import time
from decimal import Decimal
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import pytest
from nautilus_trader.common import Clock
from nautilus_trader.core import UUID4
from nautilus_trader.model import AggressorSide, BarType, BookType, OrderBook, Price, Quantity

from nautilus_futu import (
    FutuConnectionSettings,
    FutuDataClient,
    FutuDataClientConfig,
    FutuInstrumentProvider,
)
from nautilus_futu.errors import ConfigurationError, DataConflict, RequestUncertain
from nautilus_futu.evidence import Evidence
from nautilus_futu.history import history_rows
from nautilus_futu.instruments import tick_size, validate_order
from nautilus_futu.mapping import decimal, timestamp_ns
from nautilus_futu.rate_limit import SlidingWindow
from nautilus_futu.symbols import canonical_symbol, instrument_id, provider_symbol
from nautilus_futu.transport import CallbackBridge, OpenDSession, SDKWorker
from tests.fakes import RECEIVED, book, candle, static


def data_client(server, **kwargs):
    client = FutuDataClient(
        name="FUTU",
        config=FutuDataClientConfig(**kwargs),
        clock=Clock.new_test(),
        cache=None,
        context_factory=server,
    )
    client._handle_instrument = lambda _: None
    client._handle_data = lambda _: None
    client.instrument_provider.add(client.instrument_provider.from_static(static()))
    return client


@pytest.mark.parametrize(
    "symbol,expected",
    [
        ("HK.700", "00700.HK"),
        ("00700.HK", "00700.HK"),
        ("US.BRK.B", "BRK.B.US"),
        ("AAPL.US", "AAPL.US"),
    ],
)
def test_symbol_roundtrip(symbol, expected):
    assert canonical_symbol(symbol) == expected
    assert str(instrument_id(symbol)) == expected + ".FUTU"
    assert canonical_symbol(provider_symbol(symbol)) == expected


@pytest.mark.parametrize(
    "symbol,kind,value,expected",
    [
        ("00700.HK", "STOCK", "0.25", "0.001"),
        ("00700.HK", "STOCK", "0.255", "0.005"),
        ("00700.HK", "STOCK", "10.01", "0.01"),
        ("02800.HK", "ETF", "1.002", "0.002"),
        ("AAPL.US", "STOCK", ".9999", ".0001"),
        ("AAPL.US", "STOCK", "1", ".01"),
    ],
)
def test_price_rules(symbol, kind, value, expected):
    assert tick_size(symbol, kind, Decimal(value)) == Decimal(expected)


def test_real_metadata_no_synthetic_order_rules():
    provider = FutuInstrumentProvider()
    instrument = provider.from_static(static())
    assert str(instrument.raw_symbol) == "HK.00700"
    validate_order(instrument, Quantity.from_int(100), Price.from_str("10.0100"))
    for qty, px in [(99, "10.01"), (100, "10.001")]:
        with pytest.raises(ConfigurationError):
            validate_order(instrument, qty, px)
    with pytest.raises(ConfigurationError):
        provider.from_static({**static(), "stock_id": 0})
    with pytest.raises(ConfigurationError):
        provider.from_static(static("HK.83000"))
    with pytest.raises(ConfigurationError):
        validate_order(provider.from_static({**static(), "delisting": True}), 100, 100)


def test_dst_and_receipt_semantics():
    summer = timestamp_ns("2026-07-01 09:30:00", "AAPL.US")
    winter = timestamp_ns("2026-01-02 09:30:00", "AAPL.US")
    assert summer == timestamp_ns("2026-07-01T13:30:00+00:00", "AAPL.US")
    assert winter == timestamp_ns("2026-01-02T14:30:00+00:00", "AAPL.US")
    with pytest.raises(ConfigurationError):
        timestamp_ns("2026-11-01 01:30:00", "AAPL.US")


async def test_history_pagination_filter_empty_and_conflict(server):
    bridge = CallbackBridge(10, 1, lambda: None)
    session = OpenDSession(FutuConnectionSettings(), bridge, server)
    await session.open()
    try:
        server.pages = [
            (0, [candle("2026-10-23 09:29:00"), candle()], b"next"),
            (0, [candle(), candle("2026-10-23 09:31:00")], None),
        ]
        start = timestamp_ns("2026-10-23 09:30:00", "00700.HK")
        result = await history_rows(
            session, "00700.HK", "K_1M", start, start + 60_000_000_000, "NoAdjust", "RTH"
        )
        assert len(result) == 2 and result[0][1] > start
        assert [c[1]["autype"] for c in server.calls if c[0] == "request_history_kline"] == [
            "None",
            "None",
        ]
        server.pages = [(0, [], None)]
        assert (
            await history_rows(session, "00700.HK", "K_DAY", start, start, "ForwardAdjust", "RTH")
            == []
        )
        for pages in [
            [(0, [candle()], b"x"), (0, [candle()], b"x")],
            [(0, [candle()], b"x"), (0, [{**candle(), "close": 100}], None)],
        ]:
            server.pages = pages
            with pytest.raises(DataConflict):
                await history_rows(session, "00700.HK", "K_1M", start, start, "NoAdjust", "RTH")
    finally:
        await session.close()


async def test_snapshots_clear_old_levels_and_never_invent_quote(server):
    client = data_client(server)
    _, deltas, tick = await client._book("00700.HK", book(), RECEIVED)
    assert tick.ts_init == RECEIVED and tick.ts_event < tick.ts_init
    nt_book = OrderBook(instrument_id("00700.HK"), BookType.L2_MBP)
    nt_book.apply_deltas(deltas)
    assert decimal(nt_book.best_bid_price()) == 100
    payload = book(Bid=[(98, 100, 1, {})], Ask=[])
    _, deltas2, tick2 = await client._book("00700.HK", payload, RECEIVED)
    nt_book.apply_deltas(deltas2)
    assert decimal(nt_book.best_bid_price()) == 98 and nt_book.best_ask_price() is None
    assert tick2 is None
    with pytest.raises(DataConflict):
        await client._book("00700.HK", book(code="US.AAPL"), RECEIVED)


async def test_tick_identity_and_aggressor(server):
    client = data_client(server)
    row = dict(
        code="HK.00700",
        time="2026-10-23 09:30:00.001",
        price=100,
        volume=100,
        sequence=1,
        ticker_direction="BUY",
        type="AUTO_MATCH",
        push_data_type="REALTIME",
    )
    tick = await client._trade_tick("00700.HK", row, RECEIVED, deduplicate=True)
    assert tick.aggressor_side == AggressorSide.BUY
    assert (
        await client._trade_tick(
            "00700.HK", {**row, "push_data_type": "CACHE"}, RECEIVED, deduplicate=True
        )
        is None
    )
    with pytest.raises(DataConflict):
        await client._trade_tick("00700.HK", {**row, "price": 101}, RECEIVED, deduplicate=True)
    a = await client._trade_tick("00700.HK", {**row, "sequence": None}, RECEIVED, deduplicate=True)
    b = await client._trade_tick("00700.HK", {**row, "sequence": None}, RECEIVED, deduplicate=True)
    assert a.trade_id != b.trade_id


async def test_bar_policy_and_historical_response_id(server):
    client = data_client(server)
    bridge = CallbackBridge(20, 1, lambda: None)
    client._session = OpenDSession(client._futu_config.connection, bridge, server)
    await client._session.open()
    client.instrument_provider.set_session(client._session)
    bar_type = BarType.from_str("00700.HK.FUTU-1-MINUTE-LAST-EXTERNAL")
    output = []
    client._handle_response = output.append
    start = timestamp_ns("2026-10-23 09:30:00", "00700.HK")
    request = NS(bar_type=bar_type, request_id=UUID4(), start_ns=start, end_ns=start, limit=0)
    try:
        await client._request_bars(request)
        assert output[0].correlation_id == request.request_id
        assert output[0].start is None and output[0].end is None
        assert output[0].data[0].ts_event == start < output[0].data[0].ts_init
        other = data_client(server, adjustment="ForwardAdjust")
        with pytest.raises(ConfigurationError):
            other._claim_bar(bar_type)
    finally:
        await client._disconnect()


async def test_batch_quote_subscription_keeps_independent_provider_leases(server):
    client = data_client(server)
    second_static = static("HK.02800")
    second_static["stock_id"] = 2800
    client.instrument_provider.add(client.instrument_provider.from_static(second_static))
    client._activate = AsyncMock()
    command = NS(
        instrument_ids=[instrument_id("00700.HK"), instrument_id("02800.HK")],
    )

    await client._subscribe_quotes(command)

    assert client._quotes == {"00700.HK", "02800.HK"}
    assert set(client._leases) == {
        ("00700.HK", "ORDER_BOOK"),
        ("02800.HK", "ORDER_BOOK"),
    }
    assert client._activate.await_count == 2


async def test_bounded_queue_and_thread_lane(server):
    overflow = []
    bridge = CallbackBridge(1, 4, lambda: overflow.append(True))
    thread = threading.Thread(target=lambda: [bridge.push("x", n) for n in range(100)])
    thread.start()
    thread.join()
    await asyncio.sleep(0)
    assert bridge.queue.qsize() == 1 and overflow == [True]
    assert (await bridge.get()).generation == 4
    bridge.queue.task_done()
    bridge.close()
    bridge.push("x", 0)
    assert bridge.queue.empty()
    session = OpenDSession(FutuConnectionSettings(), CallbackBridge(10, 1, lambda: None), server)
    await session.open()
    assert all(call[2] != threading.get_ident() for call in server.calls)
    await session.close()
    assert all(context.closed for context in server.contexts)


async def test_timeout_poison_and_cancellation_close_late_result():
    finished = threading.Event()

    class Slow:
        def set_sync_query_connect_timeout(self, _):
            pass

        def wait(self):
            time.sleep(0.05)
            return 0, {"late": True}

        def close(self):
            finished.set()

    worker = SDKWorker(FutuConnectionSettings(request_timeout_secs=0.01), Slow)
    await worker.open()
    with pytest.raises(RequestUncertain):
        await worker.call("wait")
    with pytest.raises(RequestUncertain):
        await worker.call("wait")
    await worker.close()
    assert await asyncio.to_thread(finished.wait, 1)


async def test_rate_reservations_wait():
    rate = SlidingWindow(1, 0.02)
    await rate.acquire()
    start = time.monotonic()
    await rate.acquire()
    assert time.monotonic() - start >= 0.02


def test_evidence_hash_and_safe_config(tmp_path, monkeypatch):
    monkeypatch.setenv("FUTU_TRADE_PASSWORD_MD5", "SECRET")
    from nautilus_futu import FutuExecutionClientConfig

    config = FutuExecutionClientConfig()
    assert "SECRET" not in repr(config) + config.json().decode()
    evidence = Evidence(str(tmp_path))
    evidence.record("history", {"adjustment": "NoAdjust"}, [candle()], RECEIVED)
    record = json.loads(next(tmp_path.rglob("*.json")).read_text())
    body = json.dumps(
        record["response"], ensure_ascii=False, sort_keys=True, default=str, allow_nan=False
    ).encode()
    assert record["sha256"] == hashlib.sha256(body).hexdigest()
