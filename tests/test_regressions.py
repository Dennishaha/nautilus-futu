from types import SimpleNamespace as NS

import pytest
from nautilus_trader.model import BarType, Quantity

from nautilus_futu import FutuInstrumentProvider
from nautilus_futu.history import bar_period
from nautilus_futu.instruments import HK_ETF, HK_STOCK, tick_size
from nautilus_futu.mapping import decimal, timestamp_ns
from tests.fakes import RECEIVED, candle, fill_row, static
from tests.test_execution import execution_client, order
from tests.test_market import data_client


@pytest.mark.parametrize("kind,table", [("STOCK", HK_STOCK), ("ETF", HK_ETF)])
def test_metadata_preserves_every_price_tier_without_catalog_registry_dependency(kind, table):
    instrument = FutuInstrumentProvider().from_static({**static(), "stock_type": kind})
    assert instrument.tick_scheme is None
    assert instrument.info["price_ladder"] == [list(tier) for tier in table]
    for index, (upper, step) in enumerate(table[:-1]):
        boundary = decimal(upper)
        assert tick_size("00700.HK", kind, boundary) == decimal(step)
        following_step = decimal(table[index + 1][1])
        assert tick_size("00700.HK", kind, boundary + following_step) == following_step


@pytest.mark.parametrize(
    "spec,expected",
    [
        ("1-MINUTE", "K_1M"),
        ("5-MINUTE", "K_5M"),
        ("15-MINUTE", "K_15M"),
        ("30-MINUTE", "K_30M"),
        ("1-HOUR", "K_60M"),
        ("1-DAY", "K_DAY"),
    ],
)
def test_supported_bar_periods(spec, expected):
    assert bar_period(BarType.from_str(f"00700.HK.FUTU-{spec}-LAST-EXTERNAL")) == expected


async def test_hk_auction_and_shortened_hour_bar_completion(server):
    client = data_client(server)
    client._calendar[("00700.HK", "2026-10-23")] = "WHOLE"
    row = candle("2026-10-23 16:00:00")
    assert not await client._closed(
        "00700.HK", row, "K_1M", timestamp_ns("2026-10-23 16:02:00", "00700.HK")
    )
    assert await client._closed(
        "00700.HK", row, "K_1M", timestamp_ns("2026-10-23 16:10:03", "00700.HK")
    )
    assert await client._closed(
        "00700.HK",
        candle("2026-10-23 15:30:00"),
        "K_60M",
        timestamp_ns("2026-10-23 16:10:03", "00700.HK"),
    )
    assert not await client._closed(
        "00700.HK",
        candle("2026-10-23 00:00:00"),
        "K_DAY",
        timestamp_ns("2026-10-23 16:09:00", "00700.HK"),
    )


async def test_real_bar_trigger_queries_explicit_adjustment_and_reconnect_backfill(server):
    from nautilus_futu.transport import CallbackBridge, OpenDSession

    client = data_client(server)
    client.clock = NS(timestamp_ns=lambda: RECEIVED)
    client._session = OpenDSession(
        client._futu_config.connection, CallbackBridge(20, 1, lambda: None), server
    )
    await client._session.open()
    output = []
    client._handle_data = output.append
    bar_type = BarType.from_str("00700.HK.FUTU-5-MINUTE-LAST-EXTERNAL")
    try:
        await client._refresh_bars(bar_type)
        await client._refresh_bars(bar_type)
        assert len(output) == 1
        server.bars = [candle(), candle("2026-10-23 09:35:00")]
        await client._backfill_bars(bar_type, output[0].ts_event)
        assert len(output) == 2
        assert all(
            call[1]["autype"] == "None" for call in server.calls if call[0] == "get_cur_kline"
        )
    finally:
        await client._disconnect()


async def test_amend_quantity_fill_before_nt_cache_update_and_flat_position(server):
    client, events = await execution_client(server, dry_run=False)
    reports = []
    client._handle_report = lambda report, *args: reports.append(report)
    try:
        await client._submit_order(NS(order=order()))
        await client._modify_order(
            NS(order=client._pending["O-1"], quantity=Quantity.from_int(200), price=None)
        )
        assert client._pending["O-1"].quantity == Quantity.from_int(100)
        await client._process_fill(fill_row(qty=200))
        assert client._filled["O-1"] == 200
        server.positions = [dict(code="HK.00700", qty=200, cost_price=100, cost_price_valid=True)]
        await client.reconcile()
        server.positions = []
        await client.reconcile()
        assert reports[-1].quantity == Quantity.from_int(0)
    finally:
        await client._disconnect()


def test_optional_source_data_preserves_provider_fields(server):
    from nautilus_futu.source import FutuSourceEvent

    item = FutuSourceEvent(
        "00700.HK.FUTU",
        "trade",
        "test",
        {"sequence": 1, "type": "ODD_LOT", "push_data_type": "CACHE"},
        "provider_event_time",
        1,
        RECEIVED,
    )
    assert item.to_dict()["provider_fields"]["type"] == "ODD_LOT"


async def test_sdk_exception_after_dispatch_cannot_be_declared_rejected(server):
    client, events = await execution_client(server, dry_run=False)

    def raises(**kwargs):
        raise RuntimeError("SDK failed after request dispatch")

    client._session.trade.context.place_order = raises
    try:
        await client._submit_order(NS(order=order()))
        assert [kind for kind, _ in events] == ["order_submitted"]
        assert any(item["kind"] == "submission_uncertain" for item in client.diagnostics)
        assert client._session.trade.poisoned
    finally:
        await client._disconnect()


async def test_unified_account_unsupported_market_is_not_claimed(server):
    client, _ = await execution_client(server)
    server.orders = [{"code": "JP.7203", "order_id": "FOREIGN"}]
    try:
        await client.reconcile()
        assert client._clients == {}
        assert any(
            item["kind"] == "unsupported_external_product_omitted" for item in client.diagnostics
        )
    finally:
        await client._disconnect()
