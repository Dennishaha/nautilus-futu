import asyncio
import time
from types import SimpleNamespace as NS

import pytest
from nautilus_trader.common import Clock
from nautilus_trader.core import UUID4
from nautilus_trader.model import (
    ClientOrderId,
    LimitOrder,
    OrderSide,
    OrderStatus,
    Price,
    Quantity,
    StrategyId,
    TimeInForce,
    TraderId,
)

from nautilus_futu import FutuConnectionSettings, FutuExecutionClient, FutuExecutionClientConfig
from nautilus_futu.execution import order_status
from nautilus_futu.lifecycle import ConnectionState
from nautilus_futu.symbols import instrument_id
from nautilus_futu.transport import Callback, CallbackBridge, OpenDSession
from tests.fakes import fill_row, order_row, static


def order(client="O-1", qty=100):
    return LimitOrder(
        TraderId("TEST-001"),
        StrategyId("S-001"),
        instrument_id("00700.HK"),
        ClientOrderId(client),
        OrderSide.BUY,
        Quantity.from_int(qty),
        Price.from_str("100.0000"),
        TimeInForce.DAY,
        False,
        False,
        False,
        UUID4(),
        1,
    )


async def execution_client(server, **kwargs):
    client = FutuExecutionClient(
        name="FUTU",
        config=FutuExecutionClientConfig(
            acc_id=123, trading_environment="REAL", allow_real_trading=True, **kwargs
        ),
        clock=Clock.new_test(),
        cache=None,
        trader_id=TraderId("TEST-001"),
        context_factory=server,
    )
    client._handle_instrument = lambda _: None
    client._handle_report = lambda *a: None
    client.instrument_provider.add(client.instrument_provider.from_static(static()))
    events = []
    for name in [
        "order_submitted",
        "order_accepted",
        "order_filled",
        "order_canceled",
        "order_rejected",
        "order_denied",
        "order_updated",
        "order_modify_rejected",
        "order_cancel_rejected",
        "account_state",
    ]:
        setattr(client, "generate_" + name, lambda *args, _name=name: events.append((_name, args)))
    client._session = OpenDSession(
        client._futu_config.connection, CallbackBridge(100, 1, lambda: None), server, "HK"
    )
    await client._session.open()
    client.instrument_provider.set_session(client._session)
    client._account = dict(trdmarket_auth=["HK"])
    client._unlocked = True
    client.connection_state = ConnectionState.CONNECTED
    return client, events


async def test_dry_run_cannot_submit_or_unlock(server):
    client, events = await execution_client(server)
    try:
        await client._submit_order(NS(order=order()))
        assert events[0][0] == "order_denied"
        assert not any(c[0] in {"place_order", "unlock_trade"} for c in server.calls)
    finally:
        await client._disconnect()


async def test_push_before_submit_response_and_partial_cancel(server):
    client, events = await execution_client(server, dry_run=False)
    loop = asyncio.get_running_loop()

    def early(context, row):
        # Block only the fake SDK thread until the owner's loop has consumed
        # acceptance, reproducing the provider's documented callback ordering.
        future = asyncio.run_coroutine_threadsafe(
            client._process_callback(Callback("orders", [row], time.time_ns(), 1)), loop
        )
        future.result(2)

    server.before_response = early
    try:
        await client._submit_order(NS(order=order()))
        assert [kind for kind, _ in events] == ["order_submitted", "order_accepted"]
        assert len(server.orders[0]["remark"].encode()) <= 64
        server.fills = [fill_row()]
        server.orders[0].update(dealt_qty=40, dealt_avg_price=100, order_status="CANCELLED_PART")
        await client._process_order_row(server.orders[0])
        await client._process_order_row(server.orders[0])
        await client._process_fill(server.fills[0])
        assert [kind for kind, _ in events].count("order_filled") == 1
        assert [kind for kind, _ in events].count("order_canceled") == 1
        assert client._filled["O-1"] == 40
    finally:
        await client._disconnect()


async def test_submit_timeout_retains_identity_never_resubmits(server):
    client, events = await execution_client(
        server, dry_run=False, connection=FutuConnectionSettings(request_timeout_secs=0.01)
    )
    server.submit_delay = 0.05
    try:
        await client._submit_order(NS(order=order()))
        await client._submit_order(NS(order=order()))
        assert [kind for kind, _ in events] == ["order_submitted"]
        assert len([c for c in server.calls if c[0] == "place_order"]) == 1
        assert "O-1" in client._pending
        assert any(d["kind"] == "submission_uncertain" for d in client.diagnostics)
    finally:
        await client._disconnect()


async def test_fill_arrives_before_order_and_deferred_terminal(server):
    client, events = await execution_client(server, dry_run=False)
    try:
        await client._submit_order(NS(order=order()))
        final = {**server.orders[0], "order_status": "CANCELLED_PART", "dealt_qty": 40}
        await client._process_order_row(final)
        assert not any(kind == "order_canceled" for kind, _ in events)
        await client._process_fill(fill_row())
        assert [kind for kind, _ in events][-2:] == ["order_filled", "order_canceled"]
    finally:
        await client._disconnect()


async def test_amend_cancel_scope_external_ownership_and_foreign_account(server):
    client, events = await execution_client(server, dry_run=False)
    try:
        await client._submit_order(NS(order=order()))
        await client._modify_order(
            NS(
                order=client._pending["O-1"],
                quantity=Quantity.from_int(200),
                price=Price.from_str("101.0000"),
            )
        )
        assert any(kind == "order_updated" for kind, _ in events)
        server.orders.append(order_row(order_id="EXTERNAL", remark="OTHER-USER"))
        await client._process_callback(
            Callback("orders", [order_row(_opend_acc_id=999)], time.time_ns(), 1)
        )
        assert any(
            d["kind"] == "foreign_or_unscoped_account_push_ignored" for d in client.diagnostics
        )
        await client._cancel_all_orders(
            NS(
                instrument_id=instrument_id("00700.HK"),
                strategy_id=StrategyId("S-001"),
                order_side=None,
            )
        )
        assert server.orders[0]["order_status"] == "CANCELLED_ALL"
        assert server.orders[1]["order_status"] == "SUBMITTED"
        assert all(
            call[1]["order_id"] != "EXTERNAL" for call in server.calls if call[0] == "modify_order"
        )
    finally:
        await client._disconnect()


async def test_typed_reports_unknown_fee_and_account_valuation(server):
    client, events = await execution_client(server)
    reports = []
    client._handle_report = lambda report, *a: reports.extend(
        [report, *(a[0] or [])] if a else [report]
    )
    server.orders = [order_row(remark="EXTERNAL")]
    server.fills = [fill_row()]
    server.positions = [
        dict(code="HK.00700", qty=40, cost_price=100, cost_price_valid=True, position_side="LONG")
    ]
    try:
        await client.reconcile()
        assert {type(report).__name__ for report in reports} == {
            "OrderStatusReport",
            "FillReport",
            "PositionStatusReport",
        }
        assert client._order_report(server.orders[0]).client_order_id is None
        balance = next(args for kind, args in events if kind == "account_state")
        assert str(balance[0][0].total) == "100000.00 HKD"
        assert balance[-1]["commission_status"] == "UNREPORTED_PER_FILL"
        assert any(d["kind"] == "fill_commission_unreported" for d in client.diagnostics)
    finally:
        await client._disconnect()


@pytest.mark.parametrize(
    "name,expected",
    [
        ("CANCELLING_PART", OrderStatus.PARTIALLY_FILLED),
        ("CANCELLING_ALL", OrderStatus.ACCEPTED),
        ("CANCELLED_PART", OrderStatus.CANCELED),
        ("FILLED_ALL", OrderStatus.FILLED),
        ("FAILED", OrderStatus.REJECTED),
    ],
)
def test_status_mapping(name, expected):
    assert order_status(order_row(order_status=name)) == expected
