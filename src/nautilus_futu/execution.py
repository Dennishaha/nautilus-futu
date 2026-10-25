from __future__ import annotations

import asyncio
import hashlib
import os
from collections import OrderedDict, defaultdict
from types import SimpleNamespace
from typing import Any

from nautilus_trader.live.clients import ExecutionClient
from nautilus_trader.model import (
    AccountBalance,
    AccountId,
    AccountType,
    ClientOrderId,
    Currency,
    FillReport,
    LiquiditySide,
    MarginBalance,
    Money,
    OmsType,
    OrderSide,
    OrderStatus,
    OrderStatusReport,
    OrderType,
    PositionSide,
    PositionStatusReport,
    TimeInForce,
    TradeId,
    VenueOrderId,
)

from .config import FutuExecutionClientConfig
from .errors import ConfigurationError, DataConflict, RequestUncertain, UnsupportedOperation
from .instruments import FutuInstrumentProvider, validate_order
from .lifecycle import ConnectionState, ManagedConnection
from .mapping import decimal, local_datetime, price, quantity, rows, timestamp_ns
from .symbols import VENUE, from_instrument_id, instrument_id, provider_symbol
from .transport import Callback

TERMINAL = {OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED, OrderStatus.EXPIRED}


def order_status(row: dict) -> OrderStatus:
    name = row["order_status"]
    mapping = {
        "UNSUBMITTED": OrderStatus.SUBMITTED,
        "WAITING_SUBMIT": OrderStatus.SUBMITTED,
        "SUBMITTING": OrderStatus.SUBMITTED,
        "SUBMITTED": OrderStatus.ACCEPTED,
        "FILLED_PART": OrderStatus.PARTIALLY_FILLED,
        "FILLED_ALL": OrderStatus.FILLED,
        "CANCELLING_PART": OrderStatus.PARTIALLY_FILLED,
        "CANCELLING_ALL": OrderStatus.ACCEPTED,
        "CANCELLED_PART": OrderStatus.CANCELED,
        "CANCELLED_ALL": OrderStatus.CANCELED,
        "SUBMIT_FAILED": OrderStatus.REJECTED,
        "FAILED": OrderStatus.REJECTED,
        "DISABLED": OrderStatus.EXPIRED,
        "DELETED": OrderStatus.EXPIRED,
    }
    if name not in mapping:
        raise UnsupportedOperation("unknown OpenD order status")
    return mapping[name]


def report_side(value: str) -> OrderSide:
    if value in {"BUY", "BUY_BACK"}:
        return OrderSide.BUY
    if value in {"SELL", "SELL_SHORT"}:
        return OrderSide.SELL
    raise UnsupportedOperation("unknown OpenD trade side")


def remark_for(trader_id: Any, client_order_id: Any) -> str:
    return (
        "NT-"
        + hashlib.blake2b(f"{trader_id}|{client_order_id}".encode(), digest_size=24).hexdigest()
    )


class FutuExecutionClient(ManagedConnection, ExecutionClient):
    instrument_provider: Any

    def __init__(
        self,
        *,
        config: FutuExecutionClientConfig,
        context_factory: Any = None,
        instrument_provider: FutuInstrumentProvider | None = None,
        **kwargs: Any,
    ) -> None:
        provider = instrument_provider or FutuInstrumentProvider(
            config.instrument_provider, currency_overrides=config.currency_overrides
        )
        # One broker-valued ledger currency per actual account, never one fake
        # account per market. Native currency cash amounts remain in account info.
        currency = Currency.from_str("HKD" if config.trade_market == "HK" else "USD")
        super().__init__(
            config=config,
            venue=VENUE,
            account_id=AccountId(config.account_id),
            account_type=AccountType.MARGIN
            if config.account_type == "MARGIN"
            else AccountType.CASH,
            oms_type=OmsType.NETTING,
            base_currency=currency,
            instrument_provider=provider,
            **kwargs,
        )
        self._init_connection(config, context_factory, config.trade_market)
        self._binding = (
            config.acc_id,
            config.trading_environment,
            config.trade_market,
            config.account_type,
        )
        self._pending: dict[str, Any] = {}
        self._remarks: dict[str, str] = {}
        self._venues: dict[str, str] = {}
        self._clients: dict[str, str] = {}
        self._accepted: set[str] = set()
        self._last_status: dict[str, OrderStatus] = {}
        self._filled: defaultdict[str, Any] = defaultdict(lambda: decimal(0))
        self._order_quantities: dict[str, Any] = {}
        self._position_ids: set[Any] = set()
        self._fill_ids: OrderedDict[str, tuple] = OrderedDict()
        self._restored_deals: set[tuple[str, str]] = set()
        self._amendments: dict[str, tuple[Any, Any]] = {}
        self._deferred: dict[str, dict] = {}
        self._account: dict = {}
        self._reconcile_task: Any = None
        self._reconcile_lock = asyncio.Lock()
        self._order_lock = asyncio.Lock()
        self._unlocked = False

    def _account_args(self) -> dict:
        config = self._futu_config
        if (
            config.acc_id,
            config.trading_environment,
            config.trade_market,
            config.account_type,
        ) != self._binding:
            raise ConfigurationError("execution account binding cannot change; create a new client")
        return {
            "acc_id": self._futu_config.acc_id,
            "trd_env": self._futu_config.trading_environment,
        }

    async def _trade_call(self, operation: str, **kwargs: Any) -> tuple[Any, int, Any]:
        if self._session is None or self._session.trade is None:
            raise RuntimeError("trade context is not connected")
        session = self._session
        assert session.trade is not None
        result = await session.trade.call(operation, **kwargs)
        if session is not self._session:
            raise RequestUncertain("trade response belongs to an obsolete connection")
        return result

    async def _trade_rows(self, operation: str, **kwargs: Any) -> list[dict]:
        if self._futu_config.trading_environment == "SIMULATE" and operation in {
            "deal_list_query",
            "history_deal_list_query",
        }:
            self.diagnostics.append(
                {
                    "kind": "simulator_deal_details_unavailable",
                    "operation": operation,
                    "reconciliation_source": "NT_ORDER_STATUS_REPORT_CUMULATIVE",
                }
            )
            return []
        payload, _, _ = await self._trade_call(operation, **self._account_args(), **kwargs)
        return [{**row, "_opend_acc_id": self._futu_config.acc_id} for row in rows(payload)]

    async def _after_connect(self) -> None:
        config = self._futu_config
        if config.acc_id <= 0:
            raise ConfigurationError("execution connection requires an explicit acc_id")
        payload, _, _ = await self._trade_call("get_acc_list")
        matches = [
            row
            for row in rows(payload)
            if int(row["acc_id"]) == config.acc_id and row["trd_env"] == config.trading_environment
        ]
        if len(matches) != 1:
            raise ConfigurationError("configured account/environment not returned by OpenD")
        self._account = matches[0]
        if self._account.get(
            "acc_status"
        ) != "ACTIVE" or config.trade_market not in self._account.get("trdmarket_auth", []):
            raise ConfigurationError("account is inactive or lacks configured market permission")
        if self._account.get("acc_type") != config.account_type:
            raise ConfigurationError("configured account_type does not match OpenD")
        if config.trading_environment == "SIMULATE" and self._account.get("sim_acc_type") not in {
            "STOCK",
            "STOCK_AND_OPTION",
        }:
            raise ConfigurationError("configured simulated account is not an equity account")
        self._unlocked = False
        await self.instrument_provider.initialize()
        for instrument in self.instrument_provider.list_all():
            self._handle_instrument(instrument)
        self._restore_owned_orders()
        await self.reconcile()
        if self._reconcile_task is None or self._reconcile_task.done():
            self._reconcile_task = self.create_task(
                self._reconciliation_loop(), "futu-account-reconciliation"
            )

    def _restore_owned_orders(self) -> None:
        if self.cache is None:
            return
        for order in self.cache.orders(venue=VENUE):
            # Cache identity comes from NT. An imported external order is not
            # owned merely because its instrument venue happens to be FUTU.
            if str(getattr(order, "trader_id", "")) != str(self.trader_id) or str(
                getattr(order, "strategy_id", "")
            ).startswith("EXTERNAL"):
                continue
            if getattr(order, "account_id", None) not in {None, self.account_id}:
                continue
            client = str(order.client_order_id)
            self._pending[client] = order
            self._remarks[remark_for(self.trader_id, client)] = client
            venue = getattr(order, "venue_order_id", None)
            if venue:
                self._bind(client, str(venue))
                self._filled[client] = decimal(order.filled_qty)
                self._order_quantities[client] = decimal(order.quantity)
                self._restored_deals.update((str(venue), str(trade)) for trade in order.trade_ids)
                self._accepted.add(client)
                self._last_status[client] = order.status

    async def _disconnect(self) -> None:
        if self._reconcile_task:
            self._reconcile_task.cancel()
            await asyncio.gather(self._reconcile_task, return_exceptions=True)
            self._reconcile_task = None
        self._unlocked = False
        await super()._disconnect()

    async def _ensure(self, identifier: Any) -> Any:
        instrument = self.instrument_provider.find(identifier)
        if instrument is None and self.cache is not None:
            instrument = self.cache.instrument(identifier)
            if instrument and instrument.info.get("provider") == "FUTU_OPEND":
                self.instrument_provider.add(instrument)
            else:
                instrument = None
        if instrument is None:
            await self.instrument_provider.load_symbols_async([from_instrument_id(identifier)])
            instrument = self.instrument_provider.find(identifier)
        self._handle_instrument(instrument)
        return instrument

    async def _equity_rows(self, values: list[dict]) -> list[dict]:
        result = []
        for row in values:
            try:
                identifier = instrument_id(row["code"])
                await self._ensure(identifier)
            except (UnsupportedOperation, ValueError) as exc:
                if not isinstance(exc, UnsupportedOperation) and str(row["code"]).startswith(
                    ("HK.", "US.")
                ):
                    raise
                if (
                    str(row.get("order_id", "")) in self._clients
                    or row.get("remark") in self._remarks
                ):
                    raise DataConflict("owned order changed to an unsupported instrument") from None
                self.diagnostics.append(
                    {
                        "kind": "unsupported_external_product_omitted",
                        "code": row["code"],
                        "error_type": type(exc).__name__,
                    }
                )
            else:
                result.append(row)
        return result

    def _bind(self, client: str, venue: str) -> None:
        if client in self._venues and self._venues[client] != venue:
            raise DataConflict("one client order ID matched multiple OpenD orders")
        if venue in self._clients and self._clients[venue] != client:
            raise DataConflict("one OpenD order matched multiple NT orders")
        self._venues[client], self._clients[venue] = venue, client

    def _identify(self, row: dict) -> str | None:
        venue = str(row["order_id"])
        client = self._clients.get(venue) or self._remarks.get(str(row.get("remark", "")))
        if client:
            order = self._pending[client]
            if instrument_id(row["code"]) != order.instrument_id:
                raise DataConflict("remark matched another instrument")
            self._bind(client, venue)
        return client

    async def _require_ready(self) -> None:
        self._account_args()
        self._futu_config.require_orders()
        if self.connection_state != ConnectionState.CONNECTED:
            raise ConfigurationError("execution is not connected/reconciled")
        if self._futu_config.trading_environment == "REAL" and not self._unlocked:
            secret = os.getenv(self._futu_config.unlock_password_env)
            if (
                not secret
                or len(secret) != 32
                or any(c not in "0123456789abcdefABCDEF" for c in secret)
            ):
                raise ConfigurationError(
                    "configured unlock environment variable must contain a password MD5"
                )
            await self._trade_call("unlock_trade", password_md5=secret)
            self._unlocked = True

    async def _submit_order(self, command: Any) -> None:
        order = command.order
        client = str(order.client_order_id)
        async with self._order_lock:
            if client in self._pending:
                self.diagnostics.append(
                    {"kind": "duplicate_submit_suppressed", "client_order_id": client}
                )
                return
            try:
                await self._require_ready()
                symbol = from_instrument_id(order.instrument_id)
                market = symbol.rsplit(".", 1)[1]
                if market not in self._account.get("trdmarket_auth", []):
                    raise ConfigurationError("account lacks instrument market permission")
                if order.order_type not in {
                    OrderType.MARKET,
                    OrderType.LIMIT,
                } or order.time_in_force not in {TimeInForce.DAY, TimeInForce.GTC}:
                    raise UnsupportedOperation("only Market/Limit and DAY/GTC are implemented")
                if any(
                    bool(getattr(order, flag, False))
                    for flag in ("is_post_only", "is_reduce_only", "is_quote_quantity")
                ):
                    raise UnsupportedOperation("post-only/reduce-only are not mapped to OpenD")
                if getattr(order, "order_list_id", None) is not None or getattr(
                    order, "linked_order_ids", None
                ):
                    raise UnsupportedOperation("contingent/order-list submission is not supported")
                if order.order_type == OrderType.MARKET and (
                    self._futu_config.session != "RTH"
                    or market == "HK"
                    and order.time_in_force != TimeInForce.DAY
                ):
                    raise UnsupportedOperation(
                        "market order session/TIF combination is unsupported"
                    )
                instrument = await self._ensure(order.instrument_id)
                px = getattr(order, "price", None)
                validate_order(instrument, order.quantity, px)
            except Exception as exc:
                self.generate_order_denied(order, str(exc))
                return
            self._pending[client] = order
            self._order_quantities[client] = decimal(order.quantity)
            remark = remark_for(self.trader_id, client)
            self._remarks[remark] = client
            self.generate_order_submitted(order)
            try:
                payload, _, _ = await self._trade_call(
                    "place_order",
                    **self._account_args(),
                    code=provider_symbol(symbol),
                    qty=int(decimal(order.quantity)),
                    price=float(decimal(px)) if px is not None else 0.0,
                    trd_side="BUY" if order.side == OrderSide.BUY else "SELL",
                    order_type="NORMAL" if order.order_type == OrderType.LIMIT else "MARKET",
                    time_in_force="DAY" if order.time_in_force == TimeInForce.DAY else "GTC",
                    session=self._futu_config.session,
                    adjust_limit=0,
                    remark=remark,
                )
                response = rows(payload)
                if len(response) != 1 or not response[0].get("order_id"):
                    raise RequestUncertain("submission response lacks an unambiguous order ID")
                self._bind(client, str(response[0]["order_id"]))
                await self._process_order_row(
                    {**response[0], "_opend_acc_id": self._futu_config.acc_id}
                )
            except (RequestUncertain, asyncio.CancelledError):
                self.diagnostics.append({"kind": "submission_uncertain", "client_order_id": client})
                self._disruption("submission_uncertain")
            except Exception as exc:
                # A preceding push may already have proved acceptance.
                if client in self._venues:
                    self.diagnostics.append(
                        {
                            "kind": "submit_response_conflicts_with_push",
                            "error_type": type(exc).__name__,
                        }
                    )
                    self._disruption("submit_response_conflict")
                else:
                    self.generate_order_rejected(order, str(exc), self.clock.timestamp_ns(), False)
                    self._last_status[client] = OrderStatus.REJECTED

    async def _submit_order_list(self, command: Any) -> None:
        order_list = getattr(command, "order_list", None)
        for order in getattr(order_list, "orders", []):
            self.generate_order_denied(order, "OpenD order lists/contingencies are unsupported")

    def _command_order(self, command: Any) -> Any:
        order = getattr(command, "order", None)
        if order is None:
            client = str(command.client_order_id)
            order = self._pending.get(client)
        if order is None or str(order.client_order_id) not in self._pending:
            raise ConfigurationError("order is not owned by this execution client")
        return order

    async def _modify_order(self, command: Any) -> None:
        order = self._command_order(command)
        client = str(order.client_order_id)
        venue = self._venues.get(client)
        try:
            await self._require_ready()
            if venue is None:
                raise ConfigurationError("submission is still uncertain; amendment is unavailable")
            instrument = await self._ensure(order.instrument_id)
            qty = command.quantity if command.quantity is not None else order.quantity
            px = command.price if command.price is not None else getattr(order, "price", None)
            if (
                order.order_type != OrderType.LIMIT
                or getattr(command, "trigger_price", None) is not None
            ):
                raise UnsupportedOperation("only limit price/quantity amendments are supported")
            validate_order(instrument, qty, px)
            if decimal(px) != decimal(px).quantize(decimal("0.001")):
                raise UnsupportedOperation("OpenD modify_order supports only three price decimals")
            if decimal(qty) < self._filled[client]:
                raise ConfigurationError("amended quantity is below already filled quantity")
            self._amendments[venue] = (qty, px)
            await self._trade_call(
                "modify_order",
                **self._account_args(),
                modify_order_op="NORMAL",
                order_id=venue,
                qty=int(decimal(qty)),
                price=float(decimal(px)),
                adjust_limit=0,
            )
            records = await self._trade_rows("order_list_query", order_id=venue, refresh_cache=True)
            if records:
                await self._process_order_row(records[0])
                if venue in self._amendments:
                    self.diagnostics.append(
                        {"kind": "amendment_pending_confirmation", "venue_order_id": venue}
                    )
        except RequestUncertain:
            self.diagnostics.append({"kind": "amendment_uncertain", "venue_order_id": venue})
            self._disruption("amendment_uncertain")
        except Exception as exc:
            self.generate_order_modify_rejected(
                order, VenueOrderId(venue) if venue else None, str(exc), self.clock.timestamp_ns()
            )

    async def _cancel_order(self, command: Any) -> None:
        order = self._command_order(command)
        client = str(order.client_order_id)
        venue = self._venues.get(client)
        try:
            await self._require_ready()
            if not venue:
                raise ConfigurationError(
                    "submission is uncertain; wait for venue identity before cancel"
                )
            await self._trade_call(
                "modify_order",
                **self._account_args(),
                modify_order_op="CANCEL",
                order_id=venue,
                qty=0,
                price=0.0,
                adjust_limit=0,
            )
            records = await self._trade_rows("order_list_query", order_id=venue, refresh_cache=True)
            for row in records:
                await self._process_order_row(row)
        except RequestUncertain:
            self.diagnostics.append({"kind": "cancellation_uncertain", "venue_order_id": venue})
            self._disruption("cancellation_uncertain")
        except Exception as exc:
            self.generate_order_cancel_rejected(
                order, VenueOrderId(venue) if venue else None, str(exc), self.clock.timestamp_ns()
            )

    async def _cancel_all_orders(self, command: Any) -> None:
        for client, order in list(self._pending.items()):
            if order.instrument_id != command.instrument_id or str(order.strategy_id) != str(
                command.strategy_id
            ):
                continue
            if (
                getattr(command, "order_side", None) is not None
                and order.side != command.order_side
            ):
                continue
            if client in self._venues and self._last_status.get(client) not in TERMINAL:
                await self._cancel_order(SimpleNamespace(order=order))

    async def _process_callback(self, event: Callback) -> None:
        for row in rows(event.payload):
            if (
                row.get("_opend_acc_id") != self._futu_config.acc_id
                or row.get("trd_env") != self._futu_config.trading_environment
            ):
                self.diagnostics.append({"kind": "foreign_or_unscoped_account_push_ignored"})
                continue
            if event.kind == "orders":
                await self._process_order_row(row)
            elif event.kind == "fills":
                if str(row["order_id"]) not in self._clients:
                    records = await self._trade_rows(
                        "order_list_query", order_id=str(row["order_id"])
                    )
                    for record in records:
                        self._identify(record)
                await self._process_fill(row)

    async def _process_order_row(
        self, row: dict, available_fills: list[dict] | None = None
    ) -> None:
        if row.get("_opend_acc_id") != self._futu_config.acc_id:
            raise ConfigurationError("order report is not scoped to the configured account")
        client = self._identify(row)
        await self._ensure(instrument_id(row["code"]))
        if client is None:
            try:
                report = self._order_report(row)
            except UnsupportedOperation:
                self.diagnostics.append(
                    {
                        "kind": "unsupported_external_order_type_omitted",
                        "venue_order_id": str(row["order_id"]),
                        "code": row["code"],
                        "order_type": row["order_type"],
                        "time_in_force": row["time_in_force"],
                    }
                )
                return
            associated = [
                self._fill_report(fill)
                for fill in (available_fills or [])
                if str(fill["order_id"]) == str(row["order_id"])
            ]
            self._handle_report(report, associated or None)
            return
        order = self._pending[client]
        amendment = self._amendments.get(str(row["order_id"]))
        if (
            amendment
            and decimal(row["qty"]) == decimal(amendment[0])
            and decimal(row["price"]) == decimal(amendment[1])
        ):
            instrument = self.instrument_provider.find(order.instrument_id)
            self.generate_order_updated(
                order,
                VenueOrderId(str(row["order_id"])),
                quantity(amendment[0]),
                price(amendment[1], instrument.price_precision),
                None,
                None,
                timestamp_ns(row["updated_time"], row["code"]),
            )
            self._amendments.pop(str(row["order_id"]))
        self._order_quantities[client] = decimal(row["qty"])
        status = order_status(row)
        previous = self._last_status.get(client)
        if self._futu_config.trading_environment == "SIMULATE":
            cumulative = decimal(row.get("dealt_qty", 0))
            if cumulative < self._filled[client] or previous in TERMINAL:
                return
            # OpenD has neither simulator deals nor simulator deal pushes. The
            # NT reconciler can reconcile cumulative order quantities/average
            # price without inventing provider execution IDs in this adapter.
            self._handle_report(self._order_report(row))
            if cumulative > self._filled[client]:
                self.diagnostics.append(
                    {
                        "kind": "simulator_cumulative_fill_report",
                        "venue_order_id": str(row["order_id"]),
                        "source": "ORDER_CUMULATIVE_NOT_PROVIDER_DEALS",
                    }
                )
            self._filled[client] = cumulative
            self._last_status[client] = status
            self._accepted.add(client)
            return
        if previous in TERMINAL:
            return
        venue = VenueOrderId(str(row["order_id"]))
        stamp = timestamp_ns(row["updated_time"], row["code"])
        if (
            status not in {OrderStatus.SUBMITTED, OrderStatus.REJECTED}
            and client not in self._accepted
        ):
            self.generate_order_accepted(order, venue, stamp)
            self._accepted.add(client)
        expected_filled = decimal(row.get("dealt_qty", 0))
        if expected_filled > self._filled[client]:
            values = available_fills
            if values is None:
                values = await self._trade_rows("deal_list_query", refresh_cache=True)
            for fill in values:
                if str(fill["order_id"]) == str(row["order_id"]):
                    await self._process_fill(fill)
            created = local_datetime(
                timestamp_ns(row["create_time"], row["code"]), row["code"]
            ).date()
            today = local_datetime(self.clock.timestamp_ns(), row["code"]).date()
            if self._filled[client] < expected_filled and created < today:
                for fill in await self._trade_rows("history_deal_list_query", code=row["code"]):
                    if str(fill["order_id"]) == str(row["order_id"]):
                        await self._process_fill(fill)
            if self._filled[client] < expected_filled:
                self._deferred[str(venue)] = row
                self.diagnostics.append(
                    {"kind": "order_waiting_for_deals", "venue_order_id": str(venue)}
                )
                return
        if status == previous:
            return
        # Out-of-order submitted/accepted reports cannot regress fill-driven state.
        if previous == OrderStatus.PARTIALLY_FILLED and status in {
            OrderStatus.SUBMITTED,
            OrderStatus.ACCEPTED,
        }:
            return
        self._last_status[client] = status
        if status == OrderStatus.CANCELED:
            self.generate_order_canceled(order, venue, stamp)
        elif status == OrderStatus.REJECTED:
            self.generate_order_rejected(order, "OpenD rejected order", stamp, False)
        elif status == OrderStatus.EXPIRED:
            self.generate_order_expired(order, venue, stamp)

    async def _process_fill(self, row: dict) -> None:
        if row.get("_opend_acc_id") != self._futu_config.acc_id:
            raise ConfigurationError("fill is not scoped to configured account")
        # Canceled/corrected executions require explicit handling, never a new fill.
        if row.get("status", "OK") not in {"OK", "NORMAL", "N/A", None}:
            raise UnsupportedOperation("deal correction/cancellation needs reconciliation")
        if not row.get("deal_id"):
            raise DataConflict("execution lacks a deal ID")
        fill_id = f"{self._futu_config.acc_id}:{row['deal_id']}"
        if (str(row["order_id"]), str(row["deal_id"])) in self._restored_deals:
            return
        content = (
            str(row["order_id"]),
            row["code"],
            str(row["qty"]),
            str(row["price"]),
            row["trd_side"],
            row["create_time"],
        )
        if fill_id in self._fill_ids:
            if self._fill_ids[fill_id] != content:
                raise DataConflict("deal ID reused with different execution content")
            return
        client = self._clients.get(str(row["order_id"]))
        instrument = await self._ensure(instrument_id(row["code"]))
        if client is None:
            self._handle_report(self._fill_report(row))
        else:
            order = self._pending[client]
            if order.instrument_id != instrument.id or report_side(row["trd_side"]) != order.side:
                raise DataConflict("execution identity does not match the owned order")
            total = self._order_quantities.get(client, decimal(order.quantity))
            if decimal(row["qty"]) <= 0 or self._filled[client] + decimal(row["qty"]) > total:
                raise DataConflict("invalid cumulative execution quantity")
            stamp = timestamp_ns(row["create_time"], row["code"])
            if client not in self._accepted:
                self.generate_order_accepted(order, VenueOrderId(str(row["order_id"])), stamp)
                self._accepted.add(client)
            self.generate_order_filled(
                order,
                VenueOrderId(str(row["order_id"])),
                None,
                TradeId(str(row["deal_id"])),
                quantity(row["qty"]),
                price(row["price"], instrument.price_precision),
                instrument.quote_currency,
                None,
                LiquiditySide.NO_LIQUIDITY_SIDE,
                stamp,
            )
            self._filled[client] += decimal(row["qty"])
            self._last_status[client] = (
                OrderStatus.FILLED
                if self._filled[client] == total
                else OrderStatus.PARTIALLY_FILLED
            )
        self._fill_ids[fill_id] = content
        if len(self._fill_ids) > 200_000:
            self._fill_ids.popitem(last=False)
        self.diagnostics.append(
            {"kind": "fill_commission_unreported", "deal_id": str(row["deal_id"])}
        )
        deferred = self._deferred.pop(str(row["order_id"]), None)
        if deferred:
            await self._process_order_row(deferred, [])

    async def reconcile(self) -> None:
        async with self._reconcile_lock:
            orders = await self._equity_rows(
                await self._trade_rows("order_list_query", refresh_cache=True)
            )
            if any(
                client not in self._venues and self._last_status.get(client) not in TERMINAL
                for client in self._pending
            ):
                # Include a prior-day submission which may already have left
                # the current-order endpoint. The SDK defaults to 90 days.
                recovered = await self._equity_rows(
                    await self._trade_rows("history_order_list_query")
                )
                by_id = {str(row["order_id"]): row for row in recovered}
                by_id.update({str(row["order_id"]): row for row in orders})
                orders = list(by_id.values())
            fills = await self._equity_rows(
                await self._trade_rows("deal_list_query", refresh_cache=True)
            )
            for row in orders:
                self._identify(row)
            for row in orders:
                await self._process_order_row(row, fills)
            known_external = {
                str(row["order_id"]) for row in orders if str(row["order_id"]) not in self._clients
            }
            for row in fills:
                if str(row["order_id"]) not in known_external:
                    await self._process_fill(row)
            positions = await self._equity_rows(
                await self._trade_rows("position_list_query", refresh_cache=True)
            )
            current_ids = {instrument_id(row["code"]) for row in positions}
            prior_ids = self._position_ids.copy()
            if self.cache is not None:
                prior_ids.update(
                    position.instrument_id
                    for position in self.cache.positions_open(venue=VENUE)
                    if position.account_id == self.account_id
                )
            for row in positions:
                await self._ensure(instrument_id(row["code"]))
                self._handle_report(self._position_report(row))
            for identifier in prior_ids - current_ids:
                self._handle_report(
                    PositionStatusReport(
                        self.account_id,
                        identifier,
                        PositionSide.FLAT,
                        quantity(0),
                        self.clock.timestamp_ns(),
                        self.clock.timestamp_ns(),
                    )
                )
            self._position_ids = current_ids
            await self._emit_account_state()

    async def _reconciliation_loop(self) -> None:
        while True:
            await asyncio.sleep(self._futu_config.reconciliation_interval_secs)
            if self.connection_state == ConnectionState.CONNECTED:
                try:
                    await self.reconcile()
                except Exception as exc:
                    self.diagnostics.append(
                        {"kind": "reconciliation_failed", "error_type": type(exc).__name__}
                    )
                    self._disruption("reconciliation_failed")

    async def _emit_account_state(self) -> None:
        payload = await self._trade_rows(
            "accinfo_query", refresh_cache=True, currency=str(self.base_currency)
        )
        if len(payload) != 1:
            raise ConfigurationError("account valuation must contain one record")
        row = payload[0]
        currency = self.base_currency
        assert currency is not None
        currency_source = "provider_currency"
        if row.get("currency") != str(currency):
            if (
                self._futu_config.trading_environment == "SIMULATE"
                and row.get("currency") in {None, "N/A"}
                and self._account.get("trdmarket_auth") == [self._futu_config.trade_market]
            ):
                currency_source = "single_market_simulator_account"
            else:
                raise ConfigurationError("account valuation currency mismatch")

        def money(value):
            return Money.from_decimal(decimal(value), currency)

        margins = []
        total, locked = decimal(row["cash"]), decimal(row["frozen_cash"])
        basis = "broker_valued_cash_and_reported_frozen_cash"
        margin_status = "NOT_APPLICABLE"
        if self.account_type == AccountType.MARGIN:
            try:
                initial, maintenance = (
                    decimal(row["initial_margin"]),
                    decimal(row["maintenance_margin"]),
                )
            except (ValueError, KeyError, ArithmeticError):
                margin_status = "UNAVAILABLE"
            else:
                if initial < 0 or maintenance < 0:
                    raise ConfigurationError("invalid broker margin values")
                margins = [MarginBalance(money(initial), money(maintenance))]
                margin_status = "REPORTED"
        if locked < 0:
            raise ConfigurationError("invalid broker frozen funds")
        self.generate_account_state(
            [AccountBalance(money(total), money(locked), money(total - locked))],
            margins,
            True,
            self.clock.timestamp_ns(),
            {
                "provider": "FUTU_OPEND",
                "valuation_basis": basis,
                "currency_source": currency_source,
                "margin_status": margin_status,
                "broker_values": row,
                "commission_status": "UNREPORTED_PER_FILL",
            },
        )

    def _order_report(self, row: dict) -> OrderStatusReport:
        order_type = {"NORMAL": OrderType.LIMIT, "MARKET": OrderType.MARKET}.get(row["order_type"])
        tif = {"DAY": TimeInForce.DAY, "GTC": TimeInForce.GTC}.get(row["time_in_force"])
        if order_type is None or tif is None:
            raise UnsupportedOperation(
                "external order type/TIF is outside the supported report matrix"
            )
        client = self._identify(row)
        symbol = row["code"]
        instrument = self.instrument_provider.find(instrument_id(symbol))
        return OrderStatusReport(
            self.account_id,
            instrument.id,
            VenueOrderId(str(row["order_id"])),
            report_side(row["trd_side"]),
            order_type,
            tif,
            order_status(row),
            quantity(row["qty"]),
            quantity(row["dealt_qty"]),
            timestamp_ns(row["create_time"], symbol),
            timestamp_ns(row["updated_time"], symbol),
            self.clock.timestamp_ns(),
            client_order_id=ClientOrderId(client) if client else None,
            price=price(row["price"], instrument.price_precision)
            if order_type == OrderType.LIMIT
            else None,
            avg_px=decimal(row["dealt_avg_price"])
            if decimal(row.get("dealt_qty", 0)) > 0
            else None,
        )

    def _fill_report(self, row: dict) -> FillReport:
        self.diagnostics.append(
            {
                "kind": "fill_commission_unreported",
                "deal_id": str(row["deal_id"]),
                "representation": "required_NT_FillReport_zero_placeholder",
            }
        )
        if row.get("status", "OK") not in {"OK", "NORMAL", "N/A", None}:
            raise UnsupportedOperation("deal corrections cannot be published as ordinary fills")
        instrument = self.instrument_provider.find(instrument_id(row["code"]))
        client = self._clients.get(str(row["order_id"]))
        # NT rc7 FillReport requires Money. Zero is a structural placeholder,
        # explicitly disclosed in account info/diagnostics, never certified fees.
        return FillReport(
            self.account_id,
            instrument.id,
            VenueOrderId(str(row["order_id"])),
            TradeId(str(row["deal_id"])),
            report_side(row["trd_side"]),
            quantity(row["qty"]),
            price(row["price"], instrument.price_precision),
            Money.from_decimal(decimal(0), instrument.quote_currency),
            LiquiditySide.NO_LIQUIDITY_SIDE,
            timestamp_ns(row["create_time"], row["code"]),
            self.clock.timestamp_ns(),
            client_order_id=ClientOrderId(client) if client else None,
        )

    def _position_report(self, row: dict) -> PositionStatusReport:
        qty = decimal(row["qty"])
        short = row.get("position_side") == "SHORT" or qty < 0
        return PositionStatusReport(
            self.account_id,
            instrument_id(row["code"]),
            PositionSide.SHORT if short else PositionSide.LONG if qty else PositionSide.FLAT,
            quantity(abs(qty)),
            self.clock.timestamp_ns(),
            self.clock.timestamp_ns(),
            avg_px_open=decimal(row["cost_price"]) if row.get("cost_price_valid", True) else None,
        )

    async def _query_account(self, command: Any) -> None:
        await self._emit_account_state()

    async def _query_order(self, command: Any) -> None:
        report = await self._generate_order_status_report(command)
        if report:
            self._handle_report(report)

    def _report_filter(self, row: dict, command: Any, *, fill: bool = False) -> bool:
        wanted = getattr(command, "instrument_id", None)
        venue = getattr(command, "venue_order_id", None)
        stamp = timestamp_ns(row["create_time" if fill else "updated_time"], row["code"])
        return (
            (wanted is None or instrument_id(row["code"]) == wanted)
            and (venue is None or str(row["order_id"]) == str(venue))
            and (getattr(command, "start", None) is None or stamp >= command.start)
            and (getattr(command, "end", None) is None or stamp <= command.end)
        )

    async def _generate_order_status_reports(self, command: Any) -> list[OrderStatusReport]:
        values = await self._trade_rows("order_list_query", refresh_cache=True)
        if getattr(command, "start", None) is not None:
            start, end = self._history_dates(command)
            values += await self._trade_rows("history_order_list_query", start=start, end=end)
        result: dict[str, OrderStatusReport] = {}
        for row in await self._equity_rows(values):
            if self._report_filter(row, command):
                await self._ensure(instrument_id(row["code"]))
                try:
                    report = self._order_report(row)
                except UnsupportedOperation:
                    self.diagnostics.append(
                        {
                            "kind": "unsupported_external_order_type_omitted",
                            "venue_order_id": str(row["order_id"]),
                            "code": row["code"],
                        }
                    )
                    continue
                if not getattr(command, "open_only", False) or report.order_status not in TERMINAL:
                    prior = result.get(str(row["order_id"]))
                    if prior is None or report.ts_last >= prior.ts_last:
                        result[str(row["order_id"])] = report
        return list(result.values())

    async def _generate_order_status_report(self, command: Any) -> OrderStatusReport | None:
        venue = getattr(command, "venue_order_id", None)
        client = getattr(command, "client_order_id", None)
        if venue is None and client:
            venue = self._venues.get(str(client))
        for report in await self._generate_order_status_reports(command):
            if (
                venue
                and str(report.venue_order_id) == str(venue)
                or client
                and report.client_order_id == client
            ):
                return report
        return None

    def _history_dates(self, command: Any) -> tuple[str, str]:
        from .mapping import local_datetime

        symbol = "AAPL.US" if self._futu_config.trade_market == "US" else "00700.HK"
        start = local_datetime(command.start, symbol).date().isoformat()
        end = (
            local_datetime(
                command.end
                if getattr(command, "end", None) is not None
                else self.clock.timestamp_ns(),
                symbol,
            )
            .date()
            .isoformat()
        )
        return start, end

    async def _generate_fill_reports(self, command: Any) -> list[FillReport]:
        values = await self._trade_rows("deal_list_query", refresh_cache=True)
        if getattr(command, "start", None) is not None:
            start, end = self._history_dates(command)
            values += await self._trade_rows("history_deal_list_query", start=start, end=end)
        result = {}
        for row in await self._equity_rows(values):
            if self._report_filter(row, command, fill=True):
                await self._ensure(instrument_id(row["code"]))
                result[str(row["deal_id"])] = self._fill_report(row)
        if result:
            self.diagnostics.append(
                {"kind": "fill_report_commission_zero_placeholder", "count": len(result)}
            )
        return list(result.values())

    async def _generate_position_status_reports(self, command: Any) -> list[PositionStatusReport]:
        values = await self._equity_rows(
            await self._trade_rows("position_list_query", refresh_cache=True)
        )
        return [
            self._position_report(row)
            for row in values
            if getattr(command, "instrument_id", None) is None
            or instrument_id(row["code"]) == command.instrument_id
        ]
