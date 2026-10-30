from __future__ import annotations

import asyncio
import hashlib
import threading
import time
from collections import OrderedDict
from datetime import datetime, timedelta
from datetime import time as daytime
from typing import Any
from uuid import uuid4

from nautilus_trader.live import (
    BarsResponse,
    BookDeltasResponse,
    BookDepthResponse,
    BookResponse,
    InstrumentResponse,
    InstrumentsResponse,
    QuotesResponse,
    TradesResponse,
)
from nautilus_trader.live.clients import DataClient
from nautilus_trader.model import (
    AggressorSide,
    BookAction,
    BookOrder,
    BookType,
    CustomData,
    OrderBook,
    OrderBookDelta,
    OrderBookDeltas,
    OrderBookDepth,
    OrderSide,
    QuoteTick,
    RecordFlag,
    TradeId,
    TradeTick,
)

from .config import FutuDataClientConfig
from .errors import ConfigurationError, DataConflict, UnsupportedOperation
from .evidence import Evidence
from .history import autype, bar_period, fingerprint, history_rows, make_bar
from .instruments import FutuInstrumentProvider
from .lifecycle import ConnectionState, ManagedConnection
from .mapping import decimal, local_datetime, price, quantity, rows, timestamp_ns, timezone
from .source import FUTU_SOURCE, FutuSourceEvent
from .symbols import VENUE, canonical_symbol, from_instrument_id, instrument_id, provider_symbol
from .transport import Callback

# Process-wide guard is deliberately stricter than a node-local guard. Claims are
# retained for this process; a single NT BarType must never silently change meaning.
_BAR_POLICIES: dict[str, tuple[str, str]] = {}
_POLICY_LOCK = threading.Lock()


def _command_values(command: Any, plural: str, singular: str) -> tuple[Any, ...]:
    """Return rc8 batch fields while retaining compatibility with rc7 test doubles."""
    values = getattr(command, plural, None)
    if values is not None:
        return tuple(values)
    value = getattr(command, singular, None)
    return (value,) if value is not None else ()


class _RequestView:
    """Expose one member of an rc8 batch request to the existing single-item path."""

    def __init__(self, request: Any, **values: Any) -> None:
        self._request = request
        self.__dict__.update(values)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._request, name)


class FutuDataClient(ManagedConnection, DataClient):
    instrument_provider: Any

    def __init__(
        self,
        *,
        config: FutuDataClientConfig,
        context_factory: Any = None,
        instrument_provider: FutuInstrumentProvider | None = None,
        **kwargs: Any,
    ) -> None:
        provider = instrument_provider or FutuInstrumentProvider(
            config.instrument_provider, currency_overrides=config.currency_overrides
        )
        super().__init__(config=config, venue=VENUE, instrument_provider=provider, **kwargs)
        self._init_connection(config, context_factory)
        self._adjustment, self._trade_session = config.adjustment, config.session
        self._evidence = Evidence(config.evidence_dir)
        self._leases: dict[tuple[str, str], set[str]] = {}
        self._active: dict[tuple[str, str], float] = {}
        self._unsubscribe_tasks: dict[tuple[str, str], Any] = {}
        self._quotes: set[str] = set()
        self._trades: set[str] = set()
        self._depths: dict[str, int] = {}
        self._deltas: set[str] = set()
        self._bars: dict[str, Any] = {}
        self._bar_jobs: dict[str, Any] = {}
        self._bar_locks: dict[str, asyncio.Lock] = {}
        self._poll_task: Any = None
        self._seen_trades: OrderedDict[tuple, tuple] = OrderedDict()
        self._last_bars: OrderedDict[tuple[str, int], tuple] = OrderedDict()
        self._last_bar_ns: dict[str, int] = {}
        self._calendar: dict[tuple[str, str], str | None] = {}

    async def _after_connect(self) -> None:
        self._active.clear()
        await self.instrument_provider.initialize()
        for value in self.instrument_provider.list_all():
            self._handle_instrument(value)
        for (symbol, subtype), consumers in list(self._leases.items()):
            if consumers:
                await self._activate(symbol, subtype)
        if self._poll_task is None or self._poll_task.done():
            self._poll_task = self.create_task(self._poll_bars(), "futu-bar-poll")
        for key, bar_type in self._bars.items():
            if key in self._last_bar_ns:
                await self._backfill_bars(bar_type, self._last_bar_ns[key])
        for symbol in self._trades:
            # The endpoint only exposes recent ticks. It cannot certify gap completion.
            if self._generation > 1:
                await self._recent_trades(symbol, 1000, publish=True)
                self.diagnostics.append({"kind": "trade_gap_coverage_unverified", "symbol": symbol})

    async def _disconnect(self) -> None:
        for task in [self._poll_task, *self._bar_jobs.values(), *self._unsubscribe_tasks.values()]:
            if task:
                task.cancel()
        await asyncio.gather(
            *[
                task
                for task in [
                    self._poll_task,
                    *self._bar_jobs.values(),
                    *self._unsubscribe_tasks.values(),
                ]
                if task
            ],
            return_exceptions=True,
        )
        self._poll_task = None
        self._bar_jobs.clear()
        self._unsubscribe_tasks.clear()
        await super()._disconnect()

    async def _quote_call(self, operation: str, **kwargs: Any) -> tuple[Any, int, Any]:
        if self._session is None:
            raise RuntimeError("OpenD is not connected")
        session = self._session
        try:
            payload, received, key = await session.quote.call(operation, **kwargs)
        except TimeoutError:
            self._disruption("quote_request_timeout")
            raise
        record = rows(payload) if hasattr(payload, "to_dict") else payload
        self._evidence.record(operation, kwargs, record, received)
        if session is not self._session:
            raise ConnectionError("response belongs to an obsolete connection")
        return payload, received, key

    async def _ensure(self, symbol: str) -> Any:
        if canonical_symbol(symbol).endswith(".HK") and self._trade_session != "RTH":
            raise UnsupportedOperation(
                "ETH/ALL session selection is supported only for US equities"
            )
        identifier = instrument_id(symbol)
        value = self.instrument_provider.find(identifier)
        if value is None:
            await self.instrument_provider.load_symbols_async([symbol])
            value = self.instrument_provider.find(identifier)
        if value is None:
            raise ConfigurationError("instrument loading did not return a definition")
        self._handle_instrument(value)
        return value

    async def _activate(self, symbol: str, subtype: str) -> None:
        key = (symbol, subtype)
        if key not in self._active:
            await self._quote_call(
                "subscribe",
                code_list=[provider_symbol(symbol)],
                subtype_list=[subtype],
                session=self._trade_session,
                is_first_push=True,
            )
            self._active[key] = time.monotonic()

    async def _lease(self, symbol: str, subtype: str, consumer: str) -> None:
        await self._ensure(symbol)
        key = (symbol, subtype)
        self._leases.setdefault(key, set()).add(consumer)
        task = self._unsubscribe_tasks.pop(key, None)
        if task:
            task.cancel()
        try:
            await self._activate(symbol, subtype)
        except BaseException:
            self._leases[key].discard(consumer)
            raise

    async def _release(self, symbol: str, subtype: str, consumer: str) -> None:
        key = (symbol, subtype)
        self._leases.get(key, set()).discard(consumer)
        if self._leases.get(key) or key not in self._active or key in self._unsubscribe_tasks:
            return

        async def unsubscribe() -> None:
            try:
                await asyncio.sleep(max(0.0, self._active.get(key, 0) + 61.0 - time.monotonic()))
                if not self._leases.get(key) and self._session:
                    await self._quote_call(
                        "unsubscribe", code_list=[provider_symbol(symbol)], subtype_list=[subtype]
                    )
                    self._active.pop(key, None)
            finally:
                self._unsubscribe_tasks.pop(key, None)

        self._unsubscribe_tasks[key] = self.create_task(unsubscribe(), "futu-delayed-unsubscribe")

    def _claim_bar(self, bar_type: Any) -> None:
        bar_period(bar_type)
        policy = (self._adjustment, self._trade_session)
        with _POLICY_LOCK:
            previous = _BAR_POLICIES.get(str(bar_type))
            if previous is not None and previous != policy:
                raise ConfigurationError(
                    "one BarType cannot mix adjustment or trading-session policies; use a new process and separate Catalog"
                )
            _BAR_POLICIES[str(bar_type)] = policy

    async def _subscribe_quotes(self, command: Any) -> None:
        for instrument_id_value in _command_values(command, "instrument_ids", "instrument_id"):
            symbol = from_instrument_id(instrument_id_value)
            self._quotes.add(symbol)
            await self._lease(symbol, "ORDER_BOOK", "quotes")

    async def _subscribe(self, command: Any) -> None:
        if command.data_type != FUTU_SOURCE:
            raise UnsupportedOperation("only FutuSourceEvent custom data is provided")

    async def _unsubscribe(self, command: Any) -> None:
        return

    def _source(
        self,
        symbol: str,
        category: str,
        payload: dict,
        stamp: int,
        received: int,
        native_id: str | None = None,
        time_basis: str = "provider_event_time",
    ) -> None:
        self._handle_data(
            CustomData(
                FUTU_SOURCE,
                FutuSourceEvent(
                    str(instrument_id(symbol)),
                    category,
                    native_id,
                    dict(payload),
                    time_basis,
                    stamp,
                    received,
                ),
            )
        )

    async def _unsubscribe_quotes(self, command: Any) -> None:
        for instrument_id_value in _command_values(command, "instrument_ids", "instrument_id"):
            symbol = from_instrument_id(instrument_id_value)
            self._quotes.discard(symbol)
            await self._release(symbol, "ORDER_BOOK", "quotes")

    async def _subscribe_trades(self, command: Any) -> None:
        for instrument_id_value in _command_values(command, "instrument_ids", "instrument_id"):
            symbol = from_instrument_id(instrument_id_value)
            self._trades.add(symbol)
            await self._lease(symbol, "TICKER", "trades")

    async def _unsubscribe_trades(self, command: Any) -> None:
        for instrument_id_value in _command_values(command, "instrument_ids", "instrument_id"):
            symbol = from_instrument_id(instrument_id_value)
            self._trades.discard(symbol)
            await self._release(symbol, "TICKER", "trades")

    def _check_book_type(self, command: Any) -> None:
        if getattr(command, "book_type", BookType.L2_MBP) != BookType.L2_MBP:
            raise UnsupportedOperation(
                "OpenD provides L2 price-level snapshots, not L3 incremental orders"
            )

    async def _subscribe_book_depth(self, command: Any) -> None:
        self._check_book_type(command)
        for instrument_id_value in _command_values(command, "instrument_ids", "instrument_id"):
            symbol = from_instrument_id(instrument_id_value)
            self._depths[symbol] = getattr(command, "depth", 10) or 10
            await self._lease(symbol, "ORDER_BOOK", "depth")

    async def _unsubscribe_book_depth(self, command: Any) -> None:
        for instrument_id_value in _command_values(command, "instrument_ids", "instrument_id"):
            symbol = from_instrument_id(instrument_id_value)
            self._depths.pop(symbol, None)
            await self._release(symbol, "ORDER_BOOK", "depth")

    async def _subscribe_book_deltas(self, command: Any) -> None:
        self._check_book_type(command)
        for instrument_id_value in _command_values(command, "instrument_ids", "instrument_id"):
            symbol = from_instrument_id(instrument_id_value)
            self._deltas.add(symbol)
            await self._lease(symbol, "ORDER_BOOK", "deltas")

    async def _unsubscribe_book_deltas(self, command: Any) -> None:
        for instrument_id_value in _command_values(command, "instrument_ids", "instrument_id"):
            symbol = from_instrument_id(instrument_id_value)
            self._deltas.discard(symbol)
            await self._release(symbol, "ORDER_BOOK", "deltas")

    async def _subscribe_bars(self, command: Any) -> None:
        for bar_type in _command_values(command, "bar_types", "bar_type"):
            self._claim_bar(bar_type)
            key = str(bar_type)
            self._bars[key] = bar_type
            await self._lease(from_instrument_id(bar_type.instrument_id), bar_period(bar_type), key)
            await self._refresh_bars(bar_type)

    async def _unsubscribe_bars(self, command: Any) -> None:
        for bar_type in _command_values(command, "bar_types", "bar_type"):
            key = str(bar_type)
            self._bars.pop(key, None)
            await self._release(
                from_instrument_id(bar_type.instrument_id), bar_period(bar_type), key
            )

    async def _request_instrument(self, request: Any) -> None:
        for instrument_id_value in _command_values(request, "instrument_ids", "instrument_id"):
            value = await self._ensure(from_instrument_id(instrument_id_value))
            self._handle_response(
                InstrumentResponse(
                    self.client_id,
                    value.id,
                    value,
                    request.request_id,
                    self.clock.timestamp_ns(),
                )
            )

    async def _request_instruments(self, request: Any) -> None:
        await self.instrument_provider.initialize()
        self._handle_response(
            InstrumentsResponse(
                self.client_id,
                VENUE,
                self.instrument_provider.list_all(),
                request.request_id,
                self.clock.timestamp_ns(),
            )
        )

    async def _subscribe_instrument(self, command: Any) -> None:
        for instrument_id_value in _command_values(command, "instrument_ids", "instrument_id"):
            await self._ensure(from_instrument_id(instrument_id_value))

    async def _subscribe_instruments(self, command: Any) -> None:
        await self.instrument_provider.initialize()
        for value in self.instrument_provider.list_all():
            self._handle_instrument(value)

    async def _unsubscribe_instrument(self, command: Any) -> None:
        return

    async def _unsubscribe_instruments(self, command: Any) -> None:
        return

    async def _process_callback(self, event: Callback) -> None:
        self._evidence.record(
            "push_" + event.kind, {"generation": event.generation}, event.payload, event.received_ns
        )
        if event.kind == "book":
            symbol = canonical_symbol(event.payload["code"])
            if symbol in self._quotes or symbol in self._depths or symbol in self._deltas:
                await self._publish_book(symbol, event.payload, event.received_ns)
        elif event.kind == "trades":
            for row in rows(event.payload):
                symbol = canonical_symbol(row["code"])
                if symbol in self._trades:
                    tick = await self._trade_tick(symbol, row, event.received_ns, deduplicate=True)
                    if tick:
                        self._handle_data(tick)
        elif event.kind == "bar_trigger":
            for row in rows(event.payload):
                for key, bar_type in list(self._bars.items()):
                    if (
                        canonical_symbol(row["code"]) == str(bar_type.instrument_id.symbol)
                        and row["k_type"] == bar_period(bar_type)
                        and (key not in self._bar_jobs or self._bar_jobs[key].done())
                    ):
                        self._bar_jobs[key] = self.create_task(
                            self._refresh_bars(bar_type), "futu-bar-refresh"
                        )

    async def _book(
        self, symbol: str, payload: dict, received: int
    ) -> tuple[OrderBookDepth, OrderBookDeltas, QuoteTick | None]:
        if canonical_symbol(payload["code"]) != symbol:
            raise DataConflict("book response contains another instrument")
        instrument = await self._ensure(symbol)
        identifier = instrument.id
        stamps = []
        for name in ("svr_recv_time_bid", "svr_recv_time_ask"):
            value = payload.get(name)
            if value and str(value) not in {"0", "N/A", ""}:
                stamps.append(timestamp_ns(value, symbol))
        stamp = max(stamps) if stamps else received
        self._source(
            symbol,
            "order_book",
            payload,
            stamp,
            received,
            time_basis="opend_server_receipt" if stamps else "local_receipt_proxy",
        )
        if not stamps:
            self.diagnostics.append(
                {
                    "kind": "book_local_receipt_timestamp_proxy",
                    "symbol": symbol,
                    "received_ns": received,
                }
            )
        sides = []
        counts = []
        for name, side in (("Bid", OrderSide.BUY), ("Ask", OrderSide.SELL)):
            orders, numbers = [], []
            previous = None
            for index, row in enumerate(payload.get(name, [])):
                if decimal(row[0]) <= 0 or decimal(row[1]) <= 0:
                    continue
                current = decimal(row[0])
                if previous is not None and (
                    current >= previous if side == OrderSide.BUY else current <= previous
                ):
                    raise DataConflict("book levels are duplicated or out of price order")
                previous = current
                orders.append(
                    BookOrder(
                        side, price(row[0], instrument.price_precision), quantity(row[1]), index + 1
                    )
                )
                numbers.append(int(row[2]))
            sides.append(orders)
            counts.append(numbers)
        snapshot_flag = RecordFlag.F_SNAPSHOT.value
        depth = OrderBookDepth(
            identifier, sides[0], sides[1], counts[0], counts[1], snapshot_flag, 0, stamp, received
        )
        deltas = [OrderBookDelta.clear(identifier, 0, stamp, received)]
        for order in sides[0] + sides[1]:
            deltas.append(
                OrderBookDelta(identifier, BookAction.ADD, order, snapshot_flag, 0, stamp, received)
            )
        last = deltas[-1]
        deltas[-1] = OrderBookDelta(
            identifier,
            last.action,
            last.order,
            snapshot_flag | RecordFlag.F_LAST.value,
            0,
            stamp,
            received,
        )
        tick = None
        if sides[0] and sides[1] and decimal(sides[0][0].price) <= decimal(sides[1][0].price):
            bid, ask = sides[0][0], sides[1][0]
            tick = QuoteTick(identifier, bid.price, ask.price, bid.size, ask.size, stamp, received)
        return depth, OrderBookDeltas(identifier, deltas), tick

    async def _publish_book(self, symbol: str, payload: dict, received: int) -> None:
        depth, deltas, tick = await self._book(symbol, payload, received)
        if symbol in self._depths:
            limit = self._depths[symbol]
            self._handle_data(
                OrderBookDepth(
                    depth.instrument_id,
                    depth.bids[:limit],
                    depth.asks[:limit],
                    depth.bid_counts[:limit],
                    depth.ask_counts[:limit],
                    depth.flags,
                    0,
                    depth.ts_event,
                    received,
                )
            )
        if symbol in self._deltas:
            self._handle_data(deltas)
        if symbol in self._quotes and tick:
            self._handle_data(tick)

    async def _trade_tick(
        self, symbol: str, row: dict, received: int, *, deduplicate: bool
    ) -> TradeTick | None:
        instrument = await self._ensure(symbol)
        stamp = timestamp_ns(row["time"], symbol)
        identity = row.get("sequence")
        content = (
            row["time"],
            str(row["price"]),
            str(row["volume"]),
            row.get("ticker_direction"),
            row.get("type"),
        )
        if identity is not None and str(identity) not in {"", "0", "N/A"}:
            key = (symbol, local_datetime(stamp, symbol).date().isoformat(), str(identity))
            if deduplicate and key in self._seen_trades:
                if self._seen_trades[key] != content:
                    raise DataConflict("provider sequence reused with different trade content")
                return None
            if deduplicate:
                self._seen_trades[key] = content
                if len(self._seen_trades) > 200_000:
                    self._seen_trades.popitem(last=False)
            trade_id = "FUTU" + hashlib.blake2b("|".join(key).encode(), digest_size=16).hexdigest()
        else:
            # Preserve multiplicity. A generated ID is explicitly not a source identity.
            trade_id = "U-" + uuid4().hex
        side = {"BUY": AggressorSide.BUY, "SELL": AggressorSide.SELL}.get(
            str(row.get("ticker_direction", "")), AggressorSide.NO_AGGRESSOR
        )
        if decimal(row["volume"]) <= 0:
            raise ValueError("zero/nonpositive trade quantity")
        self._source(symbol, "trade", row, stamp, received, trade_id)
        return TradeTick(
            instrument.id,
            price(row["price"], instrument.price_precision),
            quantity(row["volume"]),
            side,
            TradeId(trade_id),
            stamp,
            received,
        )

    async def _recent_trades(self, symbol: str, limit: int, *, publish: bool = False) -> list:
        payload, received, _ = await self._quote_call(
            "get_rt_ticker", code=provider_symbol(symbol), num=min(limit or 1000, 1000)
        )
        result = []
        for row in rows(payload):
            if canonical_symbol(row["code"]) != symbol:
                raise DataConflict("ticker response contains another instrument")
            tick = await self._trade_tick(symbol, row, received, deduplicate=publish)
            if tick:
                result.append(tick)
                if publish:
                    self._handle_data(tick)
        return result

    async def _request_trades(self, request: Any) -> None:
        for instrument_id_value in _command_values(request, "instrument_ids", "instrument_id"):
            await FutuDataClient._request_trades_one(
                self, _RequestView(request, instrument_id=instrument_id_value)
            )

    async def _request_trades_one(self, request: Any) -> None:
        symbol = from_instrument_id(request.instrument_id)
        token = "request:" + str(request.request_id)
        await self._lease(symbol, "TICKER", token)
        try:
            data = await self._recent_trades(symbol, request.limit or 1000)
            data = [
                tick
                for tick in data
                if (request.start_ns is None or tick.ts_event >= request.start_ns)
                and (request.end_ns is None or tick.ts_event <= request.end_ns)
            ]
            self._handle_response(
                TradesResponse(
                    self.client_id,
                    request.instrument_id,
                    data,
                    request.request_id,
                    self.clock.timestamp_ns(),
                    None,
                    None,
                    {
                        "coverage": "recent_limited_ticks_only",
                        "requested_start_ns": request.start_ns,
                        "requested_end_ns": request.end_ns,
                    },
                )
            )
        finally:
            await self._release(symbol, "TICKER", token)

    async def _request_quotes(self, request: Any) -> None:
        for instrument_id_value in _command_values(request, "instrument_ids", "instrument_id"):
            await FutuDataClient._request_quotes_one(
                self, _RequestView(request, instrument_id=instrument_id_value)
            )

    async def _request_quotes_one(self, request: Any) -> None:
        symbol = from_instrument_id(request.instrument_id)
        token = "request:" + str(request.request_id)
        await self._lease(symbol, "ORDER_BOOK", token)
        try:
            payload, received, _ = await self._quote_call(
                "get_order_book", code=provider_symbol(symbol), num=1
            )
            _, _, tick = await self._book(symbol, payload, received)
            data = (
                [tick]
                if tick
                and (request.start_ns is None or tick.ts_event >= request.start_ns)
                and (request.end_ns is None or tick.ts_event <= request.end_ns)
                else []
            )
            self._handle_response(
                QuotesResponse(
                    self.client_id,
                    request.instrument_id,
                    data,
                    request.request_id,
                    received,
                    None,
                    None,
                    {"coverage": "current_snapshot_only"},
                )
            )
        finally:
            await self._release(symbol, "ORDER_BOOK", token)

    async def _request_book(self, request: Any, kind: str) -> None:
        for instrument_id_value in _command_values(request, "instrument_ids", "instrument_id"):
            await FutuDataClient._request_book_one(
                self, _RequestView(request, instrument_id=instrument_id_value), kind
            )

    async def _request_book_one(self, request: Any, kind: str) -> None:
        self._check_book_type(request)
        symbol = from_instrument_id(request.instrument_id)
        token = "request:" + str(request.request_id)
        await self._lease(symbol, "ORDER_BOOK", token)
        try:
            payload, received, _ = await self._quote_call(
                "get_order_book",
                code=provider_symbol(symbol),
                num=getattr(request, "depth", 10) or 10,
            )
            depth, deltas, _ = await self._book(symbol, payload, received)
            response: Any
            if kind == "depth":
                response = BookDepthResponse(
                    self.client_id, request.instrument_id, [depth], request.request_id, received
                )
            elif kind == "deltas":
                response = BookDeltasResponse(
                    self.client_id,
                    request.instrument_id,
                    deltas.deltas,
                    request.request_id,
                    received,
                )
            else:
                book = OrderBook(request.instrument_id, BookType.L2_MBP)
                book.apply_deltas(deltas)
                response = BookResponse(
                    self.client_id, request.instrument_id, book, request.request_id, received
                )
            self._handle_response(response)
        finally:
            await self._release(symbol, "ORDER_BOOK", token)

    async def _request_book_snapshot(self, request: Any) -> None:
        await self._request_book(request, "snapshot")

    async def _request_book_depth(self, request: Any) -> None:
        await self._request_book(request, "depth")

    async def _request_book_deltas(self, request: Any) -> None:
        await self._request_book(request, "deltas")

    async def _request_bars(self, request: Any) -> None:
        for bar_type in _command_values(request, "bar_types", "bar_type"):
            await FutuDataClient._request_bars_one(self, _RequestView(request, bar_type=bar_type))

    async def _request_bars_one(self, request: Any) -> None:
        self._claim_bar(request.bar_type)
        symbol = from_instrument_id(request.bar_type.instrument_id)
        await self._ensure(symbol)
        if request.start_ns is not None:
            values = await self._history_rows(
                symbol,
                bar_period(request.bar_type),
                request.start_ns,
                request.end_ns if request.end_ns is not None else self.clock.timestamp_ns(),
                self._adjustment,
                self._trade_session,
            )
        else:
            token = "request:" + str(request.request_id)
            period = bar_period(request.bar_type)
            await self._lease(symbol, period, token)
            try:
                payload, received, _ = await self._quote_call(
                    "get_cur_kline",
                    code=provider_symbol(symbol),
                    num=min(request.limit or 1000, 1000),
                    ktype=period,
                    autype=autype(self._adjustment),
                )
                values = [(row, received) for row in rows(payload)]
            finally:
                await self._release(symbol, period, token)
        values = [
            (row, received)
            for row, received in values
            if await self._closed(symbol, row, bar_period(request.bar_type), received)
        ]
        data = [make_bar(request.bar_type, row, received) for row, received in values]
        data = [
            bar
            for bar in data
            if (request.start_ns is None or bar.ts_event >= request.start_ns)
            and (request.end_ns is None or bar.ts_event <= request.end_ns)
        ]
        if request.limit:
            data = data[-request.limit :]
        self._handle_response(
            BarsResponse(
                self.client_id,
                request.bar_type,
                data,
                request.request_id,
                self.clock.timestamp_ns(),
                None,
                None,
                {
                    "adjustment": self._adjustment,
                    "session": self._trade_session,
                    "requested_start_ns": request.start_ns,
                    "requested_end_ns": request.end_ns,
                },
            )
        )

    async def _closed(self, symbol: str, row: dict, period: str, received: int) -> bool:
        stamp = timestamp_ns(row["time_key"], symbol)
        start = local_datetime(stamp, symbol)
        now = local_datetime(received, symbol)
        if start.date() < now.date():
            return True
        if period != "K_DAY":
            end = start + timedelta(minutes=int(period[2:-1]))
            possible_hk_auction = symbol.endswith(".HK") and start.time() >= daytime(12)
            if not possible_hk_auction and received >= timestamp_ns(end, symbol) + 2_000_000_000:
                return True
        # Account for a shortened final intraday bucket, lunch break and half days
        # using OpenD's own trading-day metadata, never a research calendar.
        day = start.date().isoformat()
        key = (symbol, day)
        if key not in self._calendar:
            payload, _, _ = await self._quote_call(
                "request_trading_days",
                market=symbol.rsplit(".", 1)[1],
                start=day,
                end=day,
                code=provider_symbol(symbol),
            )
            self._calendar[key] = next(
                (r["trade_date_type"] for r in rows(payload) if str(r["time"]) == day), None
            )
        day_type = self._calendar[key]
        if day_type is None:
            return False
        is_us = symbol.endswith(".US")
        close = (
            (daytime(13) if is_us else daytime(12, 10))
            if day_type == "MORNING"
            else (daytime(16) if is_us else daytime(16, 10))
        )
        closing = datetime.combine(start.date(), close, timezone(symbol))
        if period != "K_DAY":
            regular_end = start + timedelta(minutes=int(period[2:-1]))
            auction_start = daytime(12) if day_type == "MORNING" else daytime(16)
            if start.time() < auction_start:
                closing = min(closing, regular_end)
        if not is_us and period != "K_DAY" and start.time() < daytime(12):
            closing = min(closing, datetime.combine(start.date(), daytime(12), timezone(symbol)))
        if is_us and self._trade_session != "RTH":
            # ALL may span the overnight session; only a new local date is decisive.
            return period != "K_DAY" and now >= start + timedelta(
                minutes=int(period[2:-1]), seconds=2
            )
        return now >= closing + timedelta(seconds=2)

    async def _refresh_bars(self, bar_type: Any) -> None:
        async with self._bar_locks.setdefault(str(bar_type), asyncio.Lock()):
            await self._refresh_bars_inner(bar_type)

    async def _refresh_bars_inner(self, bar_type: Any) -> None:
        if self._session is None:
            return
        symbol, period = from_instrument_id(bar_type.instrument_id), bar_period(bar_type)
        payload, received, _ = await self._quote_call(
            "get_cur_kline",
            code=provider_symbol(symbol),
            num=3,
            ktype=period,
            autype=autype(self._adjustment),
        )
        for row in rows(payload):
            if canonical_symbol(row["code"]) != symbol:
                raise DataConflict("realtime bar response contains another instrument")
            if self._futu_config.completed_bars_only and not await self._closed(
                symbol, row, period, received
            ):
                continue
            self._emit_bar(bar_type, row, received)

    def _emit_bar(self, bar_type: Any, row: dict, received: int) -> None:
        bar = make_bar(bar_type, row, received)
        key = (str(bar_type), bar.ts_event)
        content = fingerprint(row)
        if key in self._last_bars:
            if self._last_bars[key] != content:
                # rc8 Bar has no revision flag. Retain evidence and surface the
                # correction; do not silently overwrite an already published bar.
                self.diagnostics.append(
                    {
                        "kind": "provider_bar_revision",
                        "bar_type": str(bar_type),
                        "ts_event": bar.ts_event,
                        "received_ns": received,
                    }
                )
            return
        self._last_bars[key] = content
        if len(self._last_bars) > 100_000:
            self._last_bars.popitem(last=False)
        self._last_bar_ns[str(bar_type)] = max(
            bar.ts_event, self._last_bar_ns.get(str(bar_type), 0)
        )
        self._handle_data(bar)

    async def _backfill_bars(self, bar_type: Any, start_ns: int) -> None:
        symbol = from_instrument_id(bar_type.instrument_id)
        values = await self._history_rows(
            symbol,
            bar_period(bar_type),
            start_ns,
            self.clock.timestamp_ns(),
            self._adjustment,
            self._trade_session,
        )
        for row, received in values:
            if await self._closed(symbol, row, bar_period(bar_type), received):
                self._emit_bar(bar_type, row, received)

    async def _history_rows(
        self,
        symbol: str,
        period: str,
        start_ns: int,
        end_ns: int,
        adjustment: str,
        trade_session: str,
    ) -> list:
        session = self._session
        if session is None:
            raise ConnectionError("OpenD is not connected")
        try:
            result = await history_rows(
                session,
                symbol,
                period,
                start_ns,
                end_ns,
                adjustment,
                trade_session,
                evidence=self._evidence,
            )
        except TimeoutError:
            self._disruption("historical_request_timeout")
            raise
        if session is not self._session:
            raise ConnectionError("historical response belongs to an obsolete connection")
        return result

    async def _poll_bars(self) -> None:
        while True:
            await asyncio.sleep(self._futu_config.bar_poll_interval_secs)
            if self.connection_state != ConnectionState.CONNECTED:
                continue
            for key, bar_type in list(self._bars.items()):
                if key in self._bar_jobs and not self._bar_jobs[key].done():
                    continue
                try:
                    await self._refresh_bars(bar_type)
                except Exception as exc:
                    self.diagnostics.append(
                        {"kind": "bar_refresh_failed", "error_type": type(exc).__name__}
                    )
                    self._disruption("bar_refresh_failed")
                    break
