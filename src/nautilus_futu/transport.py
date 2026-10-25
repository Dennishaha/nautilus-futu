from __future__ import annotations

import asyncio
import contextlib
import queue
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any

from .config import FutuConnectionSettings
from .errors import ProviderError, RequestUncertain
from .mapping import rows
from .rate_limit import limiter


@dataclass(frozen=True)
class Callback:
    kind: str
    payload: Any
    received_ns: int
    generation: int


class CallbackBridge:
    """Bound callbacks before scheduling the owner loop; at most one pending wakeup."""

    def __init__(self, size: int, generation: int, overflow: Callable[[], None]) -> None:
        self.queue: queue.Queue[Callback] = queue.Queue(size)
        self.loop = asyncio.get_running_loop()
        self.ready = asyncio.Event()
        self.generation = generation
        self.overflow = overflow
        self.closed = False
        self._wake_pending = False
        self._overflowed = False
        self._lock = threading.Lock()

    def push(self, kind: str, payload: Any) -> None:
        with self._lock:
            if self.closed:
                return
            try:
                self.queue.put_nowait(Callback(kind, payload, time.time_ns(), self.generation))
            except queue.Full:
                if not self._overflowed:
                    self._overflowed = True
                    self.loop.call_soon_threadsafe(self.overflow)
                return
            if not self._wake_pending:
                self._wake_pending = True
                self.loop.call_soon_threadsafe(self._wake)

    def _wake(self) -> None:
        with self._lock:
            self._wake_pending = False
        self.ready.set()

    async def get(self) -> Callback:
        while True:
            try:
                return self.queue.get_nowait()
            except queue.Empty:
                self.ready.clear()
                # A producer waking after this clear will set ready on the owner loop.
                if not self.queue.empty():
                    continue
                await self.ready.wait()

    def close(self) -> None:
        with self._lock:
            self.closed = True


class SDKWorker:
    """One ordered SDK lane. A timeout poisons it until close, including late results."""

    def __init__(self, settings: FutuConnectionSettings, factory: Callable[[], Any]) -> None:
        self.settings = settings
        self.factory = factory
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="futu-opend")
        self.context: Any = None
        self.poisoned = False
        self.closed = False
        self._operation_lock = asyncio.Lock()

    async def open(self) -> None:
        def create() -> None:
            self.context = self.factory()
            self.context.set_sync_query_connect_timeout(self.settings.request_timeout_secs)

        await self._run(create)

    async def _run(self, operation: Callable[[], Any]) -> Any:
        async with self._operation_lock:
            return await self._run_unlocked(operation)

    async def _run_unlocked(self, operation: Callable[[], Any]) -> Any:
        if self.closed or self.poisoned:
            raise RequestUncertain("OpenD worker is unavailable; recovery required")
        future = asyncio.get_running_loop().run_in_executor(self.executor, operation)
        try:
            return await asyncio.wait_for(
                asyncio.shield(future), self.settings.request_timeout_secs
            )
        except (TimeoutError, asyncio.CancelledError) as exc:
            self.poisoned = True
            # Retrieve late errors without treating a late submission as a failed submission.
            future.add_done_callback(
                lambda done: done.exception() if not done.cancelled() else None
            )
            if isinstance(exc, asyncio.CancelledError):
                raise
            raise RequestUncertain("OpenD request timed out; outcome may be pending") from None

    async def call(self, operation: str, **kwargs: Any) -> tuple[Any, int, Any]:
        rate = limiter(
            self.settings.host, self.settings.port, self.settings.security_firm, operation
        )
        # Subsequent history pages do not consume the first-page API quota.
        if operation != "request_history_kline" or not kwargs.get("page_req_key"):
            await rate.acquire()
        try:
            response = await self._run(lambda: getattr(self.context, operation)(**kwargs))
        except (RequestUncertain, asyncio.CancelledError):
            raise
        except Exception:
            if operation in {"place_order", "modify_order"}:
                self.poisoned = True
                raise RequestUncertain(
                    "SDK raised during an order request; reconcile outcome"
                ) from None
            raise ProviderError(operation, "SDK_EXCEPTION") from None
        received_ns = time.time_ns()
        if not isinstance(response, tuple) or len(response) < 2:
            if operation in {"place_order", "modify_order"}:
                self.poisoned = True
                raise RequestUncertain("order response shape is invalid; reconcile outcome")
            raise ProviderError(operation, "SHAPE")
        if response[0] != 0:
            message = str(response[1]).lower()
            if operation in {"place_order", "modify_order"} and any(
                word in message
                for word in (
                    "timeout",
                    "timed out",
                    "disconnect",
                    "not connected",
                    "connection",
                    "socket",
                    "超时",
                    "网络",
                    "断开",
                    "连接失败",
                )
            ):
                self.poisoned = True
                raise RequestUncertain("OpenD order request transport failed; outcome is uncertain")
            raise ProviderError(operation, response[0])
        return response[1], received_ns, response[2] if len(response) > 2 else None

    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True

        def close_context() -> None:
            if self.context is not None:
                self.context.close()
                self.context = None

        # Queue close behind an outstanding native call; never race the context with that call.
        future = asyncio.get_running_loop().run_in_executor(self.executor, close_context)
        try:
            await asyncio.wait_for(asyncio.shield(future), self.settings.request_timeout_secs)
        except TimeoutError:
            future.add_done_callback(
                lambda done: done.exception() if not done.cancelled() else None
            )
        finally:
            self.executor.shutdown(wait=False)


class OpenDContextFactory:
    """The only production constructor of provider contexts in this package."""

    @staticmethod
    def _handler(base: type, kind: str, bridge: CallbackBridge) -> Any:
        class Handler(base):
            def on_recv_rsp(self, response: Any) -> Any:
                result, payload = super().on_recv_rsp(response)
                if result == 0:
                    data = rows(payload) if hasattr(payload, "to_dict") else payload
                    if kind in {"orders", "fills"}:
                        # The SDK DataFrame discards the account in the protocol
                        # header, while the context auto-subscribes multiple accounts.
                        header = response.s2c.header
                        for row in data:
                            row["_opend_acc_id"] = int(header.accID)
                    bridge.push(kind, data)
                else:
                    bridge.push("provider_error", {"kind": kind, "code": result})
                return result, payload

        return Handler()

    def quote(self, settings: FutuConnectionSettings, bridge: CallbackBridge) -> Any:
        import futu as ft

        context = ft.OpenQuoteContext(
            host=settings.host,
            port=settings.port,
            is_encrypt=settings.is_encrypt,
            is_async_connect=True,
            security_firm=settings.security_firm,
        )
        for base, kind in (
            (ft.OrderBookHandlerBase, "book"),
            (ft.TickerHandlerBase, "trades"),
            (ft.CurKlineHandlerBase, "bar_trigger"),
            (ft.SysNotifyHandlerBase, "notify"),
        ):
            context.set_handler(self._handler(base, kind, bridge))
        return context

    def trade(self, settings: FutuConnectionSettings, bridge: CallbackBridge, market: str) -> Any:
        import futu as ft

        context = ft.OpenSecTradeContext(
            filter_trdmarket=market,
            host=settings.host,
            port=settings.port,
            is_encrypt=settings.is_encrypt,
            security_firm=settings.security_firm,
        )
        for base, kind in (
            (ft.TradeOrderHandlerBase, "orders"),
            (ft.TradeDealHandlerBase, "fills"),
            (ft.SysNotifyHandlerBase, "notify"),
        ):
            context.set_handler(self._handler(base, kind, bridge))
        return context


class OpenDSession:
    def __init__(
        self,
        settings: FutuConnectionSettings,
        bridge: CallbackBridge,
        factory: Any = None,
        trade_market: str | None = None,
    ) -> None:
        self.settings = settings
        self.bridge = bridge
        self.factory = factory or OpenDContextFactory()
        self.quote = SDKWorker(settings, lambda: self.factory.quote(settings, bridge))
        self.trade = (
            SDKWorker(settings, lambda: self.factory.trade(settings, bridge, trade_market))
            if trade_market
            else None
        )
        self.connection_id: Any = None
        self.trade_connection_id: Any = None

    async def open(self) -> None:
        try:
            await self.quote.open()
            await self.quote.call("get_global_state")
            self.connection_id = self.quote.context.get_sync_conn_id()
            if self.trade:
                await self.trade.open()
                self.trade_connection_id = self.trade.context.get_sync_conn_id()
        except BaseException:
            await self.close()
            raise

    async def healthy(self) -> bool:
        await self.quote.call("get_global_state")
        # An SDK-managed reconnect is also an epoch boundary. Rebuild state once.
        if self.quote.context.get_sync_conn_id() != self.connection_id:
            return False
        if self.trade:
            await self.trade.call("get_global_state")
            if self.trade.context.get_sync_conn_id() != self.trade_connection_id:
                return False
        return True

    async def close(self) -> None:
        self.bridge.close()
        for worker in (self.trade, self.quote):
            if worker:
                with contextlib.suppress(Exception):
                    await worker.close()
