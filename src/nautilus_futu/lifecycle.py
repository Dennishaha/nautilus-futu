from __future__ import annotations

import asyncio
import logging
from collections import deque
from enum import StrEnum
from typing import Any

from .transport import Callback, CallbackBridge, OpenDSession

logger = logging.getLogger(__name__)


class ConnectionState(StrEnum):
    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    CONNECTED = "CONNECTED"
    RECONNECTING = "RECONNECTING"
    FAILED = "FAILED"


class ManagedConnection:
    """Lifecycle mixin; subclasses own all NT conversion and output on their loop."""

    clock: Any
    instrument_provider: Any
    create_task: Any

    def _init_connection(
        self, config: Any, context_factory: Any = None, trade_market: str | None = None
    ) -> None:
        self._futu_config = config
        self._context_factory = context_factory
        self._trade_market = trade_market
        self._session: OpenDSession | None = None
        self._bridge: CallbackBridge | None = None
        self._generation = 0
        self._stopped = True
        self._consumer: Any = None
        self._health_task: Any = None
        self._reconnect_task: Any = None
        self.connection_state = ConnectionState.DISCONNECTED
        self.diagnostics: deque[dict[str, Any]] = deque(maxlen=1000)

    async def _connect(self) -> None:
        self._stopped = False
        self.connection_state = ConnectionState.CONNECTING
        try:
            await self._open_session()
            self.connection_state = ConnectionState.CONNECTED
            self._health_task = self.create_task(self._health_loop(), "futu-health")
        except BaseException:
            await self._disconnect()
            self.connection_state = ConnectionState.FAILED
            raise

    async def _open_session(self) -> None:
        self._generation += 1
        bridge = CallbackBridge(
            self._futu_config.connection.queue_size,
            self._generation,
            lambda: self._disruption("callback_queue_overflow"),
        )
        self._bridge = bridge
        self._session = OpenDSession(
            self._futu_config.connection, bridge, self._context_factory, self._trade_market
        )
        await self._session.open()
        self.instrument_provider.set_session(self._session)
        await self._after_connect()
        if bridge._overflowed or self._bridge is not bridge or bridge.closed:
            raise ConnectionError("OpenD startup callback coverage is incomplete")
        self._consumer = self.create_task(self._consume(bridge), "futu-callbacks")

    async def _after_connect(self) -> None:
        raise NotImplementedError

    async def _process_callback(self, event: Callback) -> None:
        raise NotImplementedError

    async def _consume(self, bridge: CallbackBridge) -> None:
        while True:
            event = await bridge.get()
            try:
                if event.generation != self._generation:
                    continue
                if event.kind == "provider_error":
                    self._disruption("provider_callback_error")
                elif event.kind == "notify":
                    self.diagnostics.append(
                        {
                            "kind": "provider_notification",
                            "received_ns": event.received_ns,
                            "payload": event.payload,
                        }
                    )
                else:
                    await self._process_callback(event)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.diagnostics.append(
                    {
                        "kind": "callback_failure",
                        "error_type": type(exc).__name__,
                        "received_ns": event.received_ns,
                    }
                )
                self._disruption("callback_failure")
            finally:
                bridge.queue.task_done()

    def _disruption(self, reason: str) -> None:
        if self._stopped:
            return
        self.diagnostics.append(
            {"kind": "data_gap", "reason": reason, "start_ns": self.clock.timestamp_ns()}
        )
        if self.connection_state == ConnectionState.CONNECTING:
            return
        self.connection_state = ConnectionState.RECONNECTING
        if self._reconnect_task is None or self._reconnect_task.done():
            self._reconnect_task = self.create_task(self._recover(), "futu-reconnect")

    async def _health_loop(self) -> None:
        while True:
            await asyncio.sleep(self._futu_config.connection.health_interval_secs)
            if self.connection_state != ConnectionState.CONNECTED or self._session is None:
                continue
            try:
                if not await self._session.healthy():
                    self._disruption("sdk_connection_epoch_changed")
            except Exception:
                self._disruption("health_check_failed")

    async def _close_session(self) -> None:
        bridge, session, consumer = self._bridge, self._session, self._consumer
        self._bridge, self._session, self._consumer = None, None, None
        if bridge:
            bridge.close()
        if session:
            await session.close()
        if consumer:
            consumer.cancel()
            await asyncio.gather(consumer, return_exceptions=True)
        self.instrument_provider.set_session(None)

    async def _recover(self) -> None:
        settings = self._futu_config.connection
        for attempt in range(settings.reconnect_max_attempts):
            if self._stopped:
                return
            await self._close_session()
            await asyncio.sleep(
                min(settings.reconnect_initial_secs * 2**attempt, settings.reconnect_max_secs)
            )
            try:
                await self._open_session()
                self.connection_state = ConnectionState.CONNECTED
                self.diagnostics.append(
                    {"kind": "connection_restored", "received_ns": self.clock.timestamp_ns()}
                )
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.diagnostics.append(
                    {
                        "kind": "reconnect_failed",
                        "error_type": type(exc).__name__,
                        "attempt": attempt + 1,
                    }
                )
        await self._close_session()
        self.connection_state = ConnectionState.FAILED
        logger.error("Futu reconnect attempts exhausted")

    async def _disconnect(self) -> None:
        self._stopped = True
        for task in (self._health_task, self._reconnect_task):
            if task and task is not asyncio.current_task():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        self._health_task = self._reconnect_task = None
        # Let already accepted callbacks finish before invalidating their epoch.
        if self._bridge:
            self._bridge.close()
            deadline = (
                asyncio.get_running_loop().time()
                + self._futu_config.connection.request_timeout_secs
            )
            while (
                self._bridge.queue.unfinished_tasks and self._consumer and not self._consumer.done()
            ):
                if asyncio.get_running_loop().time() >= deadline:
                    self.diagnostics.append({"kind": "shutdown_incomplete_callbacks"})
                    break
                await asyncio.sleep(0.01)
        self._generation += 1
        await self._close_session()
        self.connection_state = ConnectionState.DISCONNECTED
