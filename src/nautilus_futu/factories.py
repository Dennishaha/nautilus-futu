from __future__ import annotations

from typing import Any

from nautilus_trader.live.clients import DataClientFactory, ExecutionClientFactory

from .config import FutuDataClientConfig, FutuExecutionClientConfig
from .data import FutuDataClient
from .execution import FutuExecutionClient


class FutuDataClientFactory(DataClientFactory):
    @staticmethod
    def create(*, name: str, config: Any, cache: Any, clock: Any) -> FutuDataClient:
        if not isinstance(config, FutuDataClientConfig):
            raise TypeError("FutuDataClientConfig is required")
        return FutuDataClient(name=name, config=config, cache=cache, clock=clock)


class FutuExecutionClientFactory(ExecutionClientFactory):
    @staticmethod
    def create(
        *, name: str, config: Any, cache: Any, clock: Any, trader_id: Any
    ) -> FutuExecutionClient:
        if not isinstance(config, FutuExecutionClientConfig):
            raise TypeError("FutuExecutionClientConfig is required")
        return FutuExecutionClient(
            name=name, config=config, cache=cache, clock=clock, trader_id=trader_id
        )
