from __future__ import annotations

import json
import math
import os
from dataclasses import asdict, dataclass
from typing import Any

from nautilus_trader.live import DataClientConfig, ExecutionClientConfig
from nautilus_trader.live.config import ImportableConfig, encode_config_value

from .errors import ConfigurationError


@dataclass(frozen=True)
class FutuConnectionSettings:
    host: str = "127.0.0.1"
    port: int = 11111
    security_firm: str = "FUTUSECURITIES"
    is_encrypt: bool | None = None
    request_timeout_secs: float = 15.0
    reconnect_initial_secs: float = 1.0
    reconnect_max_secs: float = 30.0
    reconnect_max_attempts: int = 5
    health_interval_secs: float = 15.0
    queue_size: int = 4096

    def __post_init__(self) -> None:
        if not self.host.strip() or not 1 <= self.port <= 65535:
            raise ConfigurationError("invalid OpenD host/port")
        if self.security_firm not in {
            "FUTUSECURITIES",
            "FUTUINC",
            "FUTUSG",
            "FUTUAU",
            "FUTUCA",
            "FUTUMY",
            "FUTUJP",
        }:
            raise ConfigurationError("unsupported security_firm")
        for value in (
            self.request_timeout_secs,
            self.reconnect_initial_secs,
            self.reconnect_max_secs,
            self.health_interval_secs,
        ):
            if not math.isfinite(value) or value <= 0:
                raise ConfigurationError("timeouts and intervals must be finite and positive")
        if (
            self.reconnect_max_secs < self.reconnect_initial_secs
            or self.reconnect_max_attempts < 1
            or self.queue_size < 1
        ):
            raise ConfigurationError("invalid reconnect/queue settings")


class _SafeConfig:
    _values: Any

    def dict(self) -> dict[str, Any]:
        values = dict(super().dict())  # type: ignore[misc]
        values.pop("_values", None)
        values.pop("account_id", None)
        values.update(self._values)
        return values

    def json(self) -> bytes:
        return json.dumps(self.dict(), default=encode_config_value, sort_keys=True).encode()

    def to_importable(self, factory: Any = None) -> ImportableConfig:
        return ImportableConfig(
            f"{type(self).__module__}:{type(self).__qualname__}", self.dict(), factory
        )

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._values!r})"

    __str__ = __repr__


class FutuDataClientConfig(_SafeConfig, DataClientConfig):
    def __init__(
        self,
        *,
        connection: FutuConnectionSettings | None = None,
        adjustment: str = "NoAdjust",
        session: str = "RTH",
        completed_bars_only: bool = True,
        bar_poll_interval_secs: float = 10.0,
        evidence_dir: str | None = None,
        currency_overrides: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__()
        self.connection = (
            FutuConnectionSettings(**connection)
            if isinstance(connection, dict)
            else connection or FutuConnectionSettings()
        )
        if adjustment not in {"NoAdjust", "ForwardAdjust"} or session not in {"RTH", "ETH", "ALL"}:
            raise ConfigurationError("unsupported adjustment/session")
        if not completed_bars_only:
            raise ConfigurationError("rc7 connector publishes completed bars only")
        if not math.isfinite(bar_poll_interval_secs) or bar_poll_interval_secs < 1:
            raise ConfigurationError("bar_poll_interval_secs must be >= 1")
        if evidence_dir is not None and not evidence_dir.strip():
            raise ConfigurationError("evidence_dir cannot be empty")
        self.adjustment, self.session = adjustment, session
        self.completed_bars_only = completed_bars_only
        self.bar_poll_interval_secs = bar_poll_interval_secs
        self.evidence_dir = evidence_dir
        self.currency_overrides = dict(currency_overrides or {})
        self._values = {
            "connection": asdict(self.connection),
            "adjustment": adjustment,
            "session": session,
            "completed_bars_only": completed_bars_only,
            "bar_poll_interval_secs": bar_poll_interval_secs,
            "evidence_dir": evidence_dir,
            "currency_overrides": self.currency_overrides,
        }


class FutuExecutionClientConfig(_SafeConfig, ExecutionClientConfig):
    def __init__(
        self,
        *,
        connection: FutuConnectionSettings | None = None,
        acc_id: int | None = None,
        trade_market: str = "HK",
        trading_environment: str = "SIMULATE",
        account_type: str = "CASH",
        dry_run: bool = True,
        allow_real_trading: bool = False,
        unlock_password_env: str = "FUTU_TRADE_PASSWORD_MD5",
        session: str = "RTH",
        reconciliation_interval_secs: float = 30.0,
        currency_overrides: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__()
        self.connection = (
            FutuConnectionSettings(**connection)
            if isinstance(connection, dict)
            else connection or FutuConnectionSettings()
        )
        try:
            self.acc_id = int(acc_id if acc_id is not None else os.getenv("FUTU_ACC_ID", "0"))
        except ValueError as exc:
            raise ConfigurationError("FUTU_ACC_ID must be an integer") from exc
        if self.acc_id < 0 or trade_market not in {"HK", "US"}:
            raise ConfigurationError("invalid account or trade_market")
        if trading_environment not in {"SIMULATE", "REAL"} or account_type not in {
            "CASH",
            "MARGIN",
        }:
            raise ConfigurationError("invalid trading environment/account type")
        if session not in {"RTH", "ETH", "ALL"} or not unlock_password_env.isidentifier():
            raise ConfigurationError("invalid session/credential environment variable name")
        if not math.isfinite(reconciliation_interval_secs) or reconciliation_interval_secs < 1:
            raise ConfigurationError("reconciliation interval must be >= 1")
        self.trade_market, self.trading_environment = trade_market, trading_environment
        self.account_type = account_type
        self.dry_run, self.allow_real_trading = dry_run, allow_real_trading
        self.unlock_password_env = unlock_password_env
        self.session = session
        self.reconciliation_interval_secs = reconciliation_interval_secs
        self.currency_overrides = dict(currency_overrides or {})
        self.account_id = f"FUTU-{trading_environment}-{self.acc_id}"
        self._values = {
            "connection": asdict(self.connection),
            "acc_id": self.acc_id,
            "trade_market": trade_market,
            "trading_environment": trading_environment,
            "account_type": account_type,
            "dry_run": dry_run,
            "allow_real_trading": allow_real_trading,
            "unlock_password_env": unlock_password_env,
            "session": session,
            "reconciliation_interval_secs": reconciliation_interval_secs,
            "currency_overrides": self.currency_overrides,
        }

    def require_orders(self) -> None:
        if self.dry_run:
            raise ConfigurationError("dry_run prevents order operations")
        if self.acc_id <= 0:
            raise ConfigurationError("an explicit acc_id is required")
        if self.trading_environment == "REAL" and not self.allow_real_trading:
            raise ConfigurationError("REAL order operations require allow_real_trading=True")
