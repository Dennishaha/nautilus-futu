from __future__ import annotations

import asyncio
import time
from datetime import date
from decimal import Decimal
from typing import Any

from nautilus_trader.live.providers import InstrumentProvider
from nautilus_trader.model import (
    Currency,
    Equity,
    Price,
    Quantity,
    Symbol,
)

from .errors import ConfigurationError, UnsupportedOperation
from .mapping import decimal, rows
from .symbols import canonical_symbol, from_instrument_id, instrument_id, provider_symbol

# Versioned execution ladders, not an assertion about historical trading rules.
# HKEX minimum-spread phase 2: effective 2026-08-03. ETF table is distinct.
HK_STOCK = (
    ("0.25", "0.001"),
    ("10", "0.005"),
    ("20", "0.01"),
    ("50", "0.02"),
    ("100", "0.05"),
    ("200", "0.1"),
    ("500", "0.2"),
    ("1000", "0.5"),
    ("2000", "1"),
    ("5000", "2"),
    ("9995", "5"),
)
HK_ETF = (
    ("1", "0.001"),
    ("10", "0.002"),
    ("20", "0.01"),
    ("100", "0.02"),
    ("200", "0.05"),
    ("500", "0.1"),
    ("1000", "0.2"),
    ("2000", "0.5"),
    ("9999", "1"),
)


def tick_size(symbol: str, security_type: str, value: Decimal) -> Decimal:
    if value <= 0:
        raise ConfigurationError("price must be positive")
    if canonical_symbol(symbol).endswith(".US"):
        # OpenD's documented US submission precision. No automatic price adjustment.
        return Decimal("0.0001") if value < 1 else Decimal("0.01")
    if value < Decimal("0.01"):
        raise ConfigurationError("Hong Kong order price is below 0.01")
    for upper, step in HK_ETF if security_type == "ETF" else HK_STOCK:
        if value <= Decimal(upper):
            return Decimal(step)
    raise ConfigurationError("price is outside the verified HKEX spread table")


def validate_order(instrument: Equity, qty: Any, order_price: Any = None) -> None:
    value = decimal(qty)
    if value <= 0 or value != value.to_integral_value():
        raise ConfigurationError("order quantity must be positive integral shares")
    if not instrument.info.get("execution_metadata_verified"):
        raise ConfigurationError("instrument metadata is insufficient for execution")
    lot = decimal(instrument.lot_size)
    if value % lot:
        raise ConfigurationError("order quantity is not a board-lot multiple")
    if order_price is not None:
        px = decimal(order_price)
        step = tick_size(str(instrument.symbol), instrument.info["stock_type"], px)
        if px % step:
            raise ConfigurationError("order price is not on the market tick ladder")


class FutuInstrumentProvider(InstrumentProvider):
    def __init__(
        self, config: Any = None, *, currency_overrides: dict[str, str] | None = None
    ) -> None:
        super().__init__(config)
        self.session: Any = None
        self.currency_overrides = {
            canonical_symbol(k): v.upper() for k, v in (currency_overrides or {}).items()
        }
        self._load_lock = asyncio.Lock()

    def set_session(self, session: Any) -> None:
        self.session = session

    async def load_all_async(self, filters: dict | None = None) -> None:
        symbols = (filters or {}).get("symbols")
        if not symbols:
            raise UnsupportedOperation(
                "configure load_ids or explicit symbols; unbounded discovery is disabled"
            )
        await self.load_symbols_async(symbols)

    async def load_ids_async(self, instrument_ids: list, filters: dict | None = None) -> None:
        await self.load_symbols_async([from_instrument_id(value) for value in instrument_ids])

    async def load_symbols_async(self, symbols: list[str]) -> None:
        async with self._load_lock:
            if self.session is None:
                raise RuntimeError("OpenD session is not connected")
            session = self.session
            for symbol in symbols:
                canonical = canonical_symbol(symbol)
                if self.find(instrument_id(canonical)) is not None:
                    continue
                payload, received, _ = await session.quote.call(
                    "get_stock_basicinfo",
                    market=canonical.rsplit(".", 1)[1],
                    code_list=[provider_symbol(canonical)],
                )
                if session is not self.session:
                    raise ConnectionError("instrument response belongs to an obsolete connection")
                matches = [
                    row for row in rows(payload) if canonical_symbol(row["code"]) == canonical
                ]
                if len(matches) != 1:
                    raise ConfigurationError("OpenD did not return an unambiguous real instrument")
                self.add(self.from_static(matches[0], received))

    def from_static(self, row: dict, received_ns: int | None = None) -> Equity:
        symbol = canonical_symbol(row["code"])
        security_type = str(row.get("stock_type", ""))
        if security_type not in {"STOCK", "ETF"}:
            raise UnsupportedOperation("only STOCK and ETF instruments are supported")
        lot = decimal(row.get("lot_size", 0))
        if lot <= 0 or lot != lot.to_integral_value() or not row.get("stock_id"):
            raise ConfigurationError("missing real instrument lot size or stock_id")
        exchange = str(row.get("exchange_type", ""))
        if not exchange or exchange in {"N/A", "NONE"}:
            raise ConfigurationError("missing instrument exchange")
        market = symbol.rsplit(".", 1)[1]
        currency_text = row.get("currency") or self.currency_overrides.get(symbol)
        currency_source = "provider_or_explicit_override"
        if not currency_text:
            if market == "US":
                currency_text = "USD"
            elif (
                symbol.startswith("8")
                or security_type == "ETF"
                and 9000 <= int(symbol.split(".")[0]) <= 9999
            ):
                raise ConfigurationError(
                    "non-HKD counter requires an explicit verified currency override"
                )
            else:
                currency_text = "HKD"
            currency_source = "market_and_counter_convention"
        currency = Currency.from_str(str(currency_text))
        scheme = None
        if market == "HK":
            scheme = "FUTU_HK_ETF" if security_type == "ETF" else "FUTU_HK_STOCK_20260803"
        received_ns = received_ns if received_ns is not None else time.time_ns()
        info = {
            **row,
            "provider": "FUTU_OPEND",
            "currency_source": currency_source,
            "metadata_received_ns": received_ns,
            "historical_execution_metadata": False,
            "execution_metadata_verified": not bool(row.get("delisting", False)),
            "price_rule": scheme or "OPEND_US_SUBMISSION_PRECISION",
            "price_ladder": list(HK_ETF if security_type == "ETF" else HK_STOCK)
            if market == "HK"
            else None,
            "price_increment_semantics": "representation_quantum_not_order_tick",
            "price_rule_effective_from": "2026-08-03"
            if market == "HK" and security_type == "STOCK"
            else None,
        }
        if (
            market == "HK"
            and security_type == "STOCK"
            and date.fromtimestamp(received_ns / 1e9) < date(2026, 8, 3)
        ):
            info["execution_metadata_verified"] = False
        self.add_currency(currency)
        return Equity(
            instrument_id(symbol),
            Symbol(provider_symbol(symbol)),
            currency,
            4,
            Price.from_str("0.0001"),
            0,
            received_ns,
            lot_size=Quantity.from_int(int(lot)),
            tick_scheme=None,
            info=info,
        )
