from __future__ import annotations

import re

from nautilus_trader.model import InstrumentId, Symbol, Venue

from .errors import ConfigurationError

VENUE = Venue("FUTU")


def canonical_symbol(value: str) -> str:
    value = value.strip().upper()
    if value.startswith(("HK.", "US.")):
        market, ticker = value.split(".", 1)
    else:
        ticker, separator, market = value.rpartition(".")
        if not separator:
            raise ConfigurationError("symbols must include HK/US market")
    if market == "HK":
        if not ticker.isdigit() or not 1 <= len(ticker) <= 5 or int(ticker) == 0:
            raise ConfigurationError("invalid Hong Kong equity symbol")
        ticker = ticker.zfill(5)
    elif market != "US" or not re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,19}", ticker):
        raise ConfigurationError("unsupported equity symbol")
    return f"{ticker}.{market}"


def provider_symbol(value: str) -> str:
    ticker, market = canonical_symbol(value).rsplit(".", 1)
    return f"{market}.{ticker}"


def instrument_id(value: str) -> InstrumentId:
    return InstrumentId(Symbol(canonical_symbol(value)), VENUE)


def from_instrument_id(value: InstrumentId) -> str:
    if value.venue != VENUE:
        raise ConfigurationError("instrument does not belong to FUTU")
    return canonical_symbol(str(value.symbol))
