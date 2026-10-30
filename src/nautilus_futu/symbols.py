from __future__ import annotations

import re

from nautilus_trader.model import InstrumentId, Symbol, Venue

from .errors import ConfigurationError

VENUE = Venue("FUTU")


def canonical_symbol(value: str) -> str:
    original = value.strip()
    value = original.upper()
    if value.startswith(("HK.", "US.", "SH.", "SZ.")):
        market, ticker = value.split(".", 1)
        original_ticker = original.split(".", 1)[1]
    else:
        ticker, separator, market = value.rpartition(".")
        if not separator:
            raise ConfigurationError("symbols must include HK/US market")
        original_ticker = original.rpartition(".")[0]
    if market == "HK":
        if ticker.isdigit():
            if not 1 <= len(ticker) <= 5 or int(ticker) == 0:
                raise ConfigurationError("invalid Hong Kong equity symbol")
            ticker = ticker.zfill(5)
        elif re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,19}", ticker):
            # Futu uses case-sensitive `main` suffixes for continuous futures.
            if original_ticker.lower().endswith("main"):
                ticker = original_ticker[:-4].upper() + "main"
        else:
            raise ConfigurationError("invalid Hong Kong symbol")
    elif market in {"SH", "SZ"}:
        if not ticker.isdigit() or not 1 <= len(ticker) <= 6 or int(ticker) == 0:
            raise ConfigurationError("invalid mainland China equity symbol")
        ticker = ticker.zfill(6)
    elif market == "US" and re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,19}", ticker):
        if original_ticker.lower().endswith("main"):
            ticker = original_ticker[:-4].upper() + "main"
    else:
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
