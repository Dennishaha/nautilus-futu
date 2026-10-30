from __future__ import annotations

import asyncio

import pytest
from nautilus_trader.model import FuturesContract

from nautilus_futu.errors import ConfigurationError
from nautilus_futu.instruments import FutuInstrumentProvider


def test_future_static_metadata_becomes_real_nt_contract() -> None:
    instrument = FutuInstrumentProvider().from_future_static(
        {
            "code": "US.ES2606",
            "owner": "ES",
            "exchange": "CME",
            "size": 50,
            "min_change": 0.25,
            "price_currency": "USD",
            "last_trade_time": "2026-06-19 00:00:00",
        },
        1_000_000_000,
    )
    assert isinstance(instrument, FuturesContract)
    assert str(instrument.id) == "ES2606.US.FUTU"
    assert str(instrument.multiplier) == "50"
    assert str(instrument.price_increment) == "0.25"
    assert instrument.info["security_type"] == "FUTURE"
    assert instrument.expiration_ns > instrument.ts_init


def test_provider_uses_future_info_after_stock_basicinfo_marks_future() -> None:
    class Quote:
        async def call(self, operation, **kwargs):
            if operation == "get_stock_basicinfo":
                return [{"code": "US.ES2606", "stock_type": "FUTURE"}], 11, None
            assert operation == "get_future_info"
            return [
                {
                    "code": "US.ES2606",
                    "owner": "ES",
                    "exchange": "CME",
                    "size": 50,
                    "min_change": 0.25,
                    "price_currency": "USD",
                    "last_trade_time": "2026-06-19 00:00:00",
                }
            ], 12, None

    class Session:
        quote = Quote()

    async def run() -> None:
        provider = FutuInstrumentProvider()
        provider.set_session(Session())
        await provider.load_symbols_async(["ES2606.US"])
        assert isinstance(provider.list_all()[0], FuturesContract)

    asyncio.run(run())


def test_future_static_metadata_requires_currency_and_expiration() -> None:
    provider = FutuInstrumentProvider()
    row = {
        "code": "US.ES2606",
        "owner": "ES",
        "exchange": "CME",
        "size": 50,
        "min_change": 0.25,
    }
    with pytest.raises(ConfigurationError, match="currency"):
        provider.from_future_static(row)
    with pytest.raises(ConfigurationError, match="expiration"):
        provider.from_future_static({**row, "price_currency": "USD"})


def test_provider_rejects_incomplete_future_info_response() -> None:
    class Quote:
        async def call(self, operation, **kwargs):
            if operation == "get_stock_basicinfo":
                return [{"code": "US.ES2606", "stock_type": "FUTURE"}], 11, None
            assert operation == "get_future_info"
            return [], 12, None

    class Session:
        quote = Quote()

    async def run() -> None:
        provider = FutuInstrumentProvider()
        provider.set_session(Session())
        with pytest.raises(ConfigurationError, match="futures instrument"):
            await provider.load_symbols_async(["ES2606.US"])

    asyncio.run(run())
