from __future__ import annotations

from typing import Any

from nautilus_trader.model import AggregationSource, Bar, PriceType

from .errors import DataConflict, UnsupportedOperation
from .mapping import decimal, local_datetime, price, quantity, rows, timestamp_ns
from .symbols import provider_symbol

FIELDS = ("open", "high", "low", "close", "volume")


def bar_period(bar_type: Any) -> str:
    if (
        bar_type.aggregation_source != AggregationSource.EXTERNAL
        or bar_type.spec.price_type != PriceType.LAST
    ):
        raise UnsupportedOperation("only external LAST bars are supported")
    spec = str(bar_type.spec)
    mapping = {f"{n}-MINUTE-LAST": f"K_{n}M" for n in (1, 5, 15, 30, 60)}
    mapping.update({"1-HOUR-LAST": "K_60M", "1-DAY-LAST": "K_DAY"})
    if spec not in mapping:
        raise UnsupportedOperation("unsupported bar period")
    return mapping[spec]


def autype(adjustment: str) -> str:
    return "None" if adjustment == "NoAdjust" else "qfq"


def fingerprint(row: dict) -> tuple:
    return tuple(decimal(row[name]) for name in FIELDS)


def make_bar(bar_type: Any, row: dict, received_ns: int) -> Bar:
    if provider_symbol(row["code"]) != provider_symbol(str(bar_type.instrument_id.symbol)):
        raise DataConflict("bar response contains another instrument")
    opening, high, low, close, volume = fingerprint(row)
    if (
        min(opening, high, low, close) <= 0
        or low > min(opening, close)
        or high < max(opening, close)
        or low > high
    ):
        raise ValueError("invalid provider OHLC; original response must not be repaired")
    # OpenD history can exceed 8dp. Preserve source digits with a uniform
    # history precision: NT Catalog identities include price_precision.
    # Values beyond 16dp still fail exact conversion; never round them.
    prices = [price(value, 16) for value in (opening, high, low, close)]
    if any(
        value.as_decimal() != original
        for value, original in zip(prices, (opening, high, low, close), strict=True)
    ):
        raise ValueError("historical price cannot be represented exactly by NT")
    return Bar(
        bar_type,
        prices[0],
        prices[1],
        prices[2],
        prices[3],
        quantity(volume),
        timestamp_ns(row["time_key"], str(bar_type.instrument_id.symbol)),
        received_ns,
    )


async def history_rows(
    session: Any,
    symbol: str,
    period: str,
    start_ns: int,
    end_ns: int,
    adjustment: str,
    trade_session: str,
    evidence: Any = None,
) -> list[tuple[dict, int]]:
    if start_ns > end_ns:
        raise ValueError("historical start is after end")
    first = local_datetime(start_ns, symbol).date().isoformat()
    last = local_datetime(end_ns, symbol).date().isoformat()
    key: Any = None
    seen_pages: set[bytes] = set()
    by_timestamp: dict[int, tuple[dict, int]] = {}
    latest_page_ns = -1
    for _ in range(100_000):
        kwargs = {
            "code": provider_symbol(symbol),
            "start": first,
            "end": last,
            "ktype": period,
            "autype": autype(adjustment),
            "max_count": 1000,
            "page_req_key": key,
            "session": trade_session,
        }
        payload, received, next_key = await session.quote.call("request_history_kline", **kwargs)
        page = rows(payload)
        if evidence:
            evidence.record(
                "request_history_kline",
                {**kwargs, "adjustment": adjustment, "start_ns": start_ns, "end_ns": end_ns},
                page,
                received,
            )
        for row in page:
            if provider_symbol(row["code"]) != provider_symbol(symbol):
                raise DataConflict("history returned another instrument")
            stamp = timestamp_ns(row["time_key"], symbol)
            if not start_ns <= stamp <= end_ns:
                continue
            if stamp in by_timestamp and fingerprint(row) != fingerprint(by_timestamp[stamp][0]):
                raise DataConflict("history pages contain conflicting OHLCV at one timestamp")
            by_timestamp.setdefault(stamp, (row, received))
        if next_key is None:
            return [by_timestamp[stamp] for stamp in sorted(by_timestamp)]
        if not isinstance(next_key, bytes) or not next_key or next_key in seen_pages or not page:
            raise DataConflict("history pagination made no progress")
        latest = max(timestamp_ns(row["time_key"], symbol) for row in page)
        if latest <= latest_page_ns:
            raise DataConflict("history page cursor advanced without time progress")
        latest_page_ns = latest
        seen_pages.add(next_key)
        key = next_key
    raise DataConflict("history pagination exceeded the safety bound")
