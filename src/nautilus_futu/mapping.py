from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from nautilus_trader.model import Price, Quantity

from .errors import ConfigurationError
from .symbols import canonical_symbol


def decimal(value: Any) -> Decimal:
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError("nonfinite provider numeric value")
    return result


def price(value: Any, precision: int = 4) -> Price:
    result = decimal(value)
    if result <= 0 or result != result.quantize(Decimal(1).scaleb(-precision)):
        raise ValueError("nonpositive or unrepresentable price; refusing to round")
    return Price.from_decimal_dp(result, precision)


def quantity(value: Any) -> Quantity:
    result = decimal(value)
    if result < 0 or result != result.to_integral_value():
        raise ValueError("equity quantity must be nonnegative integral shares")
    return Quantity.from_int(int(result))


def timezone(symbol: str) -> ZoneInfo:
    return ZoneInfo(
        "America/New_York" if canonical_symbol(symbol).endswith(".US") else "Asia/Hong_Kong"
    )


def timestamp_ns(value: str | datetime, symbol: str) -> int:
    stamp = datetime.fromisoformat(value) if isinstance(value, str) else value
    if stamp.tzinfo is None:
        zone = timezone(symbol)
        first, second = stamp.replace(tzinfo=zone, fold=0), stamp.replace(tzinfo=zone, fold=1)
        if first.utcoffset() != second.utcoffset():
            raise ConfigurationError("ambiguous/nonexistent market-local timestamp")
        stamp = first
    epoch_delta = stamp.astimezone(UTC) - datetime(1970, 1, 1, tzinfo=UTC)
    return epoch_delta // timedelta(microseconds=1) * 1000


def local_datetime(nanoseconds: int, symbol: str) -> datetime:
    return datetime.fromtimestamp(nanoseconds // 1_000_000_000, timezone(symbol)).replace(
        microsecond=(nanoseconds % 1_000_000_000) // 1000
    )


def rows(payload: Any) -> list[dict[str, Any]]:
    if hasattr(payload, "to_dict"):
        return payload.to_dict("records")
    if isinstance(payload, list):
        return [dict(row) for row in payload]
    if isinstance(payload, dict):
        return [payload]
    raise ValueError("unexpected SDK response shape")
