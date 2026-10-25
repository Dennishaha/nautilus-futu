"""Provider provenance published through NT custom data, independent of research."""

from dataclasses import dataclass
from typing import Any

from nautilus_trader.model import DataType

FUTU_SOURCE = DataType("FutuSourceEvent")


@dataclass(frozen=True)
class FutuSourceEvent:
    instrument_id: str
    category: str
    native_id: str | None
    provider_fields: dict[str, Any]
    time_basis: str
    ts_event: int
    ts_init: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "instrument_id": self.instrument_id,
            "category": self.category,
            "native_id": self.native_id,
            "provider_fields": self.provider_fields,
            "time_basis": self.time_basis,
            "ts_event": self.ts_event,
            "ts_init": self.ts_init,
        }
