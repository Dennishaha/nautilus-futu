from __future__ import annotations

import hashlib
import json
import math
import threading
from pathlib import Path
from typing import Any
from uuid import uuid4


def json_safe(value: Any) -> Any:
    if isinstance(value, float) and not math.isfinite(value):
        return {"__provider_nonfinite__": str(value)}
    if isinstance(value, bytes):
        return {"__provider_bytes_hex__": value.hex()}
    if isinstance(value, dict):
        return {key: json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    return value


class Evidence:
    """Optional append-only public market response envelopes, independent of research."""

    def __init__(self, directory: str | None) -> None:
        self.directory = Path(directory) / str(uuid4()) if directory else None
        self._sequence = 0
        self._lock = threading.Lock()

    def record(self, operation: str, request: dict, payload: Any, received_ns: int) -> None:
        if self.directory is None:
            return
        body = {
            "provider": "FUTU_OPEND",
            "operation": operation,
            "request": request,
            "received_ns": received_ns,
            "payload": payload,
            "availability": "actual_receipt_not_historical_publication",
        }
        body = json_safe(body)
        encoded = json.dumps(
            body, ensure_ascii=False, sort_keys=True, default=str, allow_nan=False
        ).encode()
        envelope = {"sha256": hashlib.sha256(encoded).hexdigest(), "response": body}
        with self._lock:
            self.directory.mkdir(parents=True, exist_ok=True)
            self._sequence += 1
            path = self.directory / f"{self._sequence:08d}.json"
            with path.open("x") as stream:
                json.dump(
                    envelope,
                    stream,
                    ensure_ascii=False,
                    sort_keys=True,
                    default=str,
                    allow_nan=False,
                )
