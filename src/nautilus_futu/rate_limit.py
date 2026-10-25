from __future__ import annotations

import asyncio
import threading
import time
from collections import deque


class SlidingWindow:
    """Process-wide, thread-safe reservation without binding to an asyncio loop."""

    def __init__(self, count: int, seconds: float, min_interval: float = 0.0) -> None:
        self.count, self.seconds, self.min_interval = count, seconds, min_interval
        self._times: deque[float] = deque()
        self._lock = threading.Lock()

    async def acquire(self) -> None:
        while True:
            with self._lock:
                now = time.monotonic()
                while self._times and now - self._times[0] > self.seconds:
                    self._times.popleft()
                delay = max(0.0, self._times[-1] + self.min_interval - now) if self._times else 0.0
                if len(self._times) >= self.count:
                    delay = max(delay, self._times[0] + self.seconds - now + 0.001)
                if delay <= 0:
                    self._times.append(now)
                    return
            await asyncio.sleep(delay)


_LIMITERS: dict[tuple[str, int, str, str], SlidingWindow] = {}
_LOCK = threading.Lock()


def limiter(host: str, port: int, firm: str, operation: str) -> SlidingWindow:
    key = (host, port, firm, operation)
    with _LOCK:
        if key not in _LIMITERS:
            if operation in {"place_order", "modify_order"}:
                limits = (15, 30.0, 0.03)
            elif operation == "request_history_kline":
                limits = (55, 30.0, 0.0)
            else:
                limits = (20, 30.0, 0.0)
            _LIMITERS[key] = SlidingWindow(*limits)
        return _LIMITERS[key]
