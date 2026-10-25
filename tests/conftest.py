import pytest

from tests.fakes import FakeServer


@pytest.fixture(autouse=True)
def no_network_quota_wait(monkeypatch):
    class NoWait:
        async def acquire(self):
            pass

    monkeypatch.setattr("nautilus_futu.transport.limiter", lambda *args: NoWait())


@pytest.fixture
def server():
    return FakeServer()
