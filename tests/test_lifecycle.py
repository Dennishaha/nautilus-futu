import asyncio

from nautilus_futu import ConnectionState, FutuConnectionSettings
from nautilus_futu.transport import CallbackBridge
from tests.test_market import data_client


async def test_single_reconnect_restores_leases_and_discards_old_callbacks(server):
    client = data_client(
        server,
        connection=FutuConnectionSettings(
            reconnect_initial_secs=0.01, reconnect_max_secs=0.02, health_interval_secs=3600
        ),
    )
    tasks = []

    def task(coroutine, _name):
        value = asyncio.create_task(coroutine)
        tasks.append(value)
        return value

    client.create_task = task
    client._leases[("00700.HK", "ORDER_BOOK")] = {"quotes"}
    await client._connect()
    try:
        old = client._bridge
        client._disruption("test")
        coordinator = client._reconnect_task
        client._disruption("duplicate")
        assert coordinator is client._reconnect_task
        await asyncio.wait_for(asyncio.shield(coordinator), 1)
        assert client.connection_state == ConnectionState.CONNECTED
        assert client._generation == 2
        assert old.closed
        old.push("book", {})
        assert old.queue.empty()
        assert len([c for c in server.calls if c[0] == "subscribe"]) == 2
    finally:
        await client._disconnect()
    assert all(t.done() for t in tasks)


async def test_history_cancel_closes_lane_and_cannot_deliver_late_data(server):
    from nautilus_futu.transport import OpenDSession

    session = OpenDSession(FutuConnectionSettings(), CallbackBridge(10, 1, lambda: None), server)
    await session.open()
    try:
        server.contexts[0].get_cur_kline = lambda **kwargs: (__import__("time").sleep(0.04), [])[1]
        task = asyncio.create_task(session.quote.call("get_cur_kline"))
        await asyncio.sleep(0.005)
        task.cancel()
        result = await asyncio.gather(task, return_exceptions=True)
        assert isinstance(result[0], asyncio.CancelledError)
        assert session.quote.poisoned
    finally:
        await session.close()
