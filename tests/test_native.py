from datetime import datetime, timedelta

import pytest
from nautilus_trader.common import Environment, LogLevel
from nautilus_trader.config import LoggerConfig, StrategyConfig
from nautilus_trader.live import InstrumentProviderConfig, LiveNode, LiveNodeBuilder, LiveNodeConfig
from nautilus_trader.model import BarType, NautilusDataType, NautilusInstrumentType, TraderId
from nautilus_trader.persistence import DataCatalogConfig, ParquetDataCatalog, StreamingConfig
from nautilus_trader.trading import Strategy

from nautilus_futu import (
    FutuDataClient,
    FutuDataClientConfig,
    FutuDataClientFactory,
    FutuExecutionClient,
    FutuExecutionClientConfig,
    FutuExecutionClientFactory,
)
from tests.fakes import FakeServer


def test_factories_build_without_network():
    builder = LiveNode.builder("futu-lazy", TraderId("TEST-001"), Environment.LIVE)
    node = (
        builder.add_data_client("FUTU", FutuDataClientFactory, FutuDataClientConfig())
        .add_exec_client("FUTU", FutuExecutionClientFactory, FutuExecutionClientConfig(acc_id=123))
        .build()
    )
    try:
        assert str(node.trader_id) == "TEST-001"
    finally:
        node.dispose()


def test_native_callbacks_and_catalog_roundtrip(tmp_path):
    server = FakeServer()
    data_clients, exec_clients = [], []

    class DataFactory(FutuDataClientFactory):
        @staticmethod
        def create(**kwargs):
            client = FutuDataClient(context_factory=server, **kwargs)
            data_clients.append(client)
            return client

    class ExecFactory(FutuExecutionClientFactory):
        @staticmethod
        def create(**kwargs):
            client = FutuExecutionClient(context_factory=server, **kwargs)
            exec_clients.append(client)
            return client

    bar_type = BarType.from_str("00700.HK.FUTU-1-MINUTE-LAST-EXTERNAL")

    class Recorder(Strategy):
        def __new__(cls):
            return super().__new__(cls, StrategyConfig())

        def __init__(self):
            super().__init__(StrategyConfig())
            self.live, self.history, self.timeout = [], [], False

        def on_start(self):
            self.subscribe_bars(bar_type)
            self.request_bars(
                bar_type,
                datetime.fromisoformat("2026-10-23T01:30:00+00:00"),
                datetime.fromisoformat("2026-10-23T01:30:00+00:00"),
            )
            self.clock.set_timer("stop", timedelta(seconds=3), callback=self.stop_test)

        def stop_test(self, _):
            self.timeout = True
            handle.stop()

        def on_bar(self, bar):
            self.live.append(bar)
            self.maybe_stop()

        def on_historical_bars(self, bars):
            self.history.extend(bars)
            self.maybe_stop()

        def maybe_stop(self):
            if self.live and self.history:
                handle.stop()

        def on_stop(self):
            self.clock.cancel_timer("stop")

    provider = InstrumentProviderConfig(load_ids=["00700.HK.FUTU"])
    path = str(tmp_path)
    node = (
        LiveNodeBuilder.from_config(
            "futu-native",
            LiveNodeConfig(
                environment=Environment.LIVE,
                trader_id=TraderId("TEST-001"),
                logging=LoggerConfig(stdout_level=LogLevel.ERROR),
                delay_post_stop_secs=0,
                streaming=StreamingConfig(
                    writer_path=path,
                    catalog=DataCatalogConfig(path=path),
                    data_types=[NautilusDataType.Bar],
                    instrument_types=[NautilusInstrumentType("Equity")],
                    promote_on_close=True,
                    use_ts_event_for_ts_init=False,
                ),
            ),
        )
        .add_data_client("FUTU", DataFactory, FutuDataClientConfig(instrument_provider=provider))
        .add_exec_client(
            "FUTU", ExecFactory, FutuExecutionClientConfig(acc_id=123, instrument_provider=provider)
        )
        .build()
    )
    recorder = Recorder()
    handle = node.handle()
    node.add_strategy(recorder)
    try:
        node.run()
        assert not recorder.timeout
        assert len(recorder.live) == len(recorder.history) == 1
        assert recorder.history[0].ts_init > recorder.history[0].ts_event
        assert node.cache.account(exec_clients[0].account_id) is not None
    finally:
        node.dispose()
    catalog = ParquetDataCatalog(path)
    assert catalog.query_bars() == recorder.live
    assert {str(i.id) for i in catalog.instruments()} == {"00700.HK.FUTU"}
    import subprocess
    import sys

    subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; from nautilus_trader.persistence import ParquetDataCatalog; c=ParquetDataCatalog(sys.argv[1]); assert len(c.query_bars()) == 1; assert {str(i.id) for i in c.instruments()} == {'00700.HK.FUTU'}; assert 'nautilus_futu' not in sys.modules",
            path,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    assert all(context.closed for context in server.contexts)


def test_config_importable_roundtrip():
    from nautilus_trader.live.config import ImportableConfig

    for config in [
        FutuDataClientConfig(adjustment="ForwardAdjust"),
        FutuExecutionClientConfig(acc_id=123),
    ]:
        restored = ImportableConfig.parse(config.to_importable().json()).create()
        assert restored.json() == config.json()


@pytest.mark.parametrize("environment", ["SIMULATE", "REAL"])
def test_native_simulator_cumulative_fill_and_source_metadata(environment, monkeypatch):
    from nautilus_trader.model import ClientId, OrderSide, Price, Quantity

    from nautilus_futu import FUTU_SOURCE, FutuSourceEvent
    from nautilus_futu.symbols import instrument_id

    server = FakeServer()
    server.accounts = [
        dict(
            acc_id=123,
            trd_env=environment,
            acc_status="ACTIVE",
            trdmarket_auth=["HK"],
            sim_acc_type="STOCK" if environment == "SIMULATE" else "N/A",
            acc_type="CASH",
        )
    ]
    monkeypatch.setenv("FUTU_TRADE_PASSWORD_MD5", "0" * 32)

    def fill_immediately(context, row):
        row.update(
            order_status="FILLED_ALL",
            dealt_qty=row["qty"],
            dealt_avg_price=row["price"],
            trd_env=environment,
        )
        context.bridge.push("orders", [dict(row)])
        if environment == "REAL":
            from tests.fakes import fill_row

            server.fills = [fill_row(qty=row["qty"])]
            context.bridge.push("fills", server.fills)

    server.before_response = fill_immediately

    class DataFactory(FutuDataClientFactory):
        @staticmethod
        def create(**kwargs):
            client = FutuDataClient(context_factory=server, **kwargs)
            original = client._subscribe_trades

            async def subscribe(command):
                await original(command)
                client._bridge.push(
                    "trades",
                    [
                        dict(
                            code="HK.00700",
                            time="2026-10-23 09:30:00",
                            price=100,
                            volume=100,
                            sequence=5,
                            ticker_direction="BUY",
                            type="AUTO_MATCH",
                            push_data_type="CACHE",
                        )
                    ],
                )

            client._subscribe_trades = subscribe
            return client

    class ExecFactory(FutuExecutionClientFactory):
        @staticmethod
        def create(**kwargs):
            return FutuExecutionClient(context_factory=server, **kwargs)

    class Recorder(Strategy):
        def __new__(cls):
            return super().__new__(cls, StrategyConfig())

        def __init__(self):
            super().__init__(StrategyConfig())
            self.fills, self.sources, self.timeout = [], [], False

        def on_start(self):
            self.subscribe_data(FUTU_SOURCE, ClientId("FUTU"))
            self.subscribe_trades(instrument_id("00700.HK"))
            order = self.order_factory.limit(
                instrument_id("00700.HK"),
                OrderSide.BUY,
                Quantity.from_int(100),
                Price.from_str("100.0000"),
            )
            self.submit_order(order)
            self.clock.set_timer("stop", timedelta(seconds=3), callback=self.stop_test)

        def stop_test(self, _):
            self.timeout = True
            handle.stop()

        def on_order_filled(self, event):
            self.fills.append(event)
            self.maybe_stop()

        def on_data(self, data):
            self.sources.append(data)
            self.maybe_stop()

        def maybe_stop(self):
            if self.fills and self.sources:
                handle.stop()

        def on_stop(self):
            self.clock.cancel_timer("stop")

    provider = InstrumentProviderConfig(load_ids=["00700.HK.FUTU"])
    node = (
        LiveNodeBuilder.from_config(
            "futu-exec",
            LiveNodeConfig(
                environment=Environment.LIVE,
                trader_id=TraderId("TEST-001"),
                logging=LoggerConfig(stdout_level=LogLevel.ERROR),
                delay_post_stop_secs=0,
            ),
        )
        .add_data_client("FUTU", DataFactory, FutuDataClientConfig(instrument_provider=provider))
        .add_exec_client(
            "FUTU",
            ExecFactory,
            FutuExecutionClientConfig(
                acc_id=123,
                dry_run=False,
                instrument_provider=provider,
                trading_environment=environment,
                allow_real_trading=environment == "REAL",
            ),
        )
        .build()
    )
    handle = node.handle()
    recorder = Recorder()
    node.add_strategy(recorder)
    try:
        node.run()
        assert not recorder.timeout
        assert len(recorder.fills) == 1
        assert recorder.fills[0].last_qty == Quantity.from_int(100)
        assert any(
            isinstance(getattr(item, "data", item), FutuSourceEvent) for item in recorder.sources
        )
        if environment == "SIMULATE":
            assert not any(call[0] == "deal_list_query" for call in server.calls)
    finally:
        node.dispose()
