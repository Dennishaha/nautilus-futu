"""Read-only NT node; strategies access only NautilusTrader APIs."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from nautilus_trader.common import Environment, LogLevel
from nautilus_trader.config import LoggerConfig, StrategyConfig
from nautilus_trader.live import InstrumentProviderConfig, LiveNodeBuilder, LiveNodeConfig
from nautilus_trader.model import (
    AccountId,
    BarType,
    BookType,
    ClientId,
    InstrumentId,
    NautilusDataType,
    NautilusInstrumentType,
    TraderId,
)
from nautilus_trader.persistence import DataCatalogConfig, StreamingConfig
from nautilus_trader.trading import Strategy

from nautilus_futu import (
    FUTU_SOURCE,
    FutuConnectionSettings,
    FutuDataClientConfig,
    FutuDataClientFactory,
    FutuExecutionClientConfig,
    FutuExecutionClientFactory,
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbols", nargs="+", default=["00700.HK", "AAPL.US"])
    parser.add_argument("--seconds", type=float, default=10)
    parser.add_argument("--history-date", help="known trading date, YYYY-MM-DD")
    parser.add_argument(
        "--acc-id", type=int, help="explicit read-only account; omitted means data only"
    )
    parser.add_argument("--market", choices=["HK", "US"], default="HK")
    parser.add_argument("--account-type", choices=["CASH", "MARGIN"], default="CASH")
    parser.add_argument("--environment", choices=["SIMULATE", "REAL"], default="SIMULATE")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=11111)
    parser.add_argument("--catalog", type=Path)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    identifiers = [InstrumentId.from_str(symbol + ".FUTU") for symbol in args.symbols]
    bar_types = [
        BarType.from_str(str(identifier) + "-1-MINUTE-LAST-EXTERNAL") for identifier in identifiers
    ]
    counts: Counter[str] = Counter()

    class Observer(Strategy):
        def __new__(cls):
            return super().__new__(cls, StrategyConfig())  # type: ignore[call-arg]

        def __init__(self):
            super().__init__(StrategyConfig())

        def on_start(self):
            if args.acc_id:
                counts["account_state_registered"] = int(
                    self.cache.account(AccountId(f"FUTU-{args.environment}-{args.acc_id}"))
                    is not None
                )
            self.subscribe_data(FUTU_SOURCE, ClientId("FUTU"))
            for identifier, bar_type in zip(identifiers, bar_types, strict=True):
                self.subscribe_quotes(identifier)
                self.subscribe_trades(identifier)
                self.subscribe_book_deltas(identifier, BookType.L2_MBP)
                self.subscribe_bars(bar_type)
                if args.history_date:
                    zone = ZoneInfo(
                        "America/New_York"
                        if str(identifier.symbol).endswith(".US")
                        else "Asia/Hong_Kong"
                    )
                    start = datetime.fromisoformat(args.history_date).replace(tzinfo=zone)
                    self.request_bars(
                        bar_type, start, start + timedelta(days=1) - timedelta(microseconds=1)
                    )
            self.clock.set_timer(
                "finish", timedelta(seconds=args.seconds), callback=lambda _: handle.stop()
            )

        def on_quote(self, _):
            counts["quotes"] += 1

        def on_trade(self, _):
            counts["trades"] += 1

        def on_book_deltas(self, _):
            counts["l2_snapshots"] += 1

        def on_bar(self, _):
            counts["completed_bars"] += 1

        def on_data(self, _):
            counts["source_metadata"] += 1

        def on_historical_bars(self, bars):
            counts["historical_bars"] += len(bars)

        def on_stop(self):
            self.clock.cancel_timer("finish")

    provider = InstrumentProviderConfig(load_ids=[str(value) for value in identifiers])
    connection = FutuConnectionSettings(host=args.host, port=args.port)
    streaming = None
    if args.catalog:
        path = str(args.catalog.expanduser().resolve())
        streaming = StreamingConfig(
            writer_path=path,
            catalog=DataCatalogConfig(path=path),
            data_types=[NautilusDataType.Bar],
            instrument_types=[NautilusInstrumentType("Equity")],
            promote_on_close=True,
            use_ts_event_for_ts_init=False,
        )
    builder = LiveNodeBuilder.from_config(
        "futu-readonly",
        LiveNodeConfig(
            environment=Environment.LIVE,
            trader_id=TraderId("FUTU-001"),
            logging=LoggerConfig(stdout_level=LogLevel.ERROR),
            delay_post_stop_secs=0,
            streaming=streaming,
        ),
    ).add_data_client(
        "FUTU",
        FutuDataClientFactory,
        FutuDataClientConfig(connection=connection, instrument_provider=provider),
    )
    if args.acc_id:
        builder = builder.add_exec_client(
            "FUTU",
            FutuExecutionClientFactory,
            FutuExecutionClientConfig(
                connection=connection,
                acc_id=args.acc_id,
                trade_market=args.market,
                account_type=args.account_type,
                trading_environment=args.environment,
                dry_run=True,
                instrument_provider=provider,
            ),
        )
    node = builder.build()
    handle = node.handle()
    node.add_strategy(Observer())
    try:
        node.run()
    finally:
        node.dispose()
    report = {
        "node": "NT_2.0.0rc8",
        "counts": dict(counts),
        "orders": "NOT_RUN",
        "unlock": "NOT_RUN",
        "continuous_market_session": "UNVERIFIED",
    }
    print(json.dumps(report, indent=2))
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
