"""Disabled by default. An explicit opt-in submits one SIMULATE order through NT."""

import argparse
import os
from datetime import timedelta
from typing import Any

from nautilus_trader.common import Environment
from nautilus_trader.config import StrategyConfig
from nautilus_trader.live import InstrumentProviderConfig, LiveNodeBuilder, LiveNodeConfig
from nautilus_trader.model import InstrumentId, OrderSide, Price, Quantity, TimeInForce, TraderId
from nautilus_trader.trading import Strategy

from nautilus_futu import (
    FutuDataClientConfig,
    FutuDataClientFactory,
    FutuExecutionClientConfig,
    FutuExecutionClientFactory,
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--enable-simulated-order", action="store_true")
    parser.add_argument("--acc-id", type=int)
    parser.add_argument("--symbol", default="00700.HK")
    parser.add_argument("--quantity", type=int)
    parser.add_argument("--price")
    parser.add_argument("--order-type", choices=["LIMIT", "MARKET"], default="LIMIT")
    parser.add_argument("--account-type", choices=["CASH", "MARGIN"], default="CASH")
    args = parser.parse_args()
    if not args.enable_simulated_order or os.getenv("FUTU_ENABLE_SIMULATED_ORDER") != "1":
        print("DISABLED: requires --enable-simulated-order and FUTU_ENABLE_SIMULATED_ORDER=1")
        return
    if not args.acc_id or not args.quantity or args.order_type == "LIMIT" and args.price is None:
        parser.error("explicit --acc-id, --quantity and limit --price are required")
    identifier = InstrumentId.from_str(args.symbol + ".FUTU")

    class OneOrder(Strategy):
        def __new__(cls):
            return super().__new__(cls, StrategyConfig())  # type: ignore[call-arg]

        def __init__(self):
            super().__init__(StrategyConfig())

        def on_start(self):
            values: dict[str, Any] = dict(
                instrument_id=identifier,
                order_side=OrderSide.BUY,
                quantity=Quantity.from_int(args.quantity),
                time_in_force=TimeInForce.DAY,
            )
            order = (
                self.order_factory.limit(**values, price=Price.from_str(args.price))
                if args.order_type == "LIMIT"
                else self.order_factory.market(**values)
            )
            self.submit_order(order)
            self.clock.set_timer("finish", timedelta(seconds=15), callback=lambda _: handle.stop())

        def on_stop(self):
            self.cancel_all_orders(identifier)
            self.clock.cancel_timer("finish")

    provider = InstrumentProviderConfig(load_ids=[str(identifier)])
    node = (
        LiveNodeBuilder.from_config(
            "futu-simulator",
            LiveNodeConfig(
                environment=Environment.LIVE, trader_id=TraderId("SIM-001"), delay_post_stop_secs=1
            ),
        )
        .add_data_client(
            "FUTU", FutuDataClientFactory, FutuDataClientConfig(instrument_provider=provider)
        )
        .add_exec_client(
            "FUTU",
            FutuExecutionClientFactory,
            FutuExecutionClientConfig(
                acc_id=args.acc_id,
                trade_market=args.symbol.rsplit(".", 1)[1],
                account_type=args.account_type,
                trading_environment="SIMULATE",
                dry_run=False,
                instrument_provider=provider,
            ),
        )
        .build()
    )
    handle = node.handle()
    node.add_strategy(OneOrder())
    try:
        node.run()
    finally:
        node.dispose()


if __name__ == "__main__":
    main()
