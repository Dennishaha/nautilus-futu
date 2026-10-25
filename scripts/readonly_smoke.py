"""Opt-in read-only OpenD checks. No unlock or order mutations exist in this script."""

from __future__ import annotations

import argparse
import asyncio
import json
import platform
import time
from importlib.metadata import version
from pathlib import Path
from typing import Any

from nautilus_futu import FutuConnectionSettings, FutuInstrumentProvider
from nautilus_futu.history import fingerprint, history_rows
from nautilus_futu.mapping import rows, timestamp_ns
from nautilus_futu.symbols import canonical_symbol, instrument_id, provider_symbol
from nautilus_futu.transport import CallbackBridge, OpenDSession


async def smoke(args):
    events = []
    historical_samples = {}
    bridge = CallbackBridge(4096, 1, lambda: events.append({"kind": "overflow"}))
    session = OpenDSession(
        FutuConnectionSettings(host=args.host, port=args.port, request_timeout_secs=10),
        bridge,
        trade_market="HK",
    )
    report: dict[str, Any] = {
        "started_ns": time.time_ns(),
        "python": platform.python_version(),
        "nautilus_trader": version("nautilus_trader"),
        "futu-api": version("futu-api"),
        "order_operations": "NOT_RUN",
        "unlock": "NOT_RUN",
        "checks": [],
    }

    async def check(name, operation):
        try:
            result = await operation()
            report["checks"].append({"name": name, "status": "PASS", **(result or {})})
            return result
        except Exception as exc:
            report["checks"].append(
                {
                    "name": name,
                    "status": "UNVERIFIED",
                    "error_type": type(exc).__name__,
                    "operation": getattr(exc, "operation", None),
                }
            )
            return None

    try:
        await session.open()
        report["checks"].append({"name": "OpenD handshake", "status": "PASS"})
        provider = FutuInstrumentProvider()
        provider.set_session(session)
        for raw in args.symbols:
            symbol = canonical_symbol(raw)

            async def contract(symbol=symbol):
                await provider.load_symbols_async([symbol])
                instrument: Any = provider.find(instrument_id(symbol))
                return {
                    "instrument_id": str(instrument.id),
                    "raw_symbol": str(instrument.raw_symbol),
                    "lot_size": str(instrument.lot_size),
                    "currency": str(instrument.quote_currency),
                    "stock_type": instrument.info["stock_type"],
                }

            await check("contract:" + symbol, contract)
            for period in ("K_1M", "K_5M", "K_15M", "K_30M", "K_60M", "K_DAY"):

                async def history(symbol=symbol, period=period):
                    values = await history_rows(
                        session,
                        symbol,
                        period,
                        timestamp_ns(args.date + " 00:00:00", symbol),
                        timestamp_ns(args.date + " 23:59:59", symbol),
                        "NoAdjust",
                        "RTH",
                    )
                    if not values:
                        raise ValueError("empty history")
                    historical_samples[(symbol, period)] = {
                        row["time_key"]: fingerprint(row) for row, _ in values
                    }
                    return {
                        "count": len(values),
                        "adjustment": "NoAdjust",
                        "session": "RTH",
                        "event_before_receipt": all(
                            timestamp_ns(row["time_key"], symbol) < received
                            for row, received in values
                        ),
                    }

                await check("history:" + symbol + ":" + period, history)

            async def subscription(symbol=symbol):
                await session.quote.call(
                    "subscribe",
                    code_list=[provider_symbol(symbol)],
                    subtype_list=["ORDER_BOOK", "TICKER", "K_1M"],
                    session="RTH",
                    is_first_push=True,
                )
                return {"acknowledged": True}

            await check("subscribe:" + symbol, subscription)
            for method, extra in [
                ("get_order_book", {"num": 10}),
                ("get_rt_ticker", {"num": 3}),
                ("get_cur_kline", {"num": 3, "ktype": "K_1M", "autype": "None"}),
            ]:

                async def current(symbol=symbol, method=method, extra=extra):
                    payload, _, _ = await session.quote.call(
                        method, code=provider_symbol(symbol), **extra
                    )
                    if method == "get_cur_kline":
                        known = historical_samples.get((symbol, "K_1M"), {})
                        overlap = [row for row in rows(payload) if row["time_key"] in known]
                        if not overlap or any(
                            fingerprint(row) != known[row["time_key"]] for row in overlap
                        ):
                            raise ValueError(
                                "historical/realtime unadjusted bars differ or have no overlap"
                            )
                        return {
                            "records": len(rows(payload)),
                            "historical_overlap_equal": len(overlap),
                        }
                    return {"records": len(rows(payload))}

                await check(method + ":" + symbol, current)

        async def accounts():
            assert session.trade is not None
            values, _, _ = await session.trade.call("get_acc_list")
            account_rows = rows(values)
            # Query every active equity simulator explicitly by its returned ID.
            # IDs, cash, positions and order contents never enter the public report.
            for index, account in enumerate(account_rows):
                if (
                    account["trd_env"] != "SIMULATE"
                    or account["sim_acc_type"] not in {"STOCK", "STOCK_AND_OPTION"}
                    or account["acc_status"] != "ACTIVE"
                ):
                    continue
                for method in (
                    "accinfo_query",
                    "position_list_query",
                    "order_list_query",
                    "deal_list_query",
                ):

                    async def query(method=method, account=account):
                        assert session.trade is not None
                        payload, _, _ = await session.trade.call(
                            method,
                            acc_id=int(account["acc_id"]),
                            trd_env="SIMULATE",
                            refresh_cache=False,
                        )
                        return {"records": len(rows(payload))}

                    if method == "deal_list_query":
                        report["checks"].append(
                            {
                                "name": f"simulator-{index}:{method}",
                                "status": "UNSUPPORTED",
                                "reason": "official OpenD SIMULATE has no deal query or deal push; use NT cumulative order reports",
                            }
                        )
                    else:
                        await check(f"simulator-{index}:{method}", query)
            return {
                "count": len(account_rows),
                "environments": sorted({row["trd_env"] for row in account_rows}),
            }

        await check("account_discovery", accounts)
        deadline = asyncio.get_running_loop().time() + args.observe_secs
        while asyncio.get_running_loop().time() < deadline:
            try:
                event = await asyncio.wait_for(
                    bridge.get(), min(1, max(0.01, deadline - asyncio.get_running_loop().time()))
                )
                events.append({"kind": event.kind})
                bridge.queue.task_done()
            except TimeoutError:
                pass
        report["pushes"] = {
            kind: sum(event["kind"] == kind for event in events)
            for kind in {event["kind"] for event in events}
        }
        report["continuous_market_session"] = (
            "UNVERIFIED: closed-market observations do not validate continuous ticks, closing bars, or live reconnects"
        )
    finally:
        await session.close()
        report["finished_ns"] = time.time_ns()
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
    print(json.dumps(report, indent=2, ensure_ascii=False))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=11111)
    parser.add_argument("--date", default="2026-10-23", help="a known trading date, YYYY-MM-DD")
    parser.add_argument("--symbols", nargs="+", default=["00700.HK", "02800.HK", "AAPL.US"])
    parser.add_argument("--observe-secs", type=float, default=3)
    parser.add_argument("--output", type=Path, default=Path("reports/opend-readonly.json"))
    asyncio.run(smoke(parser.parse_args()))


if __name__ == "__main__":
    main()
