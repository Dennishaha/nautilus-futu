from __future__ import annotations

import threading
import time
from copy import deepcopy

from nautilus_futu.mapping import timestamp_ns

RECEIVED = timestamp_ns("2026-10-25 12:00:00", "00700.HK")


def static(code="HK.00700", **kwargs):
    return dict(
        code=code,
        stock_type="STOCK",
        stock_id=700,
        exchange_type="HK_MAINBOARD" if code.startswith("HK") else "US_NASDAQ",
        lot_size=100 if code.startswith("HK") else 1,
        delisting=False,
        **kwargs,
    )


def candle(time_key="2026-10-23 09:30:00", **kwargs):
    return dict(
        code="HK.00700",
        time_key=time_key,
        open=100,
        high=102,
        low=99,
        close=101,
        volume=1000,
        **kwargs,
    )


def book(**kwargs):
    return {
        "code": "HK.00700",
        "Bid": [(100, 100, 1, {}), (99, 200, 2, {})],
        "Ask": [(101, 100, 1, {}), (102, 200, 2, {})],
        "svr_recv_time_bid": "2026-10-23 09:30:00.123",
        "svr_recv_time_ask": "2026-10-23 09:30:00.124",
        **kwargs,
    }


def order_row(**kwargs):
    return {
        "code": "HK.00700",
        "order_id": "V-1",
        "remark": "",
        "order_status": "SUBMITTED",
        "order_type": "NORMAL",
        "time_in_force": "DAY",
        "trd_side": "BUY",
        "qty": 100,
        "price": 100,
        "dealt_qty": 0,
        "dealt_avg_price": 0,
        "create_time": "2026-10-23 09:30:00",
        "updated_time": "2026-10-23 09:30:00",
        "trd_env": "REAL",
        "_opend_acc_id": 123,
        **kwargs,
    }


def fill_row(**kwargs):
    return {
        "code": "HK.00700",
        "order_id": "V-1",
        "deal_id": "D-1",
        "qty": 40,
        "price": 100,
        "trd_side": "BUY",
        "status": "OK",
        "create_time": "2026-10-23 09:30:00",
        "trd_env": "REAL",
        "_opend_acc_id": 123,
        **kwargs,
    }


class FakeContext:
    def __init__(self, server, bridge):
        self.server, self.bridge = server, bridge
        self.closed = False
        self.conn_id = len(server.contexts) + 1
        server.contexts.append(self)

    def set_sync_query_connect_timeout(self, seconds):
        self.timeout = seconds

    def get_sync_conn_id(self):
        return self.conn_id

    def close(self):
        self.closed = True

    def __getattr__(self, name):
        def call(**kwargs):
            self.server.calls.append((name, deepcopy(kwargs), threading.get_ident()))
            if name in self.server.fail:
                return -1, self.server.fail[name]
            if name == "get_stock_basicinfo":
                return 0, [static(code) for code in kwargs["code_list"]]
            if name == "request_history_kline":
                if self.server.pages:
                    return self.server.pages.pop(0)
                return 0, self.server.bars, None
            if name == "get_cur_kline":
                return 0, self.server.bars
            if name == "request_trading_days":
                return 0, [{"time": kwargs["start"], "trade_date_type": "WHOLE"}]
            if name == "get_order_book":
                return 0, book()
            if name == "get_rt_ticker":
                return 0, self.server.ticks
            if name == "get_acc_list":
                return 0, self.server.accounts or [
                    dict(
                        acc_id=123,
                        trd_env="SIMULATE",
                        acc_status="ACTIVE",
                        trdmarket_auth=["HK"],
                        sim_acc_type="STOCK",
                        acc_type="CASH",
                    )
                ]
            if name == "accinfo_query":
                return 0, [
                    dict(
                        currency="HKD",
                        total_assets=100000,
                        cash=100000,
                        frozen_cash=100,
                        available_funds=99900,
                        initial_margin=0,
                        maintenance_margin=0,
                    )
                ]
            if name in {"order_list_query", "history_order_list_query"}:
                return 0, [
                    deepcopy(row)
                    for row in self.server.orders
                    if not kwargs.get("order_id") or row["order_id"] == kwargs["order_id"]
                ]
            if name in {"deal_list_query", "history_deal_list_query"}:
                return 0, deepcopy(self.server.fills)
            if name == "position_list_query":
                return 0, deepcopy(self.server.positions)
            if name == "place_order":
                row = order_row(
                    code=kwargs["code"],
                    remark=kwargs["remark"],
                    price=kwargs["price"],
                    qty=kwargs["qty"],
                    order_type=kwargs["order_type"],
                )
                self.server.orders.append(row)
                if self.server.before_response:
                    self.server.before_response(self, row)
                if self.server.submit_delay:
                    time.sleep(self.server.submit_delay)
                return 0, [deepcopy(row)]
            if name == "modify_order":
                for row in self.server.orders:
                    if row["order_id"] == kwargs["order_id"]:
                        if kwargs["modify_order_op"] == "CANCEL":
                            row["order_status"] = (
                                "CANCELLED_PART" if row["dealt_qty"] else "CANCELLED_ALL"
                            )
                        else:
                            row.update(qty=kwargs["qty"], price=kwargs["price"])
                return 0, [{"order_id": kwargs["order_id"]}]
            return 0, {}

        return call


class FakeServer:
    def __init__(self):
        self.contexts, self.calls, self.fail, self.pages = [], [], {}, []
        self.orders, self.fills, self.positions, self.ticks = [], [], [], []
        self.accounts = []
        self.bars = [candle()]
        self.before_response, self.submit_delay = None, 0

    def quote(self, settings, bridge):
        return FakeContext(self, bridge)

    def trade(self, settings, bridge, market):
        return FakeContext(self, bridge)
