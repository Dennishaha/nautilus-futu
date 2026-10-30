# 验收记录

日期：2026-10-30，Asia/Shanghai。平台：本机 macOS / Apple Silicon。此记录区分离线实现验证与 OpenD 只读实测，不构成实盘认证。

## 离线结果

| 检查 | Python 3.13.17 | Python 3.14.8 |
|---|---|---|
| pytest | 53 passed | 53 passed |
| Ruff check | 通过 | 通过 |
| Ruff format --check | 通过 | 通过 |
| mypy src examples scripts | 通过 | 通过 |
| 全新环境 wheel 安装 / pip check / SDK 导入 | 通过 | 通过 |
| 已安装 wheel 的 NT 双 Factory 注册 | 通过 | 通过 |
| 固定依赖 | NT 2.0.0rc8 / Futu 10.11.7108 | NT 2.0.0rc8 / Futu 10.11.7108 |

3.14 的 pytest-asyncio 0.26 产生事件循环 policy 弃用提示；没有测试失败。`uv.lock` 已生成并检查一致，包含两个 Python 版本的依赖分辨结果。

主要覆盖：

- 符号/真实合约元数据、股票/ETF 全部分档边界、整数/每手数量、元数据不足拒单。
- 分页、重复页与冲突数据、区间过滤、空结果、取消、限频等待与工作线程超时废弃；rc8 批量订阅字段及逐项租约。
- DST、真实接收时间、rc8 历史回调区间回归、复权/时段隔离、已结束周期、港股竞价末根、断线 Bar 补取。
- L2 清空并重建、缺侧报价、来源序号去重、缺失序号保留多重性、CustomData 原始条件。
- 单一恢复协调任务、订阅恢复、有界队列溢出、旧连接推送隔离、关闭资源。
- 订单推送先于提交响应、不确定下单不重发、SDK 异常后不虚假拒单、真实成交 ID 去重、部分成交/部分撤单、修改数量后 NT Cache 更新前到达的成交、撤单范围及外部账户隔离。
- 真实 NT LiveNode 的 data / execution Factory、账户状态、历史和实时回调、SIMULATE 累计报告与 REAL 假服务成交事件、NT CustomData 分发。
- NT Streaming → Catalog 写读，另起只导入 NT 的全新进程读取 Bar 与 Equity，防止供应商自定义 tick 注册表泄漏到读取端。
- 运行时依赖白名单、示例策略无富途 SDK 直连、凭证不进入配置序列化、通用证据信封哈希。

测试中的 REAL 仅是假服务的协议环境标签；没有向真实 OpenD 提交任何订单或解锁请求。

## OpenD 只读实测

本机 OpenD / 官方 SDK 版本 10.11.7108，地址 127.0.0.1:11111。历史日期为 2026-10-23，未复权 NoAdjust、RTH。

完整脱敏结果见 [reports/opend-readonly-final.json](reports/opend-readonly-final.json)：38 项 PASS，1 项上游明确不支持。

| 标的 | 合约 / 每手 | 1m | 5m | 15m | 30m | 60m | 日线 |
|---|---|---:|---:|---:|---:|---:|---:|
| 00700.HK | STOCK / 100 HKD | 331 | 66 | 22 | 11 | 6 | 1 |
| 02800.HK | ETF / 500 HKD | 331 | 66 | 22 | 11 | 6 | 1 |
| AAPL.US | STOCK / 1 USD | 390 | 78 | 26 | 13 | 7 | 1 |

每只标的合约、历史六周期、订阅应答、当前盘口、最近逐笔、显式不复权实时 K 线查询均通过。三只标的实时查询最近三根与历史重叠部分的 OHLCV 均一致；历史事件时间均早于真实接收时间。

已验证账户发现，以及明确港股股票模拟账户的资金、持仓和订单只读查询。模拟成交查询在首次实测被拒，核对官方文档确认不支持，最终脚本明确列为 UNSUPPORTED；连接器通过 NT 标准累计订单报告处理此限制。报告不包含真实账户 ID、资金金额、持仓和订单明细。

真实 NT 节点 + 真实 OpenD 结果：

- [reports/nt-node-readonly.json](reports/nt-node-readonly.json)：账户状态注册成功，历史回调 721 根，完成周期 Bar 6 根，报价 4、逐笔 2、L2 快照 3，来源事件 6。
- [reports/nt-data-catalog-readonly.json](reports/nt-data-catalog-readonly.json)：仅行情节点，历史 721 根，完成周期 Bar 6 根，报价 4、逐笔 2、L2 快照 4，来源事件 6；写入本机 `data/catalog-smoke-final`。

推送计数只是当次周末缓存/首推观察，不代表持续实时覆盖或质量认证。

[reports/catalog-independent-readonly.json](reports/catalog-independent-readonly.json) 另起未导入连接器的新进程，仅用 NT 读回真实 OpenD 节点保存的 6 根 Bar 与两个标的合约，未要求自定义 tick 注册表。

## 未验证与上游限制

| 项目 | 状态 / 原因 |
|---|---|
| 真实下单、改单、撤单、按范围撤单 | NOT_RUN；用户限定订单只运行离线测试 |
| 实盘解锁 | NOT_RUN；只读检查不读取密码或调用 unlock_trade |
| 真实成交推送、真实手续费、真实账户持仓对账 | 未做真实订单验证；假服务已覆盖对应 NT 路径 |
| SIMULATE 逐笔成交查询 / 推送 | 官方不提供；NT 按累计订单报告对账，无供应商 deal_id |
| 开市连续推送、收盘末根现场观察、真实 OpenD 断线恢复 | 未验证；实测当日为周末，离线已覆盖状态分支 |
| 前复权发生公司行动时的一致性 | 显式请求和隔离离线通过；未用实际公司行动样本验证 |
| ETH / ALL、US ETF、非港币柜台 | 实现/规则可配置，未做本机实测；不从 AAPL 或港币 ETF 的结果推定通过 |
| 远期历史与历史额度边界 | 本机仅少量单日请求；异常分页/限频在离线验证 |
| 成交修正 / 取消 | 明确拒绝自动重记账；需 NT 层核实 |
| 逐笔实际手续费 | 未提供；FillReport 零字段是 rc8 类型占位，禁止当作真实零费用 |

## 可复现命令

```bash
.venv/bin/pytest -q
.venv313/bin/pytest -q
.venv/bin/ruff check src tests examples scripts
.venv/bin/ruff format --check src tests examples scripts
.venv/bin/mypy src examples scripts
uv lock --check
.venv/bin/python scripts/readonly_smoke.py --date 2026-10-23 \
  --output reports/opend-readonly-final.json
.venv/bin/python examples/live_node.py --seconds 3 --history-date 2026-10-23 \
  --catalog data/catalog-smoke-final --report reports/nt-data-catalog-readonly.json
```

只读账户节点需自行传入明确 `--acc-id` 与匹配 `--account-type`；验收文件不保存该 ID。模拟订单示例仅检查了默认关闭分支，未开启真实 OpenD 订单操作。

源码包与 wheel 构建通过；两个独立环境均采用普通 wheel 安装，使用 Python `-I` 隔离导入验证 SDK、连接器、NT 双 Factory 注册与释放，未建立 OpenD 连接。结果及已安装 wheel 的 SHA256 见 [reports/installation.json](reports/installation.json)。分发文件位于 `dist/`。
