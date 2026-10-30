# nautilus-futu

独立的富途 OpenD ↔ NautilusTrader 连接器。固定 `nautilus_trader==2.0.0rc7`、`futu-api==10.11.7108`，支持 Python 3.13 / 3.14。

运行时只依赖 NT、官方富途 SDK/OpenD 和 Python 标准库。长桥与 IBKR 仅供实现结构参考。本包不导入或修改数据工具、研究包、其他券商连接器；策略只调用 NT API。

## 安装与启动

在本目录使用独立环境：

```bash
uv sync --extra dev --python 3.14
uv run python examples/live_node.py --seconds 10 --history-date 2026-10-23
```

OpenD 默认 `127.0.0.1:11111`，需自行登录并开通对应市场行情权限。日期参数应换成希望检查的已交易日期。示例默认只有行情客户端，没有订单行为。

通过 NT 原生 StreamingConfig 保存 Bar 与 Equity：

```bash
uv run python examples/live_node.py --seconds 10 --history-date 2026-10-23 \
  --catalog data/catalog-noadjust-rth
```

Catalog 的管理、历史任务编排、质量审计和研究均由调用方在 NT 层完成。不同复权/时段必须使用独立进程、独立 Catalog；不能因经过 NT 就把数据视为质量合格。

历史OHLC使用统一16位小数保留OpenD源数字，避免9位历史价格被原8位限制拒绝，同时满足NT Catalog同一BarType的精度一致性；超过16位仍拒绝，不四舍五入、不修复非法OHLC，真实获取时间保持不变。交易订单的价格刻度校验沿用原规则。历史日线时段与执行质量仍需调用方核实，`session=RTH`参数本身不代表日线验收通过。

## 公共接口

```python
from nautilus_trader.common import Environment
from nautilus_trader.live import InstrumentProviderConfig, LiveNode
from nautilus_trader.model import TraderId
from nautilus_futu import (
    FutuDataClientConfig, FutuDataClientFactory,
    FutuExecutionClientConfig, FutuExecutionClientFactory,
    FutuInstrumentProvider, FutuConnectionSettings,
)

instruments = InstrumentProviderConfig(load_ids=["00700.HK.FUTU", "AAPL.US.FUTU"])
node = (
    LiveNode.builder("futu", TraderId("FUTU-001"), Environment.LIVE)
    .add_data_client("FUTU", FutuDataClientFactory,
                     FutuDataClientConfig(instrument_provider=instruments))
    .build()
)
# 注册策略后 node.run()；结束后 node.dispose()。
```

Factory 构造时不连接 OpenD、不创建运行任务。合约加载、连接、回调消费与断线恢复位于 NT 生命周期中。更多示例见 `examples/live_node.py`。

标识为 `00700.HK.FUTU`、`AAPL.US.FUTU`；`raw_symbol` 为 `HK.00700`、`US.AAPL`。实际交易所、富途品种、每手股数、资料获取时间与价格规则位于 `instrument.info`。同一真实统一账户只配置一个执行客户端，避免重复建模资产；不同模拟账户分别配置。

## 支持矩阵

| 能力 | 首版行为 |
|---|---|
| 合约 | 港美股及 A 股 STOCK / ETF 行情资料；另支持 Futu `get_future_info` 返回的真实 HK/US FuturesContract（行情只读）；使用真实静态资料，拒绝缺少合约规模、最小变动、币种或到期时间的期货资料；A 股扩展只开放行情读取 |
| 历史 Bar | LAST / EXTERNAL；1、5、15、30 分钟、1 小时、日线；rc7 的 60 分钟用 `1-HOUR` 表示 |
| 复权 / 时段 | 默认 `NoAdjust` / `RTH`；显式 `ForwardAdjust`；US 可选 ETH / ALL，HK 仅 RTH |
| 实时 Bar | 推送只触发显式复权查询；只发布已结束周期；轮询覆盖无末根推送场景，重连历史补取 |
| QuoteTick | 有效双边顶层盘口；单边缺失或交叉盘口不生成 QuoteTick |
| TradeTick | 富途逐笔方向；来源序号去重；缺少序号保留多重性并分配明确非来源 ID |
| L2 | 完整价格档快照，CLEAR + ADD 重建；不提供 L3 / 逐单增量 |
| 历史 tick / quote | 只有有限最近逐笔 / 当前盘口；不是任意区间 tick 历史库 |
| 订单 | Market / Limit；DAY / GTC；市价仅 RTH，HK 市价仅 DAY；其余是否可用由券商账户与市场最终校验 |
| 修改 / 撤销 | 限价数量/价格修改、单笔撤销、当前策略和合约/方向范围撤单；不会账户级批量撤销外部订单 |
| 账户 / 持仓 | 明确账户 ID、环境、类型、状态与市场权限；资金、持仓、订单通过 NT typed reports / account state 输出 |
| REAL 成交 | 真实 deal_id 去重；成交先于响应、部分成交和撤单期间成交；查询补账 |
| SIMULATE 成交 | **供应商不提供成交明细查询或推送**；用 NT OrderStatusReport 的累计数量/均价对账，不伪造富途 deal_id |
| 手续费 | 推送未提供逐笔费用；事件 commission=None；rc7 FillReport 必填 Money 的位置使用显式披露的零占位，不能当作真实零费用 |
| 不支持 | 期权、期货下单、A 股下单、碎股委托、条件单/订单列表、GTD/IOC/FOK、自动价格调整、成交更正自动重记账 |

订单数量/价格先以 Decimal 校验，再按 SDK 的数值参数格式传入。港股股票与 ETF 使用不同分档价格表；`.0001` 是 NT 价格表示精度，**不是全价格区间的合法委托步长**。完整分档表保存在 `info.price_ladder`，执行端使用该版本的规则严格校验。合约不绑定需要额外注册的自定义 tick scheme，因此普通 NT Catalog 可独立读取；NT 的默认 `next_bid_price/next_ask_price` 不能替代本连接器的市场价格校验。港股股票规则版本为 2026-08-03 生效版本。非港币柜台币种资料不足时需显式 `currency_overrides`；不得为未知合约填入占位元数据后执行。

## 配置与数据口径

`FutuConnectionSettings` 支持地址、券商、加密开关、请求超时、重连退避/次数、健康检查周期、有界回调队列大小。每个 SDK context 拥有专用工作线程，NT 事件循环不执行同步 SDK 请求。超时会废弃该工作通道，迟到响应和旧连接推送不得当作新响应继续使用。

行情配置包括 `adjustment`、`session`、`bar_poll_interval_secs`、`currency_overrides`、可选 `evidence_dir`。客户端创建时固定行情口径；进程级 guard 阻止同一 BarType 混入不同复权/时段。`completed_bars_only=False` 明确拒绝。rc7 没有 Bar 修订标志：已发布周期发生变化会记录 `provider_bar_revision`，不静默覆盖；原始响应可在通用证据目录保留。

`ts_event` 保留供应商市场时间；`ts_init` 为实际接收时间，历史也如此。US 使用 America/New_York 夏令时规则，HK 使用 Asia/Hong_Kong。盘口时间是 OpenD 服务器接收时间；缺失时明确标记为本地接收代理，均不宣称交易所时间。

富途逐笔条件、sequence、CACHE/REALTIME 来源，以及盘口的两侧服务器接收时间，通过标准 NT `CustomData` 保留：

```python
from nautilus_futu import FUTU_SOURCE
from nautilus_trader.model import ClientId

# Strategy.on_start 中；仍需正常订阅对应 quotes/trades/book。
self.subscribe_data(FUTU_SOURCE, ClientId("FUTU"))
# Strategy.on_data 接收 FutuSourceEvent，包含 provider_fields / time_basis / ts_event / ts_init。
```

可选证据文件是追加写入的公共行情响应信封，包含请求、时间、复权/时段参数、原始内容及 SHA256；不是研究快照、质量认证或历史可用时间证明。账户、成交和交易凭证不写入此公共证据目录。

## 交易安全默认值

执行配置默认 `trading_environment="SIMULATE"`、`dry_run=True`。`acc_id` 必须显式指定（或设置 `FUTU_ACC_ID`），`account_type` 必须与 OpenD 真实账户类型一致。仅配置 REAL 并不会解锁或允许交易；真实订单还要求 `allow_real_trading=True`、`dry_run=False`，以及由 `unlock_password_env` 指定的进程环境变量中的密码 MD5。

配置只保存环境变量名称，不保存密码。解锁仅发生在明确启用 REAL 订单操作时，读账户不解锁。官方 SDK 自身可能写连接日志，运行环境应按自己的凭证和日志管理要求配置。

下单使用稳定 remark 关联 NT ClientOrderId；它是关联键，**不构成券商幂等保证**。提交超时保留未确认状态，恢复后查询对账，不自动再次下单。进程重启应恢复 NT 自身持久化的 Cache/订单身份；本包没有另一套订单数据库。

模拟交易示例默认关闭：

```bash
uv run python examples/simulated_order.py
# DISABLED，无连接、无订单。
```

显式设置 `FUTU_ENABLE_SIMULATED_ORDER=1` 并传 `--enable-simulated-order --acc-id ... --quantity ... --price ...` 后，示例才会通过 NT 提交一笔 SIMULATE 限价买单，15 秒后尝试撤销该策略未完成订单。设置 `--order-type MARKET` 会提交模拟市价单；示例可能成交，不作自动反向平仓。本轮未运行任何真实 OpenD 订单操作。

## 验证

```bash
uv run pytest -q
uv run ruff check src tests examples scripts
uv run ruff format --check src tests examples scripts
uv run mypy src examples scripts
uv run python -m build
uv run python scripts/readonly_smoke.py --date 2026-10-23
```

最后一条只执行握手、合约、历史/实时行情、订阅与模拟账户查询；默认关闭 contexts 并保留脱敏摘要，不解锁、不下单、不改单、不撤单。市场关闭时收到缓存推送不能证明连续实时行情已验收。

实现细节与官方依据见 [DESIGN.md](DESIGN.md)，离线、双版本与本机实测结果见 [VALIDATION.md](VALIDATION.md)。
