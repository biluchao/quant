# CONVENTION.md — 量化系统开发规范

> 版本：v1.0.0
> 生效日期：Step 1 启动日
> 维护人：张三（系统）、乐高（执行）、比蒙（策略）
> 变更流程：三人签字后写入 CHANGELOG.md，版本号递增

---

## 0. 总则

**0.1 规范不是给人看的，是给 CI 和 pre-commit 看的。**
每条规范附检查方式。检查方式为 `[CI]` 的，由 `.github/workflows/ci.yml` 执行；为 `[HOOK]` 的，由 `.pre-commit-config.yaml` 执行；为 `[REVIEW]` 的，由代码审查人手动检查。

**0.2 规范的目的不是限制，是让三个人写出来的代码像一个人写的。**
规范约束的是**接口和边界**，不约束**内部实现细节**。函数内部怎么写，只要不违反接口契约，不限制。

**0.3 规范变更需要三人签字。**
任何规范变更，需比蒙、乐高、张三三人在 PR 中 approve，然后写入 `CHANGELOG.md`，版本号递增（major.minor.patch）。

---

## 1. 目录与文件命名

### 1.1 目录即类型

```
quant/
├── core/          # 抽象基类（不随 Step 变化）
├── system/        # 系统层（时钟、持久化、限频、对账）
├── data/          # 数据层（WS 流、REST、历史加载）
├── factors/       # 因子层（每个文件一个因子）
├── strategies/    # 策略层（每个文件一个策略）
├── states/        # 状态机（每个状态一个文件）
├── rounds/        # 轮次管理
├── execution/     # 执行层（网关、成交模型、滑点）
├── risk/          # 风控层
├── ai/            # AI 层（Step 3+）
├── analytics/     # 分析层
├── frontend/      # 前端
├── api/           # 后端 API
├── backtest/      # 回测
├── monitoring/    # 监控（Step 5）
├── deploy/        # 部署（Step 5）
└── tests/         # 测试
```

`[CI]` 检查：`scripts/check_structure.py` 验证目录结构与规范一致，缺失目录报警。

### 1.2 文件名即功能，前缀即类型

| 前缀 | 类型 | 示例 | 反例 |
|---|---|---|---|
| `factor_` | 因子 | `factor_momentum.py` | `momentum.py`、`mom.py` |
| `strategy_` | 策略 | `strategy_trend_round.py` | `trend.py`、`trend_round.py` |
| `state_` | 状态 | `state_breakout.py` | `breakout.py` |
| `gateway_` | 执行网关 | `gateway_binance.py` | `binance.py`、`exec_binance.py` |
| `policy_` | 风控策略 | `policy_minimal.py` | `risk.py`、`minimal.py` |
| `ws_` | WebSocket 流 | `ws_kline.py` | `kline_stream.py` |
| `fill_model_` | 成交模型 | `fill_model_market.py` | `fill.py`、`market_fill.py` |
| `routes_` | API 路由 | `routes_status.py` | `status_api.py` |

`[HOOK]` 检查：`scripts/check_naming.py` 验证文件名前缀与所在目录类型匹配。

### 1.3 测试文件与源文件同名

```
factors/factor_momentum.py
tests/unit/test_factor_momentum.py
```

`[CI]` 检查：`scripts/check_test_coverage.py` 验证每个源文件有对应测试文件，缺失报警（Step 1 允许警告，Step 2 起强制）。

### 1.4 版本号

文件版本用后缀：`factor_momentum_v1.py`、`factor_momentum_v2.py`。
系统版本用 `v{step}.{major}.{minor}`：`v1.0.0` 表示 Step 1 的第一个正式版本。

`[REVIEW]` 检查：代码审查人确认版本号递增规则。

### 1.5 废弃文件

模块废弃用 `deprecated: true` 字段标记，不直接删除。保留至少一个 Step 周期，确认无引用后删除。

```python
MANIFEST = {
    ...
    "deprecated": True,
    "deprecated_since": "v1.2.0",
    "replaced_by": "factor_momentum_v2",
}
```

`[CI]` 检查：`scripts/check_deprecated.py` 扫描所有 `deprecated: true` 的模块，验证 `replaced_by` 存在且未被废弃。

---

## 2. 模块接口契约

### 2.1 每个模块必须暴露 `create()` 工厂函数

```python
def create(config: dict) -> <AbstractType>:
    """标准工厂入口。系统通过此函数创建实例。"""
    return MyClass(config)
```

- 参数只能是 `config: dict`，不接受其他位置参数
- 返回值必须是 `core/` 中定义的抽象类型的实例
- 不得在 `create()` 中执行 I/O 操作（网络、磁盘）

`[CI]` 检查：`scripts/check_interface.py` 用 AST 解析验证每个模块有 `create()`，签名正确。

### 2.2 每个模块必须有 `MANIFEST`

```python
MANIFEST = {
    # 必填
    "name": "momentum",                    # 全局唯一，与文件名去掉前缀后一致
    "type": "factor",                      # 对应 core/ 中的抽象类型
    "version": "1.0",                      # 模块版本
    "step": 1,                             # 最早引入的 Step
    "capabilities": ["momentum", "roc"],   # 能提供的能力
    "dependencies": ["bar.close"],         # 依赖的数据字段
    "owner": "bimon",                      # 负责人
    # 选填
    "fallback": None,                      # 降级模块名
    "deprecated": False,                   # 是否废弃
    "replaced_by": None,                   # 被谁替代
    "enabled": True,                       # 是否启用
}
```

**校验规则：**
- `name` 必须与文件名去掉前缀后一致（`factor_momentum.py` → `name="momentum"`）
- `type` 必须在 `core/` 中有对应的抽象类
- `step` 必须与当前部署 Step 兼容（`step <= current_step` 才加载）
- `fallback` 必须指向已存在的模块，且不能循环引用
- `capabilities` 不能为空

`[CI]` 检查：`scripts/check_manifest.py` 校验所有模块的 MANIFEST，不合法则 CI 失败。

### 2.3 MANIFEST 校验函数

```python
# core/manifest.py
def validate_manifest(manifest: dict, known_modules: set) -> list[str]:
    """返回错误列表，空列表表示通过。"""
    errors = []
    required = ["name", "type", "version", "step", "capabilities", "owner"]
    for k in required:
        if k not in manifest:
            errors.append(f"missing required field: {k}")
    if manifest.get("fallback") and manifest["fallback"] not in known_modules:
        errors.append(f"fallback '{manifest['fallback']}' not found")
    if not manifest.get("capabilities"):
        errors.append("capabilities cannot be empty")
    return errors
```

### 2.4 禁止跨层反向依赖

```
data → factors → strategies → states → rounds → execution → risk
```

**允许的依赖方向：** 上层可以依赖下层，下层不得依赖上层。
**禁止：** `factors` 不得 import `strategies`，`risk` 不得 import `execution` 的具体实现。

`[CI]` 检查：`scripts/check_import_layers.py` 解析 import 关系，发现反向依赖则 CI 失败。

### 2.5 禁止策略层直接调交易所 API

所有订单操作必须通过 `OrderRouter`。策略层只生成 `Order` 对象，不关心路由。

```python
# 禁止
self.gateway.place_order(order)

# 允许
self.order_router.submit(order)
```

`[REVIEW]` 检查：代码审查人确认策略层/状态机中无直接 `gateway.place_order` 调用。

---

## 3. 数据类型与精度

### 3.1 金额、价格、数量一律用 `Decimal`

```python
# 禁止
price = 67000.5
qty = 0.001

# 允许
from decimal import Decimal
price = Decimal("67000.5")
qty = Decimal("0.001")
```

**原因：** `float` 的精度问题在订单计算中可能导致 `0.1 + 0.2 != 0.3`，进而导致下单数量/价格不符合交易所精度要求，被拒单或滑点异常。

`[CI]` 检查：`scripts/check_decimal.py` 扫描 `execution/`、`risk/`、`rounds/` 目录，发现 `float` 类型的价格/数量变量则报警。

### 3.2 数量精度处理

```python
def round_step_size(qty: Decimal, step_size: Decimal) -> Decimal:
    """向下取整到 step_size 的整数倍。"""
    return (qty / step_size).quantize(
        Decimal("1"), rounding=ROUND_DOWN
    ) * step_size
```

下单前必须调用此函数。`[REVIEW]` 检查：`gateway_binance.py` 的 `place_order` 中必须有 `round_step_size` 调用。

### 3.3 时间戳统一

| 场景 | 用什么 | 格式 |
|---|---|---|
| 交易逻辑（K线收线、止损触发、订单超时） | `clock.now_ms()` | 单调时钟毫秒 |
| 日志 | `datetime.utcnow()` | ISO 8601 UTC |
| 数据库存储 | 毫秒时间戳 | INTEGER |
| 前端展示 | 本地时区 | `Intl.DateTimeFormat` |

**禁止：** 在交易逻辑中使用 `time.time()` 或 `datetime.now()`。所有时间必须经过 `HybridClock`。

`[CI]` 检查：`scripts/check_clock_usage.py` 扫描 `states/`、`rounds/`、`execution/`，发现 `time.time()` 或 `datetime.now()` 则报警。

### 3.4 因子输出归一化

所有因子必须输出以下两种区间之一：
- `[-1, 1]`：有方向的因子（动量、OBI、DDE）
- `[0, 1]`：无方向的因子（成交量分位数）

**禁止**输出未归一化的原始值。归一化方式必须在 `docs/FACTOR_SPEC.md` 中记录。

`[CI]` 检查：`tests/unit/test_factor.py` 中包含归一化范围测试，超出范围则测试失败。

---

## 4. 错误处理

### 4.1 错误分层

```python
class QuantError(Exception):
    """所有量化系统异常的基类。"""
    code = 1

class BootError(QuantError):
    """启动阶段可预期错误。"""
    code = 2

class DataError(QuantError):
    """数据层错误（断线、数据缺失、格式异常）。"""
    code = 3

class ExecutionError(QuantError):
    """执行层错误（下单失败、撤单失败、成交异常）。"""
    code = 4

class StrategyError(QuantError):
    """策略层错误（因子计算异常、状态机异常）。"""
    code = 5

class EmergencyError(QuantError):
    """紧急停止。"""
    code = 6
```

### 4.2 退出码约定

| 退出码 | 含义 | systemd 行为 |
|---|---|---|
| 0 | 正常退出 | 不重启 |
| 2 | 配置错误 | 不重启，需人工修 |
| 3 | 启动检查失败 | 不重启，需人工修 |
| 4 | 运行时错误 | 重启（带退避） |
| 5 | 策略层致命错误 | 不重启，需人工查 |
| 6 | 紧急停止 | 不重启，需人工查 |

`[CI]` 检查：`tests/unit/test_exit_codes.py` 验证每个异常类映射到正确退出码。

### 4.3 禁止裸 `except`

```python
# 禁止
try:
    do_something()
except:
    pass

# 禁止
try:
    do_something()
except Exception:
    pass

# 允许
try:
    do_something()
except SpecificError as e:
    logger.error(f"specific error: {e}")
    raise
```

`[HOOK]` 检查：`scripts/check_bare_except.py` 扫描所有 `.py` 文件，发现裸 `except` 或 `except Exception: pass` 则拒绝提交。

### 4.4 关键操作必须幂等

**幂等操作清单：**

| 操作 | 幂等键 | 说明 |
|---|---|---|
| 撤单 | `clientOrderId` | 重复撤同一单，交易所返回 `-2011`（已撤/已成交），视为成功 |
| 平仓 | `round_id` | 重复平同一轮，先查持仓，无持仓则跳过 |
| 对账 | `reconcile_window_ts` | 同一时间窗口只对账一次 |
| 状态持久化 | `bar_ts` | 同一根 K 线的快照只写一次，覆盖而非追加 |

`[REVIEW]` 检查：代码审查人确认关键操作有幂等设计。

### 4.5 重试规则

- **只对幂等操作重试**
- **重试最多 3 次**，指数退避（0.5s, 1s, 2s）
- **重试前必须检查原操作是否已成功**（用幂等键查询）
- **重试失败后必须报警**，不得静默放弃

```python
def retry_idempotent(func, idempotent_key, max_retries=3):
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            # 先检查是否已成功
            if check_idempotent(idempotent_key):
                return get_idempotent_result(idempotent_key)
            time.sleep(0.5 * (2 ** attempt))
```

`[REVIEW]` 检查：代码审查人确认重试逻辑符合上述规则。

---

## 5. 日志

### 5.1 日志格式

每条日志必须包含：
```
[timestamp] [level] [module] [event] [detail]
```

示例：
```
2024-06-15T14:23:00.123Z INFO  state_machine  state_transition  from=BREAKOUT to=TREND score=0.62
```

`[CI]` 检查：`scripts/check_log_format.py` 扫描日志调用，验证格式字符串包含必要字段。

### 5.2 日志分级

| 级别 | 用途 | 示例 |
|---|---|---|
| DEBUG | 中间计算值、因子快照 | `obi=0.15 dde=0.08 score=0.62` |
| INFO | 状态迁移、订单成交、轮次开始/结束 | `round_open id=abc123 dir=LONG` |
| WARN | 数据降级、健康度下降、参数偏离 | `data_degraded kline_disconnected` |
| ERROR | 订单失败、对账偏差、因子异常 | `order_rejected code=-2010` |
| CRITICAL | 紧急停止、保证金率超标、连续亏损熔断 | `emergency_stop margin_ratio=0.85` |

### 5.3 关键日志必须包含决策依据

每笔订单的日志必须包含：
```json
{
  "event": "order_submit",
  "signal_id": "sig-20240615-0001",
  "direction": "LONG",
  "trigger_score": 0.62,
  "factor_snapshot": {
    "momentum": 0.18,
    "obi": 0.14,
    "volume": 0.06
  },
  "state_from": "BREAKOUT_CONFIRM",
  "state_to": "TREND_RIDE",
  "entry_price": "67342.5",
  "stop_price": "66981.2",
  "qty": "0.032",
  "risk_amount": "11.56",
  "equity": "1000.0"
}
```

`[REVIEW]` 检查：代码审查人确认订单日志包含上述字段。

### 5.4 禁止在日志中输出密钥

API Key、Secret、Token 不得出现在日志中。`[HOOK]` 检查：`scripts/check_secrets.py` 扫描日志调用，发现 `api_key`、`secret`、`token` 等关键词则拒绝提交。

---

## 6. 测试

### 6.1 测试分层

```
tests/
├── unit/          # 单元测试：函数级，无 I/O，毫秒级
├── integration/   # 集成测试：模块间，有 I/O，秒级
└── e2e/           # 端到端：全链路，分钟级
```

`[CI]` 检查：每次 PR 跑 `unit` + `integration`，每日构建跑 `e2e`。

### 6.2 覆盖率要求

| Step | 单元测试覆盖率 | 说明 |
|---|---|---|
| Step 1 | ≥ 40% | 重点覆盖风控、轮次、状态机 |
| Step 2 | ≥ 60% | 加系统层、对账、限频 |
| Step 3 | ≥ 70% | 加加仓、反向、AI |
| Step 4 | ≥ 80% | 加多品种、相关性 |
| Step 5 | ≥ 85% | 全覆盖 |

`[CI]` 检查：`pytest --cov=quant --cov-fail-under={threshold}`。

### 6.3 必须有的测试

**每个模块必须有：**
- 正常路径测试
- 边界条件测试（空值、极值、超限）
- 异常路径测试（网络失败、数据缺失、格式错误）

**系统层必须有：**
- `test_clock_jump`: NTP 回拨不触发时间倒退
- `test_rate_limiter_concurrent`: 10 线程同时下单不超限
- `test_reconciler_ghost_order`: 幽灵订单触发冻结
- `test_round_stop_update`: 加仓后止损上移，`Risk_round <= R0`
- `test_partial_fill`: 部分成交后止损数量更新

`[CI]` 检查：`scripts/check_required_tests.py` 验证上述测试文件存在且通过。

### 6.4 测试必须可复现

所有涉及随机性的测试必须固定种子：
```python
def test_slippage_model():
    rng = np.random.default_rng(42)
    model = SlippageModel(rng=rng)
    ...
```

`[HOOK]` 检查：`scripts/check_test_seed.py` 扫描测试文件，发现随机调用但无种子则报警。

---

## 7. 配置与密钥

### 7.1 配置分层

```
config/
├── base.yaml          # 全局配置（所有 Step 共用）
├── step1.yaml         # Step 1 参数覆盖
├── step2.yaml         # Step 2 参数覆盖（Step 1 不用）
└── symbols/
    ├── btcusdt.yaml   # BTC 参数
    └── ethusdt.yaml   # ETH 参数（Step 4）
```

加载顺序：`base.yaml` → `step{N}.yaml` → `symbols/{symbol}.yaml`，后者覆盖前者。

### 7.2 关键配置必须有安全上限

```yaml
# step1.yaml
per_trade_risk: 0.007      # 0.7% 权益
daily_dd_limit: 0.02       # 2% 降频
daily_dd_halt: 0.03        # 3% 熔断
max_position_ratio: 0.3    # 最大仓位 30% 权益
```

启动时校验：
```python
if config["per_trade_risk"] > 0.02:
    raise BootError("per_trade_risk > 2%")
if config["daily_dd_halt"] > 0.05:
    raise BootError("daily_dd_halt > 5%")
```

`[CI]` 检查：`scripts/check_config_limits.py` 验证 `step*.yaml` 中的关键参数不超过安全上限。

### 7.3 密钥管理

**禁止：** 硬编码 API Key、Secret、Token。
**允许：** 通过环境变量或 `.env` 文件加载。

```python
# 禁止
api_key = "abc123..."

# 允许
from dotenv import load_dotenv
load_dotenv()
api_key = os.getenv("BINANCE_API_KEY")
```

`.env` 文件必须在 `.gitignore` 中。

`[HOOK]` 检查：`scripts/check_secrets.py` 扫描代码，发现疑似密钥则拒绝提交。

### 7.4 依赖管理

所有依赖必须锁定版本：
```
# requirements.txt
numpy==1.26.4
pandas==2.2.1
requests==2.31.0
websockets==12.0
```

`[CI]` 检查：`scripts/check_deps.py` 验证所有依赖都有固定版本号。

---

## 8. 参数管理

### 8.1 参数命名规范

| 后缀 | 含义 | 示例 |
|---|---|---|
| `_window` | 滚动窗口长度 | `volume_ma_window`, `obi_smooth_window` |
| `_threshold` | 阈值 | `entry_threshold`, `cusum_threshold` |
| `_ratio` | 比例/倍数 | `k_stop_ratio`, `pyramid_ratio` |
| `_period` | 周期 | `ma_period`, `roc_period` |
| `_limit` | 上限 | `daily_dd_limit`, `max_position_limit` |

`[CI]` 检查：`scripts/check_param_names.py` 扫描 `config/` 中的参数名，不符合后缀规范则报警。

### 8.2 参数变更必须记录

`docs/PARAM_TABLE.md` 格式：

| 参数名 | 旧值 | 新值 | 变更时间 | 变更人 | 原因 | 影响 |
|---|---|---|---|---|---|---|
| `entry_threshold` | 0.50 | 0.52 | 2024-06-15 | 比蒙 | 假信号率从 13.7% 降到 11.2% | 入场延迟增加 0.2 根 K 线 |
| `k_min_noise_buffer` | 0.6 | 0.65 | 2024-06-16 | 比蒙 | 噪声缓冲不足 | 止损更宽松，单笔风险略增 |

`[REVIEW]` 检查：代码审查人确认参数变更已记录。

### 8.3 参数热更新规则

- 参数热更新只在 **K 线边界** 生效（`param_store.commit()` 在 `on_bar_close` 后调用）
- K 线内部参数不变，保证一根 K 线内所有决策用同一套参数
- 热更新后第一根 K 线的决策日志必须标注 `param_version`

`[REVIEW]` 检查：代码审查人确认 `param_store` 双缓冲逻辑正确。

---

## 9. 策略层规范

### 9.1 因子定义规范

每个因子必须包含：
1. 计算公式（数学表达）
2. 经济学解释（为什么这个因子有效）
3. 归一化方式
4. 预期 IC 范围

`[REVIEW]` 检查：代码审查人确认 `docs/FACTOR_SPEC.md` 中有完整记录。

### 9.2 状态机迁移必须有数值条件

```python
# 禁止
if market_looks_bullish():
    transition("TREND_RIDE")

# 允许
if trend_score > entry_threshold and consistency_run >= min_run:
    transition("TREND_RIDE")
```

`[REVIEW]` 检查：代码审查人确认每次迁移都有数值条件。

### 9.3 策略逻辑变更必须回测

任何策略层逻辑变更（因子公式、权重、阈值、状态迁移条件），必须：
1. 跑 30 天历史回测
2. 与变更前对比：夏普、最大回撤、假信号率、胜率
3. 对比结果写入 PR 描述

`[REVIEW]` 检查：策略层 PR 必须包含回测对比结果。

### 9.4 假信号率定义

**假信号率 = 入场后 3 根 K 线内触发止损的轮次 / 总轮次**

计算方式统一，不得使用其他定义。

`[CI]` 检查：`analytics/round_attribution.py` 中的 `false_signal_rate` 函数必须使用上述定义。

### 9.5 轮次记录必须完整

每轮结束时，必须记录以下字段：

```python
{
    "round_id": str,
    "direction": int,           # 1=LONG, -1=SHORT
    "entry_price": Decimal,
    "exit_price": Decimal,
    "stop_price": Decimal,
    "total_qty": Decimal,
    "pyramids": int,
    "entry_ts": int,            # 毫秒时间戳
    "exit_ts": int,
    "exit_reason": str,         # stop / top_escape / reverse / end_of_backtest
    "pnl": Decimal,
    "r_multiple": float,
    "factor_snapshot_entry": dict,
    "factor_snapshot_exit": dict,
    "market_state_entry": str,
    "market_state_exit": str,
}
```

`[CI]` 检查：`tests/unit/test_round.py` 验证轮次记录包含上述字段。

---

## 10. 执行层规范

### 10.1 订单操作三原则

1. **所有订单必须经过 `OrderRouter`**，禁止直接调 `gateway.place_order`
2. **所有订单必须有 `clientOrderId`**，且确定性生成（同一逻辑订单重试时 ID 不变）
3. **所有订单必须有止损**，市价单成交后立即挂止损单

`[REVIEW]` 检查：代码审查人确认上述三原则被遵守。

### 10.2 订单状态机

```
NEW → PARTIALLY_FILLED → FILLED
  ↓
CANCEL_PENDING → CANCELED
  ↓
EXPIRED / REJECTED
```

**规则：**
- `CANCEL_PENDING` 状态下收到成交回报，**先更新仓位，再等撤单结果**
- 撤单结果返回 `already_filled` 时，忽略撤单成功，视为成交
- 状态迁移必须是幂等的（重复收到同一状态回报，不重复处理）

`[CI]` 检查：`tests/unit/test_order_state.py` 覆盖所有状态迁移路径。

### 10.3 市价单滑点保护

市价单必须设置 `max_slippage_bps`（默认 50bps）。超过则：
- 改为限价单，挂在第 5 档
- 5 秒不成交，撤单重试
- 重试 2 次仍不成交，放弃并记录 `slippage_protection_triggered`

`[REVIEW]` 检查：代码审查人确认 `gateway_binance.py` 中有滑点保护逻辑。

### 10.4 服务器端止损

- 所有持仓必须有服务器端止损单（`STOP_MARKET`，`workingType=MARK_PRICE`）
- 本地止损触发时，先撤服务器端止损，再发市价平仓单
- 进程退出时，确保服务器端止损已挂好

`[REVIEW]` 检查：代码审查人确认 `ensure_server_side_stop` 逻辑正确。

---

## 11. 系统层规范

### 11.1 时钟

- 交易逻辑一律用 `clock.now_ms()`（单调时钟）
- 日志用 UTC 时间
- 数据库存储用毫秒时间戳（UTC）
- 前端展示用本地时区

`[CI]` 检查：`scripts/check_clock_usage.py`。

### 11.2 持久化

- SQLite 必须启用 WAL 模式
- 每根 K 线收线后异步落盘
- 每次订单成交后同步落盘
- 重启后必须对账，不一致则冻结

`[REVIEW]` 检查：代码审查人确认 `persister.py` 中 WAL 和同步策略正确。

### 11.3 限频

- 所有 API 调用必须经过 `RateLimiter`
- 订单、查询、撤单共享权重池
- 优先级：撤单 > 平仓 > 下单 > 查询 > 对账
- 限频器必须线程安全（互斥锁 + 令牌桶）

`[CI]` 检查：`tests/unit/test_rate_limiter.py` 包含并发测试。

### 11.4 健康度与降级

- 每个模块有健康度评分（成功率、延迟）
- 健康度 < 0.5 自动降级到 `fallback` 模块
- 降级事件必须报警
- 降级后功能缺失必须在前端仪表盘可见

`[REVIEW]` 检查：代码审查人确认 `health_monitor.py` 降级逻辑正确。

---

## 12. 提交规范

### 12.1 Commit Message 格式

```
type(scope): description

type: feat / fix / refactor / test / docs / chore
scope: 模块名（factor_momentum / state_breakout / gateway_binance / ...）

示例：
feat(factor_momentum): add acceleration as second-order ROC
fix(round_manager): prevent stop from moving down on partial fill
docs(convention): add exit code table
```

`[HOOK]` 检查：`scripts/check_commit_msg.py` 验证 commit message 格式。

### 12.2 PR 要求

- 每个 PR 必须有描述：改了什么、为什么改、怎么测试的
- 策略层变更必须附回测对比结果
- 系统层变更必须附单元测试
- 至少一人 approve 才能合并
- CI 必须全绿

`[REVIEW]` 检查：代码审查人确认 PR 描述完整。

### 12.3 分支策略

```
main          ← 生产分支，只接受 PR 合并
step1-minimal ← Step 1 开发分支
step2-stable  ← Step 2 开发分支（Step 1 合并后创建）
...
```

`[REVIEW]` 检查：代码审查人确认分支命名和合并方向正确。

---

## 13. 文档规范

### 13.1 必须维护的文档

| 文件 | 内容 | 维护人 |
|---|---|---|
| `README.md` | 项目说明、快速启动 | 张三 |
| `docs/CONVENTION.md` | 本文件 | 三人 |
| `docs/FACTOR_SPEC.md` | 因子定义、公式、归一化方式 | 比蒙 |
| `docs/PARAM_TABLE.md` | 参数表、变更记录 | 比蒙 |
| `docs/DEPLOY_STEP{N}.md` | 第 N 步部署手册 | 张三 |
| `docs/RUNBOOK.md` | 运维手册（Step 5） | 张三 |
| `CHANGELOG.md` | 版本变更记录 | 三人 |

### 13.2 文档与代码一致性

**文档与代码不一致时，以代码为准。**
文档更新滞后于代码的，在 PR 中标注 `docs-update-pending`，下一个 PR 补上。

`[CI]` 检查：`scripts/check_docs.py` 验证 `docs/FACTOR_SPEC.md` 中的因子列表与 `factors/` 目录中的文件一致。

---

## 14. 规范检查清单

**每个 PR 提交前，作者自查：**

- [ ] 文件名前缀与目录类型匹配
- [ ] 有 `MANIFEST`，字段完整
- [ ] 有 `create()` 工厂函数
- [ ] 无跨层反向依赖
- [ ] 无裸 `except`
- [ ] 无硬编码密钥
- [ ] 金额/价格/数量用 `Decimal`
- [ ] 时间戳用 `clock.now_ms()`
- [ ] 关键日志含决策依据
- [ ] 有对应单元测试
- [ ] 参数变更已记录到 `PARAM_TABLE.md`
- [ ] commit message 符合格式

`[HOOK]` 检查：`.pre-commit-config.yaml` 自动跑上述检查，未通过则拒绝提交。

---

## 15. 规范变更流程

1. 提出变更：在 PR 中修改 `CONVENTION.md`
2. 三人 review：比蒙、乐高、张三各自 approve
3. 写入 CHANGELOG.md：
   ```
   ## v1.1.0 (2024-07-01)
   ### Changed
   - 参数命名规范新增 `_ratio` 后缀
   ### Reason
   - `k_stop` 和 `k_stop_ratio` 混用，统一为 `_ratio`
   ```
4. 版本号递增（major.minor.patch）
5. 更新 `scripts/check_*.py` 中的检查逻辑

---

## 附录 A：MANIFEST 字段速查

| 字段 | 必填 | 类型 | 说明 |
|---|---|---|---|
| `name` | 是 | str | 全局唯一，与文件名去掉前缀后一致 |
| `type` | 是 | str | `factor`/`strategy`/`state`/`gateway`/`policy`/`system` |
| `version` | 是 | str | 模块版本，如 `"1.0"` |
| `step` | 是 | int | 最早引入的 Step |
| `capabilities` | 是 | list[str] | 能提供的能力 |
| `dependencies` | 是 | list[str] | 依赖的数据字段 |
| `owner` | 是 | str | 负责人 |
| `fallback` | 否 | str | 降级模块名 |
| `deprecated` | 否 | bool | 是否废弃 |
| `replaced_by` | 否 | str | 被谁替代 |
| `enabled` | 否 | bool | 是否启用（默认 true） |

## 附录 B：退出码速查

| 退出码 | 含义 | systemd 行为 |
|---|---|---|
| 0 | 正常退出 | 不重启 |
| 2 | 配置错误 | 不重启 |
| 3 | 启动检查失败 | 不重启 |
| 4 | 运行时错误 | 重启（退避） |
| 5 | 策略层致命错误 | 不重启 |
| 6 | 紧急停止 | 不重启 |

## 附录 C：日志字段速查

| 字段 | 必填 | 说明 |
|---|---|---|
| `ts` | 是 | ISO 8601 UTC |
| `level` | 是 | DEBUG/INFO/WARN/ERROR/CRITICAL |
| `module` | 是 | 模块名 |
| `event` | 是 | 事件名（snake_case） |
| `detail` | 是 | 事件详情（JSON 或字符串） |

---

> **规范不是限制，是让三个人写出来的代码像一个人写的。**
> **自动组装的前提是每个积木都有标准接口。没有标准接口的积木，不是积木，是石头。**
> **规范靠工具强制，不靠自觉。**
