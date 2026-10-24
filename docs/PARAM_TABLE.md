# PARAM_TABLE.md — 参数表与调参记录

> 版本：v1.0.0
> 生效日期：Step 1 启动日
> 维护人：比蒙（策略参数）、乐高（执行参数）、张三（系统参数）
> 变更流程：实盘参数变更需三人签字，写入本文档变更记录，版本号递增
> 关联文档：CONVENTION.md（命名规范）、FACTOR_SPEC.md（因子参数）

---

## 0. 总则

**0.1 参数不是拍出来的，是估出来的。**
每个参数的默认值必须有来源：回测最优、理论推导、或经验值。没有来源的参数，不允许进入参数表。

**0.2 参数有身份证。**
每个参数记录：名称、类型、单位、默认值、取值范围、安全上限、所属模块、所属 Step、负责人、敏感度、来源、变更历史。

**0.3 参数变更必须可追溯。**
每次变更记录：时间、操作人、旧值、新值、原因、回测影响。没有记录的变更，视为未发生。

**0.4 参数表与代码必须一致。**
`[CI]` 检查：`scripts/check_param_table.py` 验证参数表中的 key 与代码中 `config.get()` 的 key 一一对应，缺失或多余则 CI 失败。

---

## 1. 参数分类与命名

### 1.1 参数分类

| 类别 | 前缀 | 说明 | 示例 |
|---|---|---|---|
| 结构参数 | 无前缀 | 策略骨架，手工标定，不自动漂 | `ma_period` |
| 因子参数 | `factor_` 或无 | 因子计算相关 | `roc_period` |
| 策略参数 | 无前缀 | 状态机、评分、阈值 | `entry_threshold` |
| 轮次参数 | 无前缀 | 止损、加仓、平仓 | `initial_stop_atr` |
| 风控参数 | 无前缀 | 仓位、回撤、熔断 | `per_trade_risk` |
| 执行参数 | 无前缀 | 订单类型、滑点、超时 | `max_slippage_bps` |
| 系统参数 | 无前缀 | 时钟、限频、日志 | `heartbeat_interval_s` |

### 1.2 命名规范

| 后缀 | 含义 | 单位 | 示例 |
|---|---|---|---|
| `_period` | 周期 | 根 K 线 | `ma_period` |
| `_window` | 滚动窗口 | 根 K 线 | `volume_ma_window` |
| `_threshold` | 阈值 | 无量纲 | `entry_threshold` |
| `_ratio` | 比例/倍数 | 无量纲 | `k_stop_ratio` |
| `_limit` | 上限 | 无量纲 | `daily_dd_limit` |
| `_ms` | 毫秒 | ms | `order_timeout_ms` |
| `_s` | 秒 | s | `heartbeat_interval_s` |
| `_bps` | 基点 | bps | `max_slippage_bps` |
| `_pct` | 百分比 | %（小数表示） | `slippage_tolerance` |

`[CI]` 检查：`scripts/check_param_names.py` 验证参数名后缀与单位一致。

### 1.3 参数来源标注

每个参数必须标注来源：

| 来源 | 含义 | 示例 |
|---|---|---|
| `backtest` | 历史回测最优 | `entry_threshold=0.50` |
| `theory` | 理论推导 | `k_stop_ratio = R0/(Q*ATR)` |
| `empirical` | 经验值 | `heartbeat_interval_s=30` |
| `exchange` | 交易所规定 | `min_notional=5.0` |

---

## 2. 参数加载与覆盖规则

### 2.1 加载顺序

```
base.yaml → step{N}.yaml → symbols/{symbol}.yaml
```

**覆盖规则：** 后者覆盖前者。`symbols/btcusdt.yaml` 中的参数覆盖 `step1.yaml` 中的同名参数。

### 2.2 加载伪代码

```python
def load_config(config_dir, step, symbol):
    merged = {}
    for path in [
        config_dir / "base.yaml",
        config_dir / f"step{step}.yaml",
        config_dir / "symbols" / f"{symbol}.yaml",
    ]:
        if path.exists():
            with open(path) as f:
                data = yaml.safe_load(f) or {}
            merged.update(data)
    return merged
```

### 2.3 缺失参数处理

| 参数类型 | 缺失行为 |
|---|---|
| 必填参数（风控、执行核心） | 启动报错，退出码 2 |
| 选填参数（有默认值） | 用默认值，日志打 `WARN: param missing, using default` |
| Step 2+ 参数（Step 1 不用） | 不加载，不报错 |

`[CI]` 检查：`scripts/check_required_params.py` 验证所有必填参数在每个 step 配置中存在。

---

## 3. 参数校验与安全上限

### 3.1 安全上限

**风控参数硬上限（超过拒绝启动）：**

| 参数 | 安全上限 | 原因 |
|---|---|---|
| `per_trade_risk` | 0.02（2%） | 单笔亏损不超过权益 2% |
| `daily_dd_limit` | 0.05（5%） | 日回撤降频线不超过 5% |
| `daily_dd_halt` | 0.08（8%） | 日回撤熔断线不超过 8% |
| `max_position_ratio` | 0.5（50%） | 最大仓位不超过权益 50% |
| `leverage` | 5 | 杠杆不超过 5 倍 |

**执行参数硬上限：**

| 参数 | 安全上限 | 原因 |
|---|---|---|
| `max_slippage_bps` | 100（1%） | 滑点保护不超过 1% |
| `max_price_deviation` | 0.01（1%） | 价格偏离保护不超过 1% |
| `order_timeout_ms` | 5000 | 订单超时不超过 5 秒 |

`[CI]` 检查：`scripts/check_param_limits.py` 验证所有配置文件的参数不超过安全上限。

### 3.2 参数组约束

有些参数之间存在数学关系，必须联合校验：

**参数组 1：止损与加仓**
```
约束：k_min_noise_buffer < k_stop_ratio
原因：噪声缓冲必须小于止损距离，否则止损永远无法触发
```

**参数组 2：日回撤**
```
约束：daily_dd_limit < daily_dd_halt
原因：降频线必须低于熔断线
```

**参数组 3：因子权重**
```
约束：sum(factor_weights) == 1.0
约束：每个 factor_weight <= 0.5
约束：每个 factor_weight >= 0.1
```

`[CI]` 检查：`scripts/check_param_groups.py` 验证参数组约束。

### 3.3 参数类型校验

```python
PARAM_TYPES = {
    "ma_period": int,
    "entry_threshold": float,
    "per_trade_risk": float,
    "order_type": str,
    "enable_hedge": bool,
}

def validate_types(config):
    for key, expected_type in PARAM_TYPES.items():
        if key in config and not isinstance(config[key], expected_type):
            raise ConfigError(f"{key} expected {expected_type}, got {type(config[key])}")
```

---

## 4. 参数热更新规则

### 4.1 生效时机

- 参数热更新**只在 K 线边界生效**
- K 线内部参数冻结，保证一根 K 线内所有决策用同一套参数
- 热更新后第一根 K 线的决策日志标注 `param_version`

### 4.2 双缓冲机制

```python
class ParamStore:
    def __init__(self):
        self.active = load_params()
        self.pending = None

    def update(self, new_params):
        self.pending = new_params

    def commit(self):
        # 只在 K 线收线后调用
        if self.pending:
            self.active = self.pending
            self.pending = None
            log.info(f"param committed version={self.active['version']}")
```

### 4.3 冻结期规则

- **一轮进行中，参数冻结。** 不允许热更新，避免同一轮内参数不一致。
- **轮次结束后，允许热更新。** 下一轮开始时使用新参数。
- **实盘期间，每周最多调一次参数。** 调参后必须跑 7 天影子对比。

`[REVIEW]` 检查：代码审查人确认 `param_store` 的冻结逻辑正确。

---

## 5. 参数变更流程

### 5.1 变更步骤

```
1. 提出变更：在 PARAM_TABLE.md 中记录变更提案（旧值→新值，原因）
2. 回测验证：跑 30 天历史回测，与变更前对比
3. 验证标准：夏普不降、回撤不增、假信号率不升（三项都满足）
4. 三人签字：比蒙、乐高、张三在 PR 中 approve
5. 更新文档：PARAM_TABLE.md 变更记录 + CHANGELOG.md
6. 部署：先影子跑 7 天，再实盘
```

### 5.2 验证标准

| 指标 | 要求 |
|---|---|
| 夏普比率 | 不低于变更前 |
| 最大回撤 | 不高于变更前 |
| 假信号率 | 不高于变更前 |
| 胜率 | 不低于变更前 2% |
| 盈亏比 | 不低于变更前 |

**注意：** 不是所有指标都必须变好，但**不能有指标显著变差**（变差超过 10% 视为显著）。

### 5.3 变更记录格式

| 参数 | 旧值 | 新值 | 变更时间 | 变更人 | 原因 | 回测影响 |
|---|---|---|---|---|---|---|
| `entry_threshold` | 0.50 | 0.52 | 2024-06-15 | 比蒙 | 假信号率从 13.7% 降到 11.2% | 入场延迟增加 0.2 根 K 线，夏普 1.71→1.68，回撤 -5.4%→-5.1% |

---

## 6. 参数审计与版本管理

### 6.1 审计日志

每次参数变更写入审计日志：

```json
{
  "ts": "2024-06-15T14:23:00.123Z",
  "operator": "bimon",
  "action": "update",
  "param": "entry_threshold",
  "old_value": 0.50,
  "new_value": 0.52,
  "reason": "假信号率从 13.7% 降到 11.2%",
  "backtest_impact": {
    "sharpe": "1.71→1.68",
    "max_drawdown": "-5.4%→-5.1%",
    "false_signal_rate": "13.7%→11.2%"
  },
  "approved_by": ["bimon", "lego", "zhangsan"]
}
```

### 6.2 参数快照与回滚

- 每次参数变更前，保存当前参数快照到 `config/snapshots/param_{timestamp}.yaml`
- 回滚命令：`python scripts/rollback_params.py --to {timestamp}`
- 回滚后必须跑回测确认，再部署

### 6.3 参数版本号

参数表版本号格式：`v{major}.{minor}.{patch}`
- `major`：参数结构变更（新增/删除参数组）
- `minor`：参数默认值变更
- `patch`：参数描述修正

---

## 7. 参数表

### 7.1 因子参数

| 参数 | 类型 | 单位 | 默认值 | 范围 | 敏感度 | 来源 | 模块 | Step | 负责人 |
|---|---|---|---|---|---|---|---|---|---|
| `roc_period` | int | 根 | 3 | [2, 10] | 中 | backtest | momentum | 1 | 比蒙 |
| `momentum_roc_weight` | float | - | 0.7 | [0.5, 0.9] | 低 | backtest | momentum | 1 | 比蒙 |
| `momentum_accel_weight` | float | - | 0.3 | [0.1, 0.5] | 低 | backtest | momentum | 1 | 比蒙 |
| `obi_levels` | int | 档 | 10 | [5, 20] | 中 | backtest | obi | 1 | 比蒙 |
| `obi_decay_k` | float | - | 0.15 | [0.05, 0.3] | 高 | backtest | obi | 1 | 比蒙 |
| `obi_smooth_half_life_s` | float | s | 2.0 | [1.0, 5.0] | 中 | empirical | obi | 1 | 比蒙 |
| `volume_ma_window` | int | 根 | 20 | [10, 50] | 低 | backtest | volume | 1 | 比蒙 |
| `quantile_window` | int | 根 | 500 | [200, 1000] | 高 | backtest | all | 1 | 比蒙 |
| `momentum_weight` | float | - | 0.35 | [0.1, 0.5] | 高 | backtest | strategy | 1 | 比蒙 |
| `obi_weight` | float | - | 0.40 | [0.1, 0.5] | 高 | backtest | strategy | 1 | 比蒙 |
| `volume_weight` | float | - | 0.25 | [0.1, 0.5] | 中 | backtest | strategy | 1 | 比蒙 |

### 7.2 策略参数

| 参数 | 类型 | 单位 | 默认值 | 范围 | 敏感度 | 来源 | 模块 | Step | 负责人 |
|---|---|---|---|---|---|---|---|---|---|
| `ma_period` | int | 根 | 25 | [15, 50] | 高 | backtest | state_breakout | 1 | 比蒙 |
| `ma_slope_window` | int | 根 | 3 | [2, 5] | 低 | empirical | state_breakout | 1 | 比蒙 |
| `consistency_run_min` | int | 根 | 5 | [3, 8] | 高 | backtest | state_breakout | 1 | 比蒙 |
| `consistency_decay` | int | - | 2 | [1, 3] | 低 | empirical | state_breakout | 1 | 比蒙 |
| `cusum_threshold` | float | - | 0.6 | [0.3, 1.0] | 中 | backtest | state_breakout | 1 | 比蒙 |
| `entry_threshold` | float | - | 0.50 | [0.40, 0.60] | 高 | backtest | state_breakout | 1 | 比蒙 |
| `trend_score_percentile_window` | int | 根 | 500 | [200, 1000] | 中 | theory | strategy | 1 | 比蒙 |
| `divergence_threshold` | float | - | 0.65 | [0.55, 0.80] | 高 | backtest | state_top_detect | 1 | 比蒙 |
| `divergence_persist_bars` | int | 根 | 2 | [1, 3] | 中 | backtest | state_top_detect | 1 | 比蒙 |

### 7.3 轮次与止损参数

| 参数 | 类型 | 单位 | 默认值 | 范围 | 敏感度 | 来源 | 模块 | Step | 负责人 |
|---|---|---|---|---|---|---|---|---|---|
| `initial_stop_atr` | float | ATR | 1.5 | [1.0, 2.5] | 高 | theory | position_round | 1 | 比蒙 |
| `k_min_noise_buffer` | float | ATR | 0.6 | [0.4, 0.8] | 高 | theory | position_round | 1 | 比蒙 |
| `k_stop_pyramid` | float | ATR | 1.2 | [0.8, 1.5] | 中 | theory | position_round | 3 | 比蒙 |
| `pyramid_ratio_1` | float | - | 0.8 | [0.5, 1.0] | 中 | theory | position_round | 3 | 比蒙 |
| `pyramid_ratio_2` | float | - | 0.4 | [0.2, 0.6] | 中 | theory | position_round | 3 | 比蒙 |

**参数组约束：** `k_min_noise_buffer < k_stop_pyramid`，否则加仓后止损无法触发。

### 7.4 风控参数

| 参数 | 类型 | 单位 | 默认值 | 范围 | 安全上限 | 敏感度 | 来源 | 模块 | Step | 负责人 |
|---|---|---|---|---|---|---|---|---|---|---|
| `per_trade_risk` | float | - | 0.007 | [0.003, 0.02] | 0.02 | 高 | theory | policy_minimal | 1 | 张三 |
| `daily_dd_limit` | float | - | 0.02 | [0.01, 0.05] | 0.05 | 高 | theory | policy_minimal | 1 | 张三 |
| `daily_dd_halt` | float | - | 0.03 | [0.02, 0.08] | 0.08 | 高 | theory | policy_minimal | 1 | 张三 |
| `max_position_ratio` | float | - | 0.3 | [0.1, 0.5] | 0.5 | 高 | theory | policy_minimal | 1 | 张三 |
| `leverage` | int | 倍 | 3 | [1, 5] | 5 | 高 | exchange | gateway_binance | 1 | 乐高 |
| `margin_ratio_warn` | float | - | 0.6 | [0.4, 0.8] | 0.8 | 中 | theory | policy_composite | 2 | 张三 |
| `margin_ratio_halt` | float | - | 0.8 | [0.6, 0.9] | 0.9 | 中 | theory | policy_composite | 2 | 张三 |

**参数组约束：** `daily_dd_limit < daily_dd_halt`。

### 7.5 执行参数

| 参数 | 类型 | 单位 | 默认值 | 范围 | 安全上限 | 敏感度 | 来源 | 模块 | Step | 负责人 |
|---|---|---|---|---|---|---|---|---|---|---|
| `order_type` | str | - | MARKET | {MARKET, LIMIT} | - | 中 | empirical | gateway_binance | 1 | 乐高 |
| `max_slippage_bps` | int | bps | 50 | [10, 100] | 100 | 高 | empirical | gateway_binance | 1 | 乐高 |
| `max_price_deviation` | float | - | 0.005 | [0.001, 0.01] | 0.01 | 高 | empirical | gateway_binance | 1 | 乐高 |
| `order_timeout_ms` | int | ms | 2000 | [500, 5000] | 5000 | 中 | empirical | gateway_binance | 1 | 乐高 |
| `latency_ms` | int | ms | 80 | [20, 500] | - | 中 | empirical | gateway_virtual | 1 | 乐高 |
| `slippage_tolerance` | float | - | 0.003 | [0.001, 0.01] | 0.01 | 中 | empirical | slippage_model | 1 | 乐高 |
| `min_notional` | float | USDT | 5.0 | - | - | - | exchange | gateway_binance | 1 | 乐高 |

**注意：** `min_notional` 是币安规定，不是我们的参数，标注 `source=exchange`，不参与参数调整。

### 7.6 系统参数

| 参数 | 类型 | 单位 | 默认值 | 范围 | 敏感度 | 来源 | 模块 | Step | 负责人 |
|---|---|---|---|---|---|---|---|---|---|
| `warmup_bars` | int | 根 | 500 | [200, 1000] | 高 | theory | factor_registry | 1 | 比蒙 |
| `heartbeat_interval_s` | int | s | 30 | [10, 60] | 低 | empirical | run_live | 1 | 张三 |
| `rate_limit_orders_per_10s` | int | 单 | 45 | [20, 48] | 中 | exchange | rate_limiter | 1 | 乐高 |
| `rate_limit_weight_per_min` | int | 权重 | 1000 | [500, 1100] | 中 | exchange | rate_limiter | 1 | 乐高 |
| `snapshot_stale_ms` | int | ms | 3000 | [1000, 5000] | 中 | empirical | factor_obi | 1 | 比蒙 |
| `param_commit_on_bar_close` | bool | - | True | - | - | theory | param_store | 1 | 张三 |
| `clock_offset_warn_ms` | int | ms | 200 | [100, 500] | 低 | empirical | clock_hybrid | 1 | 张三 |
| `max_consecutive_errors` | int | 次 | 5 | [3, 10] | 低 | empirical | run_live | 1 | 张三 |

### 7.7 Step 2+ 参数（预留）

| 参数 | 类型 | 默认值 | 模块 | Step | 负责人 |
|---|---|---|---|---|---|
| `dde_weight` | float | 0.20 | strategy | 2 | 比蒙 |
| `smart_money_weight` | float | 0.15 | strategy | 2 | 比蒙 |
| `sr_mapper_width_ratio` | float | 0.3 | factor_sr_mapper | 2 | 比蒙 |
| `grid_spacing_k` | float | 0.5 | strategy_grid | 2 | 比蒙 |
| `grid_levels` | int | 12 | strategy_grid | 2 | 比蒙 |
| `adx_trend_threshold` | int | 22 | regime_dispatcher | 2 | 比蒙 |
| `adx_range_threshold` | int | 18 | regime_dispatcher | 2 | 比蒙 |
| `bollinger_width_percentile` | float | 0.3 | regime_dispatcher | 2 | 比蒙 |

---

## 8. 参数敏感性分析

### 8.1 敏感度分级

| 级别 | 含义 | 调整要求 |
|---|---|---|
| 高 | 调整 10% 会显著影响夏普/回撤 | 必须回测 30 天 + 影子 7 天 |
| 中 | 调整 20% 才会显著影响 | 必须回测 30 天 |
| 低 | 调整 30% 影响不显著 | 回测 7 天即可 |

### 8.2 高敏感参数清单

| 参数 | 敏感度 | 原因 |
|---|---|---|
| `entry_threshold` | 高 | 直接决定进场频率和假信号率 |
| `per_trade_risk` | 高 | 直接决定单笔亏损和仓位 |
| `daily_dd_halt` | 高 | 直接决定熔断触发时机 |
| `initial_stop_atr` | 高 | 直接决定止损距离和R0 |
| `ma_period` | 高 | 直接决定突破确认的灵敏度 |
| `obi_decay_k` | 高 | 直接决定OBI的方向性 |
| `quantile_window` | 高 | 直接决定因子归一化的稳定性 |

### 8.3 敏感性分析记录

每个高敏感参数必须有敏感性分析记录：

```
参数：entry_threshold
分析日期：2024-06-15
分析方法：固定其他参数，entry_threshold 从 0.40 到 0.60，步长 0.02，跑 30 天回测
结果：
  0.40: 夏普 1.52, 回撤 -7.2%, 假信号率 18.3%
  0.44: 夏普 1.61, 回撤 -6.5%, 假信号率 16.1%
  0.48: 夏普 1.68, 回撤 -5.8%, 假信号率 14.5%
  0.50: 夏普 1.71, 回撤 -5.4%, 假信号率 13.7%  ← 默认
  0.52: 夏普 1.68, 回撤 -5.1%, 假信号率 11.2%
  0.56: 夏普 1.59, 回撤 -4.8%, 假信号率 9.8%
  0.60: 夏普 1.42, 回撤 -4.5%, 假信号率 8.1%
结论：0.50-0.52 区间夏普最优，0.52 回撤更低但夏普略降。选择 0.50 作为默认，0.52 作为备选。
```

---

## 9. 参数冻结与灰度

### 9.1 冻结期

- **一轮进行中，参数冻结**（不允许热更新）
- **实盘期间，每周最多调一次参数**
- **调参后，必须跑 7 天影子对比**

### 9.2 灰度发布

新参数上线流程：
```
1. 影子模式跑 7 天，对比新旧参数
2. 如果新参数优于旧参数（夏普↑或回撤↓），进入步骤 3
3. 实盘保护期（前 20 轮）用新参数，仓位减半
4. 保护期通过后，全仓使用新参数
5. 如果不通过，回滚到旧参数
```

---

## 10. 参数废弃流程

- 参数不再使用，标记 `deprecated: true`
- 保留至少一个 Step 周期
- 确认无代码引用后，从参数表和代码中删除
- 删除前在 `CHANGELOG.md` 记录

---

## 附录 A：参数速查表（Step 1 核心）

| 参数 | 默认值 | 安全上限 | 敏感度 |
|---|---|---|---|
| `per_trade_risk` | 0.007 | 0.02 | 高 |
| `daily_dd_limit` | 0.02 | 0.05 | 高 |
| `daily_dd_halt` | 0.03 | 0.08 | 高 |
| `entry_threshold` | 0.50 | - | 高 |
| `initial_stop_atr` | 1.5 | - | 高 |
| `ma_period` | 25 | - | 高 |
| `quantile_window` | 500 | - | 高 |
| `max_slippage_bps` | 50 | 100 | 高 |
| `max_price_deviation` | 0.005 | 0.01 | 高 |

## 附录 B：参数变更记录

| 日期 | 参数 | 旧值 | 新值 | 变更人 | 原因 | 回测影响 |
|---|---|---|---|---|---|---|
| 2024-06-10 | `entry_threshold` | 0.48 | 0.50 | 比蒙 | 假信号率从 14.5% 降到 13.7% | 夏普 1.68→1.71，回撤 -5.8%→-5.4% |
| 2024-06-12 | `per_trade_risk` | 0.008 | 0.007 | 张三 | 预留滑点余量 | 单笔风险降低 12.5% |
| 2024-06-15 | `max_slippage_bps` | 30 | 50 | 乐高 | 极端行情滑点超预期 | 滑点保护更宽松，成交率提升 |

## 附录 C：参数校验清单

**启动时自动校验：**

- [ ] 必填参数存在
- [ ] 参数类型正确
- [ ] 参数在允许范围内
- [ ] 参数不超过安全上限
- [ ] 参数组约束满足
- [ ] 参数表与代码一致

`[CI]` 检查：`scripts/check_params.py` 跑上述校验，失败则 CI 失败。

---

> **参数不是拍出来的，是估出来的。每个参数背后必须有一个分布。**
> **参数有身份证：名称、类型、单位、默认值、范围、安全上限、来源、负责人、变更历史。**
> **没有回测对比的参数调整，等于赌博。**
