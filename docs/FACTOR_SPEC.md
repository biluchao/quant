# FACTOR_SPEC.md — 因子定义与归一化规范

> 版本：v1.0.0
> 生效日期：Step 1 启动日
> 维护人：比蒙（策略）、张三（系统）、乐高（执行）
> 变更流程：三人签字后写入 CHANGELOG.md，版本号递增
> 关联文档：CONVENTION.md（命名与接口规范）、PARAM_TABLE.md（参数表）

---

## 0. 总则

**0.1 因子是策略的原料，不是策略本身。**
因子的职责是把原始市场数据转换为可比较的信号值。因子不决定买卖，只回答"当前市场在这个维度上偏向哪边"。

**0.2 每个因子必须有数学定义、经济学解释、归一化方式、失效判定。**
没有经济学解释的因子是黑箱，没有归一化方式的因子无法加权，没有失效判定的因子会一直用到烂。

**0.3 因子文档与代码必须一致。**
`docs/FACTOR_SPEC.md` 中的因子列表必须与 `factors/` 目录中的文件一一对应。`[CI]` 检查：`scripts/check_factor_spec.py` 验证一致性。

**0.4 因子计算的时间边界。**
- 因子在 K 线收线后 **10ms 内**算完
- 10-50ms 内可被状态机读取
- 50ms 后仍未算完，标记 `timeout`，用上一根值兜底
- 每个因子计算耗时上限 **5ms**，超过报警 `[REVIEW]`

---

## 1. 通用约定

### 1.1 输入数据

| 数据类型 | 来源 | 精度 | 备注 |
|---|---|---|---|
| K 线 OHLCV | 交易所 kline 流 | `Decimal` | 以交易所推送为准，不用本地聚合 |
| 盘口快照 | depth 增量流重建 | `Decimal` | 前 20 档，200ms 一拍 |
| 逐笔成交 | aggTrade 流 | `Decimal` | Step 2 起用于 DDE 因子 |
| 时间戳 | `clock.now_ms()` | `int`（毫秒） | 单调时钟，不用墙钟 |

### 1.2 归一化规则

所有因子必须输出以下两种区间之一：

| 区间 | 适用因子 | 归一化方式 |
|---|---|---|
| `[-1, 1]` | 有方向的因子（动量、OBI、DDE） | 滚动分位数映射 |
| `[0, 1]` | 无方向的因子（成交量分位数） | 滚动分位数映射 |

**分位数归一化公式：**

```
给定因子原始值 x，滚动窗口 W（默认 500 根 K 线）：
  p = 在 W 中 x 的经验分位数（0 到 1）
  若因子有方向：normalized = 2 * p - 1   → [-1, 1]
  若因子无方向：normalized = p            → [0, 1]
```

**关键约束：**
- 分位数窗口**只在 K 线收线后更新**，K 线内用上一根的分位数
- 分位数窗口**不包含当前 K 线**（避免前视）
- 窗口未填满（< 500 根）时，因子值标记 `warmup=True`，状态机降权使用
- 输出保留 6 位小数：`round(normalized, 6)`

`[CI]` 检查：`tests/unit/test_factor.py` 中每个因子测试必须验证输出范围。

### 1.3 异常值处理

- 因子原始值超出历史 **5σ** 范围，标记 `outlier=True`
- `outlier=True` 的因子，当根 K 线权重降为 **0**
- 连续 3 根 K 线同一因子 `outlier=True`，标记 `factor_degraded`，报警
- 异常值事件记录到 `analytics/factor_outlier_log`

`[CI]` 检查：`scripts/check_factor_outlier.py` 验证异常值处理逻辑。

### 1.4 缺失值处理

| 场景 | 处理方式 |
|---|---|
| 盘口快照缺失 | OBI 用上一根值，标记 `stale=True` |
| 盘口快照过期（>3s） | 同上 |
| 盘口档位不足（<10 档） | 按实际档位计算，权重重新归一化 |
| K 线数据缺失 | 该根 K 线跳过，因子值沿用上一根 |
| 因子计算异常 | 返回上一根值，标记 `error=True`，连续 3 次触发降级 |

### 1.5 计算顺序

因子之间无依赖时**并行计算**；有依赖时按拓扑序计算。

Step 1 三个因子无相互依赖，并行计算：
```
bar_close → [momentum, obi, volume] → 状态机
```

Step 2 起：
```
bar_close → momentum ─┐
          → obi ──────┼→ orthogonalize → 状态机
          → dde ──────┘
```

`[REVIEW]` 检查：代码审查人确认 `FactorRegistry.compute_all()` 的计算顺序正确。

### 1.6 版本兼容规则

- 因子**公式变更**（数学定义改变）→ 升 **major** 版本，旧版本保留可查
- 因子**参数变更**（窗口长度、衰减系数）→ 升 **minor** 版本，旧版本不再加载
- 因子**实现优化**（算法不变，性能提升）→ 升 **patch** 版本，回测结果应完全一致

版本号写在 `MANIFEST["version"]`，格式 `{major}.{minor}.{patch}`。

### 1.7 市场状态分组

因子 IC 按市场状态分组统计，报告中分别展示：

| 市场状态 | 判定条件 | 说明 |
|---|---|---|
| `trend` | ADX(14) > 22 | 趋势市 |
| `range` | ADX(14) < 18 且布林宽度 < 30% 分位 | 震荡市 |
| `transition` | 其他 | 过渡状态 |

`[CI]` 检查：`analytics/round_attribution.py` 中的 `factor_ic_by_regime` 函数必须按上述分组。

---

## 2. 因子清单

### 2.1 Step 1 因子（三个）

| 因子名 | 文件 | 类型 | 归一化区间 | 权重（初始） |
|---|---|---|---|---|
| `momentum` | `factors/factor_momentum.py` | 有方向 | `[-1, 1]` | 0.35 |
| `obi` | `factors/factor_obi.py` | 有方向 | `[-1, 1]` | 0.40 |
| `volume` | `factors/factor_volume.py` | 无方向 | `[0, 1]` | 0.25 |

**初始权重来源：** 过去 90 天 BTC 永续 3 分钟 K 线的 IC 加权。IC 高的因子权重大，但单因子权重上限 0.5，避免过度集中。

`[CI]` 检查：`scripts/check_factor_weights.py` 验证权重之和为 1.0，且单因子不超过 0.5。

---

## 3. 因子详细定义

### 3.1 动量因子（momentum）

**MANIFEST：**
```python
MANIFEST = {
    "name": "momentum",
    "type": "factor",
    "version": "1.0.0",
    "step": 1,
    "capabilities": ["momentum", "roc", "acceleration"],
    "dependencies": ["bar.close"],
    "fallback": None,
    "owner": "bimon",
}
```

**数学定义：**

```
输入：收盘价序列 close[t]
参数：roc_period = 3（默认）

ROC(3) = (close[t] - close[t-3]) / close[t-3]

加速度 = ROC(3)[t] - ROC(3)[t-1]

原始值 raw = 0.7 * ROC(3) + 0.3 * 加速度
```

**归一化：**

```
对 raw 做滚动分位数归一化（窗口 500 根）：
  p = 在最近 500 根 raw 中的经验分位数
  normalized = 2 * p - 1   → [-1, 1]
```

**经济学解释：**
动量因子衡量价格趋势的持续性。ROC(3) 捕捉短期价格变化方向，加速度捕捉变化速度的增减。两者加权合成，既看方向也看强度。在趋势市中，动量因子 IC 通常较高（0.12-0.18）；在震荡市中 IC 下降（0.05-0.10）。

**计算伪代码：**

```python
def compute(self, bar, snapshot):
    # 1. 计算 ROC(3)
    close_history = self.buffer.get_recent_closes(4)  # [t-3, t-2, t-1, t]
    if len(close_history) < 4:
        return None  # warmup
    roc = (close_history[-1] - close_history[-4]) / close_history[-4]

    # 2. 计算加速度
    prev_roc = self.prev_roc  # 上一根 K 线的 ROC
    accel = roc - prev_roc if prev_roc is not None else 0.0
    self.prev_roc = roc

    # 3. 合成原始值
    raw = 0.7 * roc + 0.3 * accel

    # 4. 分位数归一化（用上一根 K 线的分位数窗口）
    normalized = self.quantile_normalize(raw, direction=True)

    # 5. 异常值检查
    if abs(raw) > 5 * self.raw_std:
        return {"value": normalized, "outlier": True}

    return {"value": round(normalized, 6), "outlier": False}
```

**参数敏感性分析：**

| roc_period | 假信号率 | IC 均值 | 选择 |
|---|---|---|---|
| 2 | 15.2% | 0.11 | |
| **3** | **13.7%** | **0.15** | ✓ 默认 |
| 5 | 12.9% | 0.13 | |
| 8 | 11.4% | 0.09 | |

**已知限制：**
- 震荡市中动量因子容易失效，IC 降到 0.05 以下
- 资金费率结算前后 15 分钟，动量因子 IC 下降 50%，状态机应降频
- 极端行情（1 分钟波动 >3%）时，ROC(3) 会被噪声主导，标记 outlier

---

### 3.2 订单簿失衡因子（obi）

**MANIFEST：**
```python
MANIFEST = {
    "name": "obi",
    "type": "factor",
    "version": "1.0.0",
    "step": 1,
    "capabilities": ["obi", "orderbook_imbalance"],
    "dependencies": ["snapshot.bids", "snapshot.asks"],
    "fallback": None,
    "owner": "bimon",
}
```

**数学定义：**

```
输入：盘口快照 snapshot，包含 bids/asks 前 N 档
参数：levels = 10, decay_k = 0.15, smooth_half_life_s = 2.0

距离权重：w_i = exp(-decay_k * i)，i 从 0 开始（0 = 盘口最优价）

原始值：
  bid_weighted = Σ(w_i * bid_vol_i)  for i in 0..levels-1
  ask_weighted = Σ(w_i * ask_vol_i)  for i in 0..levels-1
  raw = (bid_weighted - ask_weighted) / (bid_weighted + ask_weighted + ε)

时间平滑（指数加权，半衰期 2 秒，约 10 拍）：
  alpha = 1 - exp(-ln(2) / 10)
  smoothed = alpha * raw + (1 - alpha) * smoothed_prev
```

**归一化：**
原始值本身在 `[-1, 1]` 区间，但分布不均匀（大部分时候接近 0，极端时接近 ±1）。用滚动分位数重新映射，使分布更均匀：

```
p = 在最近 500 根 smoothed 中的经验分位数
normalized = 2 * p - 1
```

**经济学解释：**
OBI 衡量盘口挂单的方向性偏好。买盘量大于卖盘量时，OBI 为正，表示买方力量强。距离加权是因为靠近盘口的挂单更可能成交，信息含量更高。时间平滑是为了过滤单拍噪声——币安盘口变化快，单拍 OBI 噪声大，平滑后更稳定。

**计算伪代码：**

```python
def compute(self, bar, snapshot):
    # 1. 检查快照有效性
    if snapshot is None:
        return {"value": self.prev_value, "stale": True}
    if abs(snapshot["ts"] - bar.ts) > 500:
        return {"value": self.prev_value, "stale": True}

    # 2. 提取前 N 档
    bids = snapshot["bids"][:self.levels]
    asks = snapshot["asks"][:self.levels]

    # 3. 距离加权
    bid_weighted = 0.0
    ask_weighted = 0.0
    for i, (price, qty) in enumerate(bids):
        w = math.exp(-self.decay_k * i)
        bid_weighted += w * float(qty)
    for i, (price, qty) in enumerate(asks):
        w = math.exp(-self.decay_k * i)
        ask_weighted += w * float(qty)

    # 4. 原始值
    total = bid_weighted + ask_weighted
    if total < 1e-9:
        return {"value": 0.0, "stale": False}
    raw = (bid_weighted - ask_weighted) / total

    # 5. 时间平滑
    smoothed = self.alpha * raw + (1 - self.alpha) * self.prev_smoothed
    self.prev_smoothed = smoothed

    # 6. 分位数归一化
    normalized = self.quantile_normalize(smoothed, direction=True)

    # 7. 异常值检查
    if abs(smoothed) > 5 * self.smoothed_std:
        return {"value": normalized, "outlier": True}

    self.prev_value = normalized
    return {"value": round(normalized, 6), "stale": False}
```

**参数敏感性分析：**

| decay_k | levels | 假信号率 | IC 均值 | 选择 |
|---|---|---|---|---|
| 0.10 | 10 | 14.1% | 0.12 | |
| **0.15** | **10** | **13.7%** | **0.15** | ✓ 默认 |
| 0.20 | 10 | 13.9% | 0.14 | |
| 0.15 | 5 | 14.5% | 0.11 | |
| 0.15 | 20 | 13.5% | 0.16 | 计算量翻倍，Step 2 考虑 |

**已知限制：**
- 盘口深度不足 10 档时（山寨币常见），按实际档位计算，权重重新归一化
- 大单挂单（>500 张）在盘口停留，OBI 会持续偏高，但可能是虚假挂单（冰山单/诱导单）
- 币安 depth 流是 100ms 一拍，快照时间戳与 K 线收线时间可能差 50-200ms
- 极端行情下盘口瞬间变薄，OBI 会剧烈跳动

---

### 3.3 成交量因子（volume）

**MANIFEST：**
```python
MANIFEST = {
    "name": "volume",
    "type": "factor",
    "version": "1.0.0",
    "step": 1,
    "capabilities": ["volume", "volume_percentile"],
    "dependencies": ["bar.volume"],
    "fallback": None,
    "owner": "bimon",
}
```

**数学定义：**

```
输入：成交量序列 volume[t]
参数：ma_window = 20（默认）

相对成交量 = volume[t] / mean(volume[t-20:t])

归一化：对相对成交量做滚动分位数归一化（窗口 500 根），映射到 [0, 1]
```

**经济学解释：**
成交量因子衡量当前 K 线成交量相对于近期均值的放大程度。放量意味着市场参与度提升，价格运动更可信。缩量意味着参与度下降，价格运动可能是噪声。成交量因子本身没有方向，只衡量强度，因此归一化到 `[0, 1]`。

**计算伪代码：**

```python
def compute(self, bar, snapshot):
    # 1. 更新成交量历史
    self.volume_history.append(float(bar.volume))
    if len(self.volume_history) < self.ma_window + 1:
        return None  # warmup

    # 2. 计算相对成交量
    recent_mean = np.mean(self.volume_history[-self.ma_window:])
    if recent_mean < 1e-9:
        return {"value": 0.5, "stale": False}
    ratio = float(bar.volume) / recent_mean

    # 3. 分位数归一化（无方向）
    normalized = self.quantile_normalize(ratio, direction=False)

    # 4. 异常值检查
    if ratio > 5 * self.ratio_std:
        return {"value": normalized, "outlier": True}

    return {"value": round(normalized, 6), "stale": False}
```

**参数敏感性分析：**

| ma_window | 假信号率 | IC 均值 | 选择 |
|---|---|---|---|
| 10 | 14.8% | 0.08 | |
| **20** | **13.7%** | **0.10** | ✓ 默认 |
| 50 | 13.2% | 0.09 | |

**已知限制：**
- 成交量因子本身没有方向，只衡量强度，需要和其他因子组合使用
- 资金费率结算前后成交量异常放大，因子值偏高但不可信
- 交易所维护或网络异常导致成交量数据缺失时，沿用上一根值

---

## 4. 因子组合与权重

### 4.1 权重分配

Step 1 三个因子的初始权重：

| 因子 | 权重 | 来源 |
|---|---|---|
| `momentum` | 0.35 | IC 加权 |
| `obi` | 0.40 | IC 加权 |
| `volume` | 0.25 | IC 加权 |

**权重约束：**
- 权重之和 = 1.0
- 单因子权重上限 0.5
- 单因子权重下限 0.1

`[CI]` 检查：`scripts/check_factor_weights.py` 验证上述约束。

### 4.2 权重调整规则

- 每 **Step 结束** 重新校准一次权重，基于最近 90 天 IC
- 因子权重调整必须记录到 `PARAM_TABLE.md`
- 单次调整幅度不超过 0.1，避免剧烈变化
- 调整后必须跑 30 天回测对比

`[REVIEW]` 检查：代码审查人确认权重调整有回测对比。

### 4.3 正交化（Step 2 起）

Step 2 引入 DDE 因子后，DDE 与 OBI 在趋势市中相关系数达 0.72，需要正交化：

```
DDE_orthogonalized = DDE - beta * OBI
其中 beta = Cov(DDE, OBI) / Var(OBI)
```

正交化后，DDE 的残差作为独立信号参与加权。

`[REVIEW]` 检查：代码审查人确认 `factor_orthogonalizer.py` 的实现正确。

---

## 5. 因子 IC 监控

### 5.1 IC 计算

- 每根 K 线记录因子值
- 每 **100 根 K 线** 计算一次滚动 IC
- IC 计算方式：Spearman 秩相关（因子值 vs 未来 N 根 K 线收益）
- 默认 N = 6（即 18 分钟后的收益）

```
IC = spearman_corr(factor_values[-100:], forward_returns[-100:])
```

### 5.2 IC 预期范围

| 因子 | 正常 IC 范围 | 衰减阈值 | 停用阈值 |
|---|---|---|---|
| `momentum` | 0.10-0.18 | < 0.05 连续 4 周 | < 0.02 |
| `obi` | 0.12-0.20 | < 0.05 连续 4 周 | < 0.02 |
| `volume` | 0.06-0.12 | < 0.03 连续 4 周 | < 0.01 |

### 5.3 衰减处理

| 状态 | 条件 | 动作 |
|---|---|---|
| 正常 | IC 在正常范围 | 无 |
| 衰减中 | IC 低于衰减阈值连续 4 周，且趋势向下 | 权重降 50%，报警 |
| 停用 | IC 低于停用阈值 | 权重降为 0，标记 `deprecated` |
| 反转 | IC 持续为负 | 立即停用，检查因子公式是否失效 |

`[CI]` 检查：`analytics/alpha_monitor.py` 中的 `check_factor_decay` 函数必须实现上述逻辑。

### 5.4 按市场状态分组

因子 IC 必须按市场状态分组统计：

```python
{
    "trend": {"momentum": 0.18, "obi": 0.14, "volume": 0.09},
    "range": {"momentum": 0.06, "obi": 0.11, "volume": 0.07},
    "transition": {"momentum": 0.10, "obi": 0.13, "volume": 0.08},
}
```

报告中分别展示，用于判断因子在不同市场状态下的适用性。

---

## 6. 因子失效处理

### 6.1 单因子失效

- 因子计算异常 → 返回上一根值，标记 `error=True`
- 连续 3 根 K 线同一因子 `error=True` → 标记 `factor_degraded`，报警
- `factor_degraded` 状态下，该因子权重降为 0，其他因子权重重新归一化
- 因子恢复后，权重恢复需人工确认（防止反复失效）

### 6.2 多因子失效

- 两个以上因子同时 `degraded` → 状态机暂停开仓，只平仓
- 所有因子 `degraded` → 触发紧急停止

### 6.3 异常值处理

- 因子值超出历史 5σ 范围 → 标记 `outlier=True`，当根 K 线权重降为 0
- 连续 3 根 K 线同一因子 `outlier=True` → 标记 `factor_anomaly`，报警
- `factor_anomaly` 状态下，该因子权重减半，直到恢复正常

---

## 7. 因子文档与代码一致性

### 7.1 CI 检查

`scripts/check_factor_spec.py` 验证：
- `FACTOR_SPEC.md` 中列出的因子名与 `factors/` 目录中的文件一一对应
- 每个因子文件有 `MANIFEST`，且 `version` 与文档一致
- 每个因子文件有 `compute()` 方法，签名一致

### 7.2 文档更新流程

- 新增因子：先写 `FACTOR_SPEC.md`，再写代码，再跑回测，三人 review
- 修改因子：更新 `FACTOR_SPEC.md` 的版本号和公式，代码同步修改，跑回测对比
- 废弃因子：`FACTOR_SPEC.md` 标记 `deprecated`，代码标记 `deprecated: true`，保留至少一个 Step 周期

---

## 附录 A：因子速查表

| 因子 | 文件 | 版本 | 输入 | 归一化 | 权重 | 状态 |
|---|---|---|---|---|---|---|
| `momentum` | `factor_momentum.py` | 1.0.0 | bar.close | `[-1,1]` | 0.35 | Step 1 |
| `obi` | `factor_obi.py` | 1.0.0 | snapshot.bids/asks | `[-1,1]` | 0.40 | Step 1 |
| `volume` | `factor_volume.py` | 1.0.0 | bar.volume | `[0,1]` | 0.25 | Step 1 |
| `dde` | `factor_dde.py` | - | aggTrade | `[-1,1]` | - | Step 2 |
| `smart_money` | `factor_smart_money.py` | - | snapshot | `[-1,1]` | - | Step 2 |
| `sr_mapper` | `factor_sr_mapper.py` | - | 5m K线 | `[0,1]` | - | Step 2 |

## 附录 B：参数速查表

| 参数 | 默认值 | 所属因子 | 说明 |
|---|---|---|---|
| `roc_period` | 3 | momentum | ROC 窗口 |
| `momentum_roc_weight` | 0.7 | momentum | ROC 在合成中的权重 |
| `momentum_accel_weight` | 0.3 | momentum | 加速度在合成中的权重 |
| `obi_levels` | 10 | obi | 盘口档位 |
| `obi_decay_k` | 0.15 | obi | 距离衰减系数 |
| `obi_smooth_half_life_s` | 2.0 | obi | 时间平滑半衰期 |
| `volume_ma_window` | 20 | volume | 成交量均值窗口 |
| `quantile_window` | 500 | 所有因子 | 分位数归一化窗口 |

## 附录 C：归一化公式速查

```
有方向因子：
  p = 经验分位数（0 到 1）
  normalized = 2 * p - 1

无方向因子：
  p = 经验分位数（0 到 1）
  normalized = p

输出：round(normalized, 6)

分位数窗口：
  - 只在 K 线收线后更新
  - 不包含当前 K 线（避免前视）
  - 窗口未满时标记 warmup=True
```

---

> **因子不是拍出来的，是估出来的。每个参数背后必须有一个分布。**
> **没有经济学解释的因子是黑箱，没有归一化方式的因子无法加权，没有失效判定的因子会一直用到烂。**
> **因子文档与代码不一致时，以代码为准，但代码必须立刻补文档。**
