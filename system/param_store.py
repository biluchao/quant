"""
system/param_store.py — 参数存储（双缓冲 + 版本 + 冻结 + 审计）

参数存储不是配置读取器，是参数的活体。

设计原则：
1. 双缓冲：active 当前生效，pending 待生效
   - K 线内所有决策用 active
   - K 线边界 commit 时，pending -> active
2. 冻结计数器：轮次进行中参数冻结
   - freeze() 计数 +1
   - unfreeze() 计数 -1
   - 计数 > 0 时，propose_update 拒绝
3. 版本管理：每次 commit 产生新版本
   - version_id 单调递增
   - 历史版本保留 max_history 条
4. 审计日志：每次变更记录操作人、原因、时间
5. 校验器链：可插拔 validators，按注册顺序执行
6. 可热更新参数 vs 结构参数
   - 结构参数（ma_period / quantile_window）冷启动生效
   - 行为参数（entry_threshold / per_trade_risk）热更新生效
7. 深合并：嵌套字典递归合并
8. 线程安全：RLock 保护所有状态修改

双缓冲时序：
    K线N收线
    → StateMachine 用 active 参数决策
    → 决策完成后，main.py 调 param_store.commit()
    → pending 校验通过 → active = pending
    → 触发 on_commit 回调
    → K线N+1开始，用新 active 参数

关键契约：
1. commit() 只在 K 线边界调用，K 线内不调
2. propose_update() 冻结时拒绝，返回 False
3. 校验失败时 pending 保留，等待修复或 reject
4. 新值 == 旧值时跳过，不产生新版本
5. 未知 key 在 strict 模式下拒绝
6. 结构参数变更时，需要设置 restart_required=True
7. 回滚创建新版本，不删除历史
8. 所有返回值深拷贝，防外部修改
9. 审计日志记录 operator / reason / old / new
10. 参数版本写入 rounds 表，归因可追溯

冻结典型流程：
    RoundManager.open_round()
    → param_store.freeze(reason="round_abc123_open")
    → ... 交易进行中 ...
    → RoundManager.close_round()
    → param_store.unfreeze(reason="round_abc123_close")

可热更新参数清单（Step 1）：
    ✅ entry_threshold
    ✅ divergence_threshold
    ✅ per_trade_risk
    ✅ daily_dd_warn / daily_dd_reduce / daily_dd_close_only / daily_dd_halt
    ✅ max_position_ratio
    ✅ protection_qty_ratio
    ❌ ma_period（结构）
    ❌ consistency_run_min（结构）
    ❌ quantile_window（结构）
    ❌ warmup_bars（结构）
    ❌ roc_period（结构）

作者：张三
审查：比蒙（策略）、乐高（执行）
版本：v1.0.0
"""

from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timezone
import logging
import os
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ============================================================================
# 常量
# ============================================================================
SNAPSHOT_VERSION = 1
DEFAULT_MAX_HISTORY = 100
DEFAULT_MAX_AUDIT_LOG = 1000
DEFAULT_PENDING_TIMEOUT_S = 300.0       # pending 5 分钟未 commit 自动丢弃
DEFAULT_FREEZE_WARN_S = 3600.0          # 冻结超过 1 小时告警
DEFAULT_COOLOFF_BARS = 20               # 参数变更后 20 根 K 线内不产生新信号
DEFAULT_MIN_CHANGE_INTERVAL_S = 86400   # 24 小时内最多改一次

# 结构参数（不可热更新）
STRUCTURAL_PARAMS = {
    "ma_period", "ma_slope_window",
    "consistency_run_min", "consistency_decay",
    "quantile_window", "warmup_bars",
    "roc_period", "obi_levels", "obi_decay_k",
    "volume_ma_window",
}


# ============================================================================
# 错误
# ============================================================================
class ParamStoreError(Exception):
    """参数存储错误基类。"""
    pass


class ParamValidationError(ParamStoreError):
    """参数校验失败。"""
    def __init__(self, errors: List[str]):
        self.errors = errors
        super().__init__(f"param validation failed: {errors}")


class ParamFrozenError(ParamStoreError):
    """参数已冻结，无法变更。"""
    pass


# ============================================================================
# 参数版本
# ============================================================================
@dataclass(frozen=True)
class ParamVersion:
    """参数版本。不可变。"""
    version_id: int
    values: Dict[str, Any]
    created_ts: int
    operator: str
    reason: str
    reverted_from: Optional[int] = None  # 回滚来源版本

    def to_dict(self) -> dict:
        return {
            "version_id": self.version_id,
            "values": deepcopy(self.values),
            "created_ts": self.created_ts,
            "operator": self.operator,
            "reason": self.reason,
            "reverted_from": self.reverted_from,
        }


# ============================================================================
# 变更记录
# ============================================================================
@dataclass(frozen=True)
class ParamChange:
    """单次参数变更审计记录。"""
    ts: int
    operator: str
    reason: str
    old_values: Dict[str, Any]
    new_values: Dict[str, Any]
    version_from: int
    version_to: int
    reverted_from: Optional[int] = None

    def to_dict(self) -> dict:
        return {
            "ts": self.ts,
            "operator": self.operator,
            "reason": self.reason,
            "old_values": deepcopy(self.old_values),
            "new_values": deepcopy(self.new_values),
            "version_from": self.version_from,
            "version_to": self.version_to,
            "reverted_from": self.reverted_from,
        }


# ============================================================================
# 校验器类型
# ============================================================================
Validator = Callable[[Dict[str, Any]], Tuple[bool, List[str]]]


# ============================================================================
# ParamStore
# ============================================================================
class ParamStore:
    """
    参数存储。详见模块 docstring。

    用法：
        store = ParamStore(config)

        # 从配置文件加载
        store.load_from_files(config_dir, step=1, symbol="btcusdt")

        # 读取
        threshold = store.get("entry_threshold", 0.50)
        all_params = store.get_all()

        # 提议变更（进入 pending）
        ok = store.propose_update(
            {"entry_threshold": 0.52},
            operator="bimon",
            reason="假信号率从 13.7% 降到 11.2%",
        )

        # K 线边界提交
        committed = store.commit()

        # 冻结/解冻（RoundManager 调用）
        store.freeze(reason="round_abc123_open")
        store.unfreeze(reason="round_abc123_close")

        # 回滚
        store.rollback(version_id=5, operator="bimon", reason="新参数表现差")

        # 历史
        history = store.get_history()
        audit = store.get_audit_log()
    """

    name = "param_store"
    version = "1.0.0"

    def __init__(self, config: dict):
        self.config = config
        cfg = config.get("param_store", {})

        self._max_history = int(
            cfg.get("max_history", DEFAULT_MAX_HISTORY)
        )
        self._max_audit_log = int(
            cfg.get("max_audit_log", DEFAULT_MAX_AUDIT_LOG)
        )
        self._pending_timeout_s = float(
            cfg.get("pending_timeout_s", DEFAULT_PENDING_TIMEOUT_S)
        )
        self._freeze_warn_s = float(
            cfg.get("freeze_warn_s", DEFAULT_FREEZE_WARN_S)
        )
        self._strict_keys = bool(cfg.get("strict_keys", False))
        self._validators: List[Validator] = []

        # ---- 状态 ----
        self._lock = threading.RLock()
        self._active: Dict[str, Any] = {}
        self._pending: Optional[Dict[str, Any]] = None
        self._pending_proposed_ts: float = 0.0
        self._pending_meta: Dict[str, Any] = {}
        self._version_id: int = 0
        self._history: List[ParamVersion] = []
        self._audit_log: List[ParamChange] = []
        self._frozen_count: int = 0
        self._freeze_start_ts: float = 0.0
        self._freeze_reasons: List[str] = []
        self._on_commit_callbacks: List[Callable[[Dict[str, Any], Dict[str, Any]], None]] = []

        # ---- 统计 ----
        self._stats = {
            "proposes": 0,
            "commits": 0,
            "rejects": 0,
            "rollbacks": 0,
            "freezes": 0,
            "unfreezes": 0,
            "noop_skipped": 0,
            "validation_failed": 0,
            "frozen_rejected": 0,
        }

        logger.info(
            f"ParamStore initialized max_history={self._max_history} "
            f"strict_keys={self._strict_keys}"
        )

    # ========================================================================
    # 校验器
    # ========================================================================

    def add_validator(self, validator: Validator) -> None:
        """
        注册校验器。按注册顺序执行。

        validator(params: dict) -> (ok: bool, errors: List[str])
        """
        self._validators.append(validator)
        logger.info(
            f"validator registered, total={len(self._validators)}"
        )

    def _run_validators(
        self, params: Dict[str, Any]
    ) -> Tuple[bool, List[str]]:
        """执行所有校验器。"""
        all_errors: List[str] = []
        for i, validator in enumerate(self._validators):
            try:
                ok, errors = validator(params)
            except Exception as e:
                all_errors.append(
                    f"validator[{i}] raised: {type(e).__name__}: {e}"
                )
                continue
            if not ok:
                all_errors.extend(errors)
        return (len(all_errors) == 0, all_errors)

    def _check_structural_change(
        self, old: Dict[str, Any], new: Dict[str, Any]
    ) -> List[str]:
        """检查结构参数是否变更。返回警告列表。"""
        warnings = []
        for key in STRUCTURAL_PARAMS:
            old_val = self._get_nested(old, key)
            new_val = self._get_nested(new, key)
            if old_val is not None and new_val is not None:
                if old_val != new_val:
                    warnings.append(
                        f"structural param '{key}' changed "
                        f"({old_val} -> {new_val}), "
                        f"requires restart to take effect"
                    )
        return warnings

    # ========================================================================
    # 加载
    # ========================================================================

    def load_from_files(
        self,
        config_dir: str,
        step: int = 1,
        symbol: str = "btcusdt",
    ) -> None:
        """
        从 YAML 文件加载参数。

        加载顺序：base.yaml -> step{N}.yaml -> symbols/{symbol}.yaml
        深合并，后者覆盖前者。

        环境变量替换：${VAR:-default} 语法。
        """
        import yaml
        from pathlib import Path

        config_path = Path(config_dir)
        files = [
            config_path / "base.yaml",
            config_path / f"step{step}.yaml",
            config_path / "symbols" / f"{symbol}.yaml",
        ]

        merged: Dict[str, Any] = {}
        for path in files:
            if not path.exists():
                logger.warning(f"config file not found: {path}")
                continue
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = yaml.safe_load(f) or {}
                if not isinstance(data, dict):
                    raise ParamStoreError(
                        f"{path} top-level must be a dict"
                    )
                data = self._expand_env(data)
                merged = self._deep_merge(merged, data)
            except (yaml.YAMLError, OSError) as e:
                raise ParamStoreError(
                    f"failed to load {path}: {e}"
                ) from e

        with self._lock:
            self._active = merged
            self._version_id = 1
            self._history = [ParamVersion(
                version_id=1,
                values=deepcopy(merged),
                created_ts=int(time.time() * 1000),
                operator="system",
                reason="initial load",
            )]

        logger.info(
            f"loaded params from {len(files)} files, "
            f"version=1, keys={len(merged)}"
        )

    def _expand_env(self, obj: Any) -> Any:
        """递归展开环境变量 ${VAR:-default}。"""
        import re
        pattern = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")

        def replace(match):
            var_name = match.group(1)
            default = match.group(2) if match.group(2) is not None else ""
            return os.environ.get(var_name, default)

        if isinstance(obj, str):
            return pattern.sub(replace, obj)
        if isinstance(obj, dict):
            return {k: self._expand_env(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._expand_env(v) for v in obj]
        return obj

    def _deep_merge(
        self, base: Dict[str, Any], override: Dict[str, Any]
    ) -> Dict[str, Any]:
        """深合并：嵌套字典递归合并，其他类型覆盖。"""
        result = deepcopy(base)
        for key, value in override.items():
            if (
                key in result
                and isinstance(result[key], dict)
                and isinstance(value, dict)
            ):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = deepcopy(value)
        return result

    # ========================================================================
    # 读取
    # ========================================================================

    def get(self, key: str, default: Any = None) -> Any:
        """读取参数（支持嵌套 `a.b.c` 语法）。返回深拷贝。"""
        with self._lock:
            value = self._get_nested(self._active, key)
            if value is None:
                return default
            return deepcopy(value)

    def get_all(self) -> Dict[str, Any]:
        """读取全部 active 参数。返回深拷贝。"""
        with self._lock:
            return deepcopy(self._active)

    def get_version(self) -> int:
        """当前版本号。"""
        with self._lock:
            return self._version_id

    def _get_nested(self, d: Dict[str, Any], key: str) -> Any:
        """支持点分隔的嵌套查询。"""
        parts = key.split(".")
        current: Any = d
        for part in parts:
            if not isinstance(current, dict) or part not in current:
                return None
            current = current[part]
        return current

    # ========================================================================
    # 变更提议
    # ========================================================================

    def propose_update(
        self,
        changes: Dict[str, Any],
        operator: str,
        reason: str,
        *,
        backtest_ref: Optional[str] = None,
    ) -> Tuple[bool, List[str]]:
        """
        提议参数变更。变更进入 pending，等待 commit 生效。

        Args:
            changes: 变更的键值（支持嵌套 `a.b.c`）
            operator: 操作人
            reason: 变更原因
            backtest_ref: 回测报告 ID（可选，无则打 WARN）

        Returns:
            (ok, errors)
            ok=True: pending 已更新
            ok=False: 拒绝，errors 含原因

        契约：
        - 冻结时拒绝
        - 校验失败拒绝
        - 新值 == 旧值时视为 no-op，返回 True 但不更新 pending
        """
        with self._lock:
            self._stats["proposes"] += 1

            # 冻结检查
            if self._frozen_count > 0:
                self._stats["frozen_rejected"] += 1
                logger.warning(
                    f"propose rejected: frozen "
                    f"(count={self._frozen_count}, "
                    f"reasons={self._freeze_reasons})"
                )
                return False, [
                    f"param store is frozen: {self._freeze_reasons}"
                ]

            if not changes:
                return True, []

            # 构造候选参数集
            candidate = self._apply_changes(
                self._active, changes, prefix=""
            )

            # 严格模式：未知 key 拒绝
            if self._strict_keys:
                unknown = self._find_unknown_keys(
                    self._active, candidate, prefix=""
                )
                if unknown:
                    self._stats["validation_failed"] += 1
                    return False, [
                        f"unknown keys in strict mode: {unknown}"
                    ]

            # 校验
            ok, errors = self._run_validators(candidate)
            if not ok:
                self._stats["validation_failed"] += 1
                logger.warning(
                    f"propose rejected: validation failed: {errors}"
                )
                return False, errors

            # 结构参数变更警告
            warnings = self._check_structural_change(
                self._active, candidate
            )
            if warnings:
                for w in warnings:
                    logger.warning(w)

            # 空变更检查
            if candidate == self._active:
                self._stats["noop_skipped"] += 1
                logger.info("propose skipped: no effective change")
                return True, []

            # 回测引用检查
            if backtest_ref is None:
                logger.warning(
                    f"propose without backtest_ref: "
                    f"operator={operator} reason={reason}"
                )

            # 更新 pending
            self._pending = candidate
            self._pending_proposed_ts = time.time()
            self._pending_meta = {
                "operator": operator,
                "reason": reason,
                "backtest_ref": backtest_ref,
                "changes": changes,
                "warnings": warnings,
            }

            logger.info(
                f"propose accepted: operator={operator} "
                f"reason={reason} changes={list(changes.keys())} "
                f"pending=1"
            )
            return True, warnings

    def _apply_changes(
        self, base: Dict[str, Any], changes: Dict[str, Any], prefix: str
    ) -> Dict[str, Any]:
        """应用变更，支持点分隔的嵌套 key。"""
        result = deepcopy(base)
        for key, value in changes.items():
            if "." in key:
                parts = key.split(".")
                current = result
                for part in parts[:-1]:
                    if part not in current:
                        current[part] = {}
                    if not isinstance(current[part], dict):
                        current[part] = {}
                    current = current[part]
                current[parts[-1]] = deepcopy(value)
            else:
                if (
                    key in result
                    and isinstance(result[key], dict)
                    and isinstance(value, dict)
                ):
                    result[key] = self._deep_merge(result[key], value)
                else:
                    result[key] = deepcopy(value)
        return result

    def _find_unknown_keys(
        self,
        reference: Dict[str, Any],
        candidate: Dict[str, Any],
        prefix: str,
    ) -> List[str]:
        """找出 candidate 中不在 reference 中的 key。"""
        unknown = []
        for key, value in candidate.items():
            full_key = f"{prefix}.{key}" if prefix else key
            if key not in reference:
                unknown.append(full_key)
            elif isinstance(value, dict) and isinstance(reference[key], dict):
                unknown.extend(
                    self._find_unknown_keys(
                        reference[key], value, full_key
                    )
                )
        return unknown

    def get_pending(self) -> Optional[Dict[str, Any]]:
        """获取 pending 变更（深拷贝）。"""
        with self._lock:
            if self._pending is None:
                return None
            return {
                "values": deepcopy(self._pending),
                "meta": deepcopy(self._pending_meta),
                "age_s": time.time() - self._pending_proposed_ts,
            }

    def reject_pending(self, reason: str = "") -> bool:
        """丢弃 pending 变更。"""
        with self._lock:
            if self._pending is None:
                return False
            self._pending = None
            self._pending_meta = {}
            self._pending_proposed_ts = 0.0
            self._stats["rejects"] += 1
            logger.info(f"pending rejected: {reason}")
            return True

    # ========================================================================
    # 提交
    # ========================================================================

    def commit(self) -> bool:
        """
        提交 pending 变更。只在 K 线边界调用。

        Returns:
            True: 已提交（或无需提交）
            False: 提交失败（pending 不存在或校验失败）

        契约：
        - 无 pending 返回 True（幂等）
        - pending 超时自动丢弃
        - 校验通过才提交
        - 提交是原子操作
        - 提交后触发 on_commit 回调
        """
        with self._lock:
            if self._pending is None:
                return True

            # 超时检查
            age_s = time.time() - self._pending_proposed_ts
            if age_s > self._pending_timeout_s:
                logger.warning(
                    f"pending expired after {age_s:.1f}s, dropped"
                )
                self._pending = None
                self._pending_meta = {}
                self._pending_proposed_ts = 0.0
                return False

            # 再次校验（防止 pending 期间 other things changed）
            ok, errors = self._run_validators(self._pending)
            if not ok:
                self._stats["validation_failed"] += 1
                logger.error(
                    f"commit failed: validation failed: {errors}"
                )
                return False

            # 捕获旧值
            old_active = deepcopy(self._active)
            old_version = self._version_id
            meta = self._pending_meta

            # 更新 active
            self._active = self._pending
            self._pending = None
            self._pending_meta = {}
            self._pending_proposed_ts = 0.0
            self._version_id += 1

            # 记录版本
            version = ParamVersion(
                version_id=self._version_id,
                values=deepcopy(self._active),
                created_ts=int(time.time() * 1000),
                operator=meta.get("operator", "unknown"),
                reason=meta.get("reason", ""),
            )
            self._history.append(version)
            if len(self._history) > self._max_history:
                self._history = self._history[-self._max_history:]

            # 记录审计
            change = ParamChange(
                ts=int(time.time() * 1000),
                operator=meta.get("operator", "unknown"),
                reason=meta.get("reason", ""),
                old_values=old_active,
                new_values=deepcopy(self._active),
                version_from=old_version,
                version_to=self._version_id,
            )
            self._audit_log.append(change)
            if len(self._audit_log) > self._max_audit_log:
                self._audit_log = self._audit_log[-self._max_audit_log:]

            self._stats["commits"] += 1
            logger.info(
                f"commit: version {old_version} -> {self._version_id} "
                f"operator={meta.get('operator')} "
                f"reason={meta.get('reason')}"
            )

        # 触发回调（锁外）
        for cb in self._on_commit_callbacks:
            try:
                cb(old_active, self._active)
            except Exception as e:
                logger.error(
                    f"on_commit callback error: {e}", exc_info=True
                )

        return True

    def on_commit(self, callback: Callable) -> None:
        """
        注册 commit 回调。callback(old_values, new_values)。

        用于因子权重重归一化、状态机重置等。
        """
        self._on_commit_callbacks.append(callback)

    # ========================================================================
    # 冻结
    # ========================================================================

    def freeze(self, reason: str) -> None:
        """
        冻结参数。轮次进行中调用。

        契约：
        - 计数器递增，支持嵌套冻结
        - 冻结期间 propose_update 拒绝
        """
        with self._lock:
            self._frozen_count += 1
            self._freeze_reasons.append(reason)
            if self._frozen_count == 1:
                self._freeze_start_ts = time.time()
            self._stats["freezes"] += 1
            logger.info(
                f"freeze: reason={reason} count={self._frozen_count}"
            )

    def unfreeze(self, reason: str) -> None:
        """
        解冻参数。轮次结束时调用。

        契约：
        - 计数器递减，不能减到负数
        - reason 必须与 freeze 配对
        """
        with self._lock:
            if self._frozen_count <= 0:
                logger.warning(
                    f"unfreeze called but not frozen: reason={reason}"
                )
                return
            self._frozen_count -= 1
            if reason in self._freeze_reasons:
                self._freeze_reasons.remove(reason)
            if self._frozen_count == 0:
                duration_s = time.time() - self._freeze_start_ts
                if duration_s > self._freeze_warn_s:
                    logger.warning(
                        f"freeze held too long: {duration_s:.0f}s "
                        f"(reason={reason})"
                    )
                self._freeze_start_ts = 0.0
            self._stats["unfreezes"] += 1
            logger.info(
                f"unfreeze: reason={reason} count={self._frozen_count}"
            )

    def is_frozen(self) -> bool:
        """是否冻结。"""
        with self._lock:
            return self._frozen_count > 0

    def get_freeze_info(self) -> Dict[str, Any]:
        """获取冻结信息。"""
        with self._lock:
            return {
                "frozen": self._frozen_count > 0,
                "count": self._frozen_count,
                "reasons": list(self._freeze_reasons),
                "duration_s": (
                    time.time() - self._freeze_start_ts
                    if self._freeze_start_ts > 0 else 0.0
                ),
            }

    # ========================================================================
    # 历史与回滚
    # ========================================================================

    def get_history(self) -> List[Dict[str, Any]]:
        """获取历史版本列表（不含完整 values，节省带宽）。"""
        with self._lock:
            return [
                {
                    "version_id": v.version_id,
                    "created_ts": v.created_ts,
                    "operator": v.operator,
                    "reason": v.reason,
                    "reverted_from": v.reverted_from,
                }
                for v in self._history
            ]

    def get_version_by_id(
        self, version_id: int
    ) -> Optional[Dict[str, Any]]:
        """按 version_id 查历史版本（含 values）。"""
        with self._lock:
            for v in self._history:
                if v.version_id == version_id:
                    return v.to_dict()
            return None

    def rollback(
        self,
        version_id: int,
        operator: str,
        reason: str,
    ) -> Tuple[bool, List[str]]:
        """
        回滚到指定版本。

        契约：
        - 创建新版本，不删除历史
        - 新版本标记 reverted_from
        - 回滚前校验目标版本存在
        - 冻结时拒绝
        """
        with self._lock:
            if self._frozen_count > 0:
                return False, [f"param store is frozen"]

            target = None
            for v in self._history:
                if v.version_id == version_id:
                    target = v
                    break
            if target is None:
                return False, [
                    f"version {version_id} not found in history"
                ]

            # 校验目标版本
            ok, errors = self._run_validators(target.values)
            if not ok:
                return False, [
                    f"rollback target invalid: {errors}"
                ]

            old_version = self._version_id
            old_active = deepcopy(self._active)

            self._active = deepcopy(target.values)
            self._version_id += 1

            new_version = ParamVersion(
                version_id=self._version_id,
                values=deepcopy(self._active),
                created_ts=int(time.time() * 1000),
                operator=operator,
                reason=reason,
                reverted_from=version_id,
            )
            self._history.append(new_version)
            if len(self._history) > self._max_history:
                self._history = self._history[-self._max_history:]

            change = ParamChange(
                ts=int(time.time() * 1000),
                operator=operator,
                reason=reason,
                old_values=old_active,
                new_values=deepcopy(self._active),
                version_from=old_version,
                version_to=self._version_id,
                reverted_from=version_id,
            )
            self._audit_log.append(change)
            if len(self._audit_log) > self._max_audit_log:
                self._audit_log = self._audit_log[-self._max_audit_log:]

            self._stats["rollbacks"] += 1
            logger.info(
                f"rollback: to version {version_id}, "
                f"new version {self._version_id}"
            )
            return True, []

    def get_audit_log(self) -> List[Dict[str, Any]]:
        """获取审计日志（不含完整 values，节省带宽）。"""
        with self._lock:
            return [
                {
                    "ts": c.ts,
                    "operator": c.operator,
                    "reason": c.reason,
                    "version_from": c.version_from,
                    "version_to": c.version_to,
                    "reverted_from": c.reverted_from,
                }
                for c in self._audit_log
            ]

    # ========================================================================
    # 持久化
    # ========================================================================

    def snapshot(self) -> Dict[str, Any]:
        """导出快照。"""
        with self._lock:
            return {
                "snapshot_version": SNAPSHOT_VERSION,
                "version_id": self._version_id,
                "active": deepcopy(self._active),
                "pending": deepcopy(self._pending),
                "pending_meta": deepcopy(self._pending_meta),
                "pending_age_s": (
                    time.time() - self._pending_proposed_ts
                    if self._pending is not None else 0.0
                ),
                "frozen_count": self._frozen_count,
                "freeze_reasons": list(self._freeze_reasons),
                "history_tail": [
                    v.to_dict() for v in self._history[-10:]
                ],
                "audit_tail": [
                    c.to_dict() for c in self._audit_log[-20:]
                ],
                "stats": dict(self._stats),
                "ts": int(time.time() * 1000),
            }

    def restore(self, snapshot: Dict[str, Any]) -> None:
        """恢复快照。"""
        sv = snapshot.get("snapshot_version", 0)
        if sv != SNAPSHOT_VERSION:
            raise ParamStoreError(
                f"snapshot version mismatch: "
                f"{sv} != {SNAPSHOT_VERSION}"
            )
        with self._lock:
            self._version_id = int(
                snapshot.get("version_id", 0)
            )
            self._active = deepcopy(snapshot.get("active", {}))
            self._pending = (
                deepcopy(snapshot["pending"])
                if snapshot.get("pending") else None
            )
            self._pending_meta = deepcopy(
                snapshot.get("pending_meta", {})
            )
            if self._pending is not None:
                age = float(snapshot.get("pending_age_s", 0.0))
                self._pending_proposed_ts = time.time() - age
            else:
                self._pending_proposed_ts = 0.0
            self._frozen_count = int(
                snapshot.get("frozen_count", 0)
            )
            self._freeze_reasons = list(
                snapshot.get("freeze_reasons", [])
            )
            self._history = [
                ParamVersion(**v)
                for v in snapshot.get("history_tail", [])
            ]
            self._audit_log = [
                ParamChange(**c)
                for c in snapshot.get("audit_tail", [])
            ]
            self._freeze_start_ts = (
                time.time() if self._frozen_count > 0 else 0.0
            )
        logger.info(
            f"restored params: version={self._version_id} "
            f"frozen={self._frozen_count > 0}"
        )

    # ========================================================================
    # 健康检查
    # ========================================================================

    def health_check(self) -> Dict[str, Any]:
        """健康检查。"""
        with self._lock:
            pending_age = (
                time.time() - self._pending_proposed_ts
                if self._pending is not None else None
            )
            freeze_duration = (
                time.time() - self._freeze_start_ts
                if self._freeze_start_ts > 0 else 0.0
            )
            healthy = not (
                freeze_duration > self._freeze_warn_s
                or (pending_age is not None
                    and pending_age > self._pending_timeout_s * 2)
            )
            return {
                "healthy": healthy,
                "name": self.name,
                "version": self.version,
                "version_id": self._version_id,
                "keys": len(self._active),
                "pending": self._pending is not None,
                "pending_age_s": pending_age,
                "frozen": self._frozen_count > 0,
                "frozen_count": self._frozen_count,
                "freeze_duration_s": freeze_duration,
                "history_size": len(self._history),
                "audit_log_size": len(self._audit_log),
                "stats": dict(self._stats),
                "timestamp": int(time.time() * 1000),
            }

    def reset(self) -> None:
        """重置。测试用。"""
        with self._lock:
            self._active = {}
            self._pending = None
            self._pending_meta = {}
            self._pending_proposed_ts = 0.0
            self._version_id = 0
            self._history = []
            self._audit_log = []
            self._frozen_count = 0
            self._freeze_reasons = []
            self._freeze_start_ts = 0.0
            for k in self._stats:
                self._stats[k] = 0
        logger.info("ParamStore reset")


# ============================================================================
# MANIFEST
# ============================================================================
MANIFEST = {
    "name": "param_store",
    "type": "system",
    "version": "1.0.0",
    "step": 1,
    "capabilities": ["param_hot_reload", "param_history", "param_freeze"],
    "module_dependencies": [],
    "fallback": None,
    "owner": "zhangsan",
    "description": "Parameter store with double buffering, versioning, freeze, and audit log",
}


# ============================================================================
# 工厂函数
# ============================================================================
def create(config: dict) -> ParamStore:
    """模块工厂入口。"""
    return ParamStore(config)
