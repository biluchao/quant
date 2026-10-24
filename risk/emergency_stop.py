"""
risk/emergency_stop.py — 紧急停止（红色按钮）

整个系统里唯一一个"不在乎策略、不在乎信号、不在乎状态机、
只在乎'马上停下来'"的模块。

职责（唯一）：
1. 接收触发（手动/风控/看门狗/系统/外部 API）
2. 撤掉所有未完结订单
3. 平掉所有持仓
4. 记录完整的触发事件（谁、何时、为何、执行结果）
5. 提供幂等（重复触发不重复操作）
6. 提供清除（人工恢复，留痕）

不做什么：
- 不做策略判断（策略层负责）
- 不做风控判断（policy_minimal 负责）
- 不做轮次记账（RoundManager 负责）
- 不直接下单（gateway 负责）
- 不阻塞新订单（状态机负责）

职责边界：
- EmergencyStop：编排停止流程 + 记录审计
- StateMachine：接收通知，强制切换 IDLE，阻止新订单
- Gateway：执行撤单和平仓
- RoundManager：记录轮次关闭

关键契约（"四必"）：
1. 必幂等：重复触发返回同一个事件
2. 必留痕：事件通过 persister 落库
3. 必可清除：clear() 需人工确认
4. 必降级：组件失联时用更底层的方法

关键契约（"三不阻断"）：
1. 撤单失败不阻断平仓
2. 平仓失败不阻断状态机切换
3. 落库失败不阻断停止流程

阶段流转：
    NORMAL
      ↓ trigger()
    TRIGGERING
      ├→ 成功 → TRIGGERED
      └→ 失败 → FAILED
      ↓ clear()
    CLEARED
      ↓ trigger()
    TRIGGERING ...（可重复）

触发源（TriggerSource）：
- MANUAL: 人工（信号 SIGINT / 命令行 / 前端按钮）
- RISK_POLICY: 风控（HALT 等级持续超过阈值后由状态机触发）
- WATCHDOG: 看门狗（心跳超时）
- DATA_FEED: 数据流断裂（K 线断流超过阈值）
- EXTERNAL_API: 外部 API（POST /api/emergency_stop）
- SYSTEM_ERROR: 系统异常（未捕获异常、连续错误）

停止流程（严格顺序）：
1. 设置 _phase = TRIGGERING（幂等保护）
2. 构造 TriggerEvent
3. 落库事件（失败不阻断）
4. **撤单**：gateway.cancel_all_orders()
   - 先撤单再平仓（避免平仓单被撤单竞态干扰）
   - 失败记录到 event.actions，继续
5. **平仓**：state_machine.emergency_stop(reason)
   - 优先通过状态机（它处理轮次记账）
   - 状态机失败则降级到 gateway.close_all_positions()
   - 失败记录到 event.actions，继续
6. 更新 _phase = TRIGGERED 或 FAILED
7. 触发回调列表（每个独立 try/except）
8. 记录最终状态到 event.actions

清除流程：
1. 检查 _phase 是否为 TRIGGERED 或 FAILED
2. 检查 operator 和 reason（必填）
3. 记录审计（清除事件落库）
4. 设置 _phase = CLEARED
5. 触发清除回调

为什么不用 threading 超时：
- 依赖各组件自身的超时（RestClient.timeout_ms、state_machine 内部的 close_round 超时）
- 引入线程池超时会导致线程泄漏和资源浪费
- 组件内部超时是"正确的位置"，因为它知道超时后怎么清理

为什么"撤单先于平仓"：
- 平仓时会有新的市价单（reduce_only）
- 如果撤单后于平仓，可能撤掉刚下的平仓单
- 先撤单保证"存量订单被清理"，再平仓"产生的新订单是干净的"

为什么"状态机优先于 gateway"：
- 状态机的 emergency_stop 会调用 round_manager.close_round
- round_manager.close_round 会更新 rounds 表、触发 on_round_close
- 如果直接调 gateway.close_all_positions，轮次表不会更新
- 所以优先用状态机，失败才降级到 gateway

为什么"落库失败不阻断"：
- 停止流程的目的是"平仓保命"
- 落库失败只是"事后分析缺数据"，不是"生命危险"
- 先保命，再补数据

与 main.py 的集成：
    main.py._trigger_emergency(reason)
      → emergency_stop.trigger(
          source=TriggerSource.SYSTEM_ERROR,
          reason=reason,
        )

    main.py 收到 SIGINT / SIGTERM
      → emergency_stop.trigger(source=MANUAL, reason="signal")

    run_live.py 看门狗超时
      → emergency_stop.trigger(source=WATCHDOG, reason="heartbeat_timeout")

    api/routes_emergency.py POST /api/emergency_stop
      → emergency_stop.trigger(
          source=EXTERNAL_API,
          reason=body["reason"],
          operator=body["operator"],
        )

作者：张三
审查：比蒙（策略）、乐高（执行）
版本：v1.0.0
"""

from collections import deque
from dataclasses import dataclass, field
from enum import Enum
import logging
import threading
import time
import uuid
from typing import Any, Callable, Deque, Dict, List, Optional

logger = logging.getLogger(__name__)


# ============================================================================
# MANIFEST
# ============================================================================
MANIFEST = {
    "name": "emergency_stop",
    "type": "risk",
    "version": "1.0.0",
    "step": 1,
    "owner": "zhangsan",
    "capabilities": [
        "emergency_stop", "kill_switch", "circuit_breaker",
    ],
    "module_dependencies": [],
    "fallback": None,
    "enabled": True,
    "description": "Emergency stop: cancel all orders + close all positions + audit trail",
}


# ============================================================================
# 常量
# ============================================================================
DEFAULT_MAX_EVENTS = 100


# ============================================================================
# 枚举
# ============================================================================
class TriggerSource(str, Enum):
    """触发源。"""
    MANUAL = "manual"                # 人工（信号/命令行/前端）
    RISK_POLICY = "risk_policy"      # 风控 HALT
    WATCHDOG = "watchdog"            # 看门狗
    DATA_FEED = "data_feed"          # 数据流断裂
    EXTERNAL_API = "external_api"    # 外部 API
    SYSTEM_ERROR = "system_error"    # 系统异常


class StopPhase(str, Enum):
    """紧急停止的阶段。"""
    NORMAL = "normal"                # 正常（未触发）
    TRIGGERING = "triggering"        # 正在执行停止
    TRIGGERED = "triggered"          # 已停止，所有操作成功
    FAILED = "failed"                # 已停止，但有操作失败
    CLEARED = "cleared"              # 已清除（可重新交易）


# ============================================================================
# 事件记录
# ============================================================================
@dataclass
class TriggerEvent:
    """
    单次触发事件。完整记录用于审计。

    契约：
    - 不可变（构造后不再修改）
    - to_dict() 可序列化落库
    """
    event_id: str
    ts: int
    source: TriggerSource
    reason: str
    operator: Optional[str]
    details: Dict[str, Any] = field(default_factory=dict)
    phase: StopPhase = StopPhase.TRIGGERING
    actions: List[Dict[str, Any]] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    duration_ms: float = 0.0
    cleared_ts: Optional[int] = None
    cleared_operator: Optional[str] = None
    cleared_reason: Optional[str] = None

    def add_action(
        self,
        action: str,
        ok: bool,
        detail: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
    ) -> None:
        """添加一个执行动作记录。"""
        self.actions.append({
            "action": action,
            "ok": ok,
            "detail": detail or {},
            "error": error,
        })
        if error:
            self.errors.append(f"{action}: {error}")

    def to_dict(self) -> Dict[str, Any]:
        """序列化。"""
        return {
            "event_id": self.event_id,
            "ts": self.ts,
            "source": self.source.value,
            "reason": self.reason,
            "operator": self.operator,
            "details": dict(self.details),
            "phase": self.phase.value,
            "actions": list(self.actions),
            "errors": list(self.errors),
            "duration_ms": self.duration_ms,
            "cleared_ts": self.cleared_ts,
            "cleared_operator": self.cleared_operator,
            "cleared_reason": self.cleared_reason,
        }


# ============================================================================
# EmergencyStop
# ============================================================================
class EmergencyStop:
    """
    紧急停止。详见模块 docstring。

    用法：
        em = EmergencyStop(
            state_machine=sm,
            gateway=gateway,
            round_manager=round_manager,
            persister=persister,
            clock=clock,
            config=config,
        )

        # 触发
        event = em.trigger(
            source=TriggerSource.MANUAL,
            reason="user pressed red button",
            operator="bimon",
        )

        # 检查状态
        if em.is_triggered():
            logger.critical("system is in emergency stop")

        # 清除（人工确认后）
        em.clear(operator="bimon", reason="manual reset after investigation")

        # 健康检查
        em.health()
    """

    name = "emergency_stop"
    version = "1.0.0"

    def __init__(
        self,
        state_machine: Any = None,
        gateway: Any = None,
        round_manager: Any = None,
        persister: Any = None,
        clock: Any = None,
        config: Optional[dict] = None,
    ):
        cfg = (config or {}).get("emergency_stop", {})
        self._max_events = int(cfg.get("max_events", DEFAULT_MAX_EVENTS))

        self._state_machine = state_machine
        self._gateway = gateway
        self._round_manager = round_manager
        self._persister = persister
        self._clock = clock
        self._config = config or {}

        self._lock = threading.RLock()
        self._phase = StopPhase.NORMAL
        self._active_event: Optional[TriggerEvent] = None
        self._events: Deque[TriggerEvent] = deque(maxlen=self._max_events)

        self._callbacks: List[Callable[[TriggerEvent], None]] = []
        self._clear_callbacks: List[Callable[[TriggerEvent], None]] = []

        self._stats = {
            "triggers": 0,
            "trigger_successes": 0,
            "trigger_failures": 0,
            "clears": 0,
            "callback_errors": 0,
        }

        logger.info(
            f"EmergencyStop initialized "
            f"state_machine={'yes' if state_machine else 'no'} "
            f"gateway={'yes' if gateway else 'no'} "
            f"round_manager={'yes' if round_manager else 'no'} "
            f"persister={'yes' if persister else 'no'}"
        )

    # ========================================================================
    # 触发
    # ========================================================================

    def trigger(
        self,
        source: TriggerSource,
        reason: str,
        operator: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> TriggerEvent:
        """
        触发紧急停止。

        契约：
        - 幂等：已在 TRIGGERING 状态时返回当前事件
        - 已 TRIGGERED/FAILED 状态再次触发：执行新流程（清理上一次的残余）
        - 不抛异常（内部捕获）

        Args:
            source: 触发源
            reason: 触发原因
            operator: 操作人（如人工触发则必填）
            details: 额外详情

        Returns:
            TriggerEvent
        """
        with self._lock:
            self._stats["triggers"] += 1

            # 幂等：正在执行中，返回当前事件
            if self._phase == StopPhase.TRIGGERING:
                logger.warning(
                    f"emergency_stop already triggering, "
                    f"returning current event "
                    f"{self._active_event.event_id if self._active_event else 'None'}"
                )
                return self._active_event or self._make_dummy_event()

            # 已在 TRIGGERED/FAILED/CLEARED 状态：允许再次触发
            # 构造新事件
            event = self._build_event(
                source=source,
                reason=reason,
                operator=operator,
                details=details,
            )
            self._active_event = event
            self._phase = StopPhase.TRIGGERING
            self._events.append(event)

        logger.critical(
            f"EMERGENCY STOP TRIGGERED: "
            f"event_id={event.event_id} "
            f"source={source.value} "
            f"reason={reason!r} "
            f"operator={operator!r}"
        )

        # 落库事件（失败不阻断）
        self._persist_event(event)

        # 执行停止序列
        self._execute_stop_sequence(event)

        # 触发回调
        self._fire_callbacks(event)

        return event

    def _build_event(
        self,
        source: TriggerSource,
        reason: str,
        operator: Optional[str],
        details: Optional[Dict[str, Any]],
    ) -> TriggerEvent:
        """构造触发事件。"""
        ts = self._safe_now_ms()
        event_id = f"emg-{ts}-{uuid.uuid4().hex[:8]}"
        return TriggerEvent(
            event_id=event_id,
            ts=ts,
            source=source,
            reason=reason,
            operator=operator,
            details=dict(details or {}),
            phase=StopPhase.TRIGGERING,
        )

    def _make_dummy_event(self) -> TriggerEvent:
        """幂等保护时的空事件（理论上不会返回）。"""
        return TriggerEvent(
            event_id="dummy",
            ts=self._safe_now_ms(),
            source=TriggerSource.MANUAL,
            reason="dummy",
            operator=None,
            phase=StopPhase.NORMAL,
        )

    # ========================================================================
    # 停止序列
    # ========================================================================

    def _execute_stop_sequence(self, event: TriggerEvent) -> None:
        """
        执行停止序列。

        严格顺序：
        1. 撤单
        2. 平仓
        3. 更新阶段

        契约：
        - 每个操作独立 try/except
        - 任何一个失败不阻断后续
        - 所有结果记录到 event.actions
        """
        start = time.perf_counter()

        # ---- 1. 撤单（先撤单再平仓）----
        cancel_ok = self._cancel_all_orders(event)
        event.add_action(
            "cancel_all_orders",
            ok=cancel_ok,
            error=None if cancel_ok else "see detail",
        )

        # ---- 2. 平仓（状态机优先，gateway 降级）----
        close_ok = self._close_all_positions(event)
        event.add_action(
            "close_all_positions",
            ok=close_ok,
            error=None if close_ok else "see detail",
        )

        # ---- 3. 更新阶段 ----
        event.duration_ms = (time.perf_counter() - start) * 1000

        with self._lock:
            if cancel_ok and close_ok:
                event.phase = StopPhase.TRIGGERED
                self._phase = StopPhase.TRIGGERED
                self._stats["trigger_successes"] += 1
                logger.critical(
                    f"EMERGENCY STOP COMPLETED (SUCCESS): "
                    f"event_id={event.event_id} "
                    f"duration_ms={event.duration_ms:.1f}"
                )
            else:
                event.phase = StopPhase.FAILED
                self._phase = StopPhase.FAILED
                self._stats["trigger_failures"] += 1
                logger.critical(
                    f"EMERGENCY STOP COMPLETED (FAILED): "
                    f"event_id={event.event_id} "
                    f"errors={event.errors} "
                    f"duration_ms={event.duration_ms:.1f} "
                    f"MANUAL INTERVENTION REQUIRED"
                )

        # 更新事件落库（最终状态）
        self._persist_event(event)

    def _cancel_all_orders(self, event: TriggerEvent) -> bool:
        """
        撤掉所有未完结订单。

        契约：
        - 通过 gateway.cancel_all_orders()
        - gateway 不可用：记录 CRITICAL，返回 False
        - 内部异常：记录到 event.errors，返回 False
        """
        if self._gateway is None:
            logger.critical(
                "cannot cancel orders: gateway is None"
            )
            event.errors.append("gateway_unavailable_for_cancel")
            return False

        try:
            result = self._gateway.cancel_all_orders()
            canceled = result.get("canceled", 0)
            failed = result.get("failed", [])
            event.add_action(
                "cancel_all_orders",
                ok=(len(failed) == 0),
                detail={
                    "canceled": canceled,
                    "failed_count": len(failed),
                    "failed": failed[:10],  # 截断前 10 条
                },
                error=None if not failed else f"{len(failed)} orders failed to cancel",
            )
            logger.info(
                f"cancel_all_orders: "
                f"canceled={canceled} failed={len(failed)}"
            )
            # 部分失败也算撤单成功（大多数撤掉了）
            return len(failed) == 0
        except Exception as e:
            logger.error(
                f"cancel_all_orders failed: {e}", exc_info=True
            )
            event.errors.append(f"cancel_all_orders: {e}")
            return False

    def _close_all_positions(self, event: TriggerEvent) -> bool:
        """
        平掉所有持仓。

        契约：
        - 优先通过 state_machine.emergency_stop()（处理轮次记账）
        - state_machine 不可用或失败：降级到 gateway.close_all_positions()
        - 都失败：记录 CRITICAL，返回 False
        """
        # ---- 1. 尝试状态机 ----
        if self._state_machine is not None:
            try:
                ok = self._state_machine.emergency_stop(
                    reason=event.reason
                )
                if ok:
                    event.add_action(
                        "state_machine.emergency_stop",
                        ok=True,
                        detail={"path": "state_machine"},
                    )
                    return True
                event.add_action(
                    "state_machine.emergency_stop",
                    ok=False,
                    error="returned False",
                )
            except Exception as e:
                logger.error(
                    f"state_machine.emergency_stop failed: {e}",
                    exc_info=True,
                )
                event.add_action(
                    "state_machine.emergency_stop",
                    ok=False,
                    error=str(e),
                )
                event.errors.append(
                    f"state_machine.emergency_stop: {e}"
                )

        # ---- 2. 降级到 gateway ----
        if self._gateway is None:
            logger.critical(
                "cannot close positions: "
                "both state_machine and gateway unavailable"
            )
            event.errors.append("no_way_to_close_positions")
            return False

        try:
            results = self._gateway.close_all_positions(
                reason=event.reason
            )
            closed = len(results) if results else 0
            event.add_action(
                "gateway.close_all_positions",
                ok=True,
                detail={
                    "path": "gateway_fallback",
                    "closed_count": closed,
                },
            )
            logger.info(
                f"gateway.close_all_positions: closed={closed} "
                f"(fallback from state_machine)"
            )
            return True
        except Exception as e:
            logger.critical(
                f"gateway.close_all_positions failed: {e}",
                exc_info=True,
            )
            event.add_action(
                "gateway.close_all_positions",
                ok=False,
                error=str(e),
            )
            event.errors.append(f"gateway.close_all_positions: {e}")
            return False

    # ========================================================================
    # 清除
    # ========================================================================

    def clear(self, operator: str, reason: str) -> bool:
        """
        清除紧急停止状态。

        契约：
        - 只在 TRIGGERED 或 FAILED 状态可清除
        - operator 和 reason 必填
        - 清除后阶段为 CLEARED
        - 触发清除回调

        Args:
            operator: 操作人
            reason: 清除原因

        Returns:
            True: 成功清除
            False: 无法清除（阶段不对）
        """
        if not operator:
            raise ValueError("operator is required to clear")
        if not reason:
            raise ValueError("reason is required to clear")

        with self._lock:
            if self._phase not in (
                StopPhase.TRIGGERED, StopPhase.FAILED
            ):
                logger.warning(
                    f"clear rejected: phase={self._phase.value}"
                )
                return False

            old_phase = self._phase
            self._phase = StopPhase.CLEARED
            self._stats["clears"] += 1

            if self._active_event is not None:
                self._active_event.cleared_ts = self._safe_now_ms()
                self._active_event.cleared_operator = operator
                self._active_event.cleared_reason = reason
                event_to_clear = self._active_event
            else:
                event_to_clear = None

        logger.critical(
            f"EMERGENCY STOP CLEARED: "
            f"operator={operator!r} "
            f"reason={reason!r} "
            f"from_phase={old_phase.value}"
        )

        # 落库清除事件
        if event_to_clear is not None:
            self._persist_event(event_to_clear)

        # 触发清除回调
        self._fire_clear_callbacks(event_to_clear)

        return True

    # ========================================================================
    # 查询
    # ========================================================================

    def is_triggered(self) -> bool:
        """是否处于触发状态（TRIGGERING/TRIGGERED/FAILED）。"""
        with self._lock:
            return self._phase in (
                StopPhase.TRIGGERING,
                StopPhase.TRIGGERED,
                StopPhase.FAILED,
            )

    def get_phase(self) -> StopPhase:
        """当前阶段。"""
        with self._lock:
            return self._phase

    def get_active_event(self) -> Optional[TriggerEvent]:
        """当前活跃事件（最近一次）。"""
        with self._lock:
            return self._active_event

    def get_last_event(self) -> Optional[TriggerEvent]:
        """最近一次事件（含已清除）。"""
        with self._lock:
            return self._events[-1] if self._events else None

    def get_events(self, limit: int = 10) -> List[Dict[str, Any]]:
        """最近 N 次事件。"""
        with self._lock:
            events = list(self._events)[-limit:]
            return [e.to_dict() for e in events]

    # ========================================================================
    # 回调
    # ========================================================================

    def register_callback(
        self, callback: Callable[[TriggerEvent], None]
    ) -> None:
        """
        注册触发回调。紧急停止完成时调用。

        契约：
        - 回调异常独立 try/except，不影响其他回调
        - 调用顺序按注册顺序
        """
        self._callbacks.append(callback)

    def register_clear_callback(
        self, callback: Callable[[Optional[TriggerEvent]], None]
    ) -> None:
        """注册清除回调。"""
        self._clear_callbacks.append(callback)

    def _fire_callbacks(self, event: TriggerEvent) -> None:
        """触发停止回调。"""
        for i, cb in enumerate(self._callbacks):
            try:
                cb(event)
            except Exception as e:
                with self._lock:
                    self._stats["callback_errors"] += 1
                logger.error(
                    f"emergency_stop callback[{i}] failed: {e}",
                    exc_info=True,
                )

    def _fire_clear_callbacks(
        self, event: Optional[TriggerEvent]
    ) -> None:
        """触发清除回调。"""
        for i, cb in enumerate(self._clear_callbacks):
            try:
                cb(event)
            except Exception as e:
                with self._lock:
                    self._stats["callback_errors"] += 1
                logger.error(
                    f"emergency_stop clear_callback[{i}] failed: {e}",
                    exc_info=True,
                )

    # ========================================================================
    # 落库
    # ========================================================================

    def _persist_event(self, event: TriggerEvent) -> None:
        """
        事件落库。

        契约：
        - 失败不阻断
        - 通过 persister.write_event()
        - event_type = "emergency_stop"
        - severity = "critical"
        """
        if self._persister is None:
            return

        try:
            self._persister.write_event(
                event_type="emergency_stop",
                severity="critical",
                data=event.to_dict(),
            )
        except Exception as e:
            logger.error(
                f"persist emergency event failed: {e}",
                exc_info=True,
            )

    # ========================================================================
    # 辅助
    # ========================================================================

    def _safe_now_ms(self) -> int:
        """安全获取当前毫秒时间戳。"""
        if self._clock is None:
            return int(time.time() * 1000)
        try:
            return int(self._clock.now_ms())
        except Exception:
            return int(time.time() * 1000)

    # ========================================================================
    # 健康检查与重置
    # ========================================================================

    def health(self) -> Dict[str, Any]:
        """健康检查。"""
        with self._lock:
            last_event_dict = (
                self._active_event.to_dict()
                if self._active_event is not None else None
            )
            return {
                "healthy": self._phase != StopPhase.FAILED,
                "name": self.name,
                "version": self.version,
                "phase": self._phase.value,
                "triggered": self._phase in (
                    StopPhase.TRIGGERING,
                    StopPhase.TRIGGERED,
                    StopPhase.FAILED,
                ),
                "active_event": last_event_dict,
                "events_count": len(self._events),
                "stats": dict(self._stats),
                "timestamp": self._safe_now_ms(),
            }

    def reset(self) -> None:
        """重置（测试用）。"""
        with self._lock:
            self._phase = StopPhase.NORMAL
            self._active_event = None
            self._events.clear()
            self._callbacks.clear()
            self._clear_callbacks.clear()
            for k in self._stats:
                self._stats[k] = 0
        logger.info("EmergencyStop reset")


# ============================================================================
# 工厂函数
# ============================================================================
def create(config: dict) -> EmergencyStop:
    """
    模块工厂入口。

    config 需要：
    - instances.state_machine（可选）
    - instances.gateway
    - instances.round_manager（可选）
    - instances.persister（可选）
    - instances.clock（可选）

    config 可选：
    - emergency_stop.max_events: 默认 100
    """
    instances = config.get("instances", {})
    return EmergencyStop(
        state_machine=instances.get("state_machine"),
        gateway=instances.get("gateway"),
        round_manager=instances.get("round_manager"),
        persister=instances.get("persister"),
        clock=instances.get("clock"),
        config=config,
    )


def validate_module() -> dict:
    """校验 MANIFEST 合法。"""
    from core.manifest import ManifestParser
    ManifestParser.parse(MANIFEST, "risk/emergency_stop.py")
    return MANIFEST
