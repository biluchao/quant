"""
states/state_machine.py — 状态机总控 (重铸标准版)

核心设计原则：
1. 绝对显式：绝不自动装配状态，重复注册立刻报错。
2. 事务安全：交易失败（如风控拒绝）绝不改变当前状态。
3. 零 meta：严格使用强类型字段传递跨状态数据。
4. 异常隔离：任何状态内部异常不影响主数据流。
"""

import logging
import threading
import time
from typing import Any, Dict, List, Optional

from core.state import (
    StateAction,
    StateContext,
    StateProvider,
    StateTransition,
    TransitionReason,
)
from core.strategy import StrategyContext, StrategyProvider

logger = logging.getLogger(__name__)

class StateMachineError(Exception):
    """状态机致命错误"""
    pass


class StateMachine:
    """
    状态机主引擎。
    """
    name = "state_machine"
    version = "1.0.0"

    def __init__(
        self,
        factor_registry: Any,
        strategies: Dict[str, StrategyProvider],
        config: dict,
        persister: Any,
        risk_policy: Any,
        order_gateway: Any,
        round_manager: Any,
        clock: Any,
        symbol: str,
        kline_buffer: Any,
    ):
        # 强依赖注入校验
        if not factor_registry or not round_manager or not risk_policy or not order_gateway:
            raise StateMachineError("StateMachine 缺少核心依赖，拒绝启动")

        self._factor_registry = factor_registry
        self._strategies = strategies or {}
        self._config = config
        self._persister = persister
        self._risk_policy = risk_policy
        self._order_gateway = order_gateway
        self._round_manager = round_manager
        self._clock = clock
        self._symbol = symbol.upper()
        self._kline_buffer = kline_buffer

        self._states: Dict[str, StateProvider] = {}
        self._lock = threading.RLock()
        self._in_bar_close = False
        
        self._current_state_name = "idle"
        self._bars_in_state = 0
        self._enter_reason = "system_start"
        self._initialized = False

        # 标准的合法转换路径
        self._transition_table = {
            "idle": ("breakout_confirm",),
            "breakout_confirm": ("trend_ride", "idle"),
            "trend_ride": ("idle", "top_detect"),
            "top_detect": ("idle",),
        }

        self._stats = {"bars": 0, "transitions": 0, "trade_failures": 0}

    # ========================================================================
    # 状态注册与初始化
    # ========================================================================

    def register_state(self, name: str, state: StateProvider) -> None:
        """注册状态。严禁重复注册。"""
        if not name or not isinstance(state, StateProvider):
            raise StateMachineError(f"无效的状态注册: {name}")
            
        with self._lock:
            if name in self._states:
                raise StateMachineError(f"状态 '{name}' 已经被注册，禁止重复覆盖！")
            self._states[name] = state
            logger.info(f"状态注册成功: {name}")

    def validate(self) -> None:
        """校验状态机完整性"""
        with self._lock:
            if "idle" not in self._states:
                raise StateMachineError("缺少初始状态 'idle'")
                
            for src, targets in self._transition_table.items():
                if src not in self._states:
                    raise StateMachineError(f"状态转换表中的 '{src}' 未被注册")
                for target in targets:
                    if target not in self._states:
                        raise StateMachineError(f"状态转换表中的目标 '{target}' 未被注册")

            logger.info("状态机完整性校验通过。")

    # ========================================================================
    # 核心循环
    # ========================================================================

    def on_bar_close(self, bar: Any, snapshot: Optional[dict] = None) -> Optional[StateTransition]:
        """处理 K 线收线，驱动状态机流转"""
        with self._lock:
            if self._in_bar_close:
                logger.warning("状态机重入被拦截")
                return None
            self._in_bar_close = True

        try:
            return self._on_bar_close_impl(bar, snapshot)
        except Exception as e:
            logger.critical(f"状态机运行发生严重异常: {e}", exc_info=True)
            return None
        finally:
            with self._lock:
                self._in_bar_close = False

    def _on_bar_close_impl(self, bar: Any, snapshot: Optional[dict]) -> Optional[StateTransition]:
        with self._lock:
            self._stats["bars"] += 1
            if not self._initialized:
                self._initialize(bar, snapshot)

        # 1. 因子与信号计算
        factors = self._compute_factors(bar, snapshot)
        signals = self._compute_signals(bar, snapshot, factors)

        with self._lock:
            self._bars_in_state += 1
            current_state = self._current_state_name
            active_round = self._round_manager.get_active_round()
            risk_level = self._risk_policy.get_risk_level()
            risk_approved = risk_level.value not in ("halt", "close_only")

            # 2. 组装强类型上下文
            context = StateContext(
                bar=bar,
                snapshot=snapshot,
                factors=factors,
                signals=signals,
                active_round=active_round,
                risk_level=risk_level.value,
                risk_approved=risk_approved,
                current_state=current_state,
                bars_in_state=self._bars_in_state,
                enter_reason=self._enter_reason,
                now_ms=self._clock.now_ms(),
                recent_bars=self._kline_buffer.get_recent(100) if self._kline_buffer else [],
                bar_phase="close",
                # 跨状态数据从活跃轮次中提取
                trend_direction=active_round.get("direction", 0) if active_round else 0,
                peak_close=Decimal(str(active_round.get("peak_close"))) if active_round and active_round.get("peak_close") else None,
                entry_price=Decimal(str(active_round.get("entry_price"))) if active_round and active_round.get("entry_price") else None,
                signal_id=active_round.get("signal_id") if active_round else None,
            )

        # 3. 调用当前状态
        state = self._states[current_state]
        transition = state.on_bar_close(context)

        if transition is None:
            logger.warning(f"状态 '{current_state}' 返回了 None")
            return None

        # 4. 处理止损提示 (HOLD 状态下更新止损)
        if transition.action == StateAction.HOLD and transition.stop_price_hint:
            self._update_stop(transition.stop_price_hint)

        # 5. 状态迁移与交易编排
        if transition.target is not None:
            self._handle_transition(transition, bar, snapshot, factors, signals)

        # 6. 落库
        self._persist_transition(bar.ts, transition)
        return transition

    # ========================================================================
    # 事务级状态切换
    # ========================================================================

    def _handle_transition(
        self, transition: StateTransition, bar: Any, snapshot: Any, factors: dict, signals: dict
    ) -> bool:
        """先做交易，成功了再切状态。"""
        current = self._current_state_name
        target = transition.target

        if target not in self._transition_table.get(current, ()):
            logger.error(f"非法状态迁移: {current} -> {target}")
            return False

        # --- 1. 交易操作 ---
        trade_ok = True
        if transition.action == StateAction.ENTRY:
            trade_ok = self._open_round(transition, bar)
        elif transition.action == StateAction.EXIT:
            trade_ok = self._close_round(transition, bar)

        if not trade_ok:
            self._stats["trade_failures"] += 1
            logger.warning(f"交易执行失败，状态保持为 {current}")
            return False

        # --- 2. 状态切换 ---
        with self._lock:
            self._current_state_name = target
            self._bars_in_state = 0
            self._enter_reason = transition.reason
            self._stats["transitions"] += 1

        # --- 3. 调用 on_exit / on_enter ---
        old_state = self._states[current]
        new_state = self._states[target]

        try:
            old_state.on_exit(self._build_dummy_context(bar))
        except Exception as e:
            logger.error(f"on_exit 报错: {e}")

        try:
            # 重新组装一次 context 给新状态
            new_state.on_enter(self._build_dummy_context(bar))
        except Exception as e:
            logger.error(f"on_enter 报错: {e}")

        logger.info(f"状态迁移: {current} -> {target} (原因: {transition.reason})")
        return True

    def _build_dummy_context(self, bar: Any) -> StateContext:
        """为了让 on_enter/on_exit 能跑，提供一个基础 context"""
        return StateContext(
            bar=bar, snapshot=None, factors={}, signals={}, active_round=None,
            risk_level="normal", risk_approved=True, current_state="", bars_in_state=0,
            enter_reason="", now_ms=self._clock.now_ms(), recent_bars=[], bar_phase="close"
        )

    # ========================================================================
    # 交易执行封装
    # ========================================================================

    def _open_round(self, transition: StateTransition, bar: Any) -> bool:
        try:
            res = self._round_manager.open_round(
                direction=transition.direction,
                entry_price_hint=transition.entry_price_hint,
                stop_price_hint=transition.stop_price_hint,
                signal_id=f"sig-{bar.ts}",
                bar_ts=bar.ts,
            )
            return res is not None
        except Exception as e:
            logger.error(f"开仓失败: {e}")
            return False

    def _close_round(self, transition: StateTransition, bar: Any) -> bool:
        try:
            res = self._round_manager.close_round(
                exit_reason=transition.reason,
                exit_price_hint=None
            )
            return res is not None
        except Exception as e:
            logger.error(f"平仓失败: {e}")
            return False

    def _update_stop(self, new_stop: Decimal) -> None:
        try:
            self._round_manager.update_stop(new_stop)
        except Exception as e:
            logger.warning(f"更新止损失败: {e}")

    # ========================================================================
    # 因子与信号
    # ========================================================================

    def _compute_factors(self, bar: Any, snapshot: Any) -> dict:
        try:
            return self._factor_registry.process_bar(bar, snapshot)
        except Exception as e:
            logger.error(f"因子计算异常: {e}")
            return {}

    def _compute_signals(self, bar: Any, snapshot: Any, factors: dict) -> dict:
        signals = {}
        for name, strategy in self._strategies.items():
            try:
                strategy.on_bar_close(bar)
                ctx = StrategyContext(
                    bar=bar, snapshot=snapshot, factors=factors,
                    recent_bars=[], active_round=None, current_state="", now_ms=self._clock.now_ms()
                )
                sig = strategy.compute(ctx)
                if sig: signals[name] = sig
            except Exception as e:
                logger.error(f"策略 '{name}' 异常: {e}")
        return signals

    # ========================================================================
    # 初始化与落库
    # ========================================================================

    def _initialize(self, bar: Any, snapshot: Any) -> None:
        state = self._states[self._current_state_name]
        try:
            state.on_enter(self._build_dummy_context(bar))
            self._initialized = True
            logger.info(f"状态机初始化完成，当前状态: {self._current_state_name}")
        except Exception as e:
            logger.critical(f"状态机初始化失败: {e}")

    def _persist_transition(self, bar_ts: int, transition: StateTransition) -> None:
        if not self._persister: return
        try:
            self._persister.write_state_transition(
                bar_ts=bar_ts,
                from_state=self._current_state_name,
                to_state=transition.target or self._current_state_name,
                action=transition.action.value,
                reason=transition.reason,
                trigger_score=None,
            )
        except Exception:
            pass

    @property
    def current_state(self) -> str:
        with self._lock:
            return self._current_state_name

    @property
    def bars_in_state(self) -> int:
        with self._lock:
            return self._bars_in_state

    @property
    def enter_reason(self) -> str:
        with self._lock:
            return self._enter_reason


# ============================================================================
# 模块工厂函数
# ============================================================================
MANIFEST = {
    "name": "state_machine",
    "type": "state",
    "version": "1.0.0",
    "step": 1,
    "capabilities": ["state_machine"],
    "module_dependencies": [],
    "owner": "bimon",
}

def create(config: dict) -> StateMachine:
    """占位，实际由 main.py 手动实例化注入所有依赖"""
    raise StateMachineError("StateMachine 必须通过完整参数显式实例化，禁止通过工厂无参创建。")
