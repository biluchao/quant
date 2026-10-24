"""
states/state_machine.py — 状态机总控

整个 Step 1 的神经中枢。

职责：
1. 编排每根 K 线的决策流水线（计算因子、算信号、调状态、处理转换）
2. 校验状态转换的合法性
3. 处理动作的实际执行（ENTRY 开仓 / EXIT 平仓 / HOLD 更新止损）
4. 跨状态传递 meta（逃顶时的 peak_close 等信息）
5. 持久化状态转换记录
6. 提供紧急停止接口

职责边界：
- 状态机只做编排，不生产判断（策略负责），不生产订单（round_manager 负责）
- 状态机不知道策略内部逻辑，只调 strategy.compute(context)
- 状态机不知道订单细节，只调 round_manager.open_round/close_round
- 状态机不修改 active_round，只从 round_manager 读

关键契约：
1. 重入保护：on_bar_close 不可重入，重入直接返回 None
2. 转换校验：目标必须在 transition_table 里，否则拒绝（记 invalid_transitions）
3. 交易先于状态切换：先做交易操作，成功才 on_exit + 切换
4. on_exit / on_enter 独立 try/except，异常打 ERROR 但不中断
5. 跨状态 meta 传递：transition.meta 注入到下一个状态的 context.meta
6. meta 键映射：进入 top_detect 时，若缺 trend_direction 则从 direction 补
7. bars_in_state 语义：先递增再组 context，首根 K 线 bars_in_state=1
8. 首根 K 线惰性初始化：_initialize 只调 initial_state.on_enter，不算信号
9. 策略预热：_compute_signals 逐个调 strategy.on_bar_close，再 compute
10. 异常兜底：round_manager/risk_policy/clock/buffer 全部有 _safe_* 封装
11. 持久化失败不影响状态机（_persist_transition 独立 try/except）
12. emergency_stop：先平仓，后强制切 IDLE
13. 所有统计字段齐全，健康检查聚合各状态
14. 回测/影子/实盘共用同一份代码

转换表（Step 1）：
    idle              → breakout_confirm
    breakout_confirm  → trend_ride / idle
    trend_ride        → idle / top_detect
    top_detect        → idle

动作与转换的语义对应：
    (idle, breakout_confirm) + ENTRY   → 只做状态切换，不下单
    (breakout_confirm, trend_ride) + ENTRY → 实际开仓 + 状态切换
    (breakout_confirm, idle) + EXIT    → 中止，只做状态切换（无持仓）
    (trend_ride, idle) + EXIT          → 实际平仓 + 状态切换
    (trend_ride, top_detect) + EXIT    → 实际平仓 + 状态切换 + meta 传递
    (top_detect, idle) + EXIT          → 观察结束，只做状态切换（无持仓）
    (any, any) + HOLD                  → 若有 stop_price_hint，更新止损

依赖接口（由 round_manager 提供）：
    open_round(direction, entry_price_hint, stop_price_hint,
               signal_id, bar_ts, meta) -> Optional[dict]
    close_round(exit_reason, exit_price_hint) -> Optional[dict]
    update_stop(new_stop) -> bool
    get_active_round() -> Optional[dict]

依赖接口（由 risk_policy 提供）：
    get_risk_level() -> RiskLevel

依赖接口（由 factor_registry 提供）：
    process_bar(bar, snapshot) -> Dict[str, FactorValue]

依赖接口（由 strategy 提供）：
    on_bar_close(bar) -> None
    compute(StrategyContext) -> Optional[StrategySignal]

依赖接口（由 kline_buffer 提供）：
    get_recent(n) -> List[KlineData]

依赖接口（由 persister 提供）：
    write_state_transition(bar_ts, from_state, to_state,
                           action, reason, trigger_score) -> None

依赖接口（由 clock 提供）：
    now_ms() -> int

作者：比蒙
审查：乐高（执行）、张三（系统）
版本：v1.0.0
"""

from typing import Any, Dict, List, Optional
import logging
import threading
import time

from core.state import (
    StateAction,
    StateContext,
    StateProvider,
    StateTransition,
    TransitionReason,
)
from core.strategy import (
    SignalType,
    StrategyContext,
    StrategyProvider,
)

logger = logging.getLogger(__name__)


# ============================================================================
# MANIFEST
# ============================================================================
MANIFEST = {
    "name": "state_machine",
    "type": "state",
    "version": "1.0.0",
    "step": 1,
    "owner": "bimon",
    "capabilities": ["state_machine", "state_orchestration"],
    "module_dependencies": [],
    "fallback": None,
    "enabled": True,
    "description": "State machine orchestration with transition validation and cross-state meta passing",
}


# ============================================================================
# 常量
# ============================================================================
DEFAULT_TRANSITION_TABLE = {
    "idle": ("breakout_confirm",),
    "breakout_confirm": ("trend_ride", "idle"),
    "trend_ride": ("idle", "top_detect"),
    "top_detect": ("idle",),
}

# 需要实际开仓的转换
OPEN_ROUND_TRANSITIONS = frozenset({
    ("breakout_confirm", "trend_ride"),
})

# 需要实际平仓的转换
CLOSE_ROUND_TRANSITIONS = frozenset({
    ("trend_ride", "idle"),
    ("trend_ride", "top_detect"),
})

# 跨状态 meta 键映射（target_state -> {dest_key: source_key}）
# 用于修正不同状态之间对同一概念的命名差异
META_KEY_MAPPING = {
    "top_detect": {
        "trend_direction": "direction",
    },
}

MAX_RECENT_BARS = 100


# ============================================================================
# 错误
# ============================================================================
class StateMachineError(Exception):
    """状态机错误。"""
    pass


# ============================================================================
# StateMachine
# ============================================================================
class StateMachine:
    """
    状态机总控。详见模块 docstring。

    用法：
        sm = StateMachine(
            factor_registry=registry,
            strategies={"trend_round": strategy},
            config=config,
            persister=persister,
            risk_policy=risk_policy,
            order_gateway=gateway,
            round_manager=round_mgr,
            clock=clock,
            symbol="btcusdt",
            kline_buffer=buffer,
        )

        sm.register_state("idle", IdleState(config))
        sm.register_state("breakout_confirm", BreakoutConfirmState(config))
        sm.register_state("trend_ride", TrendRideState(config))
        sm.register_state("top_detect", TopDetectState(config))
        sm.validate()

        # 每根 K 线收线
        transition = sm.on_bar_close(bar, snapshot)

        # 紧急停止
        sm.emergency_stop("manual")
    """

    name = "state_machine"
    version = "1.0.0"

    def __init__(
        self,
        factor_registry: Any = None,
        strategies: Optional[Dict[str, StrategyProvider]] = None,
        config: Optional[dict] = None,
        persister: Any = None,
        risk_policy: Any = None,
        order_gateway: Any = None,
        round_manager: Any = None,
        clock: Any = None,
        symbol: str = "btcusdt",
        kline_buffer: Any = None,
        transition_table: Optional[Dict[str, tuple]] = None,
    ):
        self._factor_registry = factor_registry
        self._strategies = dict(strategies or {})
        self._config = config or {}
        self._persister = persister
        self._risk_policy = risk_policy
        self._order_gateway = order_gateway
        self._round_manager = round_manager
        self._clock = clock
        self._symbol = symbol
        self._kline_buffer = kline_buffer
        self._transition_table = dict(
            transition_table or DEFAULT_TRANSITION_TABLE
        )

        # ---- 状态注册 ----
        self._states: Dict[str, StateProvider] = {}
        self._lock = threading.RLock()
        self._in_bar_close = False

        # ---- 状态追踪 ----
        self._current_state_name: str = "idle"
        self._bars_in_state: int = 0
        self._enter_reason: str = "system_start"
        self._initialized: bool = False

        # ---- 统计 ----
        self._stats = {
            "bar_closes": 0,
            "transitions": 0,
            "transition_failures": 0,
            "invalid_transitions": 0,
            "signals_computed": 0,
            "strategy_errors": 0,
            "open_round_attempts": 0,
            "open_round_successes": 0,
            "close_round_attempts": 0,
            "close_round_successes": 0,
            "update_stop_attempts": 0,
            "update_stop_rejections": 0,
            "emergency_stops": 0,
            "persist_failures": 0,
            "errors": 0,
        }

        logger.info(
            f"StateMachine initialized "
            f"symbol={symbol} "
            f"strategies={list(self._strategies.keys())} "
            f"transition_table={list(self._transition_table.keys())}"
        )

    # ========================================================================
    # 状态注册与校验
    # ========================================================================

    def register_state(self, name: str, state: StateProvider) -> None:
        """
        注册状态。

        契约：
        - 名称非空且唯一
        - 实例必须是 StateProvider
        """
        if not name or not isinstance(name, str):
            raise StateMachineError(f"invalid state name: {name!r}")
        if not isinstance(state, StateProvider):
            raise StateMachineError(
                f"state must be StateProvider, "
                f"got {type(state).__name__}"
            )

        with self._lock:
            if name in self._states:
                raise StateMachineError(
                    f"state '{name}' already registered"
                )
            self._states[name] = state

        logger.info(
            f"state registered: {name} v{state.version}"
        )

    def validate(self) -> None:
        """
        校验转换表。

        契约：
        - 初始状态 "idle" 必须注册
        - 转换表中所有状态必须注册
        - 所有目标必须注册
        - 不允许自转换
        """
        with self._lock:
            if "idle" not in self._states:
                raise StateMachineError(
                    "initial state 'idle' not registered"
                )

            for name in self._transition_table:
                if name not in self._states:
                    raise StateMachineError(
                        f"state '{name}' in transition_table "
                        f"but not registered"
                    )

            for src, targets in self._transition_table.items():
                for target in targets:
                    if target not in self._states:
                        raise StateMachineError(
                            f"transition '{src}' -> '{target}': "
                            f"target not registered"
                        )
                    if target == src:
                        raise StateMachineError(
                            f"self-transition '{src}' -> '{src}' "
                            f"not allowed; use NOOP/HOLD instead"
                        )

            # 注册但未在表中（警告）
            for name in self._states:
                if name not in self._transition_table:
                    logger.warning(
                        f"state '{name}' registered "
                        f"but not in transition_table"
                    )

        logger.info(
            f"StateMachine validated: {len(self._states)} states, "
            f"{sum(len(t) for t in self._transition_table.values())} "
            f"transitions"
        )

    # ========================================================================
    # 属性
    # ========================================================================

    @property
    def current_state(self) -> str:
        """当前状态名。"""
        with self._lock:
            return self._current_state_name

    @property
    def bars_in_state(self) -> int:
        """当前状态已过多少根 K 线。"""
        with self._lock:
            return self._bars_in_state

    @property
    def enter_reason(self) -> str:
        """进入当前状态的原因。"""
        with self._lock:
            return self._enter_reason

    # ========================================================================
    # 主入口
    # ========================================================================

    def on_bar_close(
        self, bar: Any, snapshot: Optional[dict] = None
    ) -> Optional[StateTransition]:
        """
        处理 K 线收线。

        契约：
        - 重入保护：不可重入，重入返回 None
        - 永不抛异常（内部 catch）
        - 首根 K 线惰性初始化

        Returns:
            StateTransition 或 None（重入/异常）
        """
        with self._lock:
            if self._in_bar_close:
                logger.warning("on_bar_close re-entry, skipping")
                return None
            self._in_bar_close = True

        try:
            return self._on_bar_close_impl(bar, snapshot)
        except Exception as e:
            logger.error(
                f"on_bar_close unhandled error: {e}",
                exc_info=True,
            )
            with self._lock:
                self._stats["errors"] += 1
            return None
        finally:
            with self._lock:
                self._in_bar_close = False

    def _on_bar_close_impl(
        self, bar: Any, snapshot: Optional[dict]
    ) -> Optional[StateTransition]:
        """on_bar_close 的实际实现。"""
        with self._lock:
            self._stats["bar_closes"] += 1
            if not self._initialized:
                self._initialize(bar, snapshot)

        # ---- 1. 计算因子 ----
        factors = self._compute_factors(bar, snapshot)

        # ---- 2. 计算信号（每根 K 线只调一次 strategy.on_bar_close）----
        signals = self._compute_signals(bar, snapshot, factors)

        # ---- 3. 组装 context ----
        with self._lock:
            self._bars_in_state += 1
            bars_in_state = self._bars_in_state
            current_state = self._current_state_name
            enter_reason = self._enter_reason

        active_round = self._safe_get_active_round()
        risk_level, risk_approved = self._safe_get_risk()
        recent_bars = self._get_recent_bars(bar)

        context = StateContext(
            bar=bar,
            snapshot=snapshot,
            factors=factors,
            signals=signals,
            active_round=active_round,
            risk_level=risk_level,
            risk_approved=risk_approved,
            current_state=current_state,
            bars_in_state=bars_in_state,
            enter_reason=enter_reason,
            now_ms=self._safe_now_ms(),
            recent_bars=recent_bars,
            bar_phase="close",
            meta={},
        )

        # ---- 4. 调用当前状态 ----
        state = self._states.get(current_state)
        if state is None:
            logger.error(
                f"state '{current_state}' not registered"
            )
            with self._lock:
                self._stats["errors"] += 1
            return None

        try:
            transition = state.on_bar_close(context)
        except Exception as e:
            logger.error(
                f"state '{current_state}'.on_bar_close error: {e}",
                exc_info=True,
            )
            with self._lock:
                self._stats["errors"] += 1
            return None

        if transition is None:
            logger.warning(
                f"state '{current_state}' returned None transition"
            )
            return None

        # ---- 5. 持久化转换 ----
        self._persist_transition(bar, transition)

        # ---- 6. 处理转换 ----
        if transition.target is None:
            # HOLD / NOOP
            if (
                transition.action == StateAction.HOLD
                and transition.stop_price_hint is not None
            ):
                self._update_stop(transition)
            return transition

        # 有目标 → 走转换流程
        self._handle_transition(
            transition, bar, snapshot,
            factors=factors,
            signals=signals,
            active_round=active_round,
            risk_level=risk_level,
            risk_approved=risk_approved,
            recent_bars=recent_bars,
        )
        return transition

    # ========================================================================
    # 初始化
    # ========================================================================

    def _initialize(self, bar: Any, snapshot: Optional[dict]) -> None:
        """
        首根 K 线惰性初始化。

        契约：
        - 只调 initial_state.on_enter，不计算因子/信号
        - 保证 strategy.on_bar_close 不被重复调用
        - 异常打 ERROR，不致命
        """
        if self._current_state_name not in self._states:
            raise StateMachineError(
                f"initial state '{self._current_state_name}' "
                f"not registered"
            )

        context = StateContext(
            bar=bar,
            snapshot=snapshot,
            factors={},
            signals={},
            active_round=None,
            risk_level="normal",
            risk_approved=True,
            current_state=self._current_state_name,
            bars_in_state=0,
            enter_reason=self._enter_reason,
            now_ms=self._safe_now_ms(),
            recent_bars=self._get_recent_bars(bar),
            bar_phase="close",
            meta={},
        )

        try:
            self._states[self._current_state_name].on_enter(context)
            logger.info(
                f"state machine initialized at "
                f"'{self._current_state_name}'"
            )
        except Exception as e:
            logger.error(
                f"initial on_enter failed: {e}", exc_info=True
            )
            with self._lock:
                self._stats["errors"] += 1
            # 不致命，状态机会继续

        self._initialized = True

    # ========================================================================
    # 转换处理
    # ========================================================================

    def _handle_transition(
        self,
        transition: StateTransition,
        bar: Any,
        snapshot: Optional[dict],
        **kwargs,
    ) -> bool:
        """
        处理状态转换。

        契约：
        - 转换必须先校验（在 transition_table 中）
        - 交易操作先于状态切换（成功才 on_exit + 切换）
        - on_exit / on_enter 独立 try/except
        - 交易失败 → 状态保持原样
        """
        target = transition.target
        current = self._current_state_name

        # ---- 1. 校验转换 ----
        if not self._validate_transition(current, target):
            with self._lock:
                self._stats["invalid_transitions"] += 1
            return False

        # ---- 2. 执行交易操作（先于状态切换）----
        trade_ok = True
        if (current, target) in OPEN_ROUND_TRANSITIONS:
            trade_ok = self._open_round(
                transition, bar, snapshot, **kwargs
            )
        elif (current, target) in CLOSE_ROUND_TRANSITIONS:
            trade_ok = self._close_round(transition, bar)
        # 其他 (idle, breakout_confirm) / (breakout_confirm, idle) /
        # (top_detect, idle) 只做状态切换，无交易操作

        if not trade_ok:
            with self._lock:
                self._stats["transition_failures"] += 1
            logger.warning(
                f"trade operation failed for "
                f"{current} -> {target}, staying in {current}"
            )
            return False

        # ---- 3. on_exit ----
        old_state = self._states[current]
        new_state = self._states[target]

        context_for_exit = self._build_context_for_exit(
            bar, snapshot, **kwargs
        )
        try:
            old_state.on_exit(context_for_exit)
        except Exception as e:
            logger.error(
                f"on_exit error for '{current}': {e}", exc_info=True
            )
            with self._lock:
                self._stats["errors"] += 1

        # ---- 4. 切换状态 ----
        with self._lock:
            self._current_state_name = target
            self._bars_in_state = 0
            self._enter_reason = transition.reason
            self._stats["transitions"] += 1

        # ---- 5. on_enter ----
        context_for_enter = self._build_context_for_enter(
            bar, snapshot, target, transition, **kwargs
        )
        try:
            new_state.on_enter(context_for_enter)
        except Exception as e:
            logger.error(
                f"on_enter error for '{target}': {e}", exc_info=True
            )
            with self._lock:
                self._stats["errors"] += 1

        logger.info(
            f"transition {current} -> {target} "
            f"reason={transition.reason} "
            f"action={transition.action.value}"
        )
        return True

    def _validate_transition(
        self, current: str, target: str
    ) -> bool:
        """校验转换是否在表里。"""
        allowed = self._transition_table.get(current, ())
        if target not in allowed:
            logger.error(
                f"invalid transition: {current} -> {target} "
                f"(allowed: {allowed})"
            )
            return False
        return True

    # ========================================================================
    # Context 构造
    # ========================================================================

    def _build_context_for_exit(
        self, bar: Any, snapshot: Optional[dict], **kwargs
    ) -> StateContext:
        """构造 on_exit 用的 context（状态还在旧状态）。"""
        with self._lock:
            current_state = self._current_state_name
            bars_in_state = self._bars_in_state
            enter_reason = self._enter_reason

        return StateContext(
            bar=bar,
            snapshot=snapshot,
            factors=kwargs.get("factors", {}),
            signals=kwargs.get("signals", {}),
            active_round=kwargs.get("active_round"),
            risk_level=kwargs.get("risk_level", "normal"),
            risk_approved=kwargs.get("risk_approved", True),
            current_state=current_state,
            bars_in_state=bars_in_state,
            enter_reason=enter_reason,
            now_ms=self._safe_now_ms(),
            recent_bars=kwargs.get("recent_bars", []),
            bar_phase="close",
            meta={},
        )

    def _build_context_for_enter(
        self,
        bar: Any,
        snapshot: Optional[dict],
        target: str,
        transition: StateTransition,
        **kwargs,
    ) -> StateContext:
        """
        构造 on_enter 用的 context（状态已切换到 target）。

        契约：
        - 重新获取 active_round（可能因交易操作而变化）
        - transition.meta 注入到 context.meta
        - 应用 META_KEY_MAPPING（top_detect 期望 trend_direction，
          但 trend_ride 用 direction）
        """
        meta = dict(transition.meta or {})

        # 跨状态键映射
        key_map = META_KEY_MAPPING.get(target, {})
        for dest_key, src_key in key_map.items():
            if dest_key not in meta and src_key in meta:
                meta[dest_key] = meta[src_key]

        return StateContext(
            bar=bar,
            snapshot=snapshot,
            factors=kwargs.get("factors", {}),
            signals=kwargs.get("signals", {}),
            active_round=self._safe_get_active_round(),
            risk_level=kwargs.get("risk_level", "normal"),
            risk_approved=kwargs.get("risk_approved", True),
            current_state=target,
            bars_in_state=0,
            enter_reason=transition.reason,
            now_ms=self._safe_now_ms(),
            recent_bars=kwargs.get("recent_bars", []),
            bar_phase="close",
            meta=meta,
        )

    # ========================================================================
    # 因子与信号
    # ========================================================================

    def _compute_factors(
        self, bar: Any, snapshot: Optional[dict]
    ) -> Dict[str, Any]:
        """计算所有因子。异常兜底返回空 dict。"""
        if self._factor_registry is None:
            return {}
        try:
            return self._factor_registry.process_bar(bar, snapshot)
        except Exception as e:
            logger.error(
                f"factor compute failed: {e}", exc_info=True
            )
            with self._lock:
                self._stats["errors"] += 1
            return {}

    def _compute_signals(
        self,
        bar: Any,
        snapshot: Optional[dict],
        factors: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        计算所有策略信号。

        契约：
        - 每个策略独立 try/except，互不影响
        - 每个策略先 on_bar_close（推进预热），再 compute
        - 异常策略跳过，不影响其他策略
        """
        if not self._strategies:
            return {}

        signals: Dict[str, Any] = {}
        active_round = self._safe_get_active_round()
        recent_bars = self._get_recent_bars(bar)
        now_ms = self._safe_now_ms()

        with self._lock:
            current_state = self._current_state_name

        for name, strategy in self._strategies.items():
            try:
                # 预热计数（每根 K 线一次）
                strategy.on_bar_close(bar)

                sctx = StrategyContext(
                    bar=bar,
                    snapshot=snapshot,
                    factors=factors,
                    recent_bars=recent_bars,
                    active_round=active_round,
                    current_state=current_state,
                    now_ms=now_ms,
                )
                sig = strategy.compute(sctx)
                if sig is not None:
                    signals[name] = sig
                    with self._lock:
                        self._stats["signals_computed"] += 1
            except Exception as e:
                logger.error(
                    f"strategy '{name}' failed: {e}",
                    exc_info=True,
                )
                with self._lock:
                    self._stats["strategy_errors"] += 1

        return signals

    # ========================================================================
    # 交易操作
    # ========================================================================

    def _open_round(
        self,
        transition: StateTransition,
        bar: Any,
        snapshot: Optional[dict],
        **kwargs,
    ) -> bool:
        """开仓。委托给 round_manager。"""
        with self._lock:
            self._stats["open_round_attempts"] += 1

        if self._round_manager is None:
            logger.error(
                "round_manager is None, cannot open round"
            )
            return False

        try:
            result = self._round_manager.open_round(
                direction=transition.direction,
                entry_price_hint=transition.entry_price_hint,
                stop_price_hint=transition.stop_price_hint,
                signal_id=(transition.meta or {}).get("signal_id"),
                bar_ts=int(bar.ts),
                meta=transition.meta or {},
            )
            if result is None:
                logger.warning(
                    "round_manager.open_round returned None (rejected)"
                )
                return False
            with self._lock:
                self._stats["open_round_successes"] += 1
            return True
        except Exception as e:
            logger.error(
                f"open_round failed: {e}", exc_info=True
            )
            with self._lock:
                self._stats["errors"] += 1
            return False

    def _close_round(
        self,
        transition: StateTransition,
        bar: Any,
    ) -> bool:
        """平仓。委托给 round_manager。"""
        with self._lock:
            self._stats["close_round_attempts"] += 1

        if self._round_manager is None:
            logger.error(
                "round_manager is None, cannot close round"
            )
            return False

        try:
            result = self._round_manager.close_round(
                exit_reason=transition.reason,
                exit_price_hint=None,  # 由 round_manager 决定
            )
            if result is None:
                logger.warning(
                    "round_manager.close_round returned None "
                    "(no active round or close failed)"
                )
                return False
            with self._lock:
                self._stats["close_round_successes"] += 1
            return True
        except Exception as e:
            logger.error(
                f"close_round failed: {e}", exc_info=True
            )
            with self._lock:
                self._stats["errors"] += 1
            return False

    def _update_stop(
        self, transition: StateTransition
    ) -> bool:
        """
        更新止损。

        契约：
        - 只朝有利方向移动（由 round_manager 强制执行）
        - new_stop 必须严格优于旧止损
        """
        with self._lock:
            self._stats["update_stop_attempts"] += 1

        if self._round_manager is None:
            return False

        try:
            ok = self._round_manager.update_stop(
                new_stop=transition.stop_price_hint
            )
            if not ok:
                with self._lock:
                    self._stats["update_stop_rejections"] += 1
                logger.warning(
                    f"update_stop rejected: "
                    f"new_stop={transition.stop_price_hint}"
                )
            return bool(ok)
        except Exception as e:
            logger.error(
                f"update_stop failed: {e}", exc_info=True
            )
            return False

    # ========================================================================
    # 持久化
    # ========================================================================

    def _persist_transition(
        self, bar: Any, transition: StateTransition
    ) -> None:
        """
        持久化状态转换。

        契约：
        - 失败不影响状态机
        - trigger_score 从 transition.meta 取（可能为 None）
        """
        if self._persister is None:
            return
        try:
            with self._lock:
                from_state = self._current_state_name
            to_state = transition.target or from_state
            trigger_score = None
            if transition.meta:
                trigger_score = transition.meta.get("trigger_score")

            self._persister.write_state_transition(
                bar_ts=int(bar.ts),
                from_state=from_state,
                to_state=to_state,
                action=transition.action.value,
                reason=transition.reason,
                trigger_score=trigger_score,
            )
        except Exception as e:
            logger.error(
                f"persist transition failed: {e}", exc_info=True
            )
            with self._lock:
                self._stats["persist_failures"] += 1

    # ========================================================================
    # 辅助方法
    # ========================================================================

    def _get_recent_bars(self, bar: Any) -> List[Any]:
        """
        获取最近 K 线。

        契约：
        - buffer 为 None 返回 [bar]
        - buffer 抛异常返回 [bar]
        """
        if self._kline_buffer is None:
            return [bar]
        try:
            recent = self._kline_buffer.get_recent(MAX_RECENT_BARS)
            return recent if recent else [bar]
        except Exception as e:
            logger.error(
                f"get_recent_bars failed: {e}", exc_info=True
            )
            return [bar]

    def _safe_get_active_round(self) -> Optional[dict]:
        """安全获取活跃轮次。"""
        if self._round_manager is None:
            return None
        try:
            return self._round_manager.get_active_round()
        except Exception as e:
            logger.error(
                f"get_active_round failed: {e}", exc_info=True
            )
            return None

    def _safe_get_risk(self) -> tuple:
        """
        安全获取风控等级和批准状态。

        契约：
        - risk_level = "normal"/"warn"/"reduce"/"close_only"/"halt"
        - risk_approved = risk_level not in {halt, close_only}
        - 异常时返回 ("unknown", False)
        """
        if self._risk_policy is None:
            return "normal", True
        try:
            level = self._risk_policy.get_risk_level()
            level_str = (
                level.value if hasattr(level, "value") else str(level)
            )
            approved = level_str not in ("halt", "close_only")
            return level_str, approved
        except Exception as e:
            logger.error(
                f"get_risk_level failed: {e}", exc_info=True
            )
            return "unknown", False

    def _safe_now_ms(self) -> int:
        """安全获取当前毫秒时间戳。"""
        if self._clock is None:
            return int(time.time() * 1000)
        try:
            return self._clock.now_ms()
        except Exception as e:
            logger.error(
                f"clock.now_ms failed: {e}", exc_info=True
            )
            return int(time.time() * 1000)

    # ========================================================================
    # 紧急停止
    # ========================================================================

    def emergency_stop(self, reason: str = "manual") -> bool:
        """
        紧急停止。

        契约：
        - 先平仓（尽力），再强制切 IDLE
        - 幂等（多次触发）
        - 统计 emergency_stops
        """
        with self._lock:
            self._stats["emergency_stops"] += 1

        logger.critical(
            f"emergency_stop triggered: reason={reason}"
        )

        # 1. 尝试通过 round_manager 平仓
        try:
            if self._round_manager is not None:
                self._round_manager.close_round(
                    exit_reason=f"emergency:{reason}",
                    exit_price_hint=None,
                )
        except Exception as e:
            logger.error(
                f"emergency close_round failed: {e}",
                exc_info=True,
            )

        # 2. 尝试通过 gateway 平所有仓
        try:
            if self._order_gateway is not None:
                self._order_gateway.close_all_positions(
                    reason=reason
                )
        except Exception as e:
            logger.error(
                f"emergency close_all failed: {e}",
                exc_info=True,
            )

        # 3. 强制切换状态到 IDLE
        with self._lock:
            old = self._current_state_name
            self._current_state_name = "idle"
            self._bars_in_state = 0
            self._enter_reason = f"emergency:{reason}"
        logger.critical(
            f"state forced {old} -> idle (emergency)"
        )

        return True

    # ========================================================================
    # 健康检查与重置
    # ========================================================================

    def health(self) -> Dict[str, Any]:
        """健康检查。聚合各状态的 health。"""
        with self._lock:
            states_health = {}
            for name, state in self._states.items():
                try:
                    states_health[name] = state.health()
                except Exception as e:
                    states_health[name] = {
                        "healthy": False,
                        "error": str(e),
                    }

            return {
                "healthy": len(self._states) > 0,
                "name": self.name,
                "version": self.version,
                "current_state": self._current_state_name,
                "bars_in_state": self._bars_in_state,
                "enter_reason": self._enter_reason,
                "registered_states": list(self._states.keys()),
                "registered_strategies": list(self._strategies.keys()),
                "stats": dict(self._stats),
                "states_health": states_health,
                "timestamp": int(time.time() * 1000),
            }

    def reset(self) -> None:
        """重置状态机。测试用。"""
        with self._lock:
            self._current_state_name = "idle"
            self._bars_in_state = 0
            self._enter_reason = "reset"
            self._initialized = False
            for k in self._stats:
                self._stats[k] = 0
            # 重置各状态
            for state in self._states.values():
                try:
                    state.reset()
                except Exception as e:
                    logger.error(
                        f"state reset failed: {e}", exc_info=True
                    )
        logger.info("StateMachine reset")

    def get_stats(self) -> Dict[str, Any]:
        """返回统计快照。"""
        with self._lock:
            return dict(self._stats)


# ============================================================================
# 工厂函数
# ============================================================================
def create(config: dict) -> StateMachine:
    """
    模块工厂入口。

    注意：状态机需要显式注入所有依赖。create() 只用于 ModuleRegistry
    兼容性，实际使用中由 main.py 直接构造。

    config 需要：
    - instances.factor_registry
    - instances.strategies (dict)
    - instances.persister
    - instances.risk_policy
    - instances.gateway
    - instances.round_manager
    - instances.clock
    - instances.kline_buffer
    """
    instances = config.get("instances", {})
    return StateMachine(
        factor_registry=instances.get("factor_registry"),
        strategies=instances.get("strategies"),
        config=config,
        persister=instances.get("persister"),
        risk_policy=instances.get("risk_policy"),
        order_gateway=instances.get("gateway"),
        round_manager=instances.get("round_manager"),
        clock=instances.get("clock"),
        symbol=config.get("symbol", "btcusdt"),
        kline_buffer=instances.get("kline_buffer"),
    )
