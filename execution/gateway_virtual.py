"""
execution/gateway_virtual.py — 虚拟券商网关（回测 + 影子模式）

回测和影子模式的"银行"。

职责（唯一）：
1. 接收 Order，模拟成交，返回 OrderResult
2. 维护虚拟持仓、虚拟余额、虚拟账户
3. 模拟市价单/限价单/止损单的成交
4. 幂等缓存（避免重复下单）
5. 支持两种数据源：LiveSnapshotFeed（影子）/ HistoricalFeed（回测）

不做什么：
- 不做风控决策（RoundManager 负责）
- 不做策略判断（StateMachine 负责）
- 不做轮次管理（RoundManager 负责）
- 不持久化（RoundManager 负责）
- 不触达真实交易所

职责边界：
- VirtualGateway：模拟成交 + 虚拟持仓/余额管理
- BinanceGateway：真实下单
- 两者接口完全一致（ExecutionGateway 抽象）

关键契约（"四保险"）：
1. 幂等缓存：_recent_orders[client_order_id] 命中直接返回
2. 与实盘一致的接口：Order / OrderResult / Position / AccountState 完全同结构
3. 悲观成交模型：滑点高估、手续费不省、部分成交更严格
4. synthetic=True 标记：所有返回的 OrderResult 都标记为虚拟

数据源（MarketFeed 接口）：
- LiveSnapshotFeed：影子模式，从 KlineAuthority 获取最新盘口
- HistoricalFeed：回测模式，从 tick_db 或 K 线合成盘口

成交模型（三层）：
1. 市价单：逐档吃对手盘，每档最多 30%，深度加权平均价，可能部分成交
2. 限价单：
   - 价格穿越（买单 price >= best_ask，卖单 price <= best_bid）→ 立即成交
   - 未穿越 → 返回 NEW，executed_qty=0
3. 止损单（STOP_MARKET）：
   - 检查 stop_price 是否触发
   - 触发后按市价（吃对手盘）

虚拟账户状态：
- _wallet_balance：初始资金 + 已实现盈亏 - 手续费
- _positions：虚拟持仓 dict
- unrealized_pnl：按当前盘口计算
- margin_ratio：Step 1 不计算（返回 0）

与 BinanceGateway 的差异（仅实现内部）：
- 无网络延迟（除影子模式的 latency_ms 模拟）
- 无 API 错误（除模拟的 reject）
- 无速率限制
- 无 server-side stop 实际挂单（虚拟记录）

与实盘的一致性（必须严格保持）：
- Order 结构
- OrderResult 结构
- Position 结构
- AccountState 结构
- OrderStatus 枚举语义
- client_order_id 幂等

为什么"虚拟必须比实盘更悲观"：
- 实盘可能以更好价格成交（maker 返佣、深度充沛）
- 虚拟必须假设它成交得更差（吃对手盘、滑点更大）
- 回测偏乐观 = 骗自己 = 实盘必亏
- 回测偏保守 = 稳中求胜 = 实盘有可能更好

作者：乐高
审查：比蒙（策略）、张三（系统）
版本：v1.0.0
"""

from collections import OrderedDict
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
import logging
import threading
import time
from typing import Any, Callable, Dict, List, Optional

from core.gateway import (
    AccountState,
    ExecutionGateway,
    Fill,
    InvalidOrderError,
    Order,
    OrderRejectedError,
    OrderResult,
    OrderSide,
    OrderStatus,
    OrderType,
    Position,
    UnknownOrderError,
    WorkingType,
)

logger = logging.getLogger(__name__)


# ============================================================================
# MANIFEST
# ============================================================================
MANIFEST = {
    "name": "gateway_virtual",
    "type": "gateway",
    "version": "1.0.0",
    "step": 1,
    "owner": "lego",
    "exchange": "virtual",
    "capabilities": [
        "place_order", "cancel_order", "close_position",
        "server_side_stop", "set_leverage", "get_positions",
    ],
    "module_dependencies": [],
    "fallback": None,
    "enabled": True,
    "description": "Virtual execution gateway for backtest and shadow mode with pessimistic fill model",
}


# ============================================================================
# 常量
# ============================================================================
DEFAULT_ORDER_CACHE_SIZE = 1000
DEFAULT_FEE_RATE = Decimal("0.0004")     # taker 0.04%
DEFAULT_LATENCY_MS = 0                    # 回测无延迟
DEFAULT_SHADOW_LATENCY_MS = 80            # 影子模式模拟延迟
DEFAULT_MAX_LEVEL_PCT = Decimal("0.3")    # 每档最多吃 30%
DEFAULT_SNAPSHOT_MAX_AGE_MS = 500         # 快照超过 500ms 标记 stale
DEFAULT_INITIAL_BALANCE = Decimal("10000")


# ============================================================================
# 虚拟持仓（内部）
# ============================================================================
@dataclass
class _VirtualPosition:
    """虚拟持仓。内部使用，对外返回 core.gateway.Position。"""
    symbol: str
    side: OrderSide
    qty: Decimal
    entry_price: Decimal

    def to_position(
        self, mark_price: Decimal, leverage: int = 1
    ) -> Position:
        """转换为对外的 Position 结构。"""
        unrealized = (
            (mark_price - self.entry_price) * self.qty
            if self.side == OrderSide.BUY
            else (self.entry_price - mark_price) * self.qty
        )
        return Position(
            symbol=self.symbol,
            side=self.side,
            qty=self.qty,
            entry_price=self.entry_price,
            mark_price=mark_price,
            unrealized_pnl=unrealized,
            liquidation_price=None,  # 虚拟无爆仓
            margin=Decimal("0"),
            leverage=leverage,
            update_time=int(time.time() * 1000),
        )


# ============================================================================
# 数据源接口
# ============================================================================
class MarketFeed:
    """
    虚拟券商的市场数据源。

    契约：
    - get_snapshot_at(ts) -> dict 或 None
    - dict 格式：{"ts": int, "bids": [(Decimal, Decimal), ...],
                  "asks": [(Decimal, Decimal), ...]}
    - bids 降序（最优价在前），asks 升序（最优价在前）
    - 返回 None 表示无数据（触发拒单）
    """

    def get_snapshot_at(self, ts: int) -> Optional[dict]:
        raise NotImplementedError


# ============================================================================
# VirtualGateway
# ============================================================================
class VirtualGateway(ExecutionGateway):
    """
    虚拟券商网关。详见模块 docstring。

    用法：
        # 回测
        from execution.gateway_virtual import VirtualGateway, HistoricalFeed

        feed = HistoricalFeed(tick_db)
        gateway = VirtualGateway(
            feed=feed,
            clock=clock,
            id_generator=id_gen,
            symbol="BTCUSDT",
            config=config,
            fee_rate=Decimal("0.0004"),
            latency_ms=0,  # 回测无延迟
            initial_balance=Decimal("10000"),
        )

        # 影子模式
        feed = LiveSnapshotFeed(kline_authority)
        gateway = VirtualGateway(
            feed=feed,
            ...
            latency_ms=80,  # 模拟延迟
        )
    """

    exchange_name = "virtual"
    version = "1.0.0"

    def __init__(
        self,
        feed: MarketFeed,
        clock: Any,
        id_generator: Any,
        symbol: str,
        config: Optional[dict] = None,
        fee_rate: Decimal = DEFAULT_FEE_RATE,
        latency_ms: int = DEFAULT_LATENCY_MS,
        initial_balance: Decimal = DEFAULT_INITIAL_BALANCE,
        order_cache_size: int = DEFAULT_ORDER_CACHE_SIZE,
        leverage: int = 1,
    ):
        super().__init__(config or {})

        if feed is None:
            raise ValueError("feed is required")
        if clock is None:
            raise ValueError("clock is required")
        if not symbol:
            raise ValueError("symbol is required")
        if fee_rate < 0:
            raise ValueError(f"fee_rate must be >= 0, got {fee_rate}")
        if latency_ms < 0:
            raise ValueError(
                f"latency_ms must be >= 0, got {latency_ms}"
            )
        if initial_balance <= 0:
            raise ValueError(
                f"initial_balance must be > 0, got {initial_balance}"
            )

        self._feed = feed
        self._clock = clock
        self._id_gen = id_generator
        self._symbol = symbol.upper()
        self._fee_rate = Decimal(str(fee_rate))
        self._latency_ms = int(latency_ms)
        self._cache_size = int(order_cache_size)
        self._leverage = int(leverage)

        # ---- 账户 ----
        self._initial_balance = Decimal(str(initial_balance))
        self._wallet_balance = self._initial_balance
        self._realized_pnl = Decimal("0")
        self._total_fees = Decimal("0")

        # ---- 持仓 ----
        self._positions: Dict[str, _VirtualPosition] = {}

        # ---- 幂等缓存 ----
        self._recent_orders: "OrderedDict[str, OrderResult]" = OrderedDict()

        # ---- 止损单（虚拟待触发）----
        # client_order_id -> {order: Order, stop_price: Decimal}
        self._stop_orders: Dict[str, Dict[str, Any]] = {}

        # ---- 线程锁 ----
        self._lock = threading.RLock()

        # ---- 统计 ----
        self._stats = {
            "place_attempts": 0,
            "place_successes": 0,
            "place_partial_fills": 0,
            "place_rejections": 0,
            "place_duplicates": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "cache_evictions": 0,
            "cancel_attempts": 0,
            "cancel_successes": 0,
            "close_position_attempts": 0,
            "close_position_successes": 0,
            "server_stop_attempts": 0,
            "server_stop_successes": 0,
            "stop_triggers": 0,
            "fills_total": 0,
            "errors": 0,
        }
        self._last_error: Optional[str] = None

        logger.info(
            f"VirtualGateway initialized "
            f"symbol={self._symbol} "
            f"fee_rate={self._fee_rate} "
            f"latency_ms={self._latency_ms} "
            f"initial_balance={self._initial_balance} "
            f"leverage={self._leverage}"
        )

    # ========================================================================
    # place_order
    # ========================================================================

    def place_order(self, order: Order) -> OrderResult:
        """
        模拟下单。

        契约：
        - 幂等：同一 client_order_id 返回缓存
        - 快照缺失/过期：抛 OrderRejectedError
        - 空盘口：抛 OrderRejectedError
        - 盘口不足：部分成交
        - 延迟模拟：latency_ms > 0 时 sleep

        Raises：
            OrderRejectedError：快照缺失/空盘口/盘口过期
            InvalidOrderError：qty <= 0
        """
        # 1. 幂等缓存查
        with self._lock:
            cached = self._recent_orders.get(order.client_order_id)
            if cached is not None:
                self._stats["cache_hits"] += 1
                self._stats["place_duplicates"] += 1
                self._recent_orders.move_to_end(
                    order.client_order_id
                )
                logger.debug(
                    f"VirtualGateway cache hit: "
                    f"client_order_id={order.client_order_id}"
                )
                return cached
            self._stats["cache_misses"] += 1
            self._stats["place_attempts"] += 1

        # 2. 基础校验
        if order.qty <= 0:
            self._stats["place_rejections"] += 1
            raise InvalidOrderError(
                f"qty must be > 0, got {order.qty}"
            )

        # 3. 延迟模拟
        if self._latency_ms > 0:
            time.sleep(self._latency_ms / 1000.0)

        # 4. 获取快照
        try:
            now_ms = self._clock.now_ms()
        except Exception:
            now_ms = int(time.time() * 1000)

        try:
            snapshot = self._feed.get_snapshot_at(now_ms)
        except Exception as e:
            self._stats["errors"] += 1
            self._stats["place_rejections"] += 1
            self._last_error = str(e)
            raise OrderRejectedError(
                f"feed.get_snapshot_at failed: {e}"
            ) from e

        if snapshot is None:
            self._stats["place_rejections"] += 1
            raise OrderRejectedError(
                "no_market_data: feed returned None"
            )

        snapshot_ts = snapshot.get("ts")
        if snapshot_ts is None:
            self._stats["place_rejections"] += 1
            raise OrderRejectedError(
                "no_market_data: snapshot missing ts"
            )

        if abs(snapshot_ts - now_ms) > DEFAULT_SNAPSHOT_MAX_AGE_MS:
            self._stats["place_rejections"] += 1
            raise OrderRejectedError(
                f"stale_market_data: snapshot ts={snapshot_ts} "
                f"now={now_ms}"
            )

        bids = snapshot.get("bids") or []
        asks = snapshot.get("asks") or []
        if not bids or not asks:
            self._stats["place_rejections"] += 1
            raise OrderRejectedError("empty_orderbook")

        # 5. 按订单类型成交
        try:
            if order.type == OrderType.MARKET:
                result = self._fill_market(order, bids, asks, now_ms)
            elif order.type == OrderType.LIMIT:
                result = self._fill_limit(order, bids, asks, now_ms)
            elif order.type == OrderType.STOP_MARKET:
                # 止损单：记录到 _stop_orders，等待触发
                result = self._place_stop_order(order, now_ms)
            else:
                self._stats["place_rejections"] += 1
                raise InvalidOrderError(
                    f"unsupported order type in virtual: {order.type}"
                )
        except (InvalidOrderError, OrderRejectedError):
            self._stats["place_rejections"] += 1
            raise
        except Exception as e:
            self._stats["errors"] += 1
            self._stats["place_rejections"] += 1
            self._last_error = str(e)
            raise OrderRejectedError(
                f"fill simulation failed: {e}"
            ) from e

        # 6. 缓存
        with self._lock:
            self._cache_order_locked(result)
            self._stats["place_successes"] += 1
            if result.status == OrderStatus.PARTIALLY_FILLED:
                self._stats["place_partial_fills"] += 1

        return result

    # ========================================================================
    # 市价单成交
    # ========================================================================

    def _fill_market(
        self,
        order: Order,
        bids: list,
        asks: list,
        now_ms: int,
    ) -> OrderResult:
        """
        市价单成交：吃对手盘。

        契约：
        - 多单买盘（BUY）吃 asks（卖盘）
        - 空单卖盘（SELL）吃 bids（买盘）
        - 每档最多吃 30%
        - 可能部分成交（盘口不足）
        """
        # 选择对手盘
        if order.side == OrderSide.BUY:
            levels = asks  # 买单吃卖盘
        else:
            levels = bids  # 卖单吃买盘

        if not levels:
            raise OrderRejectedError("empty_opposite_side")

        filled_qty = Decimal("0")
        filled_notional = Decimal("0")
        fills: List[Fill] = []
        remaining = order.qty

        for i, level in enumerate(levels):
            if remaining <= 0:
                break
            try:
                price = Decimal(str(level[0]))
                level_qty = Decimal(str(level[1]))
            except (IndexError, InvalidOperation, ValueError):
                continue

            if price <= 0 or level_qty <= 0:
                continue

            # 每档最多吃 30%
            max_qty_from_level = level_qty * DEFAULT_MAX_LEVEL_PCT
            fill_qty = min(remaining, max_qty_from_level)

            if fill_qty <= 0:
                continue

            filled_qty += fill_qty
            filled_notional += fill_qty * price
            remaining -= fill_qty

            # 构造 Fill
            fee = fill_qty * price * self._fee_rate
            fills.append(Fill(
                trade_id=f"vt-{order.client_order_id}-{i}",
                price=price,
                qty=fill_qty,
                fee=fee,
                fee_asset="USDT",
                is_maker=False,
                ts=now_ms,
            ))

        if filled_qty <= 0:
            raise OrderRejectedError(
                "no_fills: unable to fill any quantity"
            )

        avg_price = filled_notional / filled_qty
        status = (
            OrderStatus.FILLED
            if filled_qty >= order.qty
            else OrderStatus.PARTIALLY_FILLED
        )

        # 更新虚拟账户
        total_fee = sum(f.fee for f in fills)
        self._apply_fill(
            order=order,
            fill_qty=filled_qty,
            fill_price=avg_price,
            total_fee=total_fee,
        )

        return self._build_result(
            order=order,
            status=status,
            executed_qty=filled_qty,
            avg_price=avg_price,
            fills=fills,
            total_fee=total_fee,
            now_ms=now_ms,
        )

    # ========================================================================
    # 限价单成交
    # ========================================================================

    def _fill_limit(
        self,
        order: Order,
        bids: list,
        asks: list,
        now_ms: int,
    ) -> OrderResult:
        """
        限价单成交。

        契约：
        - 买单：order.price >= best_ask 时立即成交
        - 卖单：order.price <= best_bid 时立即成交
        - 否则返回 NEW（排队）
        """
        if order.price is None or order.price <= 0:
            raise InvalidOrderError(
                "LIMIT order requires positive price"
            )

        best_bid = self._best_price(bids)
        best_ask = self._best_price(asks)

        if order.side == OrderSide.BUY:
            can_fill = best_ask is not None and order.price >= best_ask
            fill_price = best_ask if can_fill else None
        else:
            can_fill = best_bid is not None and order.price <= best_bid
            fill_price = best_bid if can_fill else None

        if not can_fill:
            # 排队等待
            return self._build_result(
                order=order,
                status=OrderStatus.NEW,
                executed_qty=Decimal("0"),
                avg_price=None,
                fills=[],
                total_fee=Decimal("0"),
                now_ms=now_ms,
            )

        # 立即成交
        fill_qty = order.qty
        fee = fill_qty * fill_price * self._fee_rate
        fill = Fill(
            trade_id=f"vt-{order.client_order_id}-0",
            price=fill_price,
            qty=fill_qty,
            fee=fee,
            fee_asset="USDT",
            is_maker=False,  # 保守，视为 taker
            ts=now_ms,
        )

        self._apply_fill(
            order=order,
            fill_qty=fill_qty,
            fill_price=fill_price,
            total_fee=fee,
        )

        return self._build_result(
            order=order,
            status=OrderStatus.FILLED,
            executed_qty=fill_qty,
            avg_price=fill_price,
            fills=[fill],
            total_fee=fee,
            now_ms=now_ms,
        )

    # ========================================================================
    # 止损单（虚拟待触发）
    # ========================================================================

    def _place_stop_order(
        self, order: Order, now_ms: int
    ) -> OrderResult:
        """记录止损单，等待触发。"""
        if order.stop_price is None or order.stop_price <= 0:
            raise InvalidOrderError(
                "STOP_MARKET requires positive stop_price"
            )

        with self._lock:
            self._stop_orders[order.client_order_id] = {
                "order": order,
                "created_ts": now_ms,
            }
            self._stats["server_stop_successes"] += 1

        logger.debug(
            f"Virtual stop order registered: "
            f"client_order_id={order.client_order_id} "
            f"stop_price={order.stop_price} "
            f"side={order.side.value}"
        )

        return self._build_result(
            order=order,
            status=OrderStatus.NEW,
            executed_qty=Decimal("0"),
            avg_price=None,
            fills=[],
            total_fee=Decimal("0"),
            now_ms=now_ms,
        )

    def check_stop_orders(
        self, bids: list, asks: list, now_ms: int
    ) -> List[OrderResult]:
        """
        检查所有待触发的止损单。

        由 main.py 在每根 K 线收线或实时 tick 时调用。

        契约：
        - 多单止损（SELL，stop_price <= best_bid）：触发
        - 空单止损（BUY，stop_price >= best_ask）：触发
        - 触发后按市价成交，从 _stop_orders 移除
        """
        triggered: List[OrderResult] = []

        best_bid = self._best_price(bids)
        best_ask = self._best_price(asks)

        with self._lock:
            stop_ids = list(self._stop_orders.keys())

        for cid in stop_ids:
            with self._lock:
                entry = self._stop_orders.get(cid)
            if entry is None:
                continue

            stop_order: Order = entry["order"]
            stop_price = stop_order.stop_price

            if stop_price is None:
                continue

            triggered_flag = False
            if stop_order.side == OrderSide.SELL:
                # 多单止损（SELL），best_bid <= stop_price 时触发
                if best_bid is not None and best_bid <= stop_price:
                    triggered_flag = True
            else:
                # 空单止损（BUY），best_ask >= stop_price 时触发
                if best_ask is not None and best_ask >= stop_price:
                    triggered_flag = True

            if not triggered_flag:
                continue

            # 触发：按市价成交
            try:
                result = self._fill_market(
                    stop_order, bids, asks, now_ms
                )
                with self._lock:
                    self._stop_orders.pop(cid, None)
                    self._stats["stop_triggers"] += 1
                    self._cache_order_locked(result)
                triggered.append(result)
                logger.info(
                    f"Virtual stop triggered: "
                    f"client_order_id={cid} stop_price={stop_price} "
                    f"filled_qty={result.executed_qty} "
                    f"avg_price={result.avg_price}"
                )
            except Exception as e:
                logger.error(
                    f"stop trigger fill failed: cid={cid} error={e}"
                )
                self._stats["errors"] += 1

        return triggered

    # ========================================================================
    # 虚拟账户更新
    # ========================================================================

    def _apply_fill(
        self,
        order: Order,
        fill_qty: Decimal,
        fill_price: Decimal,
        total_fee: Decimal,
    ) -> None:
        """
        应用成交到虚拟账户。

        契约：
        - 手续费从余额扣
        - 同向累加、反向减仓
        - 反向超出部分视为反向开仓（Step 1 不做）
        """
        with self._lock:
            self._wallet_balance -= total_fee
            self._total_fees += total_fee

            pos = self._positions.get(order.symbol)

            # 平仓方向处理
            if order.reduce_only:
                if pos is None:
                    # 无持仓但 reduce_only，视为无操作
                    logger.warning(
                        f"reduce_only fill on empty position: "
                        f"{order.symbol}"
                    )
                    return

                # 反向减仓
                close_qty = min(fill_qty, pos.qty)
                realized = (
                    (fill_price - pos.entry_price) * close_qty
                    if pos.side == OrderSide.BUY
                    else (pos.entry_price - fill_price) * close_qty
                )
                self._realized_pnl += realized
                self._wallet_balance += realized

                new_qty = pos.qty - close_qty
                if new_qty <= 0:
                    del self._positions[order.symbol]
                    logger.debug(
                        f"position closed: {order.symbol} "
                        f"realized={realized}"
                    )
                else:
                    pos.qty = new_qty
                    logger.debug(
                        f"position reduced: {order.symbol} "
                        f"qty={new_qty} realized={realized}"
                    )
                return

            # 开仓方向处理
            if pos is None:
                # 新开仓
                self._positions[order.symbol] = _VirtualPosition(
                    symbol=order.symbol,
                    side=order.side,
                    qty=fill_qty,
                    entry_price=fill_price,
                )
                logger.debug(
                    f"position opened: {order.symbol} "
                    f"side={order.side.value} qty={fill_qty} "
                    f"entry={fill_price}"
                )
            elif pos.side == order.side:
                # 同向加仓
                total_cost = (
                    pos.entry_price * pos.qty + fill_price * fill_qty
                )
                new_qty = pos.qty + fill_qty
                pos.entry_price = total_cost / new_qty
                pos.qty = new_qty
                logger.debug(
                    f"position added: {order.symbol} "
                    f"new_qty={new_qty} "
                    f"new_avg={pos.entry_price}"
                )
            else:
                # 反向开仓（Step 1 不该出现）
                logger.warning(
                    f"reverse fill on position: {order.symbol}, "
                    f"existing side={pos.side.value} "
                    f"fill side={order.side.value}"
                )
                del self._positions[order.symbol]
                self._positions[order.symbol] = _VirtualPosition(
                    symbol=order.symbol,
                    side=order.side,
                    qty=fill_qty,
                    entry_price=fill_price,
                )

    # ========================================================================
    # 结果构造
    # ========================================================================

    def _build_result(
        self,
        order: Order,
        status: OrderStatus,
        executed_qty: Decimal,
        avg_price: Optional[Decimal],
        fills: List[Fill],
        total_fee: Decimal,
        now_ms: int,
    ) -> OrderResult:
        """构造 OrderResult。"""
        return OrderResult(
            client_order_id=order.client_order_id,
            exchange_order_id=f"v-{order.client_order_id}",
            symbol=order.symbol,
            status=status,
            side=order.side,
            type=order.type,
            requested_qty=order.qty,
            executed_qty=executed_qty,
            avg_price=avg_price,
            request_price=order.price,
            stop_price=order.stop_price,
            fills=fills,
            total_fee=total_fee,
            fee_asset="USDT",
            transact_time=now_ms,
            update_time=now_ms,
            reject_reason=None,
            reject_code=None,
            synthetic=True,  # 虚拟标记
        )

    # ========================================================================
    # cancel_order
    # ========================================================================

    def cancel_order(self, client_order_id: str) -> OrderResult:
        """
        撤单。虚拟撤单即标记为 CANCELED。

        契约：
        - 止损单：从 _stop_orders 移除
        - 其他订单：返回 CANCELED
        - 缓存中查到返回缓存
        """
        with self._lock:
            self._stats["cancel_attempts"] += 1

            # 从 stop_orders 移除
            self._stop_orders.pop(client_order_id, None)

            # 查缓存
            cached = self._recent_orders.get(client_order_id)
            if cached is not None:
                self._stats["cache_hits"] += 1
                return cached

        # 找不到原订单：构造一个 CANCELED 结果
        now_ms = self._safe_now_ms()
        result = OrderResult(
            client_order_id=client_order_id,
            exchange_order_id=f"v-{client_order_id}",
            symbol=self._symbol,
            status=OrderStatus.CANCELED,
            side=OrderSide.BUY,  # 未知，占位
            type=OrderType.MARKET,  # 未知，占位
            requested_qty=Decimal("0"),
            executed_qty=Decimal("0"),
            avg_price=None,
            request_price=None,
            stop_price=None,
            fills=[],
            total_fee=Decimal("0"),
            fee_asset="USDT",
            transact_time=now_ms,
            update_time=now_ms,
            synthetic=True,
        )

        with self._lock:
            self._cache_order_locked(result)
            self._stats["cancel_successes"] += 1

        return result

    def cancel_all_orders(
        self, symbol: Optional[str] = None
    ) -> Dict[str, Any]:
        """撤掉所有未完结订单。"""
        with self._lock:
            stop_ids = list(self._stop_orders.keys())

        canceled = 0
        failed: List[Dict[str, str]] = []
        for cid in stop_ids:
            try:
                self.cancel_order(cid)
                canceled += 1
            except Exception as e:
                failed.append({
                    "client_order_id": cid,
                    "error": str(e),
                })

        return {"canceled": canceled, "failed": failed}

    # ========================================================================
    # 查询
    # ========================================================================

    def get_order(self, client_order_id: str) -> OrderResult:
        """查订单。优先缓存。"""
        with self._lock:
            cached = self._recent_orders.get(client_order_id)
            if cached is not None:
                return cached
        raise UnknownOrderError(
            f"order not found: {client_order_id}"
        )

    def get_open_orders(
        self, symbol: Optional[str] = None
    ) -> List[OrderResult]:
        """
        查未完结订单。

        契约：
        - 虚拟环境中，只有待触发的止损单是"未完结"
        - 限价单排队（Step 1 不支持）
        """
        with self._lock:
            result = []
            for cid, entry in self._stop_orders.items():
                order = entry["order"]
                if symbol and order.symbol != symbol.upper():
                    continue
                result.append(OrderResult(
                    client_order_id=cid,
                    exchange_order_id=f"v-{cid}",
                    symbol=order.symbol,
                    status=OrderStatus.NEW,
                    side=order.side,
                    type=order.type,
                    requested_qty=order.qty,
                    executed_qty=Decimal("0"),
                    avg_price=None,
                    request_price=order.price,
                    stop_price=order.stop_price,
                    fills=[],
                    total_fee=Decimal("0"),
                    fee_asset="USDT",
                    transact_time=entry["created_ts"],
                    update_time=entry["created_ts"],
                    synthetic=True,
                ))
            return result

    def get_positions(self) -> List[Position]:
        """查虚拟持仓。"""
        with self._lock:
            positions = []
            now_ms = self._safe_now_ms()
            for pos in self._positions.values():
                mark = self._get_mark_price(pos.symbol, now_ms)
                positions.append(
                    pos.to_position(mark, self._leverage)
                )
            return positions

    def get_position(self, symbol: str) -> Optional[Position]:
        """查单个持仓。"""
        with self._lock:
            pos = self._positions.get(symbol.upper())
            if pos is None:
                return None
            now_ms = self._safe_now_ms()
            mark = self._get_mark_price(symbol, now_ms)
            return pos.to_position(mark, self._leverage)

    def get_account_state(self) -> AccountState:
        """
        查虚拟账户。

        契约：
        - total_wallet_balance = _wallet_balance
        - total_unrealized_pnl = Σ 未实现盈亏
        - margin_ratio = 0（Step 1 不计算爆仓）
        """
        with self._lock:
            unrealized = Decimal("0")
            now_ms = self._safe_now_ms()
            for pos in self._positions.values():
                mark = self._get_mark_price(pos.symbol, now_ms)
                if pos.side == OrderSide.BUY:
                    unrealized += (
                        (mark - pos.entry_price) * pos.qty
                    )
                else:
                    unrealized += (
                        (pos.entry_price - mark) * pos.qty
                    )

            return AccountState(
                total_wallet_balance=self._wallet_balance,
                total_unrealized_pnl=unrealized,
                total_margin_balance=(
                    self._wallet_balance + unrealized
                ),
                total_maint_margin=Decimal("0"),
                available_balance=(
                    self._wallet_balance + unrealized
                ),
                update_time=now_ms,
            )

    # ========================================================================
    # 平仓
    # ========================================================================

    def close_position(
        self, symbol: str, reason: str = "manual"
    ) -> Optional[OrderResult]:
        """
        平掉指定交易对持仓。

        契约：
        - 无持仓返回 None
        - 有持仓：反向市价单 reduce_only=True
        """
        with self._lock:
            self._stats["close_position_attempts"] += 1

            pos = self._positions.get(symbol.upper())
            if pos is None or pos.qty <= 0:
                return None

            close_side = (
                OrderSide.SELL if pos.side == OrderSide.BUY
                else OrderSide.BUY
            )
            qty = pos.qty

        try:
            from system.order_idempotent import OrderKind
            client_id = self._id_gen.generate(
                OrderKind.CLOSE_ALL, 0, role_seq=0, attempt=0
            )
        except Exception as e:
            self._stats["errors"] += 1
            raise InvalidOrderError(
                f"generate close order id failed: {e}"
            ) from e

        order = Order(
            client_order_id=client_id,
            symbol=symbol.upper(),
            side=close_side,
            type=OrderType.MARKET,
            qty=qty,
            reduce_only=True,
        )

        try:
            result = self.place_order(order)
        except Exception as e:
            logger.error(
                f"close_position failed: symbol={symbol} error={e}"
            )
            raise

        with self._lock:
            self._stats["close_position_successes"] += 1

        logger.info(
            f"close_position: symbol={symbol} reason={reason} "
            f"qty={qty} status={result.status.value}"
        )
        return result

    def close_all_positions(
        self, reason: str = "manual"
    ) -> List[OrderResult]:
        """平掉所有持仓。"""
        with self._lock:
            symbols = list(self._positions.keys())

        results = []
        for sym in symbols:
            try:
                r = self.close_position(sym, reason=reason)
                if r is not None:
                    results.append(r)
            except Exception as e:
                logger.error(
                    f"close_all_positions failed for {sym}: {e}"
                )
        return results

    # ========================================================================
    # 服务器端止损
    # ========================================================================

    def ensure_server_side_stop(
        self,
        symbol: str,
        stop_price: Decimal,
        side: OrderSide,
        qty: Decimal,
        client_order_id: str,
    ) -> OrderResult:
        """挂虚拟止损单。"""
        with self._lock:
            self._stats["server_stop_attempts"] += 1

            cached = self._recent_orders.get(client_order_id)
            if cached is not None:
                return cached

        order = Order(
            client_order_id=client_order_id,
            symbol=symbol.upper(),
            side=side,
            type=OrderType.STOP_MARKET,
            qty=qty,
            stop_price=stop_price,
            reduce_only=True,
            working_type=WorkingType.MARK_PRICE,
        )

        return self.place_order(order)

    def cancel_server_side_stop(
        self, client_order_id: str
    ) -> OrderResult:
        """撤虚拟止损单。"""
        return self.cancel_order(client_order_id)

    # ========================================================================
    # 杠杆
    # ========================================================================

    def set_leverage(self, symbol: str, leverage: int) -> bool:
        """设置虚拟杠杆。"""
        if leverage <= 0:
            return False
        with self._lock:
            self._leverage = int(leverage)
        logger.info(
            f"virtual leverage set: symbol={symbol} leverage={leverage}"
        )
        return True

    # ========================================================================
    # 辅助
    # ========================================================================

    def _best_price(self, levels: list) -> Optional[Decimal]:
        """获取最优价格（第一个有效档位）。"""
        for level in levels:
            try:
                price = Decimal(str(level[0]))
                qty = Decimal(str(level[1]))
            except (IndexError, InvalidOperation, ValueError):
                continue
            if price > 0 and qty > 0:
                return price
        return None

    def _get_mark_price(
        self, symbol: str, now_ms: int
    ) -> Decimal:
        """获取标记价格（用于计算未实现盈亏）。"""
        try:
            snapshot = self._feed.get_snapshot_at(now_ms)
            if snapshot is not None:
                bids = snapshot.get("bids") or []
                asks = snapshot.get("asks") or []
                if bids and asks:
                    best_bid = self._best_price(bids)
                    best_ask = self._best_price(asks)
                    if best_bid is not None and best_ask is not None:
                        return (best_bid + best_ask) / Decimal("2")
        except Exception as e:
            logger.debug(f"get_mark_price failed: {e}")
        # 兜底：用持仓入场价
        pos = self._positions.get(symbol.upper())
        if pos is not None:
            return pos.entry_price
        return Decimal("0")

    def _safe_now_ms(self) -> int:
        """安全获取当前毫秒时间戳。"""
        try:
            return int(self._clock.now_ms())
        except Exception:
            return int(time.time() * 1000)

    def _cache_order_locked(self, result: OrderResult) -> None:
        """缓存订单（调用者必须持锁）。"""
        if not result.client_order_id:
            return
        self._recent_orders[result.client_order_id] = result
        self._recent_orders.move_to_end(result.client_order_id)

        while len(self._recent_orders) > self._cache_size:
            evicted, _ = self._recent_orders.popitem(last=False)
            self._stats["cache_evictions"] += 1
            logger.debug(f"cache evicted: {evicted}")

    # ========================================================================
    # 重置
    # ========================================================================

    def reset(self) -> None:
        """
        重置虚拟券商。回测每跑一轮调用一次。

        契约：
        - 清空持仓、余额、缓存、止损单
        - 统计清零
        """
        with self._lock:
            self._wallet_balance = self._initial_balance
            self._realized_pnl = Decimal("0")
            self._total_fees = Decimal("0")
            self._positions.clear()
            self._recent_orders.clear()
            self._stop_orders.clear()
            for k in self._stats:
                self._stats[k] = 0
            self._last_error = None
        logger.info("VirtualGateway reset")

    # ========================================================================
    # 健康检查
    # ========================================================================

    def health_check(self) -> Dict[str, Any]:
        """健康检查。"""
        with self._lock:
            return {
                "healthy": True,
                "name": self.name,
                "version": self.version,
                "exchange": self.exchange_name,
                "symbol": self._symbol,
                "wallet_balance": str(self._wallet_balance),
                "initial_balance": str(self._initial_balance),
                "realized_pnl": str(self._realized_pnl),
                "total_fees": str(self._total_fees),
                "positions_count": len(self._positions),
                "stop_orders_count": len(self._stop_orders),
                "cache_size": len(self._recent_orders),
                "cache_capacity": self._cache_size,
                "fee_rate": str(self._fee_rate),
                "latency_ms": self._latency_ms,
                "stats": dict(self._stats),
                "last_error": self._last_error,
                "timestamp": int(time.time() * 1000),
            }


# ============================================================================
# 内置数据源：LiveSnapshotFeed
# ============================================================================
class LiveSnapshotFeed(MarketFeed):
    """
    影子模式数据源：从 KlineAuthority 获取最新盘口。

    用法：
        feed = LiveSnapshotFeed(kline_authority)
        gateway = VirtualGateway(feed=feed, ..., latency_ms=80)
    """

    def __init__(self, kline_authority: Any):
        if kline_authority is None:
            raise ValueError("kline_authority is required")
        self._authority = kline_authority

    def get_snapshot_at(self, ts: int) -> Optional[dict]:
        """获取最近盘口。"""
        try:
            snap = self._authority.get_current_snapshot()
            if snap is None:
                return None
            return {
                "ts": snap.get("ts", ts),
                "bids": snap.get("bids", []),
                "asks": snap.get("asks", []),
            }
        except Exception as e:
            logger.error(
                f"LiveSnapshotFeed.get_snapshot_at failed: {e}"
            )
            return None


# ============================================================================
# 内置数据源：HistoricalFeed
# ============================================================================
class HistoricalFeed(MarketFeed):
    """
    回测模式数据源：从 TickDB 或 K 线合成盘口。

    用法：
        feed = HistoricalFeed(tick_db, current_ts_provider=lambda: current_ts)
        gateway = VirtualGateway(feed=feed, ..., latency_ms=0)
    """

    def __init__(
        self,
        tick_db: Any,
        current_ts_provider: Optional[Callable[[], int]] = None,
        bar_close_provider: Optional[Callable[[], Decimal]] = None,
    ):
        if tick_db is None:
            raise ValueError("tick_db is required")
        self._tick_db = tick_db
        self._current_ts = current_ts_provider or (
            lambda: int(time.time() * 1000)
        )
        self._bar_close = bar_close_provider

    def set_bar_close(self, close: Decimal) -> None:
        """回测每根 K 线更新，用于合成盘口。"""
        self._bar_close = lambda: close

    def get_snapshot_at(self, ts: int) -> Optional[dict]:
        """从 TickDB 或合成盘口。"""
        try:
            snap = self._tick_db.query_snapshots(
                symbol=self._tick_db.symbol if hasattr(
                    self._tick_db, "symbol"
                ) else "BTCUSDT",
                start_ts=ts,
                end_ts=ts + 500,
            )
            if snap:
                s = snap[0]
                return {
                    "ts": s.get("ts", ts),
                    "bids": s.get("bids", []),
                    "asks": s.get("asks", []),
                }
        except Exception as e:
            logger.debug(f"query_snapshots failed: {e}")

        # 合成盘口
        close = (
            self._bar_close() if self._bar_close is not None
            else None
        )
        if close is None or close <= 0:
            return None

        # 用 close 合成 ±0.05% 的买卖盘
        spread = close * Decimal("0.0005")
        return {
            "ts": ts,
            "bids": [
                (close - spread - i * spread, Decimal("10"))
                for i in range(10)
            ],
            "asks": [
                (close + spread + i * spread, Decimal("10"))
                for i in range(10)
            ],
        }


# ============================================================================
# 工厂函数
# ============================================================================
def create(config: dict) -> VirtualGateway:
    """
    模块工厂入口。

    config 需要：
    - instances.clock
    - instances.order_idempotent_gen
    - instances.feed（由 main.py 或 run_backtest.py 提供）
    - symbol

    config 可选：
    - gateway.fee_rate: 默认 0.0004
    - gateway.latency_ms: 默认 0
    - gateway.initial_balance: 默认 10000
    """
    instances = config.get("instances", {})
    gateway_cfg = config.get("gateway", {})

    feed = instances.get("feed")
    if feed is None:
        raise ValueError(
            "VirtualGateway.create requires config.instances.feed. "
            "In backtest mode, use HistoricalFeed. "
            "In shadow mode, use LiveSnapshotFeed."
        )

    return VirtualGateway(
        feed=feed,
        clock=instances.get("clock"),
        id_generator=instances.get("order_idempotent_gen"),
        symbol=config.get("symbol", "btcusdt"),
        config=config,
        fee_rate=Decimal(str(gateway_cfg.get(
            "fee_rate", str(DEFAULT_FEE_RATE)
        ))),
        latency_ms=int(gateway_cfg.get(
            "latency_ms", DEFAULT_LATENCY_MS
        )),
        initial_balance=Decimal(str(gateway_cfg.get(
            "initial_balance", str(DEFAULT_INITIAL_BALANCE)
        ))),
        leverage=int(gateway_cfg.get("leverage", 1)),
    )


def validate_module() -> dict:
    """校验 MANIFEST 合法。"""
    from core.manifest import ManifestParser
    ManifestParser.parse(MANIFEST, "execution/gateway_virtual.py")
    return MANIFEST
