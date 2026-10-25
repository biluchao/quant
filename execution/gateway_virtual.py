"""
execution/gateway_virtual.py — 虚拟券商网关 (重铸标准版)

核心设计原则：
1. 绝对 Decimal 计算：一切与资金、价格、数量相关的运算，杜绝 float 参与。
2. 严格接口依赖：绝不吞缺失参数，缺依赖立刻抛出 ValueError。
3. 悲观成交模型：吃对手盘，每档限制成交比例，支持部分成交。
4. 真实的止损引擎：独立处理 STOP_MARKET 挂单，依据盘口实时触发。
"""

from collections import OrderedDict
from dataclasses import dataclass
from decimal import Decimal
import logging
import threading
import time
from typing import Any, Dict, List, Optional

from core.gateway import (
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
    AccountState,
    WorkingType,
)

logger = logging.getLogger(__name__)


class MarketFeed:
    """虚拟券商的数据源接口标准"""
    def get_snapshot_at(self, ts: int) -> Optional[dict]:
        """
        必须返回：{"ts": int, "bids": [(Decimal, Decimal)], "asks": [(Decimal, Decimal)]}
        价格和数量必须是 Decimal。
        """
        raise NotImplementedError


@dataclass
class _VirtualPosition:
    """内部虚拟持仓记录"""
    symbol: str
    side: OrderSide
    qty: Decimal
    entry_price: Decimal

    def to_position(self, mark_price: Decimal) -> Position:
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
            liquidation_price=None,
            margin=Decimal("0"),
            leverage=1,
            update_time=int(time.time() * 1000),
        )


class VirtualGateway(ExecutionGateway):
    """
    虚拟券商网关。
    """

    exchange_name = "virtual"
    version = "1.0.0"

    def __init__(
        self,
        feed: MarketFeed,
        clock: Any,
        id_generator: Any,
        symbol: str,
        config: dict,
        fee_rate: Decimal = Decimal("0.0004"),
        latency_ms: int = 0,
        initial_balance: Decimal = Decimal("10000"),
    ):
        # 严苛的参数校验
        if not feed or not clock or not id_generator or not symbol:
            raise ValueError("VirtualGateway 初始化缺少必要依赖 (feed, clock, id_generator, symbol)")

        self.feed = feed
        self.clock = clock
        self.id_gen = id_generator
        self.symbol = symbol.upper()
        self.config = config
        self.fee_rate = Decimal(str(fee_rate))
        self.latency_s = max(0, latency_ms) / 1000.0

        # 账户与持仓
        self._initial_balance = Decimal(str(initial_balance))
        self._wallet_balance = self._initial_balance
        self._positions: Dict[str, _VirtualPosition] = {}

        # 挂单与缓存
        self._active_stops: Dict[str, Order] = {}
        self._order_cache: "OrderedDict[str, OrderResult]" = OrderedDict()

        self._lock = threading.RLock()
        self._stats = {"orders": 0, "rejects": 0, "partial": 0, "stop_triggered": 0}

    # ========================================================================
    # 核心方法：下单
    # ========================================================================

    def place_order(self, order: Order) -> OrderResult:
        """模拟下单主入口"""
        with self._lock:
            # 幂等查重
            if order.client_order_id in self._order_cache:
                return self._order_cache[order.client_order_id]

            self._stats["orders"] += 1

            # 延迟模拟
            if self.latency_s > 0:
                time.sleep(self.latency_s)

            # 获取当前盘口
            now_ms = self.clock.now_ms()
            snapshot = self.feed.get_snapshot_at(now_ms)
            if not snapshot:
                self._stats["rejects"] += 1
                raise OrderRejectedError("no_market_data")

            # 路由分发
            if order.type == OrderType.MARKET:
                result = self._execute_market(order, snapshot, now_ms)
            elif order.type == OrderType.STOP_MARKET:
                result = self._register_stop(order, now_ms)
            else:
                raise InvalidOrderError(f"VirtualGateway 不支持订单类型: {order.type}")

            self._cache_order(result)
            return result

    def _execute_market(self, order: Order, snapshot: dict, now_ms: int) -> OrderResult:
        """吃对手盘撮合"""
        levels = snapshot.get("asks", []) if order.side == OrderSide.BUY else snapshot.get("bids", [])
        if not levels:
            raise OrderRejectedError("empty_orderbook")

        filled_qty = Decimal("0")
        filled_notional = Decimal("0")
        fills = []
        remaining = order.qty

        for i, (price, level_qty) in enumerate(levels[:10]):  # 最多吃 10 档
            if remaining <= 0:
                break
            
            # 虚拟防插针：每档最多吃档位 30%
            max_fill = level_qty * Decimal("0.3")
            fill_qty = min(remaining, max_fill)

            if fill_qty <= 0:
                continue

            fee = fill_qty * price * self.fee_rate
            fills.append(Fill(
                trade_id=f"vt-{order.client_order_id}-{i}",
                price=price,
                qty=fill_qty,
                fee=fee,
                fee_asset="USDT",
                is_maker=False,
                ts=now_ms,
            ))
            filled_qty += fill_qty
            filled_notional += (fill_qty * price)
            remaining -= fill_qty

        if filled_qty <= 0:
            raise OrderRejectedError("unable_to_fill")

        avg_price = filled_notional / filled_qty
        status = OrderStatus.FILLED if filled_qty >= order.qty else OrderStatus.PARTIALLY_FILLED
        if status == OrderStatus.PARTIALLY_FILLED:
            self._stats["partial"] += 1

        total_fee = sum(f.fee for f in fills)
        self._apply_fill(order, filled_qty, avg_price, total_fee)

        return self._build_result(order, status, filled_qty, avg_price, fills, total_fee, now_ms)

    # ========================================================================
    # 虚拟止损引擎
    # ========================================================================

    def _register_stop(self, order: Order, now_ms: int) -> OrderResult:
        """挂起止损单，等待触发"""
        if order.stop_price is None or order.stop_price <= 0:
            raise InvalidOrderError("STOP_MARKET requires positive stop_price")
        
        self._active_stops[order.client_order_id] = order
        return self._build_result(order, OrderStatus.NEW, Decimal("0"), None, [], Decimal("0"), now_ms)

    def check_stop_orders(self, now_ms: int) -> List[OrderResult]:
        """
        触发止损检查（由外部引擎每根 K 线或实时 Tick 调用）
        """
        triggered_results = []
        with self._lock:
            snapshot = self.feed.get_snapshot_at(now_ms)
            if not snapshot:
                return triggered_results

            best_bid = snapshot["bids"][0][0] if snapshot.get("bids") else None
            best_ask = snapshot["asks"][0][0] if snapshot.get("asks") else None

            for cid, order in list(self._active_stops.items()):
                triggered = False
                if order.side == OrderSide.SELL and best_bid and best_bid <= order.stop_price:
                    triggered = True
                elif order.side == OrderSide.BUY and best_ask and best_ask >= order.stop_price:
                    triggered = True

                if triggered:
                    del self._active_stops[cid]
                    self._stats["stop_triggered"] += 1
                    # 止损触发后，立刻转为市价单砸盘
                    market_order = Order(
                        client_order_id=f"trig-{cid}",
                        symbol=order.symbol,
                        side=order.side,
                        type=OrderType.MARKET,
                        qty=order.qty,
                        reduce_only=True
                    )
                    try:
                        res = self._execute_market(market_order, snapshot, now_ms)
                        triggered_results.append(res)
                        logger.info(f"虚拟止损触发: {cid} @ {order.stop_price}")
                    except Exception as e:
                        logger.error(f"止损触发成交失败: {e}")
        
        return triggered_results

    # ========================================================================
    # 账户与持仓状态
    # ========================================================================

    def _apply_fill(self, order: Order, fill_qty: Decimal, fill_price: Decimal, fee: Decimal) -> None:
        """更新虚拟账户余额与持仓"""
        self._wallet_balance -= fee

        pos = self._positions.get(order.symbol)

        if order.reduce_only:
            if not pos:
                return  # 无仓可平
            
            close_qty = min(fill_qty, pos.qty)
            realized = (fill_price - pos.entry_price) * close_qty if pos.side == OrderSide.BUY else (pos.entry_price - fill_price) * close_qty
            self._wallet_balance += realized

            if pos.qty == close_qty:
                del self._positions[order.symbol]
            else:
                pos.qty -= close_qty
        else:
            if not pos:
                self._positions[order.symbol] = _VirtualPosition(
                    symbol=order.symbol, side=order.side, qty=fill_qty, entry_price=fill_price
                )
            elif pos.side == order.side:
                # 同向加仓
                total_notional = pos.entry_price * pos.qty + fill_price * fill_qty
                new_qty = pos.qty + fill_qty
                pos.entry_price = total_notional / new_qty
                pos.qty = new_qty
            else:
                # 反向开仓（不应发生，防御性处理）
                del self._positions[order.symbol]

    def get_positions(self) -> List[Position]:
        with self._lock:
            result = []
            now_ms = self.clock.now_ms()
            snapshot = self.feed.get_snapshot_at(now_ms)
            
            for pos in self._positions.values():
                mark = pos.entry_price
                if snapshot:
                    # 取中间价作为标记价
                    best_bid = snapshot["bids"][0][0] if snapshot.get("bids") else mark
                    best_ask = snapshot["asks"][0][0] if snapshot.get("asks") else mark
                    mark = (best_bid + best_ask) / Decimal("2")
                result.append(pos.to_position(mark))
            return result

    def get_account_state(self) -> AccountState:
        with self._lock:
            unrealized = sum((p.unrealized_pnl for p in self.get_positions()), Decimal("0"))
            return AccountState(
                total_wallet_balance=self._wallet_balance,
                total_unrealized_pnl=unrealized,
                total_margin_balance=self._wallet_balance + unrealized,
                total_maint_margin=Decimal("0"),
                available_balance=self._wallet_balance + unrealized,
                update_time=self.clock.now_ms(),
            )

    # ========================================================================
    # 辅助与生命周期
    # ========================================================================

    def _build_result(self, order: Order, status: OrderStatus, exec_qty: Decimal, avg_price: Optional[Decimal], fills: List[Fill], fee: Decimal, now_ms: int) -> OrderResult:
        return OrderResult(
            client_order_id=order.client_order_id,
            exchange_order_id=f"v-{order.client_order_id}",
            symbol=order.symbol,
            status=status,
            side=order.side,
            type=order.type,
            requested_qty=order.qty,
            executed_qty=exec_qty,
            avg_price=avg_price,
            request_price=order.price,
            stop_price=order.stop_price,
            fills=fills,
            total_fee=fee,
            fee_asset="USDT",
            transact_time=now_ms,
            update_time=now_ms,
            synthetic=True,
        )

    def _cache_order(self, result: OrderResult) -> None:
        self._order_cache[result.client_order_id] = result
        if len(self._order_cache) > 1000:
            self._order_cache.popitem(last=False)

    def cancel_order(self, client_order_id: str) -> OrderResult:
        with self._lock:
            if client_order_id in self._active_stops:
                order = self._active_stops.pop(client_order_id)
                return self._build_result(order, OrderStatus.CANCELED, Decimal("0"), None, [], Decimal("0"), self.clock.now_ms())
            raise InvalidOrderError(f"Order {client_order_id} not found or already filled")

    def get_order(self, client_order_id: str) -> OrderResult:
        if client_order_id in self._order_cache:
            return self._order_cache[client_order_id]
        raise InvalidOrderError("Order not found")


# ============================================================================
# 模块工厂
# ============================================================================

MANIFEST = {
    "name": "gateway_virtual",
    "type": "gateway",
    "version": "1.0.0",
    "step": 1,
    "capabilities": ["place_order", "cancel_order", "close_position"],
    "module_dependencies": [],
    "owner": "lego",
    "exchange": "virtual",
}

def create(config: dict) -> VirtualGateway:
    """
    因为 VirtualGateway 需要注入外部具体的 Feed，这里使用延迟导入/反射来适配自动扫描。
    实际装配由 main.py 或 run_backtest.py 手动完成。
    """
    # 占位实例化，实际参数在运行时通过 setter 或手动装配注入
    from execution.gateway_virtual import MarketFeed
    class NullFeed(MarketFeed):
        def get_snapshot_at(self, ts: int) -> Optional[dict]: return None
    class NullClock:
        def now_ms(self): return int(time.time() * 1000)
    class NullIdGen:
        def generate(self, *args, **kwargs): return "null-id"

    return VirtualGateway(
        feed=NullFeed(),
        clock=NullClock(),
        id_generator=NullIdGen(),
        symbol=config.get("symbol", "BTCUSDT"),
        config=config
       )
