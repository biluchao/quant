"""
system/rate_limiter.py — 令牌桶限频器（带优先级和退避）

限频器是执行层和交易所之间的缓冲垫。

设计原则：
1. 双令牌桶：订单桶（10 秒窗口）+ 权重桶（1 分钟窗口）
2. 优先级预留：低优先级只能用部分容量，高优先级可全用
3. 指数退避：收到 -1003 后退避，退避期间所有请求等待
4. 阻塞式 acquire：调用方给 timeout，超时返回 False
5. 锁外 sleep：等待时不持锁，避免阻塞其他线程
6. 单交易所：Step 1 单实例，Step 4 多交易所多实例

优先级与预留：
- CANCEL（0.0）：撤单，可全用
- CLOSE_POSITION（0.0）：平仓，可全用
- OPEN_ORDER（0.1）：开仓，最多用 90%
- QUERY（0.3）：查询，最多用 70%
- RECONCILE（0.4）：对账，最多用 60%

预留语义：request.need + reserve <= capacity
- 高优先级 reserve=0，只要令牌够就能过
- 低优先级 reserve=0.4，必须留 40% 给高优先级

令牌桶模型：
- 启动时满桶
- 以 refill_rate 连续补充
- 令牌上限 = capacity（不溢出）
- 突发：允许一次性用完整个桶

退避机制：
- on_rate_limit_hit() 触发退避
- 退避时长翻倍，上限 max_backoff_ms
- 退避期间所有 acquire 等待
- 成功 acquire 后重置退避

关键契约：
1. 所有方法线程安全（RLock）
2. 等待期间不持锁（锁外 sleep）
3. acquire 超时返回 False，不抛异常
4. 限频器不知策略、不知订单业务
5. 限频器只在实盘注入，回测/影子不经过
6. -1021 不经过限频器，由 clock.force_resync() 处理
7. 统计字段：granted / denied / timeout / wait_total_ms / backoff_count

作者：张三
审查：比蒙（策略）、乐高（执行）
版本：v1.0.0
"""

from enum import Enum
import logging
import threading
import time
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


# ============================================================================
# 常量
# ============================================================================
DEFAULT_ORDER_CAPACITY = 45             # 10 秒窗口 50 的 90% 余量
DEFAULT_ORDER_REFILL_PER_S = 4.5        # 45 / 10 秒
DEFAULT_WEIGHT_CAPACITY = 1000          # 1 分钟窗口 1200 的 83% 余量
DEFAULT_WEIGHT_REFILL_PER_S = 16.67     # 1000 / 60 秒
DEFAULT_INITIAL_BACKOFF_MS = 1000       # 初始退避 1 秒
DEFAULT_MAX_BACKOFF_MS = 30000          # 最大退避 30 秒
DEFAULT_ACQUIRE_TIMEOUT_MS = 2000       # 默认 acquire 超时 2 秒
MAX_SLEEP_MS = 50                       # 单次 sleep 上限


# ============================================================================
# 优先级
# ============================================================================
class RequestPriority(str, Enum):
    """请求优先级。数值越小优先级越高。"""
    CANCEL = "cancel"                # 撤单，最高
    CLOSE_POSITION = "close_position"  # 平仓
    OPEN_ORDER = "open_order"        # 开仓
    QUERY = "query"                  # 查询
    RECONCILE = "reconcile"          # 对账，最低


# 预留比例（低优先级为高优先级保留的容量比例）
PRIORITY_RESERVE: Dict[RequestPriority, float] = {
    RequestPriority.CANCEL: 0.0,           # 可全用
    RequestPriority.CLOSE_POSITION: 0.0,   # 可全用
    RequestPriority.OPEN_ORDER: 0.1,       # 最多 90%
    RequestPriority.QUERY: 0.3,            # 最多 70%
    RequestPriority.RECONCILE: 0.4,        # 最多 60%
}


# ============================================================================
# 限频器
# ============================================================================
class RateLimiter:
    """
    令牌桶限频器。详见模块 docstring。

    用法：
        limiter = RateLimiter(config)

        # 开仓
        if limiter.acquire(
            priority=RequestPriority.OPEN_ORDER,
            order_need=1,
            weight_need=1,
            timeout_ms=2000,
        ):
            rest_client.place_order(...)
        else:
            logger.warning("rate limit timeout, order skipped")

        # 撤单（更高优先级）
        limiter.acquire(
            priority=RequestPriority.CANCEL,
            order_need=1,
            weight_need=1,
        )

        # 查询（不占订单桶）
        limiter.acquire(
            priority=RequestPriority.QUERY,
            order_need=0,
            weight_need=5,
        )

        # 收到 -1003
        limiter.on_rate_limit_hit()

        # 定期检查
        health = limiter.health_check()
    """

    name = "rate_limiter"
    version = "1.0.0"

    def __init__(self, config: dict):
        self.config = config
        cfg = config.get("rate_limit", {})

        # ---- 订单桶 ----
        self._order_capacity = float(
            cfg.get("orders_per_10s", DEFAULT_ORDER_CAPACITY)
        )
        self._order_refill_per_s = float(
            cfg.get("bucket_refill_rate", DEFAULT_ORDER_REFILL_PER_S)
        )
        self._order_tokens = self._order_capacity  # 启动时满桶
        self._order_refill_per_ms = self._order_refill_per_s / 1000.0

        # ---- 权重桶 ----
        self._weight_capacity = float(
            cfg.get("weight_per_min", DEFAULT_WEIGHT_CAPACITY)
        )
        self._weight_refill_per_s = (
            self._weight_capacity / 60.0
        )
        self._weight_tokens = self._weight_capacity  # 启动时满桶
        self._weight_refill_per_ms = self._weight_refill_per_s / 1000.0

        # ---- 退避 ----
        self._initial_backoff_ms = int(
            cfg.get("initial_backoff_ms", DEFAULT_INITIAL_BACKOFF_MS)
        )
        self._max_backoff_ms = int(
            cfg.get("max_backoff_ms", DEFAULT_MAX_BACKOFF_MS)
        )
        self._backoff_ms = self._initial_backoff_ms
        self._backoff_until_ms = 0.0

        # ---- 时间 ----
        self._last_refill_ms = time.monotonic() * 1000.0

        # ---- 锁 ----
        self._lock = threading.RLock()

        # ---- 统计 ----
        self._stats: Dict[str, Any] = {
            "granted": 0,
            "denied": 0,
            "timeout": 0,
            "wait_total_ms": 0.0,
            "backoff_count": 0,
            "by_priority": {
                p.value: {"granted": 0, "denied": 0, "timeout": 0}
                for p in RequestPriority
            },
        }

        logger.info(
            f"RateLimiter initialized "
            f"order_capacity={self._order_capacity} "
            f"order_refill={self._order_refill_per_s}/s "
            f"weight_capacity={self._weight_capacity} "
            f"weight_refill={self._weight_refill_per_s:.2f}/s"
        )

    # ========================================================================
    # 令牌补充
    # ========================================================================

    def _refill_locked(self) -> None:
        """补充令牌。调用者必须持锁。"""
        now_ms = time.monotonic() * 1000.0
        elapsed = now_ms - self._last_refill_ms
        if elapsed <= 0:
            return

        self._order_tokens = min(
            self._order_capacity,
            self._order_tokens + elapsed * self._order_refill_per_ms,
        )
        self._weight_tokens = min(
            self._weight_capacity,
            self._weight_tokens + elapsed * self._weight_refill_per_ms,
        )
        self._last_refill_ms = now_ms

    # ========================================================================
    # 核心：acquire
    # ========================================================================

    def acquire(
        self,
        priority: RequestPriority,
        order_need: int = 1,
        weight_need: int = 1,
        timeout_ms: int = DEFAULT_ACQUIRE_TIMEOUT_MS,
    ) -> bool:
        """
        获取令牌。阻塞直到成功或超时。

        Args:
            priority: 请求优先级
            order_need: 需要的订单令牌数（0 = 不占订单桶）
            weight_need: 需要的权重令牌数（0 = 不占权重桶）
            timeout_ms: 超时毫秒，0 表示非阻塞（立即返回）

        Returns:
            True: 成功获取
            False: 超时或非阻塞时未获取

        契约：
        - 等待期间不持锁
        - 退避期间等待
        - 超时返回 False，不抛异常
        - 统计记录到 _stats
        """
        start_ms = time.monotonic() * 1000.0
        deadline_ms = start_ms + timeout_ms

        reserve = PRIORITY_RESERVE[priority]
        reserve_order = reserve * self._order_capacity
        reserve_weight = reserve * self._weight_capacity

        while True:
            with self._lock:
                self._refill_locked()
                now_ms = time.monotonic() * 1000.0

                # 退避检查
                if now_ms < self._backoff_until_ms:
                    wait_ms = self._backoff_until_ms - now_ms
                else:
                    # 令牌检查（支持 need=0 跳过）
                    order_ok = (
                        order_need == 0
                        or self._order_tokens - order_need >= reserve_order
                    )
                    weight_ok = (
                        weight_need == 0
                        or self._weight_tokens - weight_need >= reserve_weight
                    )

                    if order_ok and weight_ok:
                        # 消耗令牌
                        self._order_tokens -= order_need
                        self._weight_tokens -= weight_need
                        self._stats["granted"] += 1
                        self._stats["by_priority"][priority.value]["granted"] += 1
                        # 成功则重置退避
                        self._backoff_ms = self._initial_backoff_ms
                        self._backoff_until_ms = 0.0
                        # 记录等待时间
                        waited = now_ms - start_ms
                        self._stats["wait_total_ms"] += waited
                        if waited > 100:
                            logger.debug(
                                f"acquire granted after {waited:.0f}ms "
                                f"priority={priority.value}"
                            )
                        return True

                    # 计算需要等多久
                    wait_ms = self._calc_wait_ms(
                        order_need, weight_need,
                        reserve_order, reserve_weight,
                    )

            # 锁外等待
            remaining = deadline_ms - time.monotonic() * 1000.0
            if remaining <= 0:
                with self._lock:
                    self._stats["timeout"] += 1
                    self._stats["by_priority"][priority.value]["timeout"] += 1
                logger.info(
                    f"acquire timeout priority={priority.value} "
                    f"order_need={order_need} weight_need={weight_need}"
                )
                return False

            sleep_ms = min(wait_ms, remaining, MAX_SLEEP_MS)
            if sleep_ms < 1:
                sleep_ms = 1
            time.sleep(sleep_ms / 1000.0)

    def _calc_wait_ms(
        self,
        order_need: int,
        weight_need: int,
        reserve_order: float,
        reserve_weight: float,
    ) -> float:
        """计算等待时间。调用者必须持锁。"""
        order_deficit = max(
            0.0,
            reserve_order + order_need - self._order_tokens,
        )
        weight_deficit = max(
            0.0,
            reserve_weight + weight_need - self._weight_tokens,
        )

        wait_order_ms = (
            order_deficit / self._order_refill_per_ms
            if self._order_refill_per_ms > 0 and order_deficit > 0
            else 0.0
        )
        wait_weight_ms = (
            weight_deficit / self._weight_refill_per_ms
            if self._weight_refill_per_ms > 0 and weight_deficit > 0
            else 0.0
        )

        return max(wait_order_ms, wait_weight_ms, 10.0)

    def try_acquire(
        self,
        priority: RequestPriority,
        order_need: int = 1,
        weight_need: int = 1,
    ) -> bool:
        """非阻塞 acquire。立即返回。"""
        return self.acquire(
            priority=priority,
            order_need=order_need,
            weight_need=weight_need,
            timeout_ms=0,
        )

    # ========================================================================
    # 退避
    # ========================================================================

    def on_rate_limit_hit(self) -> None:
        """
        收到 -1003 时调用。触发指数退避。

        契约：
        - 每次调用退避翻倍
        - 上限 max_backoff_ms
        - 退避期间所有 acquire 等待
        - 成功 acquire 后重置
        """
        with self._lock:
            now_ms = time.monotonic() * 1000.0
            self._backoff_until_ms = max(
                self._backoff_until_ms,
                now_ms + self._backoff_ms,
            )
            self._stats["backoff_count"] += 1
            self._stats["denied"] += 1
            logger.warning(
                f"rate limit hit (-1003), backoff {self._backoff_ms}ms "
                f"until {self._backoff_until_ms:.0f}ms "
                f"(count={self._stats['backoff_count']})"
            )
            # 翻倍
            self._backoff_ms = min(
                self._backoff_ms * 2,
                self._max_backoff_ms,
            )

    # ========================================================================
    # 状态查询
    # ========================================================================

    def get_tokens(self) -> Dict[str, float]:
        """当前令牌数。"""
        with self._lock:
            self._refill_locked()
            return {
                "order_tokens": self._order_tokens,
                "order_capacity": self._order_capacity,
                "weight_tokens": self._weight_tokens,
                "weight_capacity": self._weight_capacity,
                "backoff_until_ms": self._backoff_until_ms,
                "backoff_remaining_ms": max(
                    0.0,
                    self._backoff_until_ms - time.monotonic() * 1000.0,
                ),
            }

    def is_in_backoff(self) -> bool:
        """是否在退避中。"""
        with self._lock:
            return time.monotonic() * 1000.0 < self._backoff_until_ms

    def health_check(self) -> Dict[str, Any]:
        """健康检查。"""
        with self._lock:
            self._refill_locked()
            now_ms = time.monotonic() * 1000.0
            in_backoff = now_ms < self._backoff_until_ms
            # 低优先级严重不可用（令牌少于预留量）
            order_ratio = self._order_tokens / self._order_capacity
            weight_ratio = self._weight_tokens / self._weight_capacity
            healthy = (
                not in_backoff
                and order_ratio > 0.1
                and weight_ratio > 0.1
            )
            return {
                "healthy": healthy,
                "name": self.name,
                "version": self.version,
                "order_tokens": self._order_tokens,
                "order_capacity": self._order_capacity,
                "order_ratio": order_ratio,
                "weight_tokens": self._weight_tokens,
                "weight_capacity": self._weight_capacity,
                "weight_ratio": weight_ratio,
                "in_backoff": in_backoff,
                "backoff_remaining_ms": max(
                    0.0, self._backoff_until_ms - now_ms
                ),
                "backoff_ms": self._backoff_ms,
                "stats": dict(self._stats),
                "timestamp": int(time.time() * 1000),
            }

    def reset(self) -> None:
        """重置。测试用。"""
        with self._lock:
            self._order_tokens = self._order_capacity
            self._weight_tokens = self._weight_capacity
            self._last_refill_ms = time.monotonic() * 1000.0
            self._backoff_ms = self._initial_backoff_ms
            self._backoff_until_ms = 0.0
            for k in self._stats:
                if k == "by_priority":
                    for p in self._stats["by_priority"].values():
                        for kk in p:
                            p[kk] = 0
                elif k == "wait_total_ms":
                    self._stats[k] = 0.0
                else:
                    self._stats[k] = 0
        logger.info("RateLimiter reset")


# ============================================================================
# MANIFEST
# ============================================================================
MANIFEST = {
    "name": "rate_limiter",
    "type": "system",
    "version": "1.0.0",
    "step": 1,
    "capabilities": ["rate_limit"],
    "module_dependencies": [],
    "fallback": None,
    "owner": "zhangsan",
    "description": "Token bucket rate limiter with priority and backoff",
}


# ============================================================================
# 工厂函数
# ============================================================================
def create(config: dict) -> RateLimiter:
    """模块工厂入口。"""
    return RateLimiter(config)
