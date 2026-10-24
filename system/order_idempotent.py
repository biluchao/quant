"""
system/order_idempotent.py — 确定性 client_order_id 生成与解析

幂等不是"不重复下单"，是"重复下单时能识别出来"。

核心思路：
1. 给两次请求同一个 client_order_id
2. 交易所第二次收到会拒绝（-2010 Duplicate order）
3. 调用方捕获 -2010，查 client_order_id 对应的订单状态
4. 不是靠猜，是靠交易所告诉你

ID 格式：
    {prefix}-{kind}-{round:04d}-{seq:02d}-{attempt:02d}

示例：
    bm-ent-0042-00-00  第 42 轮，开仓，首次尝试
    bm-add-0042-01-00  第 42 轮，加仓第 1 层，首次尝试
    bm-add-0042-01-01  第 42 轮，加仓第 1 层，第二次尝试（已知失败后重试）
    bm-ext-0042-00-00  第 42 轮，平仓，首次尝试
    bm-stp-0042-00-00  第 42 轮，服务器端止损

字段语义：
- prefix:    全局前缀，可配（默认 "bm"），必须 [A-Za-z0-9]{1,8}
- kind:      订单类型（ENTRY/ADD/REDUCE/EXIT/STOP/TP/CLOSE_ALL）
- round:     round_index，全局轮次计数器（RoundManager 分配并持久化）
- seq:       该轮次内该 kind 的序号（ENTRY=0，ADD/REDUCE 从 1 开始）
- attempt:   逻辑重试次数（超时重试用 0，确定失败重试用 +1）

幂等语义：
- 超时重试：用相同 client_order_id（attempt 不变），交易所会识别为重复
- 确定失败重试：用新 client_order_id（attempt+1），交易所视为新订单
- 生成器不判断"应该用哪个 attempt"，由调用方决定

关键契约：
1. 确定性：相同输入必产出相同 ID
2. 无状态：生成器只读配置，天然线程安全
3. 长度上限：默认 36 字符（币安限制），生成时校验
4. 字符集：[A-Za-z0-9_.-]，正则校验
5. prefix 必须 [A-Za-z0-9]{1,8}，不含分隔符（防解析歧义）
6. 生成失败抛 OrderIdGenerationError
7. 解析失败抛 InvalidOrderIdError
8. parse(generate(...)) 必须还原原参数（往返一致性）
9. 策略层碰不到生成器，只有 OrderRouter / RoundManager 能调
10. 回测/影子用真实生成器，与实盘一致

与币安错误码的配合：
- -2010 "Duplicate order" → 视为重复，查 client_order_id 状态
- -2011 "Unknown order"   → 撤单时订单已成交，查成交明细
- 其他 -2010 message       → 视为拒单，按拒单处理

作者：张三
审查：比蒙（策略）、乐高（执行）
版本：v1.0.0
"""

from dataclasses import dataclass
from enum import Enum
import logging
import re
from typing import Dict

logger = logging.getLogger(__name__)


# ============================================================================
# 常量
# ============================================================================
DEFAULT_PREFIX = "bm"
DEFAULT_MAX_LENGTH = 36
MAX_PREFIX_LENGTH = 8
MIN_MAX_LENGTH = 17   # 最短合法 ID 长度（prefix=2, round=1 位, seq=1 位, attempt=1 位）
MAX_ALLOWED_LENGTH = 36

_ID_CHARSET_RE = re.compile(r"^[A-Za-z0-9_.\-]+$")
_PREFIX_RE = re.compile(r"^[A-Za-z0-9]{1,8}$")


# ============================================================================
# 订单类型
# ============================================================================
class OrderKind(str, Enum):
    """订单类型。kind 代码在 ID 中的表示。"""
    ENTRY = "ent"           # 开仓
    ADD = "add"             # 加仓（金字塔）
    REDUCE = "red"          # 减仓
    EXIT = "ext"            # 平仓
    STOP = "stp"            # 服务器端止损
    TAKE_PROFIT = "tp"      # 止盈
    CLOSE_ALL = "cls"       # 全平（紧急停止）


# ============================================================================
# 解析结果
# ============================================================================
@dataclass(frozen=True)
class ParsedOrderId:
    """client_order_id 解析后的结构。"""
    prefix: str
    kind: OrderKind
    round_index: int
    role_seq: int
    attempt: int
    raw: str

    def to_dict(self) -> dict:
        return {
            "prefix": self.prefix,
            "kind": self.kind.value,
            "round_index": self.round_index,
            "role_seq": self.role_seq,
            "attempt": self.attempt,
            "raw": self.raw,
        }


# ============================================================================
# 错误
# ============================================================================
class OrderIdError(Exception):
    """订单 ID 相关错误基类。"""
    pass


class OrderIdGenerationError(OrderIdError):
    """生成 ID 时出错（长度超限、参数非法）。"""
    pass


class InvalidOrderIdError(OrderIdError):
    """解析 ID 时出错（格式非法、prefix 不匹配、kind 未知）。"""
    pass


# ============================================================================
# 生成器
# ============================================================================
class OrderIdGenerator:
    """
    确定性 client_order_id 生成与解析。详见模块 docstring。

    用法：
        gen = OrderIdGenerator(config)

        # 开仓（首次）
        oid = gen.generate(OrderKind.ENTRY, round_index=42)
        # -> "bm-ent-0042-00-00"

        # 加仓第 1 层
        oid = gen.generate(OrderKind.ADD, round_index=42, role_seq=1)
        # -> "bm-add-0042-01-00"

        # 超时重试（相同 ID）
        oid_retry = gen.generate(OrderKind.ENTRY, round_index=42)
        assert oid == oid_retry  # 确定性

        # 确定失败后重试（新 attempt）
        oid_new = gen.generate(OrderKind.ENTRY, round_index=42, attempt=1)
        # -> "bm-ent-0042-00-01"

        # 解析
        parsed = gen.parse("bm-ent-0042-00-00")
        assert parsed.kind == OrderKind.ENTRY
        assert parsed.round_index == 42
        assert parsed.role_seq == 0
        assert parsed.attempt == 0

        # 校验（不关心结果时）
        gen.validate("bm-ent-0042-00-00")  # 成功返回 None，失败抛异常
    """

    name = "order_idempotent"
    version = "1.0.0"

    def __init__(self, config: dict):
        self.config = config
        cfg = config.get("order_id", {})

        prefix = cfg.get("prefix", DEFAULT_PREFIX)
        max_length = int(cfg.get("max_length", DEFAULT_MAX_LENGTH))

        # 校验 prefix
        if not isinstance(prefix, str) or not prefix:
            raise OrderIdGenerationError(
                f"prefix must be non-empty str, got {prefix!r}"
            )
        if not _PREFIX_RE.match(prefix):
            raise OrderIdGenerationError(
                f"prefix must match [A-Za-z0-9]{{1,{MAX_PREFIX_LENGTH}}}, "
                f"got {prefix!r}"
            )

        # 校验 max_length
        if max_length < MIN_MAX_LENGTH or max_length > MAX_ALLOWED_LENGTH:
            raise OrderIdGenerationError(
                f"max_length must be in [{MIN_MAX_LENGTH}, "
                f"{MAX_ALLOWED_LENGTH}], got {max_length}"
            )

        self.prefix = prefix
        self.max_length = max_length

        logger.info(
            f"OrderIdGenerator initialized "
            f"prefix={prefix!r} max_length={max_length}"
        )

    # ========================================================================
    # 生成
    # ========================================================================

    def generate(
        self,
        kind: OrderKind,
        round_index: int,
        role_seq: int = 0,
        attempt: int = 0,
    ) -> str:
        """
        生成 client_order_id。确定性：同参数必产同结果。

        Args:
            kind: 订单类型
            round_index: 轮次全局计数器（>= 0）
            role_seq: 该轮次内该 kind 的序号（>= 0）
            attempt: 逻辑重试次数（>= 0）

        Returns:
            client_order_id 字符串

        Raises:
            OrderIdGenerationError: 参数非法或 ID 过长
        """
        # 参数校验
        if not isinstance(kind, OrderKind):
            raise OrderIdGenerationError(
                f"kind must be OrderKind, got {type(kind).__name__}"
            )
        if not isinstance(round_index, int) or round_index < 0:
            raise OrderIdGenerationError(
                f"round_index must be non-negative int, got {round_index!r}"
            )
        if not isinstance(role_seq, int) or role_seq < 0:
            raise OrderIdGenerationError(
                f"role_seq must be non-negative int, got {role_seq!r}"
            )
        if not isinstance(attempt, int) or attempt < 0:
            raise OrderIdGenerationError(
                f"attempt must be non-negative int, got {attempt!r}"
            )

        # 构造
        order_id = (
            f"{self.prefix}-{kind.value}-"
            f"{round_index:04d}-{role_seq:02d}-{attempt:02d}"
        )

        # 长度校验
        if len(order_id) > self.max_length:
            raise OrderIdGenerationError(
                f"order_id length {len(order_id)} exceeds "
                f"max_length {self.max_length}: {order_id}"
            )

        # 字符集校验
        if not _ID_CHARSET_RE.match(order_id):
            raise OrderIdGenerationError(
                f"order_id contains invalid characters: {order_id}"
            )

        return order_id

    # ========================================================================
    # 解析
    # ========================================================================

    def parse(self, order_id: str) -> ParsedOrderId:
        """
        解析 client_order_id。

        Args:
            order_id: 待解析的 ID

        Returns:
            ParsedOrderId

        Raises:
            InvalidOrderIdError: 格式非法、prefix 不匹配、kind 未知
        """
        if not isinstance(order_id, str):
            raise InvalidOrderIdError(
                f"order_id must be str, got {type(order_id).__name__}"
            )
        if not order_id:
            raise InvalidOrderIdError("order_id is empty")
        if len(order_id) > self.max_length:
            raise InvalidOrderIdError(
                f"order_id length {len(order_id)} exceeds "
                f"max_length {self.max_length}: {order_id}"
            )
        if not _ID_CHARSET_RE.match(order_id):
            raise InvalidOrderIdError(
                f"order_id contains invalid characters: {order_id}"
            )

        parts = order_id.split("-")
        if len(parts) != 5:
            raise InvalidOrderIdError(
                f"expected 5 segments, got {len(parts)}: {order_id}"
            )

        prefix, kind_str, round_str, seq_str, attempt_str = parts

        if prefix != self.prefix:
            raise InvalidOrderIdError(
                f"prefix mismatch: {prefix!r} != {self.prefix!r} "
                f"in {order_id}"
            )

        try:
            kind = OrderKind(kind_str)
        except ValueError:
            raise InvalidOrderIdError(
                f"unknown kind: {kind_str!r} in {order_id}"
            )

        try:
            round_index = int(round_str)
            role_seq = int(seq_str)
            attempt = int(attempt_str)
        except ValueError:
            raise InvalidOrderIdError(
                f"non-integer field in {order_id}"
            )

        if round_index < 0 or role_seq < 0 or attempt < 0:
            raise InvalidOrderIdError(
                f"negative field in {order_id}"
            )

        return ParsedOrderId(
            prefix=prefix,
            kind=kind,
            round_index=round_index,
            role_seq=role_seq,
            attempt=attempt,
            raw=order_id,
        )

    # ========================================================================
    # 校验
    # ========================================================================

    def validate(self, order_id: str) -> None:
        """
        校验 client_order_id 合法性。

        成功返回 None，失败抛 InvalidOrderIdError。
        等价于 parse(order_id) 但不返回结果。
        """
        self.parse(order_id)

    def is_valid(self, order_id: str) -> bool:
        """非抛异常版校验。返回 True/False。"""
        try:
            self.validate(order_id)
            return True
        except InvalidOrderIdError:
            return False


# ============================================================================
# MANIFEST
# ============================================================================
MANIFEST = {
    "name": "order_idempotent",
    "type": "system",
    "version": "1.0.0",
    "step": 1,
    "capabilities": ["order_idempotent"],
    "module_dependencies": [],
    "fallback": None,
    "owner": "lego",
    "description": "Deterministic client_order_id generation and parsing",
}


# ============================================================================
# 工厂函数
# ============================================================================
def create(config: dict) -> OrderIdGenerator:
    """模块工厂入口。"""
    return OrderIdGenerator(config)
