"""
risk/emergency_stop.py — 紧急停止断路器 (重铸标准版)

核心设计原则：
1. 幂等阻断：触发后立即锁死状态，杜绝重复并发清仓。
2. 降级保命：先撤单，再平仓。若管理器平仓失败，直接调用网关无差别清仓。
3. 状态闭环：无论清算成功与否，最终强制切断当前状态机，回到安全状态。
"""

import logging
import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

class StopPhase(str, Enum):
    NORMAL = "normal"
    TRIGGERING = "triggering"
    TRIGGERED = "triggered"
    FAILED = "failed"
    CLEARED = "cleared"

class TriggerSource(str, Enum):
    MANUAL = "manual"
    RISK_POLICY = "risk_policy"
    WATCHDOG = "watchdog"
    DATA_FEED = "data_feed"
    EXTERNAL_API = "external_api"
    SYSTEM_ERROR = "system_error"

@dataclass
class TriggerEvent:
    event_id: str
    ts: int
    source: TriggerSource
    reason: str
    operator: Optional[str]
    phase: StopPhase = StopPhase.TRIGGERING
    details: Dict[str, Any] = None
    error: Optional[str] = None


class EmergencyStop:
    """
    紧急停止执行器。
    """
    def __init__(
        self,
        gateway: Any,
        round_manager: Any,
        state_machine: Any,
        clock: Any,
        config: dict,
    ):
        if not gateway or not round_manager or not state_machine or not clock:
            raise ValueError("EmergencyStop 缺少必要的依赖注入 (gateway, round_manager, state_machine, clock)")

        self.gateway = gateway
        self.round_manager = round_manager
        self.state_machine = state_machine
        self.clock = clock
        self.config = config

        self._lock = threading.RLock()
        self._phase = StopPhase.NORMAL
        self._active_event: Optional[TriggerEvent] = None

    @property
    def phase(self) -> StopPhase:
        with self._lock:
            return self._phase

    def is_triggered(self) -> bool:
        with self._lock:
            return self._phase in (StopPhase.TRIGGERING, StopPhase.TRIGGERED, StopPhase.FAILED)

    # ========================================================================
    # 触发紧急停止
    # ========================================================================

    def trigger(self, source: TriggerSource, reason: str, operator: str = "system") -> TriggerEvent:
        """
        触发紧急停止。包含严密的降级清仓逻辑。
        """
        with self._lock:
            # 幂等拦截：如果在触发中或已触发，直接返回
            if self._phase in (StopPhase.TRIGGERING, StopPhase.TRIGGERED, StopPhase.FAILED):
                logger.warning(f"紧急停止已被触发，忽略本次请求。当前状态: {self._phase.value}")
                return self._active_event
            
            self._phase = StopPhase.TRIGGERING
            ts = self.clock.now_ms()
            event = TriggerEvent(
                event_id=f"emg-{ts}",
                ts=ts,
                source=source,
                reason=reason,
                operator=operator
            )
            self._active_event = event

        logger.critical(f"🚨 触发紧急停止! 来源: {source.value}, 原因: {reason}")

        # 1. 撤掉所有挂单（包括止损单）
        try:
            # 优先通过 round_manager 撤单，保证状态同步
            if hasattr(self.round_manager, "cancel_all_orders"):
                self.round_manager.cancel_all_orders()
            else:
                self.gateway.cancel_all_orders()
            logger.info("紧急停止：所有挂单已撤回。")
        except Exception as e:
            logger.error(f"紧急停止：撤单阶段发生异常 (继续执行平仓): {e}")

        # 2. 强制平仓（核心保命环节）
        close_success = False
        try:
            # 尝试通过正常的账本平仓
            if hasattr(self.round_manager, "close_all_rounds"):
                self.round_manager.close_all_rounds(reason="emergency_stop")
            else:
                # 降级到单轮平仓
                active = self.round_manager.get_active_round()
                if active:
                    self.round_manager.close_round(exit_reason="emergency_stop")
            close_success = True
            logger.info("紧急停止：通过 RoundManager 成功平仓。")
        except Exception as e:
            logger.critical(f"紧急停止：通过 RoundManager 平仓失败！启动网关强行清仓。原因: {e}")
            
            # 3. 终极防线：绕过所有业务逻辑，直接让交易所清仓
            try:
                self.gateway.close_all_positions(reason="emergency_fallback")
                close_success = True
                logger.info("紧急停止：通过 Gateway 降级清仓成功。")
            except Exception as gw_e:
                logger.critical(f"🚨 致命错误：紧急降级平仓也失败了！系统可能裸奔！原因: {gw_e}")
                event.error = f"Fallback close failed: {str(gw_e)}"

        # 4. 强制状态机复位
        try:
            if hasattr(self.state_machine, "emergency_reset"):
                self.state_machine.emergency_reset()
        except Exception as e:
            logger.error(f"紧急停止：重置状态机失败: {e}")

        # 5. 更新最终阶段
        with self._lock:
            if close_success:
                self._phase = StopPhase.TRIGGERED
                event.phase = StopPhase.TRIGGERED
            else:
                self._phase = StopPhase.FAILED
                event.phase = StopPhase.FAILED
                
        return event

    # ========================================================================
    # 清除并恢复
    # ========================================================================

    def clear(self, operator: str, reason: str) -> bool:
        """
        解除紧急停止状态，允许系统恢复运行。
        """
        with self._lock:
            if self._phase not in (StopPhase.TRIGGERED, StopPhase.FAILED):
                logger.warning("当前状态不是已停止，无法清除。")
                return False

            self._phase = StopPhase.CLEARED
            logger.info(f"紧急停止已解除。操作员: {operator}, 原因: {reason}")
            
            # 触发解除后的回调（如果系统需要的话，比如恢复风控日切）
            return True

    # ========================================================================
    # 健康检查
    # ========================================================================

    def health_check(self) -> dict:
        with self._lock:
            return {
                "healthy": self._phase != StopPhase.FAILED,
                "phase": self._phase.value,
                "active_event": self._active_event.event_id if self._active_event else None
    }
