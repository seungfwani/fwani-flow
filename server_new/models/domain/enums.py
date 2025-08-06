import logging
from enum import Enum

logger = logging.getLogger()


class FlowExecutionStatus(Enum):
    WAITING = "waiting"
    TRIGGERED = "triggered"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELED = "canceled"
    KILLED = "killed"
    ERROR = "error"
    SKIPPED = "skipped"

    @staticmethod
    def get_terminal_states():
        return {
            FlowExecutionStatus.CANCELED,
            FlowExecutionStatus.KILLED,
            FlowExecutionStatus.SUCCESS,
            FlowExecutionStatus.FAILED,
            FlowExecutionStatus.ERROR,
            FlowExecutionStatus.SKIPPED,
        }

    @classmethod
    def from_str(cls, value: str) -> "FlowExecutionStatus":
        """기본값 fallback 로직 포함된 변환 메서드"""
        fallback_map = {
            "up_for_retry": cls.RUNNING,
            "up_for_reschedule": cls.RUNNING,
            "deferred": cls.TRIGGERED,
            "removed": cls.CANCELED,
            "shutdown": cls.CANCELED,
            "upstream_failed": cls.FAILED,
        }
        # 직접 매핑된 fallback 우선
        if value in fallback_map:
            return fallback_map[value]
        # 혹시 정확히 Enum에 존재하면 그것도 허용
        try:
            return cls(value)
        except ValueError:
            logger.warning(f'Invalid value "{value}" for flow execution status')
            return cls.ERROR
