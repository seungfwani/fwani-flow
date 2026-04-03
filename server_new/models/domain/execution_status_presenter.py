from __future__ import annotations

from typing import Literal, Protocol, runtime_checkable

from models.domain.enums import FlowExecutionStatus

ApiExecutionStatus = Literal["success", "failed", "running"]

_SUCCESS: frozenset[FlowExecutionStatus] = frozenset({FlowExecutionStatus.SUCCESS})
_FAILED: frozenset[FlowExecutionStatus] = frozenset({
    FlowExecutionStatus.FAILED,
    FlowExecutionStatus.ERROR,
    FlowExecutionStatus.KILLED,
    FlowExecutionStatus.CANCELED,
    FlowExecutionStatus.SKIPPED,
})
_RUNNING: frozenset[FlowExecutionStatus] = frozenset({
    FlowExecutionStatus.WAITING,
    FlowExecutionStatus.TRIGGERED,
    FlowExecutionStatus.RUNNING,
})


def _bucket_name(s: FlowExecutionStatus) -> ApiExecutionStatus:
    if s in _SUCCESS:
        return "success"
    if s in _FAILED:
        return "failed"
    return "running"


def _values_for_bucket(name: str) -> frozenset[str]:
    if name == "success":
        return frozenset(e.value for e in _SUCCESS)
    if name == "failed":
        return frozenset(e.value for e in _FAILED)
    if name == "running":
        return frozenset(e.value for e in _RUNNING)
    return frozenset()


@runtime_checkable
class ExecutionStatusPresenter(Protocol):
    def present(self, raw: str | None) -> ApiExecutionStatus | None: ...

    def expand_filter_values(self, requested: set[str]) -> set[str]: ...


class DefaultExecutionStatusPresenter:
    def present(self, raw: str | None) -> ApiExecutionStatus | None:
        if raw is None:
            return None
        s = FlowExecutionStatus.from_str(raw)
        return _bucket_name(s)

    def expand_filter_values(self, requested: set[str]) -> set[str]:
        out: set[str] = set()
        for token in requested:
            key = (token or "").strip().lower()
            out.update(_values_for_bucket(key))
        return out


_presenter: ExecutionStatusPresenter = DefaultExecutionStatusPresenter()


def get_execution_status_presenter() -> ExecutionStatusPresenter:
    return _presenter


def set_execution_status_presenter(presenter: ExecutionStatusPresenter) -> None:
    global _presenter
    _presenter = presenter
