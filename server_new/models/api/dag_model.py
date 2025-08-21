import logging
import re
from datetime import datetime
from typing import List, Any, Optional, Literal, Annotated, Union

from pydantic import BaseModel, Field, ConfigDict, field_validator, model_validator

logger = logging.getLogger()

# * * * * *
# │ │ │ │ └──── 요일 (0=일요일 ~ 6=토요일)
# │ │ │ └────── 월 (1~12)
# │ │ └──────── 일 (1~31)
# │ └────────── 시 (0~23)
# └──────────── 분 (0~59)
# 표현식          | 설명
# 0 8 * * *     | 매일 오전 8시
# */15 * * * *  | 15분마다
# 0 9 * * 1-5   | 평일 오전 9시
# 0 0 1 * *     | 매월 1일 자정
# @daily        | 매일 자정 (예약어)
SCHEDULE_KEYWORDS = ["once", "hourly", "daily", "weekly", "monthly", "yearly"]
SCHEDULE_KEYWORDS_EXP = r"@(" + "|".join(SCHEDULE_KEYWORDS) + ")"
MINUTE_UNIT = r"[0-5]?\d"
HOUR_UNIT = r"[01]?\d|2[0-3]"
DAY_UNIT = r"[1-9]|[12]\d|3[01]"
MONTH_UNIT = r"0?[1-9]|1[0-2]"
DAY_OF_WEEK_UNIT = r"[0-6]"

SLASH_FORMAT = "{0}/{1}"
RANGE_FORMAT = "{0}-{1}"
MULTIPLE_FORMAT = "{0},{1}"

CRON_EXP_FORMAT = r"(\*|{0}|{0}-{0})(/{0})?"
MINUTE_EXP = CRON_EXP_FORMAT.format(MINUTE_UNIT)
HOUR_EXP = CRON_EXP_FORMAT.format(HOUR_UNIT)
DAY_EXP = CRON_EXP_FORMAT.format(DAY_UNIT)
MONTH_EXP = CRON_EXP_FORMAT.format(MONTH_UNIT)
DAY_OF_WEEK_EXP = CRON_EXP_FORMAT.format(DAY_OF_WEEK_UNIT)
CRON_REGEX = (r"^("
              + SCHEDULE_KEYWORDS_EXP
              + "|"
              + "("
              + rf"{MINUTE_EXP}(,{MINUTE_UNIT})*\s+"
              + rf"{HOUR_EXP}(,{HOUR_EXP})*\s+"
              + rf"{DAY_EXP}(,{DAY_EXP})*\s+"
              + rf"{MONTH_EXP}(,{MONTH_EXP})*\s+"
              + rf"{DAY_OF_WEEK_EXP}(,{DAY_OF_WEEK_EXP})*"
              + r")"
              + r")$")


class BaseNodeData(BaseModel):
    label: Optional[str] = Field("", examples=["node name"])
    input_meta_type: Optional[dict] = Field({}, description="graphio input meta type")
    output_meta_type: Optional[dict] = Field({}, description="function output meta type")

    inputs: dict[str, Any] = Field({}, description="system, meta 의 code 실행시 필요한 input 값",
                                   examples=[{"key1": "value1", "key2": "value2"}])

    model_config = {
        "extra": "allow"
    }

    def to_json(self):
        return self.model_dump()


class CodeNodeData(BaseNodeData):
    kind: Literal["code"] = "code"
    python_libraries: List[str] = Field([], description="requirements.txt 에 작성하는 포멧",
                                        examples=[['pandas==2.3.1', 'requests==2.32.3']])
    code: str = Field("", description="python code", examples=["def run():\n    return 1"])
    builtin_func_id: None | str = Field(None, description="code 타입에서는 사용하지 않음")


class MetaNodeData(BaseNodeData):
    kind: Literal["meta"] = "meta"
    python_libraries: List[str] | None = Field([], description="requirements.txt 에 작성하는 포멧",
                                               examples=[['pandas==2.3.1', 'requests==2.32.3']])
    code: str | None = Field(None, description="meta/system 에서는 비움")
    builtin_func_id: str | None = Field(None, description="built-in function id")


class SystemNodeData(BaseNodeData):
    kind: Literal["system"] = "system"
    python_libraries: List[str] | None = Field([], description="requirements.txt 에 작성하는 포멧",
                                               examples=[['pandas==2.3.1', 'requests==2.32.3']])
    code: str | None = Field(None, description="meta/system 에서는 비움")
    builtin_func_id: str = Field(..., description="built-in function id")

    @model_validator(mode="after")
    def _enforce_system_constraints(self):
        if not self.builtin_func_id:
            raise ValueError("kind=system 에서는 builtin_func_id 가 필수입니다.")
        return self


DAGNodeData = Annotated[
    Union[CodeNodeData, MetaNodeData, SystemNodeData],
    Field(discriminator="kind"),
]


class DAGNode(BaseModel):
    id: str = Field(..., description="node id", examples=["00000000-0000-4000-9000-000000000000"])
    type: str = Field("custom", description="UI node 타입", examples=["custom"])
    position: dict[str, float] = Field(default_factory=lambda: {"x": 0, "y": 0}, description="UI node 좌표",
                                       examples=[{"x": 0, "y": 0}])
    class_: Optional[str] = Field(None, alias="class")
    data: DAGNodeData
    style: Optional[dict[str, Any]] = Field({}, examples=[{"key1": "value1", "key2": "value2"}])

    model_config = ConfigDict(extra="ignore")

    def __eq__(self, other):
        if not isinstance(other, DAGNode):
            return False
        return (
                self.id == other.id and
                self.data == other.data
        )

    def __hash__(self):
        return hash((self.id, self.data))


class DAGEdge(BaseModel):
    id: str = Field(..., description="UI edge id", examples=["00000000-0000-4000-9000-000000000000"])
    type: str = Field("custom", description="UI edge 타입 ", examples=["custom"])
    source: str = Field(..., description="Source Node ID", examples=["00000000-0000-4000-9000-000000000000"])
    target: str = Field(..., description="Target Node ID", examples=["00000000-0000-4000-9000-000000000001"])
    label: Optional[str] = Field("", description="edge label")
    labelStyle: Optional[dict[str, Any]] = Field(default_factory=dict)
    labelBgStyle: Optional[dict[str, Any]] = Field(default_factory=dict)
    labelBgPadding: Optional[List[float]] = Field(default_factory=lambda: [0, 0])
    labelBgBorderRadius: Optional[float] = Field(default_factory=float)
    style: Optional[dict[str, Any]] = Field({})

    model_config = ConfigDict(extra="ignore")

    def __eq__(self, other):
        if not isinstance(other, DAGEdge):
            return False
        return (
                self.id == other.id and
                self.source == other.source and
                self.target == other.target
        )

    def __hash__(self):
        return hash((self.id, self.source, self.target))


class DAGRequest(BaseModel):
    name: str = Field(..., description="DAG Name", examples=["DAG Name"])
    description: Optional[str] = Field(None, description="DAG Description", examples=["DAG Description"])
    owner: Optional[str] = Field(None, description="DAG Owner", examples=["DAG Owner"])
    nodes: List[DAGNode]
    edges: List[DAGEdge]
    schedule: Optional[str] = Field(None, description="DAG schedule", examples=["0 9 * * *"])
    max_retries: Optional[int] = Field(0, description="DAG max retries. default 0")
    is_draft: bool = Field(False, description="DAG Draft Status", examples=[True, False])

    @field_validator("schedule")
    @classmethod
    def check_schedule_format(cls, v):
        if v is None:
            return v
        if re.match(CRON_REGEX, v):
            return v
        raise ValueError(
            f"Invalid schedule format: '{v}'"
            f" — must be a cron expression or one of {["@" + kw for kw in SCHEDULE_KEYWORDS]}")


class DAGResponse(BaseModel):
    id: str = Field(..., description="Generated DAG ID", examples=["00000000-0000-4000-9000-000000000000"])
    name: str = Field(..., description="DAG Name", examples=["DAG Name"])
    description: Optional[str] = Field(None, description="DAG Description", examples=["DAG Description"])
    owner: Optional[str] = Field(None, description="DAG Description", examples=["DAG Description"])
    nodes: List[DAGNode]
    edges: List[DAGEdge]
    schedule: Optional[str] = Field(None, description="DAG schedule", examples=["0 9 * * *"])
    is_draft: bool = Field(False, description="DAG Draft Status", examples=[True, False])
    max_retries: Optional[int] = Field(0, description="DAG max retries. default 0")
    updated_at: Optional[datetime] = Field(None, description="DAG Updated at", examples=["2020-10-18 00:00:00"])
    active_status: bool = Field(False, description="DAG Active", examples=[True, False])
    execution_status: Optional[str] = Field(None, description="DAG Last Execution Status",
                                            examples=["waiting", "success", "failed"])


class ExecutionResponse(BaseModel):
    id: str = Field(..., description="Execution ID", examples=["00000000-0000-4000-9000-000000000000"])
    flow_id: str = Field(..., description="Flow ID", examples=["00000000-0000-4000-9000-000000000000"])
    status: str = Field(..., description="Execution Status", examples=["success", "failed"])
    scheduled_time: Optional[datetime] = Field(..., description="Execution scheduled time")
    triggered_time: Optional[datetime] = Field(..., description="Execution scheduled time")


class TaskExecutionModel(BaseModel):
    task_id: str = Field("")
    execution_date: Optional[datetime] = Field("")
    start_date: Optional[datetime] = Field("")
    end_date: Optional[datetime] = Field("")
    duration: Optional[float] = Field(0)
    operator: Optional[str] = Field("")
    queued_when: Optional[datetime] = Field("")
    status: Optional[str] = Field("")
    try_number: Optional[int] = Field(0)

    @classmethod
    def from_data(cls, data):
        return cls(
            task_id=data.task_id,
            execution_date=data.execution_date,
            start_date=data.start_date,
            end_date=data.end_date,
            duration=data.duration,
            operator=data.operator,
            queued_when=data.queued_when,
            status=data.status,
            try_number=data.try_number,
        )

    def __eq__(self, other):
        if not isinstance(other, TaskExecutionModel):
            return False
        return (
                self.task_id == other.task_id and
                self.status == other.status
        )

    def __hash__(self):
        return hash((self.task_id, self.status))


class ActiveStatusRequest(BaseModel):
    active_status: bool = Field(False, description="DAG Active", examples=[True, False])


class MultipleRequest(BaseModel):
    ids: list[str] = Field([], description="dag id list")
