import json
import logging
import re
from datetime import datetime
from typing import List, Any, Optional, Tuple

from pydantic import BaseModel, Field, model_validator, ConfigDict, field_validator

from models.airflow_dag_run_history import AirflowDagRunHistory, AirflowDagRunSnapshotTask
from models.flow_version import FlowVersion
from models.task import Task
from utils.functions import string2datetime

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


class AirflowTaskInstanceModel(BaseModel):
    task_id: str
    execution_date: Optional[datetime]
    start_date: Optional[datetime]
    end_date: Optional[datetime]
    duration: Optional[float]
    operator: Optional[str]
    queued_when: Optional[datetime]
    status: Optional[str]
    try_number: Optional[int]

    @classmethod
    def from_json(cls, data: dict):
        return cls(
            task_id=data["task_id"],
            execution_date=string2datetime(data.get("execution_date")),
            start_date=string2datetime(data.get("start_date")),
            end_date=string2datetime(data.get("end_date")),
            duration=data.get("duration"),
            operator=data.get("operator"),
            queued_when=string2datetime(data.get("queued_when")),
            status=data.get("state"),
            try_number=data.get("try_number"),
        )

    def __eq__(self, other):
        if not isinstance(other, AirflowTaskInstanceModel):
            return False
        return (
                self.task_id == other.task_id and
                self.status == other.status
        )

    def __hash__(self):
        return hash((self.task_id, self.status))


# DAG 데이터 모델 정의
class DAGNodeData(BaseModel):
    function_id: str = Field(..., description="실행할 UDF ID", examples=["00000000-0000-4000-9000-000000000000"])
    inputs: dict[str, Any] = Field({}, description="UDF 실행시 input 값", examples=[{"key1": "value1", "key2": "value2"}])
    label: Optional[str] = Field("", examples=["function name"])

    extra_data: Optional[AirflowTaskInstanceModel] = Field(default_factory=lambda: None)

    def __eq__(self, other):
        if not isinstance(other, DAGNodeData):
            return False
        return (
                self.function_id == other.function_id and
                self.extra_data == other.extra_data
        )

    def __hash__(self):
        return hash((self.function_id, self.extra_data))


class DAGNode(BaseModel):
    id: str = Field(..., description="node id", examples=["00000000-0000-4000-9000-000000000000"])
    type: str = Field("custom", description="UI node 타입", examples=["custom"])
    position: dict[str, float] = Field(default_factory=lambda: {"x": 0, "y": 0}, description="UI node 좌표",
                                       examples=[{"x": 0, "y": 0}])
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

    @model_validator(mode="before")
    @classmethod
    def fill_defaults(cls, values):
        # ✅ type 이 None 이거나 빈 문자열이면 "custom" 으로 세팅
        if not values.get("label"):
            values["label"] = ""
        return values

    @classmethod
    def from_data(cls, task: Task, ti: dict = None):
        return cls(
            id=task.id,
            type=task.task_ui.type if task.task_ui else "custom",
            position=task.task_ui.position if task.task_ui else {"x": 0, "y": 0},
            style=task.task_ui.style if task.task_ui else {},
            data=DAGNodeData(
                function_id=task.function_id,
                inputs={inp.key: inp.value for inp in task.inputs},
                label=task.function.name if task.function else "",
                extra_data=AirflowTaskInstanceModel.from_json(ti) if ti else None,
            )
        )

    @classmethod
    def from_data_with_ti_of_airflow(cls, task: AirflowDagRunSnapshotTask, ti: dict):
        return cls(
            id=task.task_id,
            type=task.type,
            position=task.position,
            style=task.style,
            data=DAGNodeData(
                function_id=task.function_id,
                inputs=task.inputs,
                label=task.label,
                extra_data=AirflowTaskInstanceModel.from_json(ti) if ti else None,
            )
        )


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
    description: str = Field(..., description="DAG Description", examples=["DAG Description"])
    owner: Optional[str] = Field(None, description="DAG Owner", examples=["DAG Owner"])
    nodes: List[DAGNode]
    edges: List[DAGEdge]
    schedule: Optional[str] = Field(None, description="DAG schedule", examples=["0 9 * * *"])

    @field_validator("schedule")
    @classmethod
    def check_docker_image_required(cls, v):
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
    description: str = Field(..., description="DAG Description", examples=["DAG Description"])
    is_draft: bool
    version: Optional[int]
    nodes: List[DAGNode]
    edges: List[DAGEdge]
    schedule: Optional[str] = Field(None, description="DAG schedule", examples=["0 9 * * *"])

    @classmethod
    def from_dag(cls, flow_version: FlowVersion):
        if not flow_version:
            return None
        try:
            nodes = [DAGNode.from_data(task) for task in flow_version.tasks]
        except Exception as e:
            logger.warning(e)
            nodes = []

        try:
            edges = [DAGEdge(
                id=edge.id,
                type=edge.edge_ui.type if edge.edge_ui else "custom",
                source=edge.from_task_id,
                target=edge.to_task_id,
                label=edge.edge_ui.label if edge.edge_ui else "",
                style=edge.edge_ui.style if edge.edge_ui else {},
            ) for edge in flow_version.edges]
        except Exception as e:
            logger.warning(e)
            edges = []

        return cls(
            id=flow_version.flow.id,
            name=flow_version.flow.name,
            description=flow_version.flow.description,
            is_draft=flow_version.is_draft,
            version=flow_version.version,
            nodes=nodes,
            edges=edges,
            schedule=flow_version.schedule,
        )


class AirflowDagRunModel(BaseModel):
    id: str
    dag_id: str
    version: int
    is_draft: bool
    run_id: str
    execution_date: Optional[datetime]
    start_date: Optional[datetime]
    end_date: Optional[datetime]
    status: Optional[str]
    external_trigger: Optional[bool] = True
    run_type: Optional[str]
    conf: Optional[dict] = {}  # JSON 문자열을 dict로 역직렬화
    source: Optional[str] = "airflow"

    def __eq__(self, other):
        if not isinstance(other, AirflowDagRunModel):
            return False
        return (
                self.id == other.id and
                self.run_id == other.run_id and
                self.status == other.status
        )

    def __hash__(self):
        return hash((self.id, self.run_id, self.status))

    @classmethod
    def from_orm(cls, data: AirflowDagRunHistory):
        return cls(
            id=data.id,
            dag_id=data.flow_version.flow_id,
            version=data.flow_version.version,
            is_draft=data.flow_version.is_draft,
            run_id=data.run_id,
            execution_date=data.execution_date,
            start_date=data.start_date,
            end_date=data.end_date,
            status=data.status,
            external_trigger=data.external_trigger,
            run_type=data.run_type,
            conf=json.loads(data.conf),
            source=data.source,
        )


class TaskInstanceResponse(BaseModel):
    id: str = Field(..., description="Generated DAG ID", examples=["00000000-0000-4000-9000-000000000000"])
    name: str = Field(..., description="DAG Name", examples=["DAG Name"])
    description: str = Field(..., description="DAG Description", examples=["DAG Description"])
    is_draft: bool
    version: Optional[int]
    nodes: List[DAGNode]
    edges: List[DAGEdge]

    def __eq__(self, other):
        if not isinstance(other, TaskInstanceResponse):
            return False
        return (
                self.id == other.id and
                self.nodes == other.nodes and
                self.edges == other.edges
        )

    def __hash__(self):
        return hash((self.id, self.nodes, self.edges))

    @classmethod
    def from_data(cls, airflow_dag_run_history: AirflowDagRunHistory,
                  data: List[Tuple[AirflowDagRunSnapshotTask, dict]]):
        try:
            nodes = [DAGNode.from_data_with_ti_of_airflow(task, ti) for task, ti in data]
        except Exception as e:
            logger.warning(e)
            nodes = []

        try:
            edges = [DAGEdge(
                id=edge.edge_id,
                type=edge.type,
                source=edge.source,
                target=edge.target,
                label=edge.label,
                labelStyle=edge.labelStyle,
                labelBgStyle=edge.labelBgStyle,
                labelBgPadding=edge.labelBgPadding,
                labelBgBorderRadius=edge.labelBgBorderRadius,
                style=edge.style,
            ) for edge in airflow_dag_run_history.snapshot_edges]
        except Exception as e:
            logger.warning(e)
            edges = []

        return cls(
            id=airflow_dag_run_history.flow_version.flow.id,
            name=airflow_dag_run_history.flow_version.flow.name,
            description=airflow_dag_run_history.flow_version.flow.description,
            is_draft=airflow_dag_run_history.flow_version.is_draft,
            version=airflow_dag_run_history.flow_version.version,
            nodes=nodes,
            edges=edges,
        )
