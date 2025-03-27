import logging
from typing import List, Any, Optional

from pydantic import BaseModel, Field, model_validator, ConfigDict

from models.flow import Flow

logger = logging.getLogger()


# DAG 데이터 모델 정의
class DAGNodeData(BaseModel):
    function_id: str = Field(..., description="실행할 UDF ID", examples=["00000000-0000-4000-9000-000000000000"])
    inputs: dict[str, Any] = Field({}, description="UDF 실행시 input 값", examples=[{"key1": "value1", "key2": "value2"}])
    label: Optional[str] = Field("", examples=["function name"])


class DAGNode(BaseModel):
    id: str = Field(..., description="node id", examples=["00000000-0000-4000-9000-000000000000"])
    type: str = Field("custom", description="UI node 타입", examples=["custom"])
    position: dict[str, float] = Field(default_factory=lambda: {"x": 0, "y": 0}, description="UI node 좌표",
                                       examples=[{"x": 0, "y": 0}])
    data: DAGNodeData
    style: Optional[dict[str, Any]] = Field({}, examples=[{"key1": "value1", "key2": "value2"}])

    model_config = ConfigDict(extra="ignore")

    @model_validator(mode="before")
    @classmethod
    def fill_defaults(cls, values):
        # ✅ type 이 None 이거나 빈 문자열이면 "custom" 으로 세팅
        if not values.get("label"):
            values["label"] = ""
        return values


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


class DAGRequest(BaseModel):
    name: str = Field(..., description="DAG Name", examples=["DAG Name"])
    description: str = Field(..., description="DAG Description", examples=["DAG Description"])
    nodes: List[DAGNode]
    edges: List[DAGEdge]


class DAGResponse(BaseModel):
    id: str = Field(..., description="Generated DAG ID", examples=["00000000-0000-4000-9000-000000000000"])
    name: str = Field(..., description="DAG Name", examples=["DAG Name"])
    description: str = Field(..., description="DAG Description", examples=["DAG Description"])
    nodes: List[DAGNode]
    edges: List[DAGEdge]

    @classmethod
    def from_dag(cls, dag: Flow):
        try:
            nodes = [DAGNode(
                id=task.id,
                type=task.task_ui.type if task.task_ui else "custom",
                position=task.task_ui.position if task.task_ui else {"x": 0, "y": 0},
                style=task.task_ui.style if task.task_ui else {},
                data=DAGNodeData(
                    function_id=task.function_id,
                    inputs={inp.key: inp.value for inp in task.inputs},
                    label=task.function.name if task.function else "",
                )
            ) for task in dag.tasks]
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
            ) for edge in dag.edges]
        except Exception as e:
            logger.warning(e)
            edges = []

        return cls(
            id=dag.id,
            name=dag.name,
            description=dag.description,
            nodes=nodes,
            edges=edges,
        )
