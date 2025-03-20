from typing import List, Any, Optional

from pydantic import BaseModel, Field, model_validator

from models.flow import Flow


# DAG 데이터 모델 정의
class DAGNodeData(BaseModel):
    function_id: str = Field(..., description="실행할 UDF ID")
    inputs: dict[str, Any] = Field({}, description="UDF 실행시 input 값")


class DAGNode(BaseModel):
    id: str
    type: str = Field("custom", description="UI node 타입 ")
    label: str
    position: dict[str, float] = Field(default_factory=lambda: {"x": 0, "y": 0}, description="UI node 좌표")
    data: DAGNodeData
    style: Optional[dict[str, Any]] = Field({})

    @model_validator(mode="before")
    @classmethod
    def fill_defaults(cls, values):
        # ✅ type 이 None 이거나 빈 문자열이면 "custom" 으로 세팅
        if not values.get("label"):
            values["label"] = ""
        return values


class DAGEdge(BaseModel):
    id: str
    type: str = Field("custom", description="UI edge 타입 ")
    source: str = Field(..., description="Source Node ID")
    target: str = Field(..., description="Target Node ID")
    label: Optional[str] = Field("", description="edge label")
    labelStyle: Optional[dict[str, Any]] = Field(default_factory=dict)
    labelBgStyle: Optional[dict[str, Any]] = Field(default_factory=dict)
    labelBgPadding: Optional[float] = Field(default_factory=float)
    labelBgBorderRadius: Optional[float] = Field(default_factory=float)
    style: Optional[dict[str, Any]] = Field({})


class DAGRequest(BaseModel):
    name: str = Field(..., description="DAG Name")
    description: str = Field(..., description="DAG Description")
    nodes: List[DAGNode] = Field(..., description="Task 정의")
    edges: List[DAGEdge] = Field(..., description="Task 실행 관계 정의")


class DAGResponse(BaseModel):
    id: str = Field(..., description="Generated DAG ID")
    name: str = Field(..., description="DAG Name")
    description: str = Field(..., description="DAG Description")
    nodes: List[DAGNode]
    edges: List[DAGEdge]

    @classmethod
    def from_dag(cls, dag: Flow):
        return cls(
            id=dag.id,
            name=dag.name,
            description=dag.description,
            nodes=[DAGNode(
                id=task.id,
                type=task.task_ui.type if task.task_ui else "custom",
                position=task.task_ui.position if task.task_ui else {"x": 0, "y": 0},
                style=task.task_ui.style if task.task_ui else {},
                label=task.task_ui.label if task.task_ui else "",
                data=DAGNodeData(
                    function_id=task.function_id,
                    inputs={inp.key: inp.value for inp in task.inputs},
                )
            ) for task in dag.tasks],
            edges=[DAGEdge(
                id=edge.id,
                type=edge.edge_ui.type if edge.edge_ui else "custom",
                source=edge.from_task_id,
                target=edge.to_task_id,
                label=edge.edge_ui.label if edge.edge_ui else "",
                style=edge.edge_ui.style if edge.edge_ui else {},
            ) for edge in dag.edges],
        )
