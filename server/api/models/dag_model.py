from typing import List, Dict, Any, Optional

from pydantic import BaseModel, Field

from models.flow import Flow


# DAG 데이터 모델 정의
class DAGNode(BaseModel):
    id: str = Field(..., description="Task Name 역할")
    function_id: str = Field(..., description="실행할 UDF ID")
    inputs: Dict[str, Any] = Field({}, description="UDF 실행시 input 값")
    ui_type: Optional[str] = Field(..., description="UI node 타입 ")
    position: Optional[dict[str, int]] = Field({"x": 0, "y": 0}, description="UI node 좌표")


class DAGEdge(BaseModel):
    source: str = Field(..., description="이전 노드")
    target: str = Field(..., description="다음 노드")

    class Config:
        populate_by_alias = True


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
                id=task.variable_id,
                function_id=task.function_id,
                inputs={inp.key: inp.value for inp in task.inputs}
            ) for task in dag.tasks],
            edges=[DAGEdge(source=edge.from_task_id, target=edge.to_task_id) for edge in dag.edges],
        )
