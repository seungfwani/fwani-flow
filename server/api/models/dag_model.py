from typing import List, Dict, Any

from pydantic import BaseModel, Field


# DAG 데이터 모델 정의
class DAGNode(BaseModel):
    id: str = Field(..., description="Task Name 역할")
    function_id: str = Field(..., description="실행할 UDF ID")
    inputs: Dict[str, Any] = Field({}, description="UDF 실행시 input 값")


class DAGEdge(BaseModel):
    from_: str = Field(..., alias="from", description="이전 노드")
    to_: str = Field(..., alias="to", description="다음 노드")


class DAGRequest(BaseModel):
    name: str = Field(..., description="DAG Name")
    description: str = Field(..., description="DAG Description")
    nodes: List[DAGNode] = Field(..., description="Task 정의")
    edges: List[DAGEdge] = Field(..., description="Task 실행 관계 정의")
