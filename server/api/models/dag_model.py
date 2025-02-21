from typing import List

from pydantic import BaseModel, Field


# DAG 데이터 모델 정의
class DAGNode(BaseModel):
    id: str = Field(..., description="Task ID 역할")
    filename: str = Field(..., description="실행할 UDF 파일 이름")
    function: str = Field(..., description="실행할 함수 이름")


class DAGEdge(BaseModel):
    from_: str = Field(..., description="이전 노드")
    to_: str = Field(..., description="다음 노드")


class DAGRequest(BaseModel):
    nodes: List[DAGNode] = Field([], description="Task 정의")
    edges: List[DAGEdge] = Field([], description="Task 실행 관계 정의")
