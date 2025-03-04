from typing import List, Optional

from pydantic import BaseModel, Field


# DAG 데이터 모델 정의
class UDFInputSchema(BaseModel):
    name: str = Field(..., description="input Name")
    type: str = Field(..., description="input Type")
    required: bool = Field(..., description="필수/옵션")
    default_value: Optional[str] = Field(..., description="input default value")
    description: Optional[str] = Field(..., description="input 설명")


class UDFOutputSchema(BaseModel):
    name: str = Field(..., description="output Name")
    type: str = Field(..., description="output Type")
    description: Optional[str] = Field(..., description="output 설명")


class UDFUploadRequest(BaseModel):
    inputs: List[UDFInputSchema]
    output: UDFOutputSchema
