from pydantic import BaseModel, Field


class TemplateRequest(BaseModel):
    name: str = Field(..., description="템플릿 이름")
    description: str = Field(..., description="템플릿 설명")
    python_libraries: list[str] = Field([], description="requirements.txt 에 작성하는 포멧",
                                        examples=[['pandas==2.3.1', 'requests==2.32.3']])
    code: str = Field(..., description="code")


class TemplateResponse(BaseModel):
    id: str = Field(..., description="id")
    name: str = Field(..., description="템플릿 이름")
    description: str = Field(..., description="템플릿 설명")
    python_libraries: list[str] = Field([], description="requirements.txt 에 작성하는 포멧",
                                        examples=[['pandas==2.3.1', 'requests==2.32.3']])
    code: str = Field(..., description="code")
