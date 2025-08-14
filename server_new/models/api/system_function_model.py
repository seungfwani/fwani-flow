from pydantic import BaseModel, Field


class SystemFunctionResponse(BaseModel):
    id: str = Field(..., description="id")
    name: str = Field(..., description="시스템 함수 이름")
    description: str = Field(..., description="시스템 함수 설명")
    param_schema: list[dict[str, str | bool]] = Field({}, description="함수 인자 스키마",
                                                      examples=[
                                                          {
                                                              "name": "param1",
                                                              "type": "string",
                                                              "required": True,
                                                          },
                                                          {
                                                              "name": "param2",
                                                              "type": "integer",
                                                              "required": False,
                                                          },
                                                      ])
    is_deprecated: bool = Field(False, description="deprecated")
