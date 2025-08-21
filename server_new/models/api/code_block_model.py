from pydantic import BaseModel, Field


class CodeRequest(BaseModel):
    code: str = Field(..., description="code string", examples=["def run():\n    pass"])