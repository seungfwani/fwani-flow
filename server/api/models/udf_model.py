import json
from typing import List, Optional, Any

from pydantic import BaseModel, Field, model_validator, field_validator

from models.function_library import FunctionLibrary


# DAG 데이터 모델 정의
class UDFInputSchema(BaseModel):
    name: str = Field(..., description="input Name", examples=["data"])
    type: str = Field(..., description="input Type", examples=["string"])
    required: bool = Field(..., description="필수/옵션", examples=[True, False])
    default_value: Optional[str] = Field(..., description="input default value", examples=[""])
    description: Optional[str] = Field(..., description="input 설명", examples=["input description"])


class UDFOutputSchema(BaseModel):
    name: str = Field(..., description="output Name", examples=["result"])
    type: str = Field(..., description="output Type", examples=["string"])
    description: Optional[str] = Field(..., description="output 설명", examples=["output description"])


class UDFUploadRequest(BaseModel):
    name: str = Field(..., description="UDF Name", examples=["UDF Name"])
    description: Optional[str] = Field(None, description="udf 설명", examples=["udf description"])
    main_filename: Optional[str] = Field(None, description="UDF main file name (default: first python file of request)", examples=["main_python_file_name"])
    function_name: str = Field("run", description="main function name (default: run)", examples=["run"])
    operator_type: str = Field("python", description="operator type [python, python_virtual, docker]", examples=["python", "python_virtual", "docker"])
    docker_image: Optional[str] = Field(..., description="docker image name", examples=["python:latest"])

    inputs: List[UDFInputSchema]
    output: UDFOutputSchema

    @model_validator(mode='before')
    @classmethod
    def validate_to_json(cls, value: Any) -> Any:
        print(f"validate_to_json : {value}")
        if isinstance(value, str):
            return json.loads(value)
        return value

    @field_validator("docker_image", mode='before')
    @classmethod
    def check_docker_image_required(cls, v, validation_info):
        print(f"check_docker_image_required : {v}, {validation_info}")
        if validation_info.data.get("operator_type") == "docker" and not v:
            raise ValueError("docker_image is required when operator_type is 'docker'")
        return v


class UDFResponse(BaseModel):
    id: str = Field(..., description="Generated UDF id", examples=["00000000-0000-4000-9000-000000000000"])
    name: str = Field(..., description="UDF name", examples=["UDF name"])
    description: Optional[str] = Field(..., description="UDF description", examples=["UDF description"])
    main_filename: Optional[str] = Field(None, description="UDF main file name (default: first python file of request)", examples=["main_python_file_name"])
    function_name: str = Field("run", description="main function name (default: run)", examples=["run"])
    operator_type: str = Field("python", description="operator type [python, python_virtual, docker]", examples=["python", "python_virtual", "docker"])
    docker_image: Optional[str] = Field(..., description="docker image name", examples=["python:latest"])

    inputs: List[UDFInputSchema]
    output: UDFOutputSchema

    @classmethod
    def from_function_library(cls, function_library: FunctionLibrary):
        return cls(
            id=function_library.id,
            name=function_library.name,
            description=function_library.description,
            main_filename=function_library.main_filename,
            function_name=function_library.function,
            operator_type=function_library.operator_type,
            docker_image=function_library.docker_image_tag,
            inputs=[UDFInputSchema(
                name=inp.name,
                type=inp.type,
                required=inp.required,
                default_value=inp.default_value,
                description=inp.description,
            ) for inp in function_library.inputs],
            output=UDFOutputSchema(
                name=function_library.output.name,
                type=function_library.output.type,
                description=function_library.output.description,
            ),
        )
