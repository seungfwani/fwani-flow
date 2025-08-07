import logging

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from core.database import get_db
from errors import WorkflowError
from models.api.api_model import api_response_wrapper, APIResponse
from models.api.template_model import TemplateRequest, TemplateResponse
from models.db.function_template import FunctionTemplate
from utils.functions import get_hash

logger = logging.getLogger()

# 워크플로우 블루프린트 생성
router = APIRouter(
    prefix="/template",
    tags=["template"],
)


@router.post("",
             response_model=APIResponse[TemplateResponse],
             )
@api_response_wrapper
async def create_template(data: TemplateRequest, db: Session = Depends(get_db)):
    """
    template 생성
    """
    ft = FunctionTemplate(
        name=data.name,
        description=data.description,
        python_libraries=data.python_libraries,
        code_string=data.code,
        code_hash=get_hash(data.code),
    )
    db.add(ft)
    db.commit()
    return TemplateResponse(
        id=ft.id,
        name=ft.name,
        description=ft.description,
        python_libraries=ft.python_libraries,
        code=ft.code_string,
    )


@router.patch("/{template_id}",
              response_model=APIResponse[TemplateResponse],
              )
@api_response_wrapper
async def update_dag(template_id: str, data: TemplateRequest, db: Session = Depends(get_db)):
    """
    template update
    """
    ft = db.query(FunctionTemplate).filter(FunctionTemplate.id == template_id).first()
    if not ft:
        raise WorkflowError(f"No such template [{template_id}")
    ft.name = data.name
    ft.description = data.description
    ft.python_libraries = data.python_libraries
    ft.code_string = data.code
    ft.code_hash = get_hash(data.code)
    db.commit()
    return TemplateResponse(
        id=ft.id,
        name=ft.name,
        description=ft.description,
        python_libraries=ft.python_libraries,
        code=ft.code_string,
    )


@router.get("",
            response_model=APIResponse[list[TemplateResponse]],
            )
@api_response_wrapper
async def get_template_list(db: Session = Depends(get_db)):
    """
    template 리스트 조회
    """
    data = db.query(FunctionTemplate).all()
    return [TemplateResponse(
        id=ft.id,
        name=ft.name,
        description=ft.description,
        python_libraries=ft.python_libraries,
        code=ft.code_string,
    ) for ft in data]


@router.get("/{template_id}",
            response_model=APIResponse[TemplateResponse],
            )
@api_response_wrapper
async def get_template(template_id: str, db: Session = Depends(get_db)):
    """
    template 조회
    """
    template = db.query(FunctionTemplate).filter(FunctionTemplate.id == template_id).first()
    if not template:
        raise WorkflowError(f"No such template [{template_id}")
    return TemplateResponse(
        id=template.id,
        name=template.name,
        description=template.description,
        python_libraries=template.python_libraries,
        code=template.code_string,
    )


@router.delete("/{template_id}",
               response_model=APIResponse[str],
               )
@api_response_wrapper
async def delete_template(template_id: str, db: Session = Depends(get_db)):
    """
    template 영구 삭제
    """
    template = db.query(FunctionTemplate).filter(FunctionTemplate.id == template_id).first()
    if not template:
        raise WorkflowError(f"No such template [{template_id}")
    db.delete(template)
    db.commit()
    return template.id
