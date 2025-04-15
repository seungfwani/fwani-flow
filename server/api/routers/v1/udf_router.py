import logging
import os.path
import shutil
import uuid
from typing import List

from fastapi import APIRouter, UploadFile, HTTPException, File, Depends, Form
from sqlalchemy.orm import Session

from api.models.api_model import api_response_wrapper, APIResponse
from api.models.udf_model import UDFUploadRequest, UDFResponse
from config import Config
from core.database import get_db
from models.function_input import FunctionInput
from models.function_library import FunctionLibrary
from models.function_output import FunctionOutput
from utils.decorator import zip_executable_udf
from utils.functions import generate_udf_filename
from utils.udf_validator import validate_udf

logger = logging.getLogger()

# 워크플로우 블루프린트 생성
router = APIRouter(
    prefix="/udf",
    tags=["Udf"],
)

ALLOWED_EXTENSIONS = ['py', 'txt']


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@router.post("",
             response_model=APIResponse[UDFResponse],
             )
@api_response_wrapper
async def upload_udf(udf_metadata: UDFUploadRequest = Form(...),
                     files: List[UploadFile] = File(...),
                     db: Session = Depends(get_db)):
    """
    Upload a python UDF file
    :param udf_metadata:
    :param files:
    :param db: SqlAlchemy session
    :return:
    """
    if not files:
        raise HTTPException(status_code=404, detail="No files uploaded")
    logger.info(f"Request to upload UDF; {udf_metadata}")
    python_files = []
    requirements_file = None
    for file in files:
        if not allowed_file(file.filename):
            raise HTTPException(status_code=400,
                                detail=f"Only {', '.join(map(lambda x: f'.{x}', ALLOWED_EXTENSIONS))} files are allowed")
        if file.filename.endswith(".py"):
            python_files.append(file)
        elif file.filename.endswith(".txt"):
            requirements_file = file

    udf_dir = os.path.abspath(Config.UDF_DIR)
    udf_name = generate_udf_filename(udf_metadata.name)
    file_dir = os.path.join(os.path.abspath(Config.UDF_DIR), udf_name)
    try:
        os.makedirs(file_dir, exist_ok=True, mode=0o777)
        main_filename = \
            (python_files[0].filename if udf_metadata.main_filename is None else udf_metadata.main_filename).rsplit(
                ".")[0]
        is_validate_udf = False
        for python_file in python_files:
            file_path = os.path.join(file_dir, python_file.filename)
            with open(file_path, "wb") as f:
                shutil.copyfileobj(python_file.file, f)
                logger.info(f"✅ 파일 저장 완료: {file_path}")
            if not is_validate_udf and validate_udf(file_path, udf_metadata.function_name):
                is_validate_udf = True
        if not is_validate_udf:
            raise HTTPException(status_code=400, detail="UDF is not valid")

        if requirements_file:
            requirements_file_path = os.path.join(file_dir, "requirements.txt")
            with open(requirements_file_path, "wb") as f:
                shutil.copyfileobj(requirements_file.file, f)
                logger.info(f"✅ 파일 저장 완료: {requirements_file_path}")
        zip_executable_udf(udf_dir, udf_name)

        udf_id = str(uuid.uuid4())
        udf_data = FunctionLibrary(
            id=udf_id,
            name=udf_name,
            main_filename=main_filename,
            path=file_dir,
            function=udf_metadata.function_name,
            operator_type=udf_metadata.operator_type,
            docker_image_tag=udf_metadata.docker_image,
            dependencies="requirements.txt",
        )

        for i in udf_metadata.inputs:
            udf_data.inputs.append(FunctionInput(
                name=i.name,
                type=i.type,
                required=i.required,
                default_value=i.default_value,
                description=i.description,
            ))

        udf_data.output = FunctionOutput(
            name=udf_metadata.output.name,
            type=udf_metadata.output.type,
            description=udf_metadata.output.description,
        )
        db.add(udf_data)
        db.commit()
        db.refresh(udf_data)
        logger.info(f"✅ 메타데이터 저장 완료: {udf_data}")

        return UDFResponse.from_function_library(udf_data)

    except Exception as e:
        logger.error(f"❌ 오류 발생: {e}")
        db.rollback()
        logger.info(f"🔄 메타데이터 롤백")

        # ✅ 파일 저장 후 DB 실패 시 파일 삭제
        if os.path.exists(file_dir):
            shutil.rmtree(file_dir)
            logger.info(f"🗑️ 저장된 파일 삭제: {file_dir}")

        raise


@router.delete("/{udf_id}",
               response_model=APIResponse[UDFResponse],
               )
@api_response_wrapper
async def delete_udf(udf_id: str, db: Session = Depends(get_db)):
    """
    Delete a python UDF file
    :param udf_id:
    :return:
    """

    if not (udf_data := db.query(FunctionLibrary).filter(FunctionLibrary.id == udf_id).first()):
        return {"message": f"UDF {udf_id} not found"}

    if not os.path.exists(udf_data.path):
        raise HTTPException(status_code=404, detail="UDF file not found")

    if os.path.isdir(udf_data.path):
        shutil.rmtree(udf_data.path)
        os.remove(udf_data.path)
        logger.info(f"🗑️ 저장된 파일 삭제 완료: {udf_data.path}")
    db.delete(udf_data)
    db.commit()
    logger.info(f"🗑️ 메타데이터 삭제: {udf_data}")

    return UDFResponse.from_function_library(udf_data)


@router.get("",
            response_model=APIResponse[List[UDFResponse]],
            )
@api_response_wrapper
async def get_udf_list(db: Session = Depends(get_db)):
    """
    Get all available UDF files
    :return:
    """
    logger.info(f"▶️ udf 리스트 조회")
    return [UDFResponse.from_function_library(udf_data)
            for udf_data in db.query(FunctionLibrary).all()]
