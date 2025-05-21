import io
import logging
import os.path
import shutil
import uuid
import zipfile
from typing import List, Optional

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

ALLOWED_EXTENSIONS = ['py', 'txt', 'zip']


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def extract_zip_file(zip_file: UploadFile) -> List[UploadFile]:
    # Implement ZIP file extraction logic here
    # For example, you can use the `zipfile` library in Python
    extracted_files = []
    with zipfile.ZipFile(zip_file.file, 'r') as zip_ref:
        for file_info in zip_ref.infolist():
            if file_info.is_dir():
                continue
            if file_info.filename.endswith(".py") or file_info.filename.endswith(".txt"):
                with zip_ref.open(file_info) as extracted:
                    file_bytes = extracted.read()
                    file_size = len(file_bytes)
                    file_like = io.BytesIO(file_bytes)
                    file_like.seek(0)
                    extracted_file = UploadFile(file=file_like, filename=file_info.filename, size=file_size)
                    extracted_files.append(extracted_file)
    return extracted_files


def get_python_files_and_requirements(files: List[UploadFile]):
    python_files = []
    requirements_file = None
    for file in files:
        if not allowed_file(file.filename):
            raise HTTPException(status_code=400,
                                detail=f"Only {', '.join(map(lambda x: f'.{x}', ALLOWED_EXTENSIONS))} files are allowed")
        if file.filename.endswith(".zip"):
            try:
                extracted = extract_zip_file(file)
                for inner_file in extracted:
                    if inner_file.filename.endswith(".py"):
                        python_files.append(inner_file)
                    elif inner_file.filename.endswith(".txt") and requirements_file is None:
                        requirements_file = inner_file
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"Invalid ZIP file: {e}")
        elif file.filename.endswith(".py"):
            python_files.append(file)
        elif file.filename.endswith(".txt"):
            requirements_file = file
    return python_files, requirements_file


def save_udf_files(python_files, requirements_file, file_dir: str, udf_name: str, function_name: str, ):
    if os.path.exists(file_dir):
        shutil.rmtree(file_dir)
        logger.info(f"🗑️ 저장된 파일 삭제: {file_dir}")

    os.makedirs(file_dir, exist_ok=True, mode=0o777)
    udf_dir = os.path.abspath(Config.UDF_DIR)
    is_validate_udf = False
    for python_file in python_files:
        file_path = os.path.join(file_dir, python_file.filename)
        os.makedirs(os.path.dirname(file_path), exist_ok=True, mode=0o777)
        with open(file_path, "wb") as f:
            shutil.copyfileobj(python_file.file, f)
            logger.info(f"✅ 파일 저장 완료: {file_path}")
        if not is_validate_udf and validate_udf(file_path, function_name):
            is_validate_udf = True
    if not is_validate_udf:
        raise HTTPException(status_code=400, detail="UDF is not valid")

    if requirements_file:
        requirements_file_path = os.path.join(file_dir, "requirements.txt")
        with open(requirements_file_path, "wb") as f:
            shutil.copyfileobj(requirements_file.file, f)
            logger.info(f"✅ 파일 저장 완료: {requirements_file_path}")
    zip_executable_udf(udf_dir, udf_name)


def set_input_output(udf_data: FunctionLibrary, udf_metadata: UDFUploadRequest):
    udf_data.inputs = []
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
    return udf_data


@router.post("",
             response_model=APIResponse[UDFResponse],
             )
@api_response_wrapper
async def upload_udf(udf_metadata: UDFUploadRequest = Form(...),
                     files: List[UploadFile] = File(...),
                     db: Session = Depends(get_db)):
    """
    UDF 업로드
    """
    if not files:
        raise HTTPException(status_code=404, detail="No files uploaded")
    logger.info(f"Request to upload UDF; {udf_metadata}")
    python_files, requirements_file = get_python_files_and_requirements(files)

    udf_name = generate_udf_filename(udf_metadata.name)
    file_dir = os.path.join(os.path.abspath(Config.UDF_DIR), udf_name)
    try:
        save_udf_files(python_files, requirements_file, file_dir, udf_name, udf_metadata.function_name)
        main_filename = \
            (python_files[0].filename if udf_metadata.main_filename is None else udf_metadata.main_filename).rsplit(
                ".")[0]
        udf_id = str(uuid.uuid4())
        udf_data = FunctionLibrary(
            id=udf_id,
            name=udf_name,
            description=udf_metadata.description,
            main_filename=main_filename,
            path=file_dir,
            function=udf_metadata.function_name,
            operator_type=udf_metadata.operator_type,
            docker_image_tag=udf_metadata.docker_image,
            dependencies="requirements.txt",
        )
        udf_data = set_input_output(udf_data, udf_metadata)
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


@router.patch("/{udf_id}",
              response_model=APIResponse[UDFResponse],
              )
@api_response_wrapper
async def update_udf(udf_id: str, udf_metadata: UDFUploadRequest = Form(...),
                     files: Optional[List[UploadFile]] = File(None),
                     db: Session = Depends(get_db)):
    if not (udf_data := db.query(FunctionLibrary).filter(FunctionLibrary.id == udf_id).first()):
        raise Exception(f"Udf({udf_id}) not found")
    try:
        udf_data.description = udf_metadata.description
        udf_data.operator_type = udf_metadata.operator_type
        udf_data.docker_image_tag = udf_metadata.docker_image
        if files and len(files) > 0:
            python_files, requirements_file = get_python_files_and_requirements(files)
            udf_data.main_filename = \
                (python_files[0].filename if udf_metadata.main_filename is None else udf_metadata.main_filename).rsplit(
                    ".")[0]
            udf_data.function = udf_metadata.function_name
            save_udf_files(python_files, requirements_file, udf_data.path, udf_data.name, udf_data.function)
        udf_data = set_input_output(udf_data, udf_metadata)
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
        if os.path.exists(udf_data.path):
            shutil.rmtree(udf_data.path)
            logger.info(f"🗑️ 저장된 파일 삭제: {udf_data.path}")

        raise


@router.delete("/{udf_id}",
               response_model=APIResponse[UDFResponse],
               )
@api_response_wrapper
async def delete_udf(udf_id: str, db: Session = Depends(get_db)):
    """
    UDF 삭제
    """

    if not (udf_data := db.query(FunctionLibrary).filter(FunctionLibrary.id == udf_id).first()):
        return {"message": f"UDF {udf_id} not found"}

    if os.path.exists(udf_data.path) and os.path.isdir(udf_data.path):
        shutil.rmtree(udf_data.path)
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
    이용가능 한 UDF 리스트 조회
    """
    logger.info(f"▶️ udf 리스트 조회")
    return [UDFResponse.from_function_library(udf_data)
            for udf_data in db.query(FunctionLibrary).all()]


@router.get("/{udf_id}",
            response_model=APIResponse[UDFResponse],
            )
@api_response_wrapper
async def get_udf(udf_id: str, db: Session = Depends(get_db)):
    if not (udf_data := db.query(FunctionLibrary).filter(FunctionLibrary.id == udf_id).first()):
        return {"message": f"UDF {udf_id} not found"}
    return UDFResponse.from_function_library(udf_data)
