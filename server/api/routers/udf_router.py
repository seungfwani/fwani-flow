import logging
import os.path
import shutil

from fastapi import APIRouter, UploadFile, HTTPException, File, Depends
from sqlalchemy.orm import Session

from config import Config
from core.database import get_db
from models.function_library import FunctionLibrary
from utils.functions import generate_udf_filename
from utils.udf_validator import validate_udf

logger = logging.getLogger("fwani-flow")

# 워크플로우 블루프린트 생성
router = APIRouter(
    prefix="/udf",
    tags=["Udf"],
)

ALLOWED_EXTENSIONS = ['py']


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@router.post("/")
async def upload_udf(file: UploadFile = File(...), db: Session = Depends(get_db)):
    """
    Upload a python UDF file
    :param file:
    :param db: SqlAlchemy session
    :return:
    """
    if not allowed_file(file.filename):
        raise HTTPException(status_code=400,
                            detail=f"Only {', '.join(map(lambda x: f'.{x}', ALLOWED_EXTENSIONS))} files are allowed")

    file_name = generate_udf_filename(file.filename)
    file_path = os.path.join(os.path.abspath(Config.UDF_DIR), file_name)
    try:
        with open(file_path, "wb") as f:
            shutil.copyfileobj(file.file, f)
            logger.info(f"✅ 파일 저장 완료: {file_path}")

        if not validate_udf(file_path):
            raise HTTPException(status_code=400, detail="UDF is not valid")

        udf_data = FunctionLibrary(name=file_name.replace(".py", ""),
                                   filename=file_name,
                                   path=file_path,
                                   function="run")
        db.add(udf_data)
        db.commit()
        db.refresh(udf_data)
        logger.info(f"✅ 메타데이터 저장 완료: {udf_data}")

        return {"message": f"{file.filename} UDF file uploaded successfully"}

    except Exception as e:
        logger.error(f"❌ 오류 발생: {e}")
        db.rollback()
        logger.info(f"🔄 메타데이터 롤백")

        # ✅ 파일 저장 후 DB 실패 시 파일 삭제
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"🗑️ 저장된 파일 삭제: {file_path}")

        raise


@router.delete("/{udf_id}")
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

    os.remove(udf_data.path)
    logger.info(f"🗑️ 저장된 파일 삭제: {udf_data.path}")
    db.delete(udf_data)
    db.commit()
    logger.info(f"🗑️ 메타데이터 삭제: {udf_data}")

    return {"message": f"{udf_id} UDF file deleted successfully"}


@router.get("/")
async def get_udf_list(db: Session = Depends(get_db)):
    """
    Get all available UDF files
    :return:
    """
    logger.info(f"▶️ udf 리스트 조회")
    print(f"📌 현재 logger 핸들러 목록: {logger.handlers}")  # ✅ 로깅 핸들러 체크
    return {"udfs": db.query(FunctionLibrary).all()}
