import os.path
import shutil

from fastapi import APIRouter, UploadFile, HTTPException

from config import Config

# 워크플로우 블루프린트 생성
router = APIRouter(
    prefix="/udf",
    tags=["Udf"],
)

ALLOWED_EXTENSIONS = ['py']


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@router.post("/udf")
async def upload_udf(file: UploadFile):
    """
    Upload a python UDF file
    :param file:
    :return:
    """
    if not allowed_file(file.filename):
        raise HTTPException(status_code=400,
                            detail=f"Only {', '.join(map(lambda x: f'.{x}', ALLOWED_EXTENSIONS))} files are allowed")

    file_path = os.path.join(Config.UDF_DIR, file.filename)
    with open(file_path, "wb") as f:
        shutil.copyfileobj(file.file, f)
    return {"message": f"{file.filename} UDF file uploaded successfully"}


@router.delete("/udf/{filename}")
async def delete_udf(filename: str):
    """
    Delete a python UDF file
    :param filename:
    :return:
    """
    file_path = os.path.join(Config.UDF_DIR, filename)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="UDF file not found")

    os.remove(file_path)
    return {"message": f"{filename} UDF file deleted successfully"}


@router.get("/udf")
async def get_udf_list():
    """
    Get all available UDF files
    :return:
    """
    files = [f for f in os.listdir(Config.UDF_DIR) if f.endswith(".py")]
    return {"files": files}
