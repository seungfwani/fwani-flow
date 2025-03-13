import hashlib
import os
import re
import uuid
from typing import List


def generate_udf_filename(original_filename: str) -> str:
    """
    사용자의 UDF 파일을 저장할 때 중복 방지를 위해 새로운 파일명을 생성.
    abc.py -> abc_sefesce (.py 는 제거하여 리턴)

    :param original_filename: 사용자가 업로드한 원본 파일명
    :return: 중복 방지를 위한 새로운 파일명 (import 가능한 Python 모듈 형태)
    """
    base_name, ext = os.path.splitext(original_filename)  # 파일명과 확장자 분리
    if ext != ".py":
        raise ValueError("Only .py files are allowed!")

    # 해시 값 생성 (파일명 + UUID 조합)
    hash_suffix = hashlib.md5((original_filename + str(uuid.uuid4())).encode()).hexdigest()[:6]

    # Python import를 위한 규칙 적용 (알파벳, 숫자, _ 만 허용)
    # 새로운 파일 명 리턴
    return re.sub(r"[^a-zA-Z0-9_]", "_", f"{base_name}_{hash_suffix}")


def get_udf_requirements(requirements_txt: str) -> List[str]:
    try:
        with open(requirements_txt, "r", encoding="utf-8") as f:
            return [line.strip() for line in f.readlines() if line.strip()]
    except FileNotFoundError:
        return []


if __name__ == "__main__":
    print(generate_udf_filename("fetch_data.py"))
    # 출력 예시: fetch_data_ab12c.py
