import hashlib
import os
import uuid


def generate_udf_filename(original_filename: str) -> str:
    """
    사용자의 UDF 파일을 저장할 때 중복 방지를 위해 새로운 파일명을 생성.

    :param original_filename: 사용자가 업로드한 원본 파일명
    :return: 중복 방지를 위한 새로운 파일명 (import 가능한 Python 모듈 형태)
    """
    base_name, ext = os.path.splitext(original_filename)  # 파일명과 확장자 분리
    if ext != ".py":
        raise ValueError("Only .py files are allowed!")

    # 해시 값 생성 (파일명 + UUID 조합)
    hash_suffix = hashlib.md5((original_filename + str(uuid.uuid4())).encode()).hexdigest()[:6]

    # 새로운 파일명 생성
    new_filename = f"{base_name}_{hash_suffix}{ext}"

    # Python import를 위한 규칙 적용 (알파벳, 숫자, _ 만 허용)
    new_filename = new_filename.replace("-", "_")

    return new_filename


if __name__ == "__main__":
    print(generate_udf_filename("fetch_data.py"))
    # 출력 예시: fetch_data_ab12c.py
