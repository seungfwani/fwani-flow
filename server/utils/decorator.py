import os
import pickle
import zipfile
from functools import wraps
from typing import Dict, List, Any


def xcom_decorator(inputs: List[Dict[str, Any]]):
    """airflow 기본 방식 (64KB 제한이 있음)"""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            from airflow.operators.python import get_current_context
            print(f"args: {args}, kwargs: {kwargs}")
            context = get_current_context()
            ti = context['ti']

            task = ti.task
            upstream_tasks = task.upstream_list
            downstream_tasks = task.downstream_list

            validated_inputs = {}
            if not upstream_tasks:
                print(f"‼️ 처음 태스크임.")
                # 정의된 inpput 정리
                for inp in inputs:
                    key = inp["name"]
                    expected_type = inp["type"]
                    value = kwargs.get(key)
                    validated_inputs[key] = value
                # input_data = kwargs.get("input_data")
            else:
                print(f"📌 이전 태스크에서 데이터 가져오기")
                before_task_outputs = [
                    ti.xcom_pull(task_ids=t_id, key="output") for t_id in kwargs.get("before_task_ids", [])
                ]
                # xcom에서 가져온 데이터를 validated_inputs에 반영
                for i, inp in enumerate(inputs):
                    key = inp["name"]
                    if i < len(before_task_outputs):  # 데이터를 순서대로 매핑
                        validated_inputs[key] = before_task_outputs[i]

            result = func(*args, **validated_inputs)

            if not downstream_tasks:
                print(f"‼️ 마지막 태스크임. output = {result}")
            else:
                print(f"📌 결과값 저장")
                ti.xcom_push(key="output", value=result)
            return result

        return wrapper

    return decorator


def rabbitmq_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


def file_decorator(inputs: List[Dict[str, Any]]):
    def decorator(func):
        """파일 기반 데이터 전달 (대용량 지원)"""
        # ✅ 파일 저장 경로 설정 (Airflow 컨테이너 내부 공유 가능하도록 설정)
        base_dir = "/app/shared"
        os.makedirs(base_dir, exist_ok=True)

        def get_input_data(task_id, is_first_task, **kwargs):
            validated_inputs = {}
            if is_first_task:
                print(f"‼️ 처음 태스크 실행: {task_id}")
                # 정의된 inpput 정리
                for inp in inputs:
                    key = inp["name"]
                    expected_type = inp["type"]
                    value = kwargs.get(key)
                    validated_inputs[key] = value
            else:
                print(f"📥 {task_id} → 이전 Task 데이터 로드 중...")
                before_task_outputs = []
                for t_id in kwargs.get("before_task_ids", []):
                    print(f"📥 ({t_id}) 데이터 로드 중...")
                    prev_file_path = os.path.join(base_dir, f"{t_id}.pkl")
                    if os.path.exists(prev_file_path):
                        with open(prev_file_path, "rb") as f:
                            before_task_outputs.append(pickle.load(f))
                    else:
                        print(f"⚠️ {prev_file_path} 파일 없음. 이전 Task 실행이 완료되지 않았을 수 있음.")
                for i, inp in enumerate(inputs):
                    key = inp["name"]
                    if i < len(before_task_outputs):  # 데이터를 순서대로 매핑
                        validated_inputs[key] = before_task_outputs[i]
            return validated_inputs

        def write_output_data(task_id, is_last_task, output):
            file_path = os.path.join(base_dir, f"{task_id}.pkl")
            # ✅ 결과를 파일에 저장
            with open(file_path, "wb") as f:
                pickle.dump(output, f)
            print(f"📥 {task_id} → 결과 저장: {file_path}")

            if is_last_task:
                print(f"‼️ 마지막 태스크 완료: {task_id} → output = {output}")

        @wraps(func)
        def wrapper(*args, **kwargs):
            print(args, kwargs)
            if kwargs.get("operator_type") == "airflow":
                from airflow.operators.python import get_current_context
                context = get_current_context()
                ti = context['ti']

                task_id = ti.task.task_id
                is_first_task = len(list(ti.task.upstream_list)) == 0
                is_last_task = len(list(ti.task.downstream_list)) == 0
            else:
                task_id = kwargs.pop("task_id")
                is_first_task = kwargs.pop("is_first_task", True)
                is_last_task = kwargs.pop("is_last_task", True)

            input_data = get_input_data(task_id, is_first_task, **kwargs)
            # ✅ 실제 UDF 실행
            result = func(input_data, *args, **kwargs)
            write_output_data(task_id, is_last_task, result)

            return result

        return wrapper

    return decorator


def save_executable_udf(udf_dir: str, udf_name: str):
    source_path = os.path.join(udf_dir, udf_name)  # 압축할 폴더
    zip_path = os.path.join(udf_dir, f"{udf_name}.zip")  # 저장할 ZIP 파일 경로

    # 🔹 ZIP 파일 생성
    with zipfile.ZipFile(zip_path, "w") as zipf:
        for root, _, files in os.walk(source_path):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, source_path)  # ZIP 내부 경로 유지
                zipf.write(file_path, arcname)

    print(f"✅ ZIP 파일 생성 완료: {zip_path}")


# 🔹 UDF 실행 함수 (ZIP 파일 사용)
def execute_udf(udf_name, function_name, *args, **kwargs):
    import os
    import sys
    import zipfile
    udf_dir = "/opt/airflow/udfs"
    zip_path = os.path.join(udf_dir, f"{udf_name}.zip")
    extract_path = f"/tmp/{udf_name}"

    # 🔹 ZIP 파일 해제 (UDF 파일은 유지됨)
    with zipfile.ZipFile(zip_path, "r") as zipf:
        zipf.extractall(extract_path)

    # 🔹 Python 경로 추가 (UDF 실행 가능)
    sys.path.append(extract_path)

    # 🔹 메타데이터 조회하여 입력값 적용
    # from example_udf_fetch_64a6ca import run
    module = __import__(f"{udf_name}.udf", fromlist=[function_name])
    udf_function = getattr(module, function_name, None)
    return udf_function(*args, **kwargs)
