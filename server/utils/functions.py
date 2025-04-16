import base64
import hashlib
import json
import re
import uuid
from datetime import datetime
from typing import List, Any

from models.task import Task


def generate_udf_filename(udf_name: str) -> str:
    """
    사용자의 UDF 파일을 저장할 때 중복 방지를 위해 새로운 파일명을 생성.
    abc.py -> abc_sefesce (.py 는 제거하여 리턴)

    :param udf_name: 사용자가 업로드한 원본 파일명
    :return: 중복 방지를 위한 새로운 파일명 (import 가능한 Python 모듈 형태)
    """
    # 해시 값 생성 (파일명 + UUID 조합)
    hash_suffix = hashlib.md5((udf_name + str(uuid.uuid4())).encode()).hexdigest()[:6]

    # Python import를 위한 규칙 적용 (알파벳, 숫자, _ 만 허용)
    # 새로운 파일 명 리턴
    return re.sub(r"[^a-zA-Z0-9_ㄱ-ㅎ가-힣]", "_", f"{udf_name}_{hash_suffix}")


def get_udf_requirements(requirements_txt: str) -> List[str]:
    try:
        with open(requirements_txt, "r", encoding="utf-8") as f:
            return [line.strip() for line in f.readlines() if line.strip()]
    except FileNotFoundError:
        return []


def make_flow_id_by_name(name: str) -> str:
    return "dag_" + base64.urlsafe_b64encode(name.encode()).rstrip(b'=').decode('ascii')


def normalize_dag(dag) -> dict[str, Any]:
    def normalize_node(node):
        return {
            "function_id": node.data.function_id,
            "inputs": sorted(node.data.inputs.items()),
            "type": node.type,
            "position": node.position,
            "style": node.style,
        }

    return {
        "name": dag.name,
        "nodes": [normalize_node(n) for n in dag.nodes],
        "edge_count": len(dag.edges),
    }


def normalize_task(n: Task):
    return {
        "function_id": n.function_id,
        "inputs": sorted([(i.key, i.value) for i in n.inputs]),
        "type": n.task_ui.type,
        "position": n.task_ui.position,
        "style": n.task_ui.style,
    }


def calculate_dag_hash(dag) -> str:
    hash_input = json.dumps(normalize_dag(dag), sort_keys=True)
    return hashlib.sha256(hash_input.encode("utf-8")).hexdigest()


def string2datetime(s: str, f: str = None) -> datetime:
    if not s:
        return datetime.now()

    formats_to_try = [f] if f else []
    # 백업 포맷 추가
    formats_to_try += [
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z"
    ]

    for fmt in formats_to_try:
        try:
            return datetime.strptime(s, fmt)
        except (ValueError, TypeError):
            continue

    raise ValueError(f"Unsupported datetime format: {s}")


def get_airflow_dag_id(flow_version) -> str:
    return f"{flow_version.flow_id}__" + ("draft" if flow_version.is_draft else f"v{flow_version.version}")


def split_airflow_dag_id_to_flow_and_version(dag_id) -> (str, int | str, bool):
    matched = re.match(r"([a-zA-Z0-9_]+)__(v\d+|draft)", dag_id)
    if not matched:
        return dag_id, 0, False
    flow_id, version_raw = matched.groups()
    if version_raw == "draft":
        return flow_id, 1, True
    else:
        return flow_id, int(version_raw[1:]), False


if __name__ == "__main__":
    print(generate_udf_filename("fetch_data.py"))
    # 출력 예시: fetch_data_ab12c.py
