import base64
import hashlib
import json
import re
import uuid
from typing import List, Any

from api.models.dag_model import DAGRequest, DAGNode
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


def normalize_dag(dag: DAGRequest) -> dict[str, Any]:
    def normalize_node(node: DAGNode):
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


def calculate_dag_hash(dag: DAGRequest) -> str:
    hash_input = json.dumps(normalize_dag(dag), sort_keys=True)
    return hashlib.sha256(hash_input.encode("utf-8")).hexdigest()


if __name__ == "__main__":
    print(generate_udf_filename("fetch_data.py"))
    # 출력 예시: fetch_data_ab12c.py
