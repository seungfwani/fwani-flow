import base64
import hashlib
import json
import re
from datetime import datetime
from typing import List


def get_udf_requirements(requirements_txt: str) -> List[str]:
    try:
        with open(requirements_txt, "r", encoding="utf-8") as f:
            return [line.strip() for line in f.readlines() if line.strip()]
    except FileNotFoundError:
        return []


def make_flow_id_by_name(name: str, is_draft: bool = False) -> str:
    dag_name = ("dag_"
                + base64.urlsafe_b64encode(name.encode())
                .rstrip(b'=').decode('ascii'))
    return f"{dag_name}__draft" if is_draft else dag_name


def get_stable_hash(*args):
    def normalize(value):
        if isinstance(value, (dict, list)):
            return json.dumps(value, sort_keys=True)
        return str(value)

    serialized = "|".join(normalize(arg) for arg in args)
    return int(get_hash(serialized), 16)


def get_hash(data: str | bytes) -> str:
    if isinstance(data, str):
        return hashlib.sha256(data.encode("utf-8")).hexdigest()
    else:
        return hashlib.sha256(data).hexdigest()


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


def to_bool(v: str) -> bool:
    v = v.lower()
    if v in ("true", "1"): return True
    if v in ("false", "0"): return False
    raise ValueError(f"Invalid boolean: {v}")

def to_snake(s: str) -> str:
    # CamelCase / camelCase / kebab-case -> snake_case 로 변환
    s = re.sub(r'(.)([A-Z][a-z0-9]+)', r'\1_\2', s)
    s = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s)
    return s.replace("-", "_").lower()