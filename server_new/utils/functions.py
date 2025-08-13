import base64
import hashlib
from datetime import datetime
from typing import List


def get_udf_requirements(requirements_txt: str) -> List[str]:
    try:
        with open(requirements_txt, "r", encoding="utf-8") as f:
            return [line.strip() for line in f.readlines() if line.strip()]
    except FileNotFoundError:
        return []


def make_flow_id_by_name(name: str, is_draft: bool=False) -> str:
    if is_draft:
        source_name = name + "__draft"
    else:
        source_name = name
    return ("dag_"
            + base64.urlsafe_b64encode(source_name.encode())
            .rstrip(b'=').decode('ascii'))


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
