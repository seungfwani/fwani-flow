import logging
from typing import Any, Dict, Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)

def run(*_, params: Optional[Dict[str, Any]] = None):
    """
    params 예시:
    {
      "host": "http://192.168.100.170:30842",
      "endpoint": "/meta_type_table/{meta_type_id}",
      "meta_type_id": "abcd-1234",
      "headers": {"Authorization": "Bearer ..."},
      "timeout_sec": 20
    }
    """
    if not params:
        raise ValueError("params is required")

    host         = params.get("host")
    endpoint_tpl = params.get("endpoint", "/meta_type_table/{meta_type_id}")
    meta_type_id = params.get("meta_type_id")
    headers      = params.get("headers", {})
    timeout      = params.get("timeout_sec", 20)

    if not host or not meta_type_id:
        raise ValueError("host, meta_type_id are required in params")

    # URL 조립
    endpoint = endpoint_tpl.format(meta_type_id=meta_type_id)
    url = f"{host.rstrip('/')}/{endpoint.lstrip('/')}"

    try:
        resp = requests.get(url, headers=headers, timeout=timeout)
    except requests.RequestException as e:
        logger.exception("request failed: %s", e)
        raise

    if not resp.ok:
        # 가능한 범위에서 상세 메시지 추출
        try:
            payload = resp.json()
            msg = payload.get("error") or payload.get("message") or str(payload)
        except ValueError:
            msg = resp.text
        raise RuntimeError(f"GET {url} failed ({resp.status_code}): {msg}")

    try:
        data = resp.json()
    except ValueError:
        raise RuntimeError("response is not valid JSON")

    # list/dict 모두 대응
    if isinstance(data, list):
        df = pd.DataFrame(data)
    elif isinstance(data, dict):
        df = pd.DataFrame([data])
    else:
        raise RuntimeError(f"unexpected JSON type: {type(data)}")

    return df