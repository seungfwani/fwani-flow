import logging
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd
import requests

logger = logging.getLogger(__name__)


def run(*dfs, params: Optional[Dict[str, Any]] = None):
    """
    params 예시:
    {
      "host": "http://192.168.100.170:30842",
      "endpoint": "/graphio/v1/meta-type/workflow/create",
      "meta_type_id": "abcd-1234",
      "headers": {"Authorization": "Bearer ..."},
      "timeout_sec": 20
    }
    """
    if not params:
        raise ValueError("params is required")

    if _ := params.get("is_test", True):
        print("⚠️ This is test. So do not request Save API")
        return None
    host = params.get("host")
    endpoint = "/graphio/v1/meta-type/workflow/create"
    metatype_id = params.get("id")
    owner_id = params.get("ownerId")
    name = params.get("name")
    description = params.get("description")
    connection_instance_id = params.get("connectionInstanceId")
    schema_name = params.get("schemaName")
    tag_ids = params.get("tagIds", [])
    meta_type_ids = params.get("metaTypeIds", [])
    property_mapper = params.get("properties", [])
    before_task_ids = params.get("before_task_ids", [])

    if not host:
        raise ValueError("host is required in params")

    properties = []
    before_task_index = {bti: i for i, (bti, _) in enumerate(before_task_ids)}
    origin_property_mapper = {
        mti['id']: {
            p['id']: p['metaTypePropertyName'] for p in mti['properties']
        } for mti in meta_type_ids
    }
    # 이전 태스크 결과 df 별 컬럼 매핑 생성
    new_series_list = []
    for p in property_mapper:
        properties.append({
            "rawDataPropertyName": p.get('name'),
            "description": p.get("description"),
            "dataType": p.get('dataType'),
        })
        if metaTypeProperties := p.get('metaTypeProperties', []):
            origin_node_id = metaTypeProperties[0].get("metaTypeId")
            origin_property_id = metaTypeProperties[0].get("metaTypePropertyId")
            origin_property_name = origin_property_mapper[origin_node_id][origin_property_id]
            df_idx = before_task_index[origin_node_id]
            selected_series = dfs[df_idx][origin_property_name].rename(p.get('name'))
            new_series_list.append(selected_series)
        else:
            new_series_list.append(pd.Series(name=p.get('name')))

    new_df = pd.concat(new_series_list, axis=1)
    new_df = new_df.replace({np.nan: None})
    url = f"{host.rstrip('/')}/{endpoint.lstrip('/')}"
    payload = {
        "id": metatype_id,
        "ownerId": owner_id,
        "connectionInstanceId": connection_instance_id,
        "metaTypeTableSchema": schema_name,
        "name": name,
        "description": description,
        "tagIds": tag_ids,
        "properties": properties,
        "dataFrame": new_df.to_dict(orient='records'),
    }  # body 데이터 (dict 형태)
    headers = {"Content-Type": "application/json"}  # JSON 형식 요청
    response = requests.post(url, json=payload, headers=headers)

    if response.ok:
        response_json = response.json()
        if "status" in response_json and response_json["status"]:
            print(f"meta type table: {response_json.get('status')}")
            return new_df
        else:
            error_message = response.json()
            print(f"An Error Occured : {error_message}")
            raise Exception(f"{error_message}")
    else:
        print("An Error Occured : " + response.text)
        raise Exception(response.text)


if __name__ == "__main__":
    df_A = pd.DataFrame([[1, 2, 3], [4, 5, 6]], columns=["A", "B", "C"])
    df_B = pd.DataFrame([["가", "나", "다"], ["라", "마", "바"], ['사', '아', '자']], columns=["A", "B", "C"])
    df = run(df_A, df_B, params={
        "host": "http://192.168.109.254:30820",
        "endpoint": "/graphio/v1/meta-type/workflow/create",
        "id": "00000000-0000-4000-9000-000000000003",
        "saveType": "new",
        "ownerId": None,
        "name": "workflow-test-003",
        "description": "workflow-test-001",
        "connectionInstanceId": "c5f92508-4317-4e3c-9e1c-0571c8d23a2a",
        "tagIds": ["62f0ec21-bb9b-490b-985b-a33400173105"],
        "metaTypeIds": [
            {
                "id": "11",
                "name": "메타티입 1",
                "properties": [
                    {
                        "id": "11-p1",
                        "metaTypePropertyName": "A",
                    },
                    {
                        "id": "11-p2",
                        "metaTypePropertyName": "B",
                    },
                    {
                        "id": "11-p3",
                        "metaTypePropertyName": "C",
                    },
                ]
            },
            {
                "id": "22",
                "name": "메타티입 2",
                "properties": [
                    {
                        "id": "22-p1",
                        "metaTypePropertyName": "A",
                    },
                    {
                        "id": "22-p2",
                        "metaTypePropertyName": "B",
                    },
                    {
                        "id": "22-p3",
                        "metaTypePropertyName": "C",
                    },
                ]
            },
        ],
        "properties": [
            {
                "name": "new col 1",
                "description": "df A의 A column 입니다.",
                "dataType": "INTEGER",
                "metaTypeProperties": [
                    {
                        "metaTypeId": "11",
                        "metaTypePropertyId": "11-p1",
                    }
                ]
            },
            {
                "name": "new col 2",
                "description": "df b의 A column 입니다.",
                "dataType": "TEXT",
                "metaTypeProperties": [
                    {
                        "metaTypeId": "22",
                        "metaTypePropertyId": "22-p1",
                    }
                ]
            },
            {
                "name": "new col 3",
                "description": "연결안된 컬럼",
                "dataType": "TEXT",
                "metaTypeProperties": [
                ]
            }
        ],
        "before_task_ids": [
            ("11", "a"),
            ("22", "a"),
            ("33", "a"),
        ],
        "is_test": False,
    })
    pd.options.display.max_colwidth = 1000
    print(df)
