import logging
from typing import List, Callable, Optional, Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from core.airflow_client import AirflowClient, get_airflow_client
from core.database import get_db, get_airflow
from core.services.flow_definition_service import FlowDefinitionService
from models.api.api_model import api_response_wrapper, APIResponse
from models.api.dag_model import DAGResponse, DAGRequest, ActiveStatusRequest, MultipleRequest
from utils.functions import to_bool

logger = logging.getLogger()

# 워크플로우 블루프린트 생성
router = APIRouter(
    prefix="/dag-management",
    tags=["DagManagement"],
)


@router.post("/dag",
             response_model=APIResponse[dict[str, str]],
             responses={
                 200: {
                     "content": {
                         "application/json": {
                             "example": {
                                 "success": True,
                                 "message": "요청이 정상 처리 되었습니다.",
                                 "data": {
                                     "id": "f9d52759-4e66-4d99-8279-a0b236b9fdc9"
                                 },
                                 "error": {}
                             }
                         }
                     }
                 }
             }
             )
@api_response_wrapper
async def save_dag(dag: DAGRequest, db: Session = Depends(get_db), airflow: Session = Depends(get_airflow)):
    """
    DAG 저장 api
    """
    dag_service = FlowDefinitionService(db, airflow)
    return {"id": dag_service.save_dag(dag)}


@router.patch("/dag/{dag_id}/update",
              response_model=APIResponse[dict[str, str]],
              responses={
                  200: {
                      "content": {
                          "application/json": {
                              "example": {
                                  "success": True,
                                  "message": "요청이 정상 처리 되었습니다.",
                                  "data": {
                                      "id": "f9d52759-4e66-4d99-8279-a0b236b9fdc9"
                                  },
                                  "error": {}
                              }
                          }
                      }
                  }
              }
              )
@api_response_wrapper
async def update_dag(dag_id: str, dag: DAGRequest, db: Session = Depends(get_db)):
    """
    DAG 업데이트 api
    """
    dag_service = FlowDefinitionService(db)
    return {"id": dag_service.update_dag(dag_id, dag)}


@router.patch("/dag/{dag_id}/restore-deleted",
              response_model=APIResponse[dict[str, str]],
              responses={
                  200: {
                      "content": {
                          "application/json": {
                              "example": {
                                  "success": True,
                                  "message": "요청이 정상 처리 되었습니다.",
                                  "data": {
                                      "id": "f9d52759-4e66-4d99-8279-a0b236b9fdc9"
                                  },
                                  "error": {}
                              }
                          }
                      }
                  }
              }
              )
@api_response_wrapper
async def restore_deleted_dag(dag_id: str, db: Session = Depends(get_db)):
    """
    임시 삭제된 DAG 재생성
    """
    dag_service = FlowDefinitionService(db)
    return {"id": dag_service.restore_deleted_dag(dag_id)}


@router.patch("/dag/{dag_id}/restore-snapshot/{version}",
              response_model=APIResponse[dict[str, str]],
              responses={
                  200: {
                      "content": {
                          "application/json": {
                              "example": {
                                  "success": True,
                                  "message": "요청이 정상 처리 되었습니다.",
                                  "data": {
                                      "id": "f9d52759-4e66-4d99-8279-a0b236b9fdc9"
                                  },
                                  "error": {}
                              }
                          }
                      }
                  }
              }
              )
@api_response_wrapper
async def restore_snapshot_dag(dag_id: str, version: int, db: Session = Depends(get_db)):
    """
    DAG snapshot 형태로 복구
    """
    dag_service = FlowDefinitionService(db)
    return {"id": dag_service.restore_flow_by_snapshot(dag_id, version)}


@router.patch("/dag/{dag_id}/active-status",
              response_model=APIResponse[dict[str, bool]],
              responses={
                  200: {
                      "content": {
                          "application/json": {
                              "example": {
                                  "success": True,
                                  "message": "요청이 정상 처리 되었습니다.",
                                  "data": {
                                      "active_status": True
                                  },
                                  "error": {}
                              }
                          }
                      }
                  }
              }
              )
@api_response_wrapper
async def update_dag_active_status(dag_id: str,
                                   request: ActiveStatusRequest,
                                   db: Session = Depends(get_db),
                                   airflow_client: AirflowClient = Depends(get_airflow_client)
                                   ):
    """
    DAG active 상태 변경 api
    """
    dag_service = FlowDefinitionService(db, airflow_client=next(airflow_client))
    return {"active_status": dag_service.update_dag_active_status(dag_id, request.active_status)}


@router.delete("/dag/{dag_id}/permanently",
               response_model=APIResponse[dict[str, str]],
               responses={
                   200: {
                       "content": {
                           "application/json": {
                               "example": {
                                   "success": True,
                                   "message": "요청이 정상 처리 되었습니다.",
                                   "data": {
                                       "id": "f9d52759-4e66-4d99-8279-a0b236b9fdc9"
                                   },
                                   "error": {}
                               }
                           }
                       }
                   }
               }
               )
@api_response_wrapper
async def delete_dag_permanently(dag_id: str, db: Session = Depends(get_db)):
    """
    DAG 영구 삭제

    (주의!) 실행 기록도 삭제됩니다.
    """
    dag_service = FlowDefinitionService(db)
    return {"id": dag_service.delete_dag_permanently(dag_id)}


@router.delete("/dag/{dag_id}/temporary",
               response_model=APIResponse[dict[str, str]],
               responses={
                   200: {
                       "content": {
                           "application/json": {
                               "example": {
                                   "success": True,
                                   "message": "요청이 정상 처리 되었습니다.",
                                   "data": {
                                       "id": "f9d52759-4e66-4d99-8279-a0b236b9fdc9"
                                   },
                                   "error": {}
                               }
                           }
                       }
                   }
               }
               )
@api_response_wrapper
async def delete_dag_temporary(dag_id: str, db: Session = Depends(get_db)):
    """
    DAG 임시 삭제 (airflow 에서만 삭제)

    (주의!) 실행 기록도 삭제됩니다.
    """
    dag_service = FlowDefinitionService(db)
    return {"id": dag_service.delete_dag_temporary(dag_id)}


@router.delete("/dag",
               response_model=APIResponse[list[dict[str, str]]],
               responses={
                   200: {
                       "content": {
                           "application/json": {
                               "example": {
                                   "success": True,
                                   "message": "요청이 정상 처리 되었습니다.",
                                   "data": [
                                       {
                                           "id": "f9d52759-4e66-4d99-8279-a0b236b9fdc9"
                                       }
                                   ],
                                   "error": {}
                               }
                           }
                       }
                   }
               }
               )
@api_response_wrapper
async def delete_dag_list(dag_ids: MultipleRequest, db: Session = Depends(get_db)):
    """
    여러 DAG 임시 삭제 (airflow 에서만 삭제)

    (주의!) 실행 기록도 삭제됩니다.
    """
    dag_service = FlowDefinitionService(db)
    result = []
    for dag_id in dag_ids.ids:
        result.append({"id": dag_service.delete_dag_temporary(dag_id)})
    return result


@router.get("/dag-count",
            response_model=APIResponse[dict[str, int]],
            responses={
                200: {
                    "content": {
                        "application/json": {
                            "example": {
                                "success": True,
                                "message": "요청이 정상 처리 되었습니다.",
                                "data": {
                                    "total_count": 10
                                },
                                "error": {}
                            }
                        }
                    }
                }
            }
            )
@api_response_wrapper
async def get_total_workflow_count(db: Session = Depends(get_db)):
    dag_service = FlowDefinitionService(db)
    return {"total_count": dag_service.get_dag_total_count()}


def parse_comma_query(default: None, alias: str, description: str, cast: Callable[[str], object]):
    def dep(value: Optional[str] = Query(default, alias=alias, description=description)) -> set[object]:
        if not value:
            return set()
        return {cast(v.strip()) for v in value.split(",") if v.strip()}

    return dep


@router.get("/dag",
            response_model=APIResponse[dict[str, List[DAGResponse] | int]],
            responses={
                200: {
                    "content": {
                        "application/json": {
                            "example": {
                                "success": True,
                                "message": "요청이 정상 처리 되었습니다.",
                                "data": {
                                    "total_count": 10,
                                    "filtered_count": 5,
                                    "result_count": 2,
                                    "list": [

                                        {
                                            "id": "f9d52759-4e66-4d99-8279-a0b236b9fdc9",
                                            "name": "DAG Na23me",
                                            "description": "DAG Description",
                                            "owner": "DAG Owner",
                                            "nodes": [
                                                {
                                                    "id": "3b6d1e59-2847-4f87-9baf-2581b65f353c",
                                                    "type": "custom",
                                                    "position": {
                                                        "x": 0,
                                                        "y": 0
                                                    },
                                                    "data": {
                                                        "label": "node name",
                                                        "kind": "code",
                                                        "python_libraries": [
                                                            "pandas==2.3.1",
                                                            "requests==2.32.3"
                                                        ],
                                                        "code": "def run():\n    print(1)\n    return 1",
                                                        "input_meta_type": {},
                                                        "output_meta_type": {},
                                                        "inputs": {
                                                            "key1": "value1",
                                                            "key2": "value2"
                                                        }
                                                    },
                                                    "style": {
                                                        "key1": "value1",
                                                        "key2": "value2"
                                                    }
                                                },
                                                {
                                                    "id": "4b05d07c-55cb-4d70-ab2f-2cc335cb68de",
                                                    "type": "custom",
                                                    "position": {
                                                        "x": 0,
                                                        "y": 0
                                                    },
                                                    "data": {
                                                        "label": "node name",
                                                        "kind": "code",
                                                        "python_libraries": [
                                                            "pandas==2.3.1",
                                                            "requests==2.32.3"
                                                        ],
                                                        "code": "def run(a):\n    print(1+a)\n    return 1+a",
                                                        "input_meta_type": {},
                                                        "output_meta_type": {},
                                                        "inputs": {
                                                            "key1": "value1",
                                                            "key2": "value2"
                                                        }
                                                    },
                                                    "style": {
                                                        "key1": "value1",
                                                        "key2": "value2"
                                                    }
                                                }
                                            ],
                                            "edges": [
                                                {
                                                    "id": "d2314f44-da47-4446-a177-a8a14987cffc",
                                                    "type": "custom",
                                                    "source": "3b6d1e59-2847-4f87-9baf-2581b65f353c",
                                                    "target": "4b05d07c-55cb-4d70-ab2f-2cc335cb68de",
                                                    "label": "",
                                                    "labelStyle": {},
                                                    "labelBgStyle": {},
                                                    "labelBgPadding": [
                                                        0
                                                    ],
                                                    "labelBgBorderRadius": 0,
                                                    "style": {}
                                                }
                                            ],
                                            "schedule": "0 9 * * *",
                                            "is_draft": True,
                                            "max_retries": 0,
                                            "updated_at": "2025-08-13T06:44:55",
                                            "active_status": False,
                                            "execution_status": None
                                        },
                                        {
                                            "id": "...",
                                            "...": "..."
                                        }
                                    ],
                                },
                                "error": {}
                            }
                        }
                    }
                }
            }
            )
@api_response_wrapper
async def get_dag_list(
        active_status: set[bool] = Depends(parse_comma_query(None,
                                                             "active_status",
                                                             "active status filter (ex. true,false)",
                                                             to_bool)),
        execution_status: set[str] = Depends(parse_comma_query(None,
                                                               "execution_status",
                                                               "execution status filter (ex. success,failed)",
                                                               str)),
        name: str = Query(None, description="dag name filter"),
        sort: str = Query(None, description="dag sort filter"),
        offset: int = Query(0, description="dag list offset"),
        limit: int = Query(10, description="dag list limit"),
        include_deleted: bool = Query(False, description="삭제된 DAG 포함 여부"),
        db: Session = Depends(get_db)):
    """
    모든 이용가능한 DAG 리스트를 조회
    """
    dag_service = FlowDefinitionService(db)
    dag_list, result_count, filtered_count, total_count = dag_service.get_dag_list(active_status,
                                                                                   execution_status,
                                                                                   name,
                                                                                   sort,
                                                                                   offset,
                                                                                   limit,
                                                                                   include_deleted)
    return {
        "total_count": total_count,
        "filtered_count": filtered_count,
        "result_count": result_count,
        "list": dag_list,
    }


@router.get("/dag-snapshot/{dag_id}",
            response_model=APIResponse[list[dict[str, Any]]],
            responses={
                200: {
                    "content": {
                        "application/json": {
                            "example": {
                                "success": True,
                                "message": "요청이 정상 처리 되었습니다.",
                                "data": [
                                    {
                                        "id": "34e28f73-b6e1-477d-97b0-0de578a9b265",
                                        "version": 2,
                                        "is_current": False,
                                        "is_draft": True
                                    },
                                    {
                                        "id": "a714b5b7-4d9d-4683-b316-d10c4000e95a",
                                        "version": 1,
                                        "is_current": True,
                                        "is_draft": False
                                    }
                                ],
                                "error": {}
                            }
                        }
                    }
                }
            }
            )
@api_response_wrapper
async def get_dag_snapshots(dag_id: str, db: Session = Depends(get_db)):
    """
    모든 이용가능한 DAG 리스트를 조회
    """
    dag_service = FlowDefinitionService(db)
    snapshots = dag_service.get_snapshot_list(dag_id)
    return snapshots


@router.get("/dag/{dag_id}",
            response_model=APIResponse[dict[str, DAGResponse | None]],
            responses={
                200: {
                    "content": {
                        "application/json": {
                            "example": {
                                "success": True,
                                "message": "요청이 정상 처리 되었습니다.",
                                "data": {
                                    "origin": {
                                        "id": "f9d52759-4e66-4d99-8279-a0b236b9fdc9",
                                        "name": "DAG Na23me",
                                        "description": "DAG Description",
                                        "owner": "DAG Owner",
                                        "nodes": [
                                            {
                                                "id": "3b6d1e59-2847-4f87-9baf-2581b65f353c",
                                                "type": "custom",
                                                "position": {
                                                    "x": 0,
                                                    "y": 0
                                                },
                                                "data": {
                                                    "label": "node name",
                                                    "kind": "code",
                                                    "python_libraries": [
                                                        "pandas==2.3.1",
                                                        "requests==2.32.3"
                                                    ],
                                                    "code": "def run():\n    print(1)\n    return 1",
                                                    "input_meta_type": {},
                                                    "output_meta_type": {},
                                                    "inputs": {
                                                        "key1": "value1",
                                                        "key2": "value2"
                                                    }
                                                },
                                                "style": {
                                                    "key1": "value1",
                                                    "key2": "value2"
                                                }
                                            },
                                            {
                                                "id": "4b05d07c-55cb-4d70-ab2f-2cc335cb68de",
                                                "type": "custom",
                                                "position": {
                                                    "x": 0,
                                                    "y": 0
                                                },
                                                "data": {
                                                    "label": "node name",
                                                    "kind": "code",
                                                    "python_libraries": [
                                                        "pandas==2.3.1",
                                                        "requests==2.32.3"
                                                    ],
                                                    "code": "def run(a):\n    print(1+a)\n    return 1+a",
                                                    "input_meta_type": {},
                                                    "output_meta_type": {},
                                                    "inputs": {
                                                        "key1": "value1",
                                                        "key2": "value2"
                                                    }
                                                },
                                                "style": {
                                                    "key1": "value1",
                                                    "key2": "value2"
                                                }
                                            }
                                        ],
                                        "edges": [
                                            {
                                                "id": "d2314f44-da47-4446-a177-a8a14987cffc",
                                                "type": "custom",
                                                "source": "3b6d1e59-2847-4f87-9baf-2581b65f353c",
                                                "target": "4b05d07c-55cb-4d70-ab2f-2cc335cb68de",
                                                "label": "",
                                                "labelStyle": {},
                                                "labelBgStyle": {},
                                                "labelBgPadding": [
                                                    0
                                                ],
                                                "labelBgBorderRadius": 0,
                                                "style": {}
                                            }
                                        ],
                                        "schedule": "0 9 * * *",
                                        "is_draft": True,
                                        "max_retries": 0,
                                        "updated_at": "2025-08-13T06:44:55",
                                        "active_status": False,
                                        "execution_status": None
                                    },
                                    "temp": {
                                        "id": "...",
                                        "...": "..."
                                    }
                                },
                                "error": {}
                            }
                        }
                    }
                }
            }
            )
@api_response_wrapper
async def get_dag(dag_id: str, db: Session = Depends(get_db)):
    """
    DAG 의 상세 정보 조회
    """
    dag_service = FlowDefinitionService(db)
    current, draft = dag_service.get_dag(dag_id)
    return {"origin": current, "temp": draft}


@router.get("/check-dag-name",
            response_model=APIResponse[dict[str, bool]],
            responses={
                200: {
                    "content": {
                        "application/json": {
                            "example": {
                                "success": True,
                                "message": "요청이 정상 처리 되었습니다.",
                                "data": {
                                    "available": True
                                },
                                "error": {}
                            }
                        }
                    }
                }
            }
            )
@api_response_wrapper
async def check_dag_name(name: str = Query(..., description="체크 할 dag name"), db: Session = Depends(get_db)):
    """
    DAG 이름 체크
    """
    dag_service = FlowDefinitionService(db)
    return {"available": dag_service.check_available_dag_name(dag_name=name)}
