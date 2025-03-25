import base64
import logging
import os.path
import traceback
from typing import List

from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session

from api.models.api_model import api_response_wrapper, APIResponse
from api.models.dag_model import DAGRequest, DAGResponse
from api.render_template import render_dag_script
from config import Config
from core.database import get_db
from models.edge import Edge
from models.flow import Flow
from models.function_library import FunctionLibrary
from models.task import Task
from models.task_input import TaskInput
from models.task_ui import TaskUI
from utils.udf_validator import get_validated_inputs

logger = logging.getLogger()

# 워크플로우 블루프린트 생성
router = APIRouter(
    prefix="/dag",
    tags=["Dag"],
)


@router.post("",
             response_model=APIResponse[DAGResponse],
             )
@api_response_wrapper
async def create_dag(dag: DAGRequest, db: Session = Depends(get_db)):
    """DAG 생성 및 DB 에 저장"""
    logger.info(f"Request Data: {dag}")
    dag_id = "dag_" + base64.urlsafe_b64encode(dag.name.encode()).rstrip(b'=').decode('ascii')
    dag_file_path = os.path.join(Config.DAG_DIR, f"{dag_id}.py")
    try:
        with db.begin():
            # 한 번의 쿼리로 조회
            udf_functions: {str, FunctionLibrary} = {
                udf.id: udf
                for udf in db.query(FunctionLibrary)
                .filter(FunctionLibrary.id.in_([node.data.function_id for node in dag.nodes]))
                .all()
            }

            # 없는 UDF 찾기
            missing_udfs = [node for node in dag.nodes
                            if node.data.function_id not in udf_functions.keys()]

            # UDF가 누락되었다면 에러 반환
            if missing_udfs:
                logger.info(f"UDFs not found: {missing_udfs}")
                return {"message": f"UDFs not found: {missing_udfs}"}

            # Flow 생성
            flow = Flow(id=dag_id, name=dag.name, description=dag.description)
            db.add(flow)
            db.flush()

            # tasks 생성
            id_to_variable_id = {}
            for i, node in enumerate(dag.nodes):
                variable_id = f"task_{i}"
                id_to_variable_id[node.id] = variable_id
            tasks = {}
            for i, node in enumerate(dag.nodes):
                # 첫 번째 노드인지 확인
                is_first_task = all(edge.target != node.id for edge in dag.edges)

                task_inputs = get_validated_inputs(udf_functions[node.data.function_id].inputs, node.data.inputs)
                if not is_first_task:
                    # 부모 노드를 찾아서 before_task_id 설정
                    task_inputs.append({
                        "key": "before_task_ids",
                        "value": [id_to_variable_id[edge.source] for edge in dag.edges if edge.target == node.id],
                        "type": "string"
                    })
                task_data = Task(
                    variable_id=id_to_variable_id[node.id],
                    flow_id=flow.id,
                    function_id=node.data.function_id,
                    decorator="file_decorator",
                )
                for inp in task_inputs:
                    task_data.inputs.append(TaskInput(
                        key=inp.get("key"),
                        type=inp.get("type"),
                        value=inp.get("value"),
                    ))
                logger.info(f"Task Data: {task_data}")
                task_data.task_ui = TaskUI(type=node.type,
                                           position=node.position,
                                           style=node.style, )
                tasks[node.id] = task_data

            # edge 생성
            edges = []
            for edge in dag.edges:
                edges.append(Edge(flow_id=flow.id,
                                  from_task=tasks[edge.source],
                                  to_task=tasks[edge.target]
                                  ))

            # save dag metadata to DB
            db.add_all(tasks.values())
            db.add_all(edges)
            db.flush()

            # write dag
            with open(dag_file_path, 'w') as dag_file:
                dag_file.write(render_dag_script(dag_id, tasks.values(), edges))
            db.commit()
        return DAGResponse.from_dag(flow)
    except Exception as e:
        logger.error(f"❌ 오류 발생: {e}")
        logger.error(traceback.format_exc())
        db.rollback()
        logger.warning(f"🔄 메타데이터 롤백")

        # ✅ 파일 저장 후 DB 실패 시 파일 삭제
        if os.path.exists(dag_file_path):
            os.remove(dag_file_path)
            logger.warning(f"🗑️ 저장된 파일 삭제: {dag_file_path}")
        raise HTTPException(status_code=500, detail=f"DAG creation failed. {e}")


@router.delete("/{dag_id}",
               response_model=APIResponse[DAGResponse],
               )
@api_response_wrapper
async def delete_dag(dag_id: str, db: Session = Depends(get_db)):
    """
    Delete a python DAG file
    :param dag_id:
    :param db:
    :return:
    """

    if not (dag_data := db.query(Flow).filter(Flow.id == dag_id).first()):
        return {"message": f"DAG {dag_id} not found"}
    dag_file_path = os.path.join(Config.DAG_DIR, f"{dag_id}.py")

    if not os.path.exists(dag_file_path):
        logger.warning(f"Warning: No file to delete {dag_file_path}")
    else:
        os.remove(dag_file_path)
        logger.warning(f"🗑️ 저장된 DAG 파일 삭제: {dag_file_path}")

    db.query(Edge).filter(Edge.flow_id == dag_data.id).delete()
    db.query(Task).filter(Task.flow_id == dag_data.id).delete()
    db.delete(dag_data)
    db.commit()
    logger.warning(f"🗑️ DAG 메타데이터 삭제: {dag_data}")

    return DAGResponse.from_dag(dag_data)


@router.get("",
            response_model=APIResponse[List[DAGResponse]],
            )
@api_response_wrapper
async def get_dag_list(db: Session = Depends(get_db)):
    """
    Get all available DAG
    :return:
    """
    logger.info(f"▶️ DAG 리스트 조회")
    return [DAGResponse.from_dag(dag) for dag in db.query(Flow).all()]


@router.get("/{dag_id}",
            response_model=APIResponse[DAGResponse],
            )
@api_response_wrapper
async def get_dag(dag_id: str, db: Session = Depends(get_db)):
    """
    Get DAG
    :return:
    """
    logger.info(f"Get DAG {dag_id}")
    return DAGResponse.from_dag(db.query(Flow).filter(Flow.id == dag_id).first())
