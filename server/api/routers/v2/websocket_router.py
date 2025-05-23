import asyncio
import json
import logging

from fastapi import APIRouter, WebSocket, Depends, Query, WebSocketDisconnect
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from api.models.api_model import api_response_wrapper
from api.models.dag_model import AirflowDagRunModel, TaskInstanceResponse
from core.database import get_db
from core.services.dag_run_service import get_all_tasks_by_run_id
from core.services.dag_service import get_all_dag_runs_of_all_versions
from utils.airflow_client import get_airflow_client

logger = logging.getLogger()

router = APIRouter(
    prefix="/ws",
    tags=["websocket"],
)


@router.websocket("/dag-runs-history")
async def websocket_dag_history(websocket: WebSocket,
                                dag_id: str = Query(...),
                                user_id: str = Query("anonymous"),
                                db: Session = Depends(get_db)):
    await websocket.accept()
    client_ip = websocket.client.host
    logger.info(f"🔌 WebSocket connected from IP: {client_ip}, dag_id: {dag_id}, user_id: {user_id}")

    old_dag_runs = []
    old_tasks_data = None
    current_run_id = None

    try:
        while True:
            # 클라이언트 메시지 수신
            try:
                data = await asyncio.wait_for(websocket.receive_json(), timeout=0.1)
                if isinstance(data, dict):
                    if "run_id" in data:
                        current_run_id = data["run_id"]
                        old_tasks_data = None
                        if current_run_id is None:
                            logger.info("🔕 Task streaming disabled by client."
                                        f" IP: {client_ip}, dag_id: {dag_id}, user_id: {user_id}")
                        else:
                            logger.info(f"📨 Task streaming enabled for run_id: {current_run_id}"
                                        f" IP: {client_ip}, dag_id: {dag_id}, user_id: {user_id}")
            except asyncio.TimeoutError:
                pass

            # run_id 가 설정된 경우 주기적으로 task 정보도 전송
            if current_run_id:
                try:
                    with next(get_airflow_client()) as airflow_client:
                        airflow_dag_run_history, tasks = get_all_tasks_by_run_id(current_run_id, airflow_client, db)
                        tasks_data = TaskInstanceResponse.from_data(airflow_dag_run_history, tasks)
                        if tasks_data != old_tasks_data:
                            logger.info(f"🙆 Have a different tasks of run_id: {current_run_id}")

                            async def get_tasks():
                                return tasks_data

                            task_response = (await api_response_wrapper(get_tasks)()).model_dump()
                            task_response["type"] = "tasks"
                            await websocket.send_json(jsonable_encoder(task_response))
                            old_tasks_data = tasks_data
                        else:
                            logger.info(f"🤷 Nothing to different tasks of run_id: {current_run_id}")
                except Exception as e:
                    logger.warning(f"🚫 Failed to fetch task info: {e}")

            # DAG 실행 이력 조회
            logger.info(f"🔄 Check dag runs of dag_id: {dag_id}")
            dag_runs = get_all_dag_runs_of_all_versions(dag_id, db)
            if old_dag_runs == dag_runs:
                logger.info(f"🤷 Nothing to different dag runs of dag_id: {dag_id}")
                await asyncio.sleep(3)
                continue
            logger.info(f"🙆 Have a different dag runs of dag_id: {dag_id}")

            response_data = [AirflowDagRunModel.from_orm(data) for data in dag_runs]
            async def get_dag_runs():
                return response_data

            # 클라이언트에게 전송
            response = (await api_response_wrapper(get_dag_runs)()).model_dump()
            response["type"] = "dag_runs"
            await websocket.send_json(jsonable_encoder(response))
            old_dag_runs = dag_runs
            # 일정 시간 대기 후 반복 (원한다면 주기적 push도 가능)

            await asyncio.sleep(5)

    except WebSocketDisconnect:
        logger.info(f"⛓️‍💥 Client disconnected from IP: {client_ip}, dag_id: {dag_id}, user_id: {user_id}")

    except Exception as e:
        await websocket.send_text(json.dumps({"error": str(e)}))
