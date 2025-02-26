import base64
import logging
import os.path

from fastapi import APIRouter, HTTPException, Depends
from jinja2 import Template
from sqlalchemy.orm import Session

from api.models.dag_model import DAGRequest
from config import Config
from core.database import get_db
from models.edge import Edge
from models.flow import Flow
from models.function_library import FunctionLibrary
from models.task import Task

logger = logging.getLogger()

# 워크플로우 블루프린트 생성
router = APIRouter(
    prefix="/dag",
    tags=["Dag"],
)


@router.post("/")
async def create_dag(dag: DAGRequest, db: Session = Depends(get_db)):
    """DAG 생성 및 DB 에 저장"""
    plugin_directory = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../..")
    dag_id = "dag_" + base64.urlsafe_b64encode(dag.name.encode()).rstrip(b'=').decode('ascii')
    dag_file_path = os.path.join(Config.DAG_DIR, f"{dag_id}.py")
    try:
        with db.begin():

            # 한 번의 쿼리로 조회
            udf_functions: {str, FunctionLibrary} = {
                udf.id: udf
                for udf in db.query(FunctionLibrary)
                .filter(FunctionLibrary.id.in_([node.function_id for node in dag.nodes]))
                .all()
            }

            # 없는 UDF 찾기
            missing_udfs = [node for node in dag.nodes
                            if node.function_id not in udf_functions.keys()]

            # UDF가 누락되었다면 에러 반환
            if missing_udfs:
                print(f"UDFs not found: {missing_udfs}")
                return {"message": f"UDFs not found: {missing_udfs}"}

            flow = Flow(id=dag_id, name=dag.name, description=dag.description)
            db.add(flow)
            db.flush()

            # Generate DAG content
            tasks = []
            # create tasks
            for i, node in enumerate(dag.nodes):
                current_task_id = node.id

                # 첫 번째 노드인지 확인
                is_first_task = all(edge.to_ != current_task_id for edge in dag.edges)

                if is_first_task:
                    python_callable = f"xcom_decorator({udf_functions[node.function_id].function})"
                    options = f"op_kwargs={{}}"
                else:
                    # 부모 노드를 찾아서 before_task_id 설정
                    parent_tasks = [edge.from_ for edge in dag.edges if edge.to_ == current_task_id]
                    python_callable = f"""lambda **kwargs: xcom_decorator({udf_functions[node.function_id].function})(
                        before_task_ids={parent_tasks})"""
                    options = ""

                tasks.append(Task(
                    variable_id=current_task_id,
                    flow_id=flow.id,
                    function_id=node.function_id,
                    python_callable=python_callable,
                    options=options,
                ))
            task_rules = []
            edges = []
            for edge in dag.edges:
                task_rules.append(f"{edge.from_} >> {edge.to_}")
                edges.append(Edge(flow_id=flow.id, from_task_id=edge.from_, to_task_id=edge.to_))

            db.add_all(tasks)
            db.add_all(edges)
            db.flush()

            # create dag
            with open(os.path.join(plugin_directory, "dag_template.tpl"), "r") as f:
                template_str = f.read()
            dag_template = Template(template_str)

            filled_code = dag_template.render(
                dag_id=dag_id,
                task_rules=task_rules,
                tasks=tasks,
            )

            # write dag
            with open(dag_file_path, 'w') as dag_file:
                dag_file.write(filled_code)
            db.commit()
        return {
            "message": f"DAG {dag_id} created successfully",
            "dag_file": dag_file_path
        }
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        db.rollback()
        print(f"🔄 메타데이터 롤백")

        # ✅ 파일 저장 후 DB 실패 시 파일 삭제
        if os.path.exists(dag_file_path):
            os.remove(dag_file_path)
            print(f"🗑️ 저장된 파일 삭제: {dag_file_path}")
        raise HTTPException(status_code=500, detail=f"DAG creation failed {e}")


@router.delete("/{dag_id}")
async def delete_udf(dag_id: str, db: Session = Depends(get_db)):
    """
    Delete a python UDF file
    :param dag_id:
    :return:
    """

    if not (dag_data := db.query(Flow).filter(Flow.id == dag_id).first()):
        return {"message": f"UDF {dag_id} not found"}
    dag_file_path = os.path.join(Config.DAG_DIR, f"{dag_id}.py")

    if not os.path.exists(dag_file_path):
        print(f"Warning: No file to delete {dag_file_path}")
    else:
        os.remove(dag_file_path)
        print(f"🗑️ 저장된 DAG 파일 삭제: {dag_file_path}")

    db.query(Edge).filter(Edge.flow_id == dag_data.id).delete()
    db.query(Task).filter(Task.flow_id == dag_data.id).delete()
    db.delete(dag_data)
    db.commit()
    print(f"🗑️ DAG 메타데이터 삭제: {dag_data}")

    return {"message": f"{dag_data} DAG fil e deleted successfully"}

# @router.get("/udf")
# async def get_udf_list():
#     """
#     Get all available UDF files
#     :return:
#     """
#     files = [f for f in os.listdir(Config.UDF_DIR) if f.endswith(".py")]
#     return {"files": files}
