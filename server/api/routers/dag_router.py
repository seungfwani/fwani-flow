import base64
import os.path
import uuid

from fastapi import APIRouter, HTTPException
from jinja2 import Template

from api.models.dag_model import DAGRequest
from config import Config

# 워크플로우 블루프린트 생성
router = APIRouter(
    prefix="/dag",
    tags=["Dag"],
)


@router.post("/dag")
async def create_dag(dag: DAGRequest):
    plugin_directory = os.path.dirname(os.path.abspath(__file__))
    dag_id = "dag_" + base64.urlsafe_b64encode(uuid.uuid4().bytes).rstrip(b'=').decode('ascii')

    missing_udfs = [node for node in dag.nodes if
                    not os.path.exists(os.path.join(Config.UDF_DIR, node.filename + ".py"))]
    if missing_udfs:
        return {"message": f"UDFs not found: {missing_udfs}"}

    # Generate DAG content
    tasks = []
    # create tasks
    for i, node in enumerate(dag.nodes):
        current_task_id = node.id

        # 첫 번째 노드인지 확인
        is_first_task = all(edge.to_ != current_task_id for edge in dag.edges)

        if is_first_task:
            python_callable = f"xcom_decorator({node.function})"
            options = f"op_kwargs={{}}"
        else:
            # 부모 노드를 찾아서 before_task_id 설정
            parent_tasks = [edge.from_ for edge in dag.edges if edge.to_ == current_task_id]
            python_callable = f"""lambda **kwargs: xcom_decorator({node.function})(
                before_task_ids={parent_tasks})"""
            options = ""

        tasks.append({
            "function": node.function,
            "filename": node.filename,
            "task_variable_name": current_task_id,
            "current_task_id": current_task_id,
            "python_callable": python_callable,
            "options": options,
        })
    task_rules = []
    for edge in dag.edges:
        task_rules.append(f"{edge.from_} >> {edge.to_}")

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
    dag_file_path = os.path.join(Config.DAG_DIR, f"{dag_id}.py")
    with open(dag_file_path, 'w') as dag_file:
        dag_file.write(filled_code)

    return {
        "message": f"DAG {dag_id} created successfully",
        "dag_file": dag_file_path
    }


# @router.delete("/udf/{filename}")
# async def delete_udf(filename: str):
#     """
#     Delete a python UDF file
#     :param filename:
#     :return:
#     """
#     file_path = os.path.join(Config.UDF_DIR, filename)
#     if not os.path.exists(file_path):
#         raise HTTPException(status_code=404, detail="UDF file not found")
#
#     os.remove(file_path)
#     return {"message": f"{filename} UDF file deleted successfully"}
#
#
# @router.get("/udf")
# async def get_udf_list():
#     """
#     Get all available UDF files
#     :return:
#     """
#     files = [f for f in os.listdir(Config.UDF_DIR) if f.endswith(".py")]
#     return {"files": files}
