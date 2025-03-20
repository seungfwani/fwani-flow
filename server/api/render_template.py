import json
import os

from jinja2 import Environment, FileSystemLoader

from config import Config
from utils.functions import get_udf_requirements


def render_dag_script(dag_id, task_rules, tasks):
    print(dag_id, task_rules, tasks)
    base_directory = os.path.dirname(os.path.abspath(__file__))
    template_directory = os.path.join(base_directory, "templates")
    # create dag file
    # with open(os.path.join(template_directory, "dag_template.tpl"), "r") as f:
    #     template_str = f.read()

    env = Environment(loader=FileSystemLoader(template_directory))
    env.filters["from_json"] = json.loads
    env.globals["get_udf_requirements"] = get_udf_requirements
    dag_template = env.get_template("dag_template.tpl")
    # dag_template = Template(template_str)

    return dag_template.render(
        dag_id=dag_id,
        task_rules=task_rules,
        tasks=tasks,
        container_mount_udf_source_path=os.path.abspath(Config.UDF_DIR),
        container_mount_udf_target_path="/opt/airflow/udfs",
        container_mount_shared_source_path=os.path.abspath(Config.SHARED_DIR),
        container_mount_shared_target_path="/app/shared",
    )
