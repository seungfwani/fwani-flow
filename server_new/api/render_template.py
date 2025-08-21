import json
import logging
import os
import textwrap

from jinja2 import Environment, FileSystemLoader

logger = logging.getLogger()


def render_dag_script(dag_id, tasks, edges, tags=None, schedule=None):
    logger.info(f"▶️ render dag with {dag_id}, {tasks}, {edges}")
    base_directory = os.path.dirname(os.path.abspath(__file__))
    template_directory = os.path.join(base_directory, "templates")
    # create dag file
    # with open(os.path.join(template_directory, "dag_template.tpl"), "r") as f:
    #     template_str = f.read()

    env = Environment(loader=FileSystemLoader(template_directory))
    env.filters["from_json"] = json.loads
    dag_template = env.get_template("dag_template.tpl")
    # dag_template = Template(template_str)

    return dag_template.render(
        dag_id=dag_id,
        schedule=schedule,
        tags=tags,
        tasks=tasks,
        edges=edges,
    )


def render_task_code_script(task_code: str, kind: str, params: dict, impl_namespace: str = None,
                            impl_callable: str = None, ):
    logger.info(f"▶️ render task code {task_code}")
    base_directory = os.path.dirname(os.path.abspath(__file__))
    template_directory = os.path.join(base_directory, "templates")
    env = Environment(loader=FileSystemLoader(template_directory))
    task_code_template = env.get_template("task_code.tpl")
    rendered = task_code_template.render(
        # task_code=task_code,
        task_code=textwrap.indent(task_code, "    ") if task_code else "",
        kind=kind,
        impl_namespace=impl_namespace,
        impl_callable=impl_callable,
        params=params,
    )
    logger.debug(f"rendered task_code:\n{rendered}")
    return rendered


def render_udf_code_block(params_count: int) -> str:
    logger.info(f"▶️ render udf template: {params_count}")
    base_directory = os.path.dirname(os.path.abspath(__file__))
    template_directory = os.path.join(base_directory, "templates")
    env = Environment(loader=FileSystemLoader(template_directory))
    template = env.get_template("udf_code_block.tpl")
    rendered = template.render(
        params=', '.join([f"param_{i}" for i in range(params_count)]),
    )
    logger.debug(f"rendered udf template: {rendered}")
    return rendered
