{{ task.variable_id }} = DockerOperator(
    task_id='{{ task.variable_id }}',
    dag=dag,
    image="{{ task.function.docker_image_tag }}",
    api_version="auto",
    auto_remove=True,
    docker_url="unix:///var/run/docker.sock",
    working_dir="/app",
    environment={
        "PYTHONPATH": "{{ container_mount_udf_target_path }}",
        "input_schema": json.dumps([
        {% for inp in task.function.inputs -%}
            {"name": "{{ inp.name }}", "type": "{{ inp.type }}"},
        {% endfor -%}
        ]),
        "kwargs": json.dumps({
            {% for inp in task.inputs -%}
            "{{ inp.key }}": {% if inp.type == "string" %}"{{ inp.value }}"{% else %}{{ inp.value }}{% endif %},
            {% endfor -%}
            "operator_type": "{{ task.function.operator_type }}",
            {% raw -%}
            "dag_id": "{{ ti.dag_id }}",
            "run_id": "{{ ti.run_id }}",
            "task_id": "{{ ti.task.task_id }}",
            "is_last_task": "{% if ti.task.downstream_list | length == 0 %}True{% else %}False{% endif %}",
            "is_first_task": "{% if ti.task.upstream_list | length == 0 %}True{% else %}False{% endif %}",
            {% endraw -%}
        }),
    },
    mounts=[
        Mount(source="{{ container_mount_udf_source_path }}",
              target="{{ container_mount_udf_target_path }}",
              type="bind"),
        Mount(source="{{ container_mount_shared_source_path }}",
              target="{{ container_mount_shared_target_path }}",
              type="bind"),
    ],
    command="""python3 -c '
import os
import json

from utils.decorator import file_decorator
from {{ task.function.name }}.{{ task.function.main_filename }} import {{ task.function.function }}


inputs = json.loads(os.getenv("input_schema"))
kwargs = json.loads(os.getenv("kwargs"))
print(inputs, kwargs)

result = file_decorator(inputs=inputs)({{ task.function.function }})(**kwargs)
print(f"result: {result}")
'""",
)
