{{ task.variable_id }} = DockerOperator(
    task_id='{{ task.variable_id }}',
    dag=dag,
    image="{{ task.function.docker_image_tag }}",
    api_version="auto",
    auto_remove=True,
    docker_url="unix:///var/run/docker.sock",
    working_dir="/app",
    environment={"PYTHONPATH": {{ container_mount_udf_source_path }}},
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
from {{ task.function.name }} import {{ task.function.function }}


inputs = json.loads(os.getenv("inputs"))
kwargs = json.loads(os.getenv("kwargs"))
print(inputs, kwargs)

result = {{ task.function.function }}(**validated_inputs)
print(f"result: {result}")
'""",
)
