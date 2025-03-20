"""
template 을 통해 만들어진 DAG 입니다.
"""
from datetime import datetime

from airflow import DAG

{% set operator_types = [] -%}

{% for task in tasks -%}
    {% if task.function.operator_type not in operator_types -%}
        {% set _ = operator_types.append(task.function.operator_type) -%}
    {% endif -%}
{% endfor -%}
{% if "docker" in operator_types -%}
import json

from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
{% endif %}
{% if "python_virtual" in operator_types -%}
from airflow.operators.python import PythonVirtualenvOperator
from utils.decorator import wrapped_callable
{% endif %}
{% if "python" in operator_types -%}
from airflow.operators.python import PythonOperator
from utils.decorator import file_decorator
{% endif %}

default_args = {
    'owner': 'code_generator',
    'start_date': datetime.now(),
    'retries': 1,
}

dag = DAG(
    dag_id='{{ dag_id }}',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)
{% for task in tasks -%}
{% if task.function.operator_type == 'docker' -%}
{% include 'docker_operator.tpl' %}
{% elif task.function.operator_type == 'python_virtual' -%}
{% include 'python_virtual_operator.tpl' %}
{% else -%}
{% include 'python_operator.tpl' %}
{% endif %}
{% endfor %}

{% for edge in edges -%}
{{ edge.from_task.variable_id }} >> {{ edge.to_task.variable_id }}
{% endfor %}
