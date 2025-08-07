"""
template 을 통해 만들어진 DAG 입니다.
"""
from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.python import PythonVirtualenvOperator

default_args = {
    'owner': 'code_generator',
    'start_date': days_ago(1),
    'retries': 1,
}
dag = DAG(
    dag_id='{{ dag_id }}',
    default_args=default_args,
    schedule_interval={% if schedule is string %}"{{ schedule }}"{% else %}{{ schedule }}{% endif %},
    catchup=False,
    tags={{ tags }}
)
{% for task in tasks -%}
{% set before_task_ids = [] -%}
    {% for edge in edges -%}
        {% if edge.target.id == task.id -%}
            {% set _ = before_task_ids.append(edge.source.variable_id) -%}
        {% endif %}
    {% endfor %}
from {{ dag_id }}.func_{{ task.variable_id }} import wrapper_run as run_{{ task.variable_id }}

{{ task.variable_id }} = PythonVirtualenvOperator(
    task_id='{{ task.variable_id }}',
    dag=dag,
    python_callable=run_{{ task.variable_id }},
    op_args=[],
    op_kwargs={
        "before_task_ids": {{ before_task_ids }},
        "base_dir": "{{ base_dir }}",
        {% raw -%}
        "dag_id": "{{ ti.dag_id }}",
        "run_id": "{{ ti.run_id }}",
        "task_id": "{{ ti.task.task_id }}",
        {% endraw -%}
    },
    system_site_packages=False,
    requirements={{ task.python_libraries }},
)
{% endfor %}

{% for edge in edges -%}
{{ edge.source.variable_id }} >> {{ edge.target.variable_id }}
{% endfor %}
