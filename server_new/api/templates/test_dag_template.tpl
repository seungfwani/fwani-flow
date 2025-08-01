"""
template 을 통해 만들어진 DAG 입니다.
"""
from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.python import PythonVirtualenvOperator
from utils.decorator import wrapped_callable

default_args = {
    'owner': 'code_generator',
    'start_date': days_ago(1),
    'retries': 1,
}
dag = DAG(
    dag_id='{{ dag_id }}',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags={{ tags }}
)
{% for task in tasks -%}
{{ task.code }}

{{ task.variable_id }} = PythonVirtualenvOperator(
    task_id='{{ task.variable_id }}',
    dag=dag,
    python_callable=wrapped_callable,
    op_args=[""],
    op_kwargs={
    },
    system_site_packages=False,
    requirements={{ task.python_libraries }},
)


{% endfor %}

{% for edge in edges -%}
{{ edge.source.variable_id }} >> {{ edge.target.variable_id }}
{% endfor %}
