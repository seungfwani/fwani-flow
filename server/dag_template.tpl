"""
template 을 통해 만들어진 DAG 입니다.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context

from utils.decorator import xcom_decorator

{% for task in tasks -%}
from {{ task.function.name }} import {{ task.function.function }}
{% endfor %}

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
{% for task in tasks %}
{{ task.variable_id }} = PythonOperator(
    task_id='{{ task.variable_id }}',
    dag=dag,
    {%- if task.decorator %}
    python_callable={{ task.decorator }}(
        inputs={{ task.decorator_parameters }}
    )({{ task.function.function }}),
    {% else %}
    python_callable={{ task.function.function }},
    {% endif -%}
    op_kwargs={{ task.options }}
)
{% endfor %}

{% for rule in task_rules -%}
{{ rule }}
{% endfor %}
