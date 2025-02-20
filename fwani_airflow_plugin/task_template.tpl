"""
template 을 통해 만들어진 Task 입니다.
"""

{{ task_variable_name }} = PythonOperator(
    task_id='{{ current_task_id }}',
    dag=dag,
    python_callable={{ python_callable }},
    {{ options }}
)
