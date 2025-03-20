from {{ task.function.name }}.{{ task.function.main_filename }} import {{ task.function.function }} as {{ task.function.name }}_{{ task.function.main_filename }}_{{ task.function.function }}


{{ task.variable_id }} = PythonOperator(
    task_id='{{ task.variable_id }}',
    dag=dag,
    {%- if task.decorator %}
    python_callable={{ task.decorator }}(
        inputs={{ task.decorator_parameters }}
    )({{ task.function.name }}_{{ task.function.main_filename }}_{{ task.function.function }}),
    {% else %}
    python_callable={{ task.function.name }}_{{ task.function.main_filename }}_{{ task.function.function }},
    {% endif -%}
    op_kwargs={
        {% set options = task.options | from_json -%}
        {% for k, v in options.items() -%}
        "{{ k }}": {% if v is string %}"{{ v }}"{% else %}{{ v }}{% endif %},
        {% endfor -%}
        "operator_type": "{{ task.function.operator_type }}",
    },
)
