sys.path.append(os.path.join("{{ container_mount_udf_target_path }}", "{{ task.function.name }}"))
{% set module_path = task.function.main_filename | replace('/', '.') %}
from {{ task.function.name }}.{{ module_path }} import {{ task.function.function }} as {{ task.function.name }}_{{ task.function.main_filename }}_{{ task.function.function }}


{{ task.variable_id }} = PythonOperator(
    task_id='{{ task.variable_id }}',
    dag=dag,
    {%- if task.decorator %}
    python_callable={{ task.decorator }}(
        inputs=[
        {% for inp in task.function.inputs -%}
            {"name": "{{ inp.name }}", "type": "{{ inp.type }}"},
        {% endfor -%}
        ]
    )({{ task.function.name }}_{{ module_path }}_{{ task.function.function }}),
    {% else %}
    python_callable={{ task.function.name }}_{{ module_path }}_{{ task.function.function }},
    {% endif -%}
    op_kwargs={
        {% for inp in task.inputs -%}
        "{{ inp.key }}": {% if inp.type == "string" %}"{{ inp.value }}"{% else %}{{ inp.value }}{% endif %},
        {% endfor -%}
        "operator_type": "{{ task.function.operator_type }}",
    },
)
