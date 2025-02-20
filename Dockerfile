FROM apache/airflow:2.10.4-python3.11

WORKDIR app

COPY ./fwani_airflow_plugin/requirements.txt /app/requirements.txt

RUN pip install -r /app/requirements.txt
