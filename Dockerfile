FROM apache/airflow:2.10.4-python3.11

USER root
RUN apt-get update && apt-get upgrade -y \
    && apt-get install -y procps \
    && apt-get clean

USER airflow
WORKDIR app

COPY ./fwani_airflow_plugin/requirements.txt /app/requirements.txt

RUN pip install -r /app/requirements.txt
