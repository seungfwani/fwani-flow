FROM apache/airflow:2.10.4-python3.11

USER root
RUN apt-get update && apt-get upgrade -y \
    && apt-get install -y procps \
    && apt-get clean

USER airflow
WORKDIR app
