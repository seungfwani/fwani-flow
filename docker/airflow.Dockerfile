FROM apache/airflow:2.10.4-python3.11

USER root
#
## msodbcsql18 설치를 위한 Microsoft EULA 동의 절차
#RUN ACCEPT_EULA=Y apt-get update && \
#    apt-get upgrade -y && \
#    apt-get install -y procps && \
#    apt-get clean && \
#    rm -rf /var/lib/apt/lists/*

COPY ./requirements.txt ./requirements.txt
COPY ./requirements-graphio.txt ./requirements-graphio.txt

RUN apt update && apt install -y build-essential \
    git

RUN pip3 install --upgrade pip \
    && pip3 install --no-cache-dir -r ./requirements.txt \
    && pip3 install --no-cache-dir --prefix=/usr/local -r ./requirements-graphio.txt

USER airflow
WORKDIR /app

COPY ./docker/airflow.docker_init.sh /docker-init.sh
COPY ./docker/airflow.scheduler.entrypoint.sh /scheduler-entrypoint.sh
COPY ./docker/airflow.triggerer.entrypoint.sh /triggerer-entrypoint.sh
COPY ./docker/airflow.webserver.entrypoint.sh /webserver-entrypoint.sh
COPY ./docker/airflow.worker.entrypoint.sh /worker-entrypoint.sh
