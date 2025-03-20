FROM apache/airflow:2.10.4-python3.11

USER root

# msodbcsql18 설치를 위한 Microsoft EULA 동의 절차
RUN ACCEPT_EULA=Y apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow
WORKDIR app
