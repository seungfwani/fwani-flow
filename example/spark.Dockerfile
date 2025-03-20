FROM ubuntu:jammy

ENV PYSPARK_PYTHON=python3
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64

RUN apt-get update && apt-get upgrade -y \
    && apt-get install -y python3.10 python3-pip openjdk-11-jdk \
    && apt-get clean

RUN pip install --no-cache-dir pyspark

CMD ["pyspark"]
