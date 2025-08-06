FROM python:3.12.9

COPY ./server_new/requirements.txt /app/
RUN pip install -r /app/requirements.txt

COPY ./server_new /app
WORKDIR /app
