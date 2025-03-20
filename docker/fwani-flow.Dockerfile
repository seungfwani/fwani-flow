FROM python:3.12.9

COPY ./server/requirements.txt /app/
RUN pip install -r /app/requirements.txt

COPY ./server /app
WORKDIR /app
