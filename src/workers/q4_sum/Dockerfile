FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY workers/q4_edge_store .

ENTRYPOINT ["python3", "main.py"]
