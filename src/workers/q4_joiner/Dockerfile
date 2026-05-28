FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY workers/q4_block_joiner .

ENTRYPOINT ["python3", "main.py"]
