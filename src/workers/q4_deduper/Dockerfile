FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY workers/q4_account_deduper .

ENTRYPOINT ["python3", "main.py"]
