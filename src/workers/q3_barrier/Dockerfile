FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY workers/q3_barrier .

ENTRYPOINT ["python3", "main.py"]
