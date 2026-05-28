FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY workers/q4_source_prefilter .

ENTRYPOINT ["python3", "main.py"]
