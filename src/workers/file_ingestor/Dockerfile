FROM python:3-alpine

RUN pip3 install pika

WORKDIR /app

COPY common ./common
COPY workers/common ./workers/common
COPY workers/file_ingestor ./workers/file_ingestor

ENV PYTHONPATH=/app

ENTRYPOINT ["python3", "workers/file_ingestor/main.py"]
