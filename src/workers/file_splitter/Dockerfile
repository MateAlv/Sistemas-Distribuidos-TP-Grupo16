FROM python:3-alpine

RUN pip3 install pika

WORKDIR /app
COPY common ./common
COPY workers/common ./workers/common
COPY workers/file_splitter ./workers/file_splitter

ENV PYTHONPATH=/app

ENTRYPOINT ["python3", "workers/file_splitter/main.py"]
