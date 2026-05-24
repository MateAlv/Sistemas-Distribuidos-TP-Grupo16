FROM python:3-alpine

RUN pip3 install pika

WORKDIR /app

COPY common ./common
COPY workers/common ./workers/common
COPY workers/q2_bank_name_joiner ./workers/q2_bank_name_joiner

ENV PYTHONPATH=/app

ENTRYPOINT ["python3", "workers/q2_bank_name_joiner/main.py"]
