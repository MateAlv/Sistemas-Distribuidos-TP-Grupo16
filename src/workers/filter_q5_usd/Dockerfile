FROM python:3-alpine

RUN pip3 install pika requests

COPY common ./common
COPY workers/filter_q5_usd .

ENTRYPOINT ["python3", "main.py"]
