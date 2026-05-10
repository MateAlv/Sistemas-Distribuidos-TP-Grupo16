FROM python:3-alpine

COPY common/message_protocol/external ./common/message_protocol/external
COPY client/common ./common
COPY client/main.py .

ENTRYPOINT ["python3", "main.py"]
