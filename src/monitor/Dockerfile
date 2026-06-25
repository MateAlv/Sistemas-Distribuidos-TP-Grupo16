FROM docker:26-cli

RUN apk add --no-cache python3

WORKDIR /app

COPY common ./common
COPY monitor ./monitor

ENV PYTHONPATH=/app

ENTRYPOINT ["python3", "-m", "monitor.main"]
