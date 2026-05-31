FROM python:3.12-slim

# Install a newer Docker CLI (API >= 1.44). Adjust version if needed.
ARG DOCKER_CLI_VERSION=26.1.4
RUN apt-get update && apt-get install -y curl && \
    curl -fsSL https://download.docker.com/linux/static/stable/x86_64/docker-${DOCKER_CLI_VERSION}.tgz -o docker.tgz && \
    tar xzvf docker.tgz --strip 1 -C /usr/local/bin docker/docker && \
    rm docker.tgz && \
    apt-get remove -y curl && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

COPY chaos_monkey /app/chaos_monkey
WORKDIR /app/chaos_monkey

ENTRYPOINT ["python3", "main.py"]
