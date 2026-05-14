#!/bin/bash

# Script para levantar RabbitMQ y rates_service en Docker para pruebas

# Detener y limpiar contenedores previos
docker stop rates-test-rabbitmq rates-test-service 2>/dev/null || true
docker rm rates-test-rabbitmq rates-test-service 2>/dev/null || true

# Pull the image
docker pull rabbitmq:3-management

# Levantar RabbitMQ
docker run -d --user 999:999 --name rates-test-rabbitmq -p 5673:5672 -p 15673:15672 rabbitmq:3-management

# Esperar a que RabbitMQ esté listo
echo "Esperando RabbitMQ..."
for i in {1..30}; do
  if docker exec rates-test-rabbitmq rabbitmqctl status >/dev/null 2>&1; then
    echo "RabbitMQ listo."
    break
  fi
  echo "Esperando... ($i/30)"
  sleep 2
done

if [ $i -eq 30 ]; then
  echo "RabbitMQ no se inició correctamente."
  docker logs rates-test-rabbitmq
  exit 1
fi

# Levantar rates_service
docker run -d --name rates-test-service --link rates-test-rabbitmq:rabbitmq \
  -e RABBIT_HOST=rabbitmq \
  -e CACHE_PATH=/tmp/cache.json \
  -e START_DATE=2022-09-01 \
  -e END_DATE=2022-09-30 \
  -v $(pwd)/src:/app/src \
  python:3.11-slim \
  bash -c "cd /app && export PYTHONPATH=/app && pip install pika requests && python src/rates_service/main.py"

echo "Rates service levantado. Esperando 5s para inicialización..."
sleep 5

echo "Servicio listo. Ejecuta el script cliente ahora."