FROM python:3.11-slim

# Evita archivos .pyc y habilita logs en tiempo real
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

WORKDIR /app

# Copiamos las dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos todo el código fuente (para que vea src.common)
COPY src/ ./src/

# Ejecutamos el servicio de cotizaciones
CMD ["python3", "src/rates_service/main.py"]
