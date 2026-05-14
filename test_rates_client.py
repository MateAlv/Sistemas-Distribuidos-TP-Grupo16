#!/usr/bin/env python3

import sys
import time
from src.common.middleware.middleware_rabbitmq import MessageMiddlewareRpcClientRabbitMQ

def test_rpc_call():
    print("Iniciando cliente RPC para probar rates_service...")

    with MessageMiddlewareRpcClientRabbitMQ("localhost", "rates_requests", port=5673) as client:
        client.connect()
        print("Cliente conectado. Enviando call(b'') con timeout=30s...")

        start_time = time.time()
        try:
            response = client.call(b"", timeout=30)
            elapsed = time.time() - start_time
            print(f"Respuesta recibida en {elapsed:.2f}s: {len(response)} bytes")
            print("No timeout - servicio respondió correctamente.")
        except Exception as e:
            elapsed = time.time() - start_time
            print(f"Error/Timeout en {elapsed:.2f}s: {e}")
            if "timed out" in str(e).lower():
                print("Timeout confirmado - el servicio no respondió en 30s.")
            else:
                print("Error inesperado.")

if __name__ == "__main__":
    test_rpc_call()