# Protocolo de Mensajería y Serialización

Este documento explica cómo viajan los datos a través de nuestro sistema y las decisiones técnicas detrás de la implementación.

## 1. ¿Por qué Binario (`struct`) y no JSON?

Aunque JSON es más fácil de leer para nosotros, para un sistema distribuido que procesa millones de transacciones es muy ineficiente. 
- **Eficiencia de Ancho de Banda:** Una transacción en JSON puede pesar 200 bytes. Con nuestra serialización binaria, pesa exactamente **63 bytes**.
- **Velocidad de CPU:** La librería `struct` de Python trabaja casi a la velocidad de C. No tiene que "parsear" texto, solo copia bits directamente de la memoria.

## 2. El motor del sistema: `struct`

Usamos la librería `struct` para definir un "mapa" exacto de bytes para cada transacción.
- **Formato:** `!10s Q Q Q Q d 3s 10s`
- **Significado:** El `!` indica orden de red (Big-endian). Las `Q` son enteros grandes de 8 bytes, la `d` es el monto decimal y las `s` son cadenas de texto de longitud fija.

> **Importante:** Al usar longitud fija (ej: 10 bytes para la fecha), evitamos mandar el tamaño de cada campo por separado, ahorrando aún más espacio.

## 3. Estrategia de Batches (Escalabilidad)

Mandar un mensaje por cada transacción a través de RabbitMQ es lento debido al "overhead" de la red. 
- **Solución:** Agrupamos muchas transacciones en un solo bloque de bytes (**Batch**).
- El sistema procesa estos bloques enteros, lo que multiplica el rendimiento (throughput) significativamente.

## 4. Protocolo Externo vs Interno

Existen dos capas independientes:

### Protocolo Externo (Cliente ↔ Gateway)
- Usa **TCP Sockets**.
- Como TCP es un flujo continuo, cada mensaje tiene un prefijo: `[Tipo (1B)] + [Largo del Payload (4B)]`.
- Esto permite que el Gateway sepa exactamente cuántos bytes leer antes de procesar el siguiente lote.

### Protocolo Interno (Gateway ↔ Workers ↔ Sink)
- Usa **RabbitMQ**.
- Cada mensaje incluye un **Header** con el `tipo_de_mensaje` y el `client_id` (16 bytes del UUID).
- Esto permite que los workers identifiquen de qué cliente es la transacción y si es un mensaje de control (como el fin de archivo o EOF).

## 5. Cómo usarlo 

- Para mandar datos: Usar `TransactionSerializer.serialize_batch(lista_de_tx)`.
- Para recibir datos: Usar `TransactionSerializer.deserialize_batch(bytes)`.
- Siempre verificar el `msg_type` antes de procesar el payload.
