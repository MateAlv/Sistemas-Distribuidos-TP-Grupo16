# Protocolo de comunicacion del cliente

Este README describe el protocolo TCP usado por el cliente para enviar los archivos de entrada al gateway. El cliente no interpreta ni procesa el contenido de los CSV: lee los archivos configurados, los parte en chunks de bytes y los transmite con metadata suficiente para que el resto del pipeline sepa de que archivo viene cada payload y que tipo de archivo es.

## Entidades

- `Client`: orquesta la ejecucion. Lee configuracion desde variables de entorno, abre la conexion, envia los archivos, informa fin de transmision y queda esperando resultados.
- `ClientConfig`: agrupa la configuracion runtime del contenedor: `CLIENT_ID`, `DATA_DIR`, `TRANSACTIONS_FILE`, `ACCOUNTS_FILE`, `SERVER_HOST`, `SERVER_PORT`, `CHUNK_MAX_BYTES`, timeouts y nivel de logging.
- `InputFile`: representa un archivo de entrada configurado. Incluye path absoluto, path relativo y tipo logico (`transactions` o `accounts`).
- `ChunkReader`: abre cada archivo en modo binario y produce `FileChunk` sin cargar el archivo completo en memoria.
- `FileChunk`: representa una porcion de un archivo. Incluye `client_id`, tipo de archivo, path relativo, offset dentro del archivo y payload crudo.
- `FileChunkHeader`: define la metadata fija del chunk antes del payload.
- `Sender`: maneja la conexion TCP persistente, serializa el tipo de mensaje, envia con `sendall` y espera ACK despues de cada mensaje relevante.
- `socket_utils`: contiene primitivas de bajo nivel para asegurar socket abierto, enviar todos los bytes y recibir exactamente N bytes.

## Flujo

1. El cliente abre una conexion TCP persistente contra `SERVER_HOST:SERVER_PORT`.
2. Envia un mensaje de handshake con su `client_id`.
3. Espera un ACK del gateway.
4. Resuelve `TRANSACTIONS_FILE` y `ACCOUNTS_FILE` usando `DATA_DIR` como base si los paths son relativos.
5. Para cada archivo, genera chunks cuyo mensaje completo no supera `CHUNK_MAX_BYTES`.
6. Envia cada chunk y espera un ACK antes de continuar con el siguiente.
7. Cuando no quedan archivos, envia un mensaje de fin.
8. Espera el ACK final.
9. Luego queda leyendo lineas de resultado desde el socket y las loggea hasta que el gateway cierre la conexion.

## Tipos de mensaje

Cada mensaje empieza con un byte de tipo:

| Tipo | Nombre | Direccion | Payload |
| --- | --- | --- | --- |
| `1` | Handshake | Cliente -> Gateway | `client_id` como unsigned int de 4 bytes |
| `2` | File chunk | Cliente -> Gateway | `FileChunkHeader` + payload crudo |
| `3` | Finish | Cliente -> Gateway | Sin payload |
| `4` | ACK | Gateway -> Cliente | Sin payload |

El cliente espera un ACK despues del handshake, despues de cada chunk y despues del finish. Si recibe otro tipo de mensaje en ese punto, considera inválida la comunicación.

## Formato de `FileChunk`

Un mensaje de chunk tiene esta estructura:

```text
1 byte   message_type = 2
4 bytes  client_id
1 byte   file_type
4 bytes  payload_size
4 bytes  path_size
8 bytes  offset
N bytes  rel_path en UTF-8
M bytes  payload crudo
```

Donde:

- `client_id`: identifica al cliente que envio el archivo.
- `file_type`: `1` para transactions, `2` para accounts.
- `payload_size`: cantidad de bytes del payload.
- `path_size`: cantidad de bytes del path relativo codificado en UTF-8.
- `offset`: posicion del payload dentro del archivo original.
- `rel_path`: path relativo al `DATA_DIR`.
- `payload`: bytes originales del archivo.

`CHUNK_MAX_BYTES` limita el tamano total del mensaje TCP enviado por el cliente: incluye el byte de tipo, el header fijo, el path relativo y el payload. Por eso el tamaño útil del payload puede ser menor que `CHUNK_MAX_BYTES`.

## Propiedades

- No hay parseo de CSV en el cliente.
- El tipo logico del archivo viaja en el protocolo; no se infiere en el gateway ni en el file ingestor desde el nombre del archivo.
- No se carga un archivo completo en memoria.
- La transmision usa una unica conexion TCP persistente.
- El orden de envio de archivos es estable: primero transactions, despues accounts.
- El protocolo actual prioriza simplicidad: ACK por mensaje y resultados como lineas de texto al final.
