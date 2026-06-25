# PROTOCOLO PENSADO

## Definiciones

**message_id:** identificador único de cada mensaje que el worker genera. Se saca del input que lo originó, tipo (worker_id, message_id_de_entrada, client_id, índice). Así, si me caigo y reproceso el mismo input, genero el mismo id de salida que antes, y downstream lo deduplica.

**WAL:** archivo append-only donde el worker anota cada paso importante antes de hacer ACK. Si el worker cae, usa este archivo para reconstruirse.

**LastState:** snapshot completo del estado del worker en un momento dado. Sirve para no leer un WAL infinito.

**inbox:** acá se anota cada mensaje de entrada y en qué estado quedó. tiene dos estados posibles:

- **applied:** "ya cambié mi estado interno por este mensaje, pero todavía no terminé de mandar lo que tenía que mandar". Ej: ya sumé los $50 al estado interno, pero el resultado todavía no salió a downstream.
- **done:** "terminé todo con este mensaje: cambié mi estado, mandé las salidas, y mandé las salidas, y Rabbit me dio el publisher confirm de todas". Ya está ese mensaje y si lo recibo no debo hacer nada.

**outbox:** acá anotamos los mensajes que tenemos que mandar downstream como consecuencia de procesar una entrada, antes de mandarlos de verdad. Cada uno con su message_id. El outbox guarda las salidas que todavía no terminé de mandar. Cuando una entrada queda done, hay qie borrar las salidas que generó. Al recuperarme, republico todo lo que haya quedado sin mandar.

**publisher confirm:** confirmación de RabbitMQ de que un mensaje publicado por el worker quedó aceptado por RabbitMQ.

**queue personal:** cola durable asignada a un worker/shard específico. Si el worker cae, al levantar vuelve a consumir la misma cola. Ya no podemos usar colas compartidas, ahora tienen que ser personales para cada worker.

## Estado Durable Por Worker

Cada worker guarda en disco algo así:

```text
worker_state/
  last_state.current
  last_state.previous
  wal.current
```

Dentro del estado:

```text
clients:
  client_id:
    estado_logica_de_negocio
    inbox_done
    inbox_applied
    outbox
    eof_state
    closed
```

Ej:

```text
clients[123].inbox_done = {
  "filter_usd_0:123:tx:0001",
  "filter_usd_0:123:tx:0002"
}

outbox = {
  "filter_q2_3:client_id1:tx:0001#0": {
    "destination": "sum_q2_1",
    "message_type": "Q2PartialSum",
    "client_id": "123",
    "payload": {
      "account_id": "A",
      "amount": 50
    }
  }
}
```

## Protocolo para levantar Un Worker

1. Arranca el worker. Memoria en cero, no sabe nada todavía.

2. Va a buscar su último snapshot (LastState), que es la foto completa de su estado en algún momento del pasado. Es el punto de partida para no tener que reproducir un WAL infinito.

3. Si encontró last_state.current, lo carga. Esa foto trae todo: el estado de negocio por cliente, qué mensajes ya había procesado, qué tenía pendiente en el outbox, los EOFs que había recibido, los contadores. Todo.

4. Si last_state.current no está, cae a last_state.previous. Esto pasa solo en un caso muy puntual: me caí justo en el medio de hacer un snapshot nuevo, después de mover el current viejo a previous pero antes de dejar el nuevo en su lugar. Es la peor ventana posible para morir, pero bueno, la vida. El previous sigue siendo una foto válida, y el WAL todavía no lo rotamos, así que tiene todo lo que pasó desde esa foto. Reconstruyo igual, no perdió nada.

5. Si no hay ningún snapshot, arranco con el estado vacío. Es el caso "recién prendí el sistema de cero, nunca me caí, no tengo snapshot".

6. Abro el wal.current. Acá está todo lo que hice después de la última foto y que todavía no quedó guardado en un snapshot.

7. Lo reproduzco desde el wal_checkpoint_record del snapshot, mensaje por mensaje, como si estuviera consumiendo de Rabbit. PERO SIN MANDAR MENSAJES, porque en esta etapa solo necesito recalcular el estado. Ignoro todos los records anteriores al wal_checkpoint_record, ya están incorporados en el snapshot.

8. Cuando llego al final, miro el último record. Si quedó cortado a la mitad, lo ignoro y listo. Me había caído ahí, pero nunca ackee a rabbit

9. Con la foto cargada + el WAL reproducido, ya tengo de nuevo TODO en memoria:

   - el estado de negocio de cada cliente,
   - qué mensajes ya procesé (para no procesarlos de nuevo si me llegan repetidos),
   - qué mensajes apliqué pero todavía no cerré,
   - qué tengo pendiente en el outbox,
   - los EOFs que recibí,
   - los contadores.

10. Antes de tocar un mensaje nuevo, reviso el outbox. Acá está la clave de no perder salidas: puede haber mensajes que ya calculé y guardé, pero que nunca llegué a mandar downstream porque me morí en el medio.

11. Todo lo que esté en el outbox sin confirmar lo vuelvo a mandar. Si el siguiente worker ya lo había recibido antes de mi caída, le va a llegar duplicado, Por eso tenemos deduplicación.

12. Recién con todo regal y empiezo a consumir mensajes nuevos. Ahora sí estoy igual que antes de caerme.

## Protocolo Para Procesar Un Mensaje

llega un mensaje M.

1. Rabbit me entrega M. No ackeo nada todavía.

2. Leo del mensaje: message_id, client_id, tipo y payload.

3. message_id está en inbox_done? Entonces es un duplicado ya cerrado. ACK Rabbit y listo, ya se procesó.

4. message_id está en inbox_applied? Entonces ya apliqué el estado pero me caí antes de cerrar. NO vuelvo a aplicar la lógica (si no, cuento doble). Salto directo a PUBLICAR (paso 8). Esto es clave porque re-aplicar acá te cambia los resultados.

5. No está en ninguno, entonces es un mensaje nuevo

### PROCESO (solo mensajes nuevos)

6. Proceso M en memoria: aplico la lógica, calculo todo, y genero los mensajes de salida. A cada salida le asigno su message_id deterministico.

7. Escribo en el WAL: INPUT_APPLIED(message_id, client_id, cambios_de_estado, salidas_generadas) y mando el WAL a disco.

8. Aplico en memoria: marco message_id como applied y agrego las salidas al outbox.

### PUBLICAR (acá llegan tanto los nuevos como los redelivered en applied)

9. Publico a Rabbit todas las salidas de este input que estén en el outbox.

10. Espero el publisher confirm de TODAS (esto es en memoria, no va al WAL).

11. Cuando están todas confirmadas, escribo en el WAL: INPUT_DONE(message_id, client_id) y mando el WAL a disco.

12. Aplico en memoria: muevo message_id de inbox_applied a inbox_done, y borro del outbox las salidas de este input.

13. ACK Rabbit. Termina el procesamiento de M.

## Protocolo Para Hacer Snapshot

Esto es para evitar que el WAL crezca para siempre, pero a la vez no hacer snapshot todo el tiempo que es super caro.

1. Cada cierta cantidad de mensajes, el worker decide hacer snapshot.

2. Toma una copia consistente del estado bajo lock (pausando brevemente la escritura de nuevos cambios). Bajo ese mismo lock anota el wal_checkpoint_record = número del último registro del WAL ya aplicado.

3. Escribe todo el estado actual en: last_state.tmp

4. El contenido incluye:

   - wal_checkpoint_record
   - estado de negocio por cliente
   - inbox_done por cliente
   - inbox_applied si hubiera algo pendiente
   - outbox pendiente
   - EOFs recibidos
   - contadores
   - clientes cerrados todavía no limpiados

5. Commitea last_state.tmp a disco.

6. Renombra: last_state.current -> last_state.previous. Entiendo que esto es atómico

7. Renombra atómicamente: last_state.tmp -> last_state.current

8. Commit el directorio a disco.

9. Recién ahora puede rotar/borrar WAL viejo.

10. Crea un wal.current nuevo.

11. Sigue procesando y mandando al wal.current nuevo.

## Qué Pasa Si El Worker Se Cae En Cada Etapa

- **Cae antes de recibir el mensaje:** No ocurre nada. RabbitMQ mantiene el mensaje en la cola y será consumido al reiniciar.
- **Cae antes de escribir INPUT_APPLIED:** Los cambios en memoria se pierden al no haberse persistido en el WAL. RabbitMQ reentrega el mensaje.
- **Cae después de escribir INPUT_APPLIED, pero antes de terminar de publicar el outbox:** El estado y el outbox son durables. Al recuperar, el worker no reaplica la lógica, reconstruye el estado del WAL, publica los mensajes pendientes en el outbox y luego confirma.
- **Cae mientras publica una salida del outbox:** Si RabbitMQ no la recibió, se republica. Si ya la recibió pero no se registró, se republica (el receptor deduplica mediante el message_id).
- **Cae después de publicar/confirmar, pero antes de escribir INPUT_DONE:** El worker republica el outbox completo. Downstream deduplica los duplicados. Al confirmar todo, el worker escribe INPUT_DONE y hace ACK.
- **Cae después de INPUT_DONE, pero antes de ACK a RabbitMQ:** El input ya está marcado como terminado en disco. Al recuperar, el worker ve inbox_done y hace ACK directo sin reprocesar.
- **Cae después de ACK a RabbitMQ:** Todo el proceso se completó. Nada que recuperar.
- **Cae durante un snapshot:**
  - **Escritura en .tmp:** Se ignora el archivo temporal incompleto. Se utiliza last_state.current y el WAL filtrado desde su wal_checkpoint_record.
  - **Movimiento de current a previous:** Se utiliza last_state.previous y el WAL para reconstruir filtrado desde su wal_checkpoint_record para reconstruir.
  - **Finalización del snapshot:** Se utiliza el nuevo last_state.current y el WAL filtrado por LSN.
- **Cae durante la limpieza de un cliente:** El worker detecta el estado (CLIENT_CLEANUP_STARTED) al recuperar y reintenta la operación o realiza el ACK si ya fue completada.

| Momento de la caída | RabbitMQ reentrega | Estado en disco | Acción al recuperar |
|---|---|---|---|
| Antes de recibir M | Sí | Sin cambio | Procesar como nuevo |
| Después de recibir, antes de INPUT_APPLIED | Sí | Sin cambio | Procesar como nuevo |
| Después de procesar en memoria, antes de INPUT_APPLIED | Sí | Sin cambio | Procesar como nuevo |
| Después de INPUT_APPLIED, antes de publicar outbox | Sí | Estado + outbox durables | Publicar outbox, luego INPUT_DONE |
| Durante publicación de salidas | Sí | Estado + outbox durables | Republicar salidas, downstream deduplica |
| Después de publicar, antes de INPUT_DONE | Sí | Estado durable, confirms perdidos | Republicar outbox completo, downstream deduplica, INPUT_DONE |
| Después de INPUT_DONE, antes de ACK Rabbit | Sí | Todo durable | Ver message_id en inbox_done, ACK inmediato |
| Después de ACK Rabbit | No | Todo durable | Nada que hacer |
| Durante snapshot (escribiendo .tmp) | — | last_state.current válido | Ignorar .tmp, usar current + WAL desde su wal_checkpoint_record |
| Durante snapshot (current movido, nuevo no escrito) | — | last_state.previous válido | Usar previous + WAL desde su wal_checkpoint_record |
| Durante snapshot (nuevo current escrito, WAL no rotado) | — | last_state.current nuevo válido | Usar current + WAL desde su wal_checkpoint_record |
| Durante rotación del WAL (wal.current borrado, nuevo no creado) | — | last_state.current válido | Tratar WAL como vacío, arrancar solo con el snapshot |

## Garantía Final

RabbitMQ puede duplicarse downstream.  
Un worker puede republicar.  
Un mensaje puede llegar duplicado.

Pero:

**ningún mensaje se aplica dos veces,**  
porque cada worker deduplica con inbox durable.

Y:

ningún mensaje confirmado se pierde,  
porque antes de hacer ACK del input  
**el estado y el outbox quedaron en disco**

## Protocolo de deduplicacion

**El problema:**

Cuando un worker se recupera de una caída, puede republicar mensajes que ya mandó (eso está cubierto por el outbox). El worker downstream los va a recibir dos veces. Necesitamos que los descarte sin reprocesarlos.

El approach naive sería guardar en inbox_done todos los message_id que ya procesé. Funciona, pero el set crece indefinidamente.

**La solución:**

En vez de guardar todos los IDs vistos, cada worker receptor mantiene por cada (client_id, sender_id):

- **biggest:** el mayor message_id recibido hasta ahora
- **pending:** los IDs menores que biggest que todavía no llegaron (huecos)

La regla para detectar duplicados:

```text
si message_id <= biggest Y message_id no está en pending → duplicado, descartar
```

Ejemplo:

```text
Llegan 1, 2, 5:
  biggest = 5
  pending = {3, 4}
Vuelve a llegar 2:
  2 <= 5 y 2 no está en pending → duplicado ✓
Llega 3:
  3 <= 5 y 3 está en pending → nuevo, sacar de pending ✓
  pending = {4}
```

Cuando el cliente se cierra, se borra el tracker de ese cliente. Por eso el pending nunca crece indefinidamente.

### El contador del emisor

El receptor necesita que el emisor nunca reutilice un message_id con otro contenido. Si el emisor reinicia con contador = 0, el receptor tiene biggest = 50 y descarta todos los mensajes nuevos como duplicados.

Por eso el emisor persiste su contador a disco, igual que el receptor persiste su tracker. El emisor guarda dos cosas:

- **committed_seq:** último seq_num cuyo input fue ACKed con éxito
- **pending_seq:** seq_num que estaba usando cuando se cayó (si había algo en curso)

Al reiniciar:

- **Si pending_seq > committed_seq:** había trabajo en curso. RabbitMQ va a reentregarme ese mensaje. Lo proceso con el mismo pending_seq de antes.
- **Si son iguales:** no había nada en curso. El próximo mensaje usa committed_seq + 1.

### Cuándo falla

Un solo caso: el emisor reinicia sin haber persistido su contador. Resetea a 0 y el receptor descarta mensajes nuevos como si fueran duplicados.

El caso de N réplicas compartiendo una cola no aplica si cada worker tiene su propia cola personal (como establece este protocolo), porque la cola misma identifica al emisor y los message_id de distintos emisores no se mezclan.

### Dónde vive en el estado del worker

El tracker se incorpora al LastState y al WAL como cualquier otro estado:

```text
clients[C].dedup:
  sender_id_0:
    biggest: 42
    pending: {38, 41}
  sender_id_1:
    biggest: 17
    pending: {}
```

Se persiste antes de hacer ACK a RabbitMQ, igual que el resto del estado. Si el worker cae después de persistir el tracker pero antes del ACK, RabbitMQ reentrega el mensaje y el tracker lo detecta como duplicado.

## Brainstorming tolerancia a fallos

1. Nodo A le pasa mensajes a Nodo B.
2. A le manda el mensaje y espera ACK.
3. B hace flush a disco. Pone el mensaje al final de su archivo Write Ahead Log. Al terminar, directamente ACK a RabbitMQ.
4. Rabbit recibe ACK y sabe que puede borrar su mensaje.
5. ?Archivo WAL crece infinitamente? (idea: hacer archivo por cliente. Este archivo se borra al eliminar el estado del cliente). Entonces cada t tiempo, el nodo backupea todo su estado y lo guarda en un archivo LastState. Inmediatamente borra el archivo WAL, ya que su estado entero ya está en LastState.
6. El nodo B eventualmente puede caerse y para levantarse tiene que leer el LastState y después procesar los mensajes posteriores a guardar su estado, en el archivo WAL.

Message id por cada mensaje y dedup en cada worker. Hashing consistente para garantizar que siempre le lleguen los mismos mensajes al mismo worker.

Nodos “pasamano” no necesitan bajar a disco. Sí la cant de mensajes que procesó por cliente, para tener trazabilidad en el EOF.

id según (worker id y contador interno).

cambiar todo a específicas a cada worker.

Hacer protocolo línea por línea y ver que pasa si se cae en cada una

### Bajar a disco no es atómico

Trabajar escalonado, siempre tener un fallback, checkpoint anterior. De tal forma de poder siempre reconstruir.

Escribir en un archivo temporal en el mismo dir del archivo original y forzarlo a escribir en disco, luego sincronizar con el archivo original (reemplazar el original con el temporal?) , para hacer operación atómica

### Monitores de Levantamiento de Contenedores

Los monitores se encargan de detectar cuando un contenedor se cayó mediante el envío de heartbeats por TCP (monitor envía un mensaje de estado, el contenedor contesta). Estos monitores se pueden caer, por lo que hay N (3) monitores al mismo tiempo. Solo un monitor líder envía y recibe heartbeats.

Cuando el monitor líder se cae por el chaos monkey se realiza una elección de líder. Los otros monitores se dan cuenta ya que las instancias de monitor se van controlando entre sí mediante heartbeats también. El “primer” worker que se da cuenta hace una comunicación de bully para elección con el resto de monitores. El nuevo Monitor líder se encarga

Un worker se levanta y larga un thread con HeartbeatSender. Este manda primero un mensaje “che existo, teneme en cuenta” (por TCP o UDP?  y el Monitor lo debe anotar. Luego el HeartbeatSender debe mandar cada t tiempo un heartbeat unprompted y el monitor debe registrar cada uno. Si el Monitor deja de recibir heartbeats de un worker después de timeout 5t, entonces declara el monitor como “muerto” y lo levanta con comando docker start Docker in Docker, arrancando ciclo de vuelta.

Los nodos tienen un thread más para recibir las updates de quien es el lider, en caso se caiga y cambie el leader.

punto bonus, integrar el heartbeat al worker loop.
