# Enunciado

Contenido original del enunciado del trabajo práctico.

# Sistemas Distribuidos TP Grupo 16

Trabajo Práctico "Money Laundering Analysis" de la materia Sistemas Distribuidos I, FIUBA.

## Datos de la materia

- Materia: Sistemas Distribuidos I
- Código: 75.74
- Trabajo práctico: Money Laundering Analysis
- Instancia: TP Diseño
- Curso: Sistemas Distribuidos (75.74)

## Consigna

El blanqueo de capital o lavado de activos consiste en cambiar bienes o dinero obtenidos mediante actos ilícitos por dinero legítimo. Dentro de la red digital de pagos existen una serie de estrategias comunes para ocultar, disimular u ofuscar los circuitos de transferencia. Entre ellas se encuentran [1]:

- Fan-out: Transferencias pequeñas hacia múltiples cuentas desde una cuenta principal.
- Fan-in: Transferencias pequeñas desde múltiples cuentas hacia una cuenta principal.
- Scatter-Gather: Fan-out desde una cuenta seguido de Fan-in hacia otra.
- Bipartito: Múltiples transferencias, en donde origen y destino pertenecen a conjuntos distintos. El agregado de transacciones forma un grafo bipartito.
- Stack: Transferir pequeñas cantidades a lo largo del tiempo hacia otra cuenta.

## Requerimientos Funcionales

Se solicita un sistema distribuido que analice el extracto de transacciones realizadas entre cuentas bancarias en busca de anomalías.

Se debe obtener:

1. Cuenta de origen, cuenta de destino y monto para transacciones USD menores a 50.
2. Nombre de banco, cuenta de origen y monto de la max. transacción USD de cada banco.
3. Cuenta de origen y monto de transacciones USD en el período [2022-09-06, 2022-09-15] con monto menor a 1 centésimo del promedio encontrado para el mismo formato de pago en el período [2022-09-01, 2022-09-05].
4. Cuentas que cumplan con el patrón scatter-gather con una sola cuenta de separación, para cuentas que hayan realizado y cuya cuenta de origen haya realizado transferencias USD hacia entre 5 cuentas distintas dentro del período [2022-09-01, 2022-09-05].
5. Cantidad de transacciones del período [2022-09-01, 2022-09-05] con formato de pago "Wire" o "ACH" cuyo monto convertido a USD sea menor a 1.

## Requerimientos No Funcionales

- El sistema debe estar optimizado para entornos multicomputadoras.
- Se debe soportar el incremento de los elementos de cómputo para escalar los volúmenes de información a procesar.
- Se requiere del desarrollo de un Middleware para abstraer la comunicación basada en grupos.
- Se debe soportar una única ejecución del procesamiento y proveer graceful quit frente a señales SIGTERM.

## Datasets, notebook patrón y librerías

- Para construir una simulación realista, se trabajará sobre el siguiente dataset: <https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml/data>.
- La conversión a USD requiere un valor de cotización diaria Además de un csv que conviertan desde las distintas divisas hacia una de referencia. (<https://api.frankfurter.app>)
- Se usarán los valores del siguiente notebook como resultados patrón:
  - <https://www.kaggle.com/code/pablodroca/money-laundering-analysis>

## Normas de Trabajo

Se espera del alumno:

- Empleo del tiempo de consultas en clase para resolver dudas y clarificar el negocio del sistema a construir previo a su diseño.
- Exposición y verificación en clase de la arquitectura propuesta antes de iniciar su implementación.
- Empleo del grupo de correos para realizar consultas que no pudieran ser resueltas en clase.
- Consideración de prácticas distribuidas según lo estudiado en clase para elaborar una arquitectura flexible, escalable y robusta.
- Aprobación del cuerpo docente para el uso de cualquier librería.
- Demo del sistema en funcionamiento previamente ensayada.

## Condiciones de Aprobación

### General

Criterios aplicables tanto a trabajos prácticos individuales, como grupales.

Los criterios de desaprobación pueden ser aplicados retroactivamente.

Los criterios de observación o quita de puntos dependen del contexto, gravedad, cantidad de ocurrencias, entre otros factores.

Los criterios referidos a código no rigen para el Hito 1 del trabajo grupal o la presentación del paper.

#### Implica desaprobación

- No entregar.
- No asistir a una instancia de defensa o exposición.
- No emplear alguno de los lenguajes canónicos para la materia: Golang y Python.
- No cumplir con los puntos solicitados en el enunciado.
- Copiar código de terceros o generarlo mediante herramientas como LLMs.

#### Implica observación o quita de puntos

- No cumplir con las buenas prácticas de codificación.
- No modularizar y/o presentar gran cantidad de código repetido, ej. no encapsular la lógica común en clases, módulos o paquetes common o utils.
- Hardcodear valores, en lugar de utilizar archivos de config o al menos de constantes.
- Abusar de funciones largas. Depende del lenguaje de programación, ej. más de 100 líneas es un “code smell” en Python.
- No incluir comentarios de ayuda en código de alta complejidad.
- Emplear más de un coding standard.
- No implementar “graceful shutdowns” en los procesos, salvo en el trabajo individual de Middleware.
- No redactar un README mínimo que indique cómo levantar y correr el sistema.
- No proveer todos los archivos para poder ejecutar la entrega.
- Omitir la protección de secciones críticas mediante mecanismos de sincronización (ej mutex, locks).
- Omitir el chequeo de cant. de bytes leídos/escritos en FDs (sockets, archivos).
- Omitir el cierre de FDs (sockets, archivos).

### TP Grupal

Los miembros del grupo responden de forma solidaria respecto a la calidad del trabajo entregado. Se espera que cualquier discrepancia o falencia en la organización del trabajo se resuelva internamente y se acuda a la cátedra solo en casos críticos y con un márgen de tiempo pertinente hasta la entrega del siguiente hito.

La desaprobación de un hito, cuando se ha realizado una entrega y ésta cumple con una porción sustancial del enunciado, no lleva a recursar la materia, pero si conlleva la responsabilidad de corregir los errores para el siguiente hito.

### TP-Escalabilidad: Hito 2

#### Implica desaprobación

- No desplegar el sistema con múltiples containers usando docker-compose.
- No utilizar un framework MOM (Rabbit, Kafka, ZMQ) para comunicar mensajes entre containers. El uso de un framework distinto a RabbitMQ debe ser pactado con el corrector asignado.
- No utilizar un middleware para ocultar la complejidad de comunicación y grupos de procesos, quedando expuesta la lógica de conectividad en el código del negocio.
- No sincronizar correctamente la recepción de mensajes en los controllers que tienen dependencias, ej: joiners, groupers, sorters, etc.
- No implementar un sistema en donde al menos el 50% de los tipos de controllers pueda escalar.
- No realizar streaming de los registros del cliente hacia la fuente del sistema distribuido o no realizar el streaming de los registros del sumidero del sistema hacia el cliente.
- Utilizar sleeps para sincronizar o spin locks para poolear.
- No automatizar la comparación de los archivos de salida con los de una ejecución serial para validar el sistema.

#### Implica observación o quita de puntos

- Propagar innecesariamente columnas en etapas donde ya no son necesarias.
- No haber realizado un diseño organizado que permita una lógica de negocio simple (orientada a procesar registros de string, tuplas o DTOs) y desacoplada de la comunicación.
- Presentar código repetido en donde claramente se podría haber reutilizado.
- Abusar de la affinity con ciertos procesos para implementar la funcionalidad.

### TP-Tolerancia: Hito 3

#### Implica desaprobación

- Entregar un sistema que no sea tolerante a fallos que puedan ocurrir por la caída de procesos.
- No incluir al menos el escenario “Chaos Monkey” para poner a prueba la tolerancia a fallos del sistema.
- No automatizar los escenarios de prueba o al menos el escenario “Chaos Monkey”.
- Utilizar bibliotecas externas para el consenso, la tolerancia a fallos o el protocolo de recuperación.
- Utilizar docker-in-docker para recabar información y tomar decisiones. Solo se admite su uso para reiniciar un contenedor o re-instanciar una imágen.
- No implementar un mecanismo para eliminar archivos y liberar recursos para consultas resueltas o fallidas.

#### Implica observación o quita de puntos

- No garantizar la entrega “exactly once” de mensajes al cliente.
- Abusar de la affinity con ciertos procesos para implementar el control del sistema sin mitigar su impacto en el procesamiento de datos.
