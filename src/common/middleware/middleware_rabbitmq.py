import os
import logging
import pika
import uuid
import time
import random
import threading
from common.logging_utils import get_flow_logger

for _pika_logger in ("pika", "pika.adapters", "pika.connection", "pika.channel"):
    logging.getLogger(_pika_logger).setLevel(logging.WARNING)

from .middleware import (
    MessageMiddlewareQueue, 
    MessageMiddlewareExchange, 
    MessageMiddlewareCloseError, 
    MessageMiddlewareMessageError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareRpcClient,
    MessageMiddlewareRpcServer,
)

_DURABLE = os.environ.get("RABBITMQ_DURABLE", "false").lower() == "true"
_DELIVERY_MODE = 2 if _DURABLE else 1
_PUBLISHER_CONFIRMS = os.environ.get("RABBITMQ_PUBLISHER_CONFIRMS", "false").lower() == "true"
_MANDATORY_PUBLISH = _PUBLISHER_CONFIRMS
_PREFETCH_COUNT = int(os.environ.get("PREFETCH_COUNT", "1"))
_CONNECT_RETRIES = int(os.environ.get("RABBITMQ_CONNECT_RETRIES", "10"))
_CONNECT_RETRY_DELAY = float(os.environ.get("RABBITMQ_CONNECT_RETRY_DELAY", "3.0"))

_CONNECTION_ERRORS = (
    pika.exceptions.AMQPConnectionError,
    pika.exceptions.AMQPChannelError,
    pika.exceptions.StreamLostError
)

class _RabbitMQBase:
    def __init__(self, host):
        self._connection = None
        self._channel = None
        self._user_callback = None
        self._queue_name = None
        self._flow_logger = get_flow_logger()
        last_exc = None
        for attempt in range(_CONNECT_RETRIES):
            try:
                self._connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=host,
                        heartbeat=0,
                        blocked_connection_timeout=300,
                    )
                )
                self._channel = self._connection.channel()
                if _PUBLISHER_CONFIRMS:
                    self._channel.confirm_delivery()
                return
            except Exception as e:
                last_exc = e
                cap = _CONNECT_RETRY_DELAY * 10
                delay = random.uniform(0, min(cap, _CONNECT_RETRY_DELAY * (2 ** attempt)))
                logging.warning(
                    "rabbitmq_connect_retry | attempt=%s/%s | retry_in=%.1fs | error=%s",
                    attempt + 1, _CONNECT_RETRIES, delay, e,
                )
                time.sleep(delay)
        raise MessageMiddlewareDisconnectedError(last_exc)

    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def _on_messaging_callback_adapter(self, ch, method, properties, body):
        self._record_flow("consume", body)
        ack = lambda: ch.is_open and ch.basic_ack(delivery_tag=method.delivery_tag)
        def nack(requeue=False):
            return ch.is_open and ch.basic_nack(
                delivery_tag=method.delivery_tag, requeue=requeue
            )
        self._user_callback(body, ack, nack)

    def _record_flow(self, direction, message, routing_key=None):
        endpoint_type = "exchange" if getattr(self, "_exchange_name", None) else "queue"
        endpoint = (
            getattr(self, "_exchange_name", None)
            or getattr(self, "_queue_name", None)
            or getattr(self, "_request_queue", None)
            or "unknown"
        )
        try:
            self._flow_logger.observe(
                direction=direction,
                endpoint_type=endpoint_type,
                endpoint=endpoint,
                routing_key=routing_key,
                message=message,
            )
        except Exception as e:
            logging.debug("middleware_flow_log_error | endpoint=%s | error=%s", endpoint, e)

    def start_consuming(self, on_message_callback):
        try:
            self._user_callback = on_message_callback
            self._channel.basic_qos(prefetch_count=_PREFETCH_COUNT)
            self._channel.basic_consume(
                queue=self._queue_name, 
                on_message_callback=self._on_messaging_callback_adapter)
            self._channel.start_consuming()
        except _CONNECTION_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(e)
        except Exception as e:
            raise MessageMiddlewareMessageError(e)

    def stop_consuming(self):
        try:
            if not self._channel or not self._channel.is_open:
                return
            self._channel.stop_consuming()
        except _CONNECTION_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(e)
        except Exception:
            return

    def request_stop_consuming(self):
        try:
            if not self._connection or not self._connection.is_open:
                return
            self._connection.add_callback_threadsafe(self.stop_consuming)
        except Exception:
            return

    def close(self):
        errors = []
        for resource in (self._channel, self._connection):
            try:
                if resource and resource.is_open:
                    resource.close()
            except Exception as e:
                errors.append(e)

        if errors:
            raise MessageMiddlewareCloseError(errors[0])



class LazyQueue:
    """Wraps MessageMiddlewareQueueRabbitMQ with deferred connection.

    The AMQP connection is not established until the first send() call.
    Safe to close() even if never connected.
    """

    def __init__(self, host: str, queue_name: str):
        self._host = host
        self._queue_name = queue_name
        self._queue: "MessageMiddlewareQueueRabbitMQ | None" = None

    def send(self, message, routing_key=None):
        if self._queue is None:
            self._queue = MessageMiddlewareQueueRabbitMQ(self._host, self._queue_name)
        self._queue.send(message, routing_key)

    def send_to_shard(self, message, shard):
        if int(shard) != 0:
            raise MessageMiddlewareMessageError(
                f"queue publisher only supports shard 0, got {shard}"
            )
        self.send(message)

    def close(self):
        if self._queue is not None:
            self._queue.close()
            self._queue = None


class MessageMiddlewareQueueRabbitMQ(_RabbitMQBase, MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        super().__init__(host)
        self._queue_name = queue_name
        self._channel.queue_declare(queue=queue_name, durable=_DURABLE)

    def send(self, message, routing_key=None):
        if routing_key is not None:
            raise MessageMiddlewareMessageError(
                "queue publishers do not accept routing_key"
            )
        try:
            self._channel.basic_publish(
                exchange="",
                routing_key=self._queue_name,
                body=message,
                properties=pika.BasicProperties(delivery_mode=_DELIVERY_MODE),
                mandatory=_MANDATORY_PUBLISH,
            )
            self._record_flow("publish", message, self._queue_name)
        except pika.exceptions.NackError as e:
            raise MessageMiddlewareMessageError(f"broker nacked the publish: {e}")
        except pika.exceptions.UnroutableError as e:
            raise MessageMiddlewareMessageError(f"message unroutable: {e}")
        except _CONNECTION_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(e)
        except MessageMiddlewareMessageError:
            raise
        except Exception as e:
            raise MessageMiddlewareMessageError(e)

    def send_to_shard(self, message, shard):
        if int(shard) != 0:
            raise MessageMiddlewareMessageError(
                f"queue publisher only supports shard 0, got {shard}"
            )
        self.send(message)



        
class MessageMiddlewareExchangeRabbitMQ(_RabbitMQBase, MessageMiddlewareExchange):

    def __init__(
        self,
        host,
        exchange_name,
        routing_keys,
        exchange_type="direct",
        queue_name=None,
        exclusive=True,
    ):
        super().__init__(host)
        self._exchange_name = exchange_name
        self._exchange_type = exchange_type
        self._queue_name = queue_name
        self._exclusive = exclusive
        self._channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=exchange_type,
            durable=_DURABLE,
        )
        self._routing_keys = routing_keys

    
    def send(self, message, routing_key=None):
        try:
            if self._exchange_type == 'fanout':
                if routing_key is not None:
                    raise MessageMiddlewareMessageError(
                        "fanout exchange publishers do not accept routing_key"
                    )
                self._channel.basic_publish(
                    exchange=self._exchange_name,
                    routing_key='',
                    body=message,
                    properties=pika.BasicProperties(delivery_mode=_DELIVERY_MODE),
                    mandatory=_MANDATORY_PUBLISH,
                )
                self._record_flow("publish", message, "")
            else:
                routing_keys = [routing_key] if routing_key is not None else self._routing_keys
                if not routing_keys:
                    raise MessageMiddlewareMessageError("No routing keys provided")

                for key in routing_keys:
                    self._channel.basic_publish(
                        exchange=self._exchange_name,
                        routing_key=key,
                        body=message,
                        properties=pika.BasicProperties(delivery_mode=_DELIVERY_MODE),
                        mandatory=_MANDATORY_PUBLISH,
                    )
                    self._record_flow("publish", message, key)
        except pika.exceptions.NackError as e:
            raise MessageMiddlewareMessageError(f"broker nacked the publish: {e}")
        except pika.exceptions.UnroutableError as e:
            raise MessageMiddlewareMessageError(f"message unroutable: {e}")
        except _CONNECTION_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(e)
        except MessageMiddlewareMessageError:
            raise
        except Exception as e:
            raise MessageMiddlewareMessageError(e)

    def send_to_shard(self, message, shard):
        if int(shard) != 0:
            raise MessageMiddlewareMessageError(
                f"fixed-routing exchange publisher only supports shard 0, got {shard}"
            )
        self.send(message)


    def start_consuming(self, on_message_callback):
        try:
            self._init_queue()
            super().start_consuming(on_message_callback)
        except _CONNECTION_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(e)
        except (MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError):
            raise
        except Exception as e:
            raise MessageMiddlewareMessageError(e)

    def _init_queue(self):
        if self._queue_name:
            self._channel.queue_declare(
                queue=self._queue_name,
                durable=_DURABLE and not self._exclusive,
                exclusive=self._exclusive,
            )
        else:
            queue_result = self._channel.queue_declare(queue='', exclusive=True)
            self._queue_name = queue_result.method.queue

        if self._exchange_type == 'fanout':
            self._channel.queue_bind(
                queue=self._queue_name,
                exchange=self._exchange_name,
                routing_key=''
            )
        else:
            if not self._routing_keys:
                raise MessageMiddlewareMessageError("Routing keys required for non-fanout exchange")
            for key in self._routing_keys:
                self._channel.queue_bind(
                    queue=self._queue_name,
                    exchange=self._exchange_name,
                    routing_key=key
                )


def ensure_exchange_queue_bindings(
    host: str,
    exchange_name: str,
    bindings: dict[str, str],
    exchange_type: str = "direct",
) -> None:
    try:
        with _RabbitMQBase(host) as rabbit:
            rabbit._channel.exchange_declare(
                exchange=exchange_name,
                exchange_type=exchange_type,
                durable=_DURABLE,
            )
            for queue_name, routing_key in bindings.items():
                rabbit._channel.queue_declare(queue=queue_name, durable=_DURABLE)
                rabbit._channel.queue_bind(
                    queue=queue_name,
                    exchange=exchange_name,
                    routing_key=routing_key,
                )
    except _CONNECTION_ERRORS as e:
        raise MessageMiddlewareDisconnectedError(e)
    except Exception as e:
        raise MessageMiddlewareMessageError(e)


class MessageMiddlewareRpcClientRabbitMQ(_RabbitMQBase, MessageMiddlewareRpcClient):
    def __init__(self, host, request_queue_name):
        self._request_queue = request_queue_name
        super().__init__(host)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def connect(self):
        self._channel.queue_declare(queue=self._request_queue, durable=_DURABLE)

    def call(self, message, timeout=30, cancel_event=None):
        correlation_id = str(uuid.uuid4())
        reply_queue = self._channel.queue_declare(queue='', exclusive=True).method.queue

        response = None

        def on_response(ch, method, props, body):
            nonlocal response
            if props.correlation_id == correlation_id:
                response = body
                self._record_flow("rpc", body, reply_queue)
                ch.basic_ack(delivery_tag=method.delivery_tag)

        self._channel.basic_consume(queue=reply_queue, on_message_callback=on_response, auto_ack=False)

        self._channel.basic_publish(
            exchange="",
            routing_key=self._request_queue,
            body=message,
            properties=pika.BasicProperties(
                reply_to=reply_queue,
                correlation_id=correlation_id,
                delivery_mode=_DELIVERY_MODE,
            )
        )
        self._record_flow("publish", message, self._request_queue)

        deadline = time.monotonic() + timeout
        while response is None:
            if cancel_event is not None and cancel_event.is_set():
                raise MessageMiddlewareMessageError("RPC call cancelled")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise MessageMiddlewareMessageError("RPC call timed out")
            self._connection.process_data_events(time_limit=min(1, remaining))

        return response


class MessageMiddlewareRpcServerRabbitMQ(_RabbitMQBase, MessageMiddlewareRpcServer):
    def __init__(self, host, request_queue_name):
        self._request_queue = request_queue_name
        super().__init__(host)
        self._consuming = False
        self._stop_requested = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def connect(self):
        self._channel.queue_declare(queue=self._request_queue, durable=_DURABLE)

    def start(self, on_request_callback):
        def _on_request(ch, method, properties, body):
            def reply(response_message):
                ch.basic_publish(
                    exchange="",
                    routing_key=properties.reply_to,
                    body=response_message,
                    properties=pika.BasicProperties(correlation_id=properties.correlation_id)
                )
                self._record_flow("publish", response_message, properties.reply_to)
            self._record_flow("consume", body, self._request_queue)
            on_request_callback(body, reply)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        
        try:
            self._channel.basic_qos(prefetch_count=_PREFETCH_COUNT)
            self._channel.basic_consume(
                queue=self._request_queue,
                on_message_callback=_on_request,
            )
            self._consuming = True
            if self._stop_requested:
                self.stop_consuming()
                return
            self._channel.start_consuming()
        except _CONNECTION_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(e)
        except Exception as e:
            raise MessageMiddlewareMessageError(e)
        finally:
            self._consuming = False

    def stop(self):
        if self._stop_requested:
            return

        self._stop_requested = True
        if not self._consuming:
            return

        self.request_stop_consuming()

    def close(self):
        self.stop()
        super().close()
