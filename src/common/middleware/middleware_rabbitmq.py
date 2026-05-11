import pika
import uuid
import threading
from .middleware import (
    MessageMiddlewareQueue, 
    MessageMiddlewareExchange, 
    MessageMiddlewareCloseError, 
    MessageMiddlewareMessageError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareRpcClient,
    MessageMiddlewareRpcServer,
)

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
        try:
            self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            self._channel = self._connection.channel()
        except _CONNECTION_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(e)

    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def _on_messaging_callback_adapter(self, ch, method, properties, body):
        ack = lambda: ch.is_open and ch.basic_ack(delivery_tag=method.delivery_tag)
        nack = lambda: ch.is_open and ch.basic_nack(delivery_tag=method.delivery_tag)
        self._user_callback(body, ack, nack)

    def start_consuming(self, on_message_callback):
        try:
            self._user_callback = on_message_callback
            self._channel.basic_qos(prefetch_count=1)
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



class MessageMiddlewareQueueRabbitMQ(_RabbitMQBase, MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        super().__init__(host)
        self._queue_name = queue_name
        self._channel.queue_declare(queue=queue_name, durable=True)

    def send(self, message):
        try:
            self._channel.basic_publish(
                exchange="",
                routing_key=self._queue_name,
                body=message,
                properties=pika.BasicProperties(delivery_mode=2),
            )
        except _CONNECTION_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(e)
        except MessageMiddlewareMessageError:
            raise
        except Exception as e:
            raise MessageMiddlewareMessageError(e)



        
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
            durable=True,
        )
        self._routing_keys = routing_keys

    
    def send(self, message):
        try:
            if self._exchange_type == 'fanout':
                self._channel.basic_publish(
                    exchange=self._exchange_name,
                    routing_key='',
                    body=message,
                    properties=pika.BasicProperties(delivery_mode=2),
                )
            else:
                if not self._routing_keys:
                    raise MessageMiddlewareMessageError("No routing keys provided")
                
                for key in self._routing_keys:
                    self._channel.basic_publish(
                        exchange=self._exchange_name,
                        routing_key=key,
                        body=message,
                        properties=pika.BasicProperties(delivery_mode=2),
                    )
        except _CONNECTION_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(e)
        except MessageMiddlewareMessageError:
            raise
        except Exception as e:
            raise MessageMiddlewareMessageError(e)


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
                durable=not self._exclusive,
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
        self._channel.queue_declare(queue=self._request_queue, durable=True)

    def call(self, message, timeout=30):
        correlation_id = str(uuid.uuid4())
        reply_queue = self._channel.queue_declare(queue='', exclusive=True).method.queue
        
        response = None
        response_received = threading.Event()
        
        def on_response(ch, method, props, body):
            nonlocal response
            if props.correlation_id == correlation_id:
                response = body
                response_received.set()
                ch.basic_ack(delivery_tag=method.delivery_tag)
        
        self._channel.basic_consume(queue=reply_queue, on_message_callback=on_response, auto_ack=False)
        
        self._channel.basic_publish(
            exchange="",
            routing_key=self._request_queue,
            body=message,
            properties=pika.BasicProperties(
                reply_to=reply_queue,
                correlation_id=correlation_id,
                delivery_mode=2,
            )
        )
        
        if response_received.wait(timeout=timeout):
            return response
        else:
            raise MessageMiddlewareMessageError("RPC call timed out")


class MessageMiddlewareRpcServerRabbitMQ(_RabbitMQBase, MessageMiddlewareRpcServer):
    def __init__(self, host, request_queue_name):
        self._request_queue = request_queue_name
        super().__init__(host)
        self._consuming = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def connect(self):
        self._channel.queue_declare(queue=self._request_queue, durable=True)

    def start(self, on_request_callback):
        def _on_request(ch, method, properties, body):
            def reply(response_message):
                ch.basic_publish(
                    exchange="",
                    routing_key=properties.reply_to,
                    body=response_message,
                    properties=pika.BasicProperties(correlation_id=properties.correlation_id)
                )
            on_request_callback(body, reply)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(queue=self._request_queue, on_message_callback=_on_request)
        self._consuming = True
        self._channel.start_consuming()

    def stop(self):
        if self._consuming:
            self._channel.stop_consuming()
            self._consuming = False

    def close(self):
        self.stop()
        super().close()
