import pika
from .middleware import (
    MessageMiddlewareQueue, 
    MessageMiddlewareExchange, 
    MessageMiddlewareCloseError, 
    MessageMiddlewareMessageError,
    MessageMiddlewareDisconnectedError
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
    
    def __init__(self, host, exchange_name, routing_keys, exchange_type="direct"):
        super().__init__(host)
        self._exchange_name = exchange_name
        self._exchange_type = exchange_type
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
            return
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


    