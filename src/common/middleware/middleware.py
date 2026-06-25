from abc import ABC, abstractmethod


class MessageMiddlewareMessageError(Exception):
    pass


class MessageMiddlewareDisconnectedError(Exception):
    pass


class MessageMiddlewareCloseError(Exception):
    pass


class MessageMiddlewareDeleteError(Exception):
    pass


class MessageMiddleware(ABC):

    # Start listening on the queue/exchange and call on_message_callback(message,
    # ack, nack) for each data or control message, where message is the body as
    # passed to send() and ack/nack acknowledge the message being consumed.
    # Raises MessageMiddlewareDisconnectedError if the connection is lost,
    # MessageMiddlewareMessageError on an unrecoverable internal error.
    @abstractmethod
    def start_consuming(self, on_message_callback):
        pass

    # Stop listening on the queue/exchange; a no-op if not consuming. Raises
    # MessageMiddlewareDisconnectedError if the connection is lost.
    @abstractmethod
    def stop_consuming(self):
        pass

    # Send a message to the queue or to the exchange topic. Raises
    # MessageMiddlewareDisconnectedError if the connection is lost,
    # MessageMiddlewareMessageError on an unrecoverable internal error.
    @abstractmethod
    def send(self, message, routing_key=None):
        pass

    # Disconnect from the queue or exchange. Raises MessageMiddlewareCloseError
    # on an unrecoverable internal error.
    @abstractmethod
    def close(self):
        pass


class MessageMiddlewareExchange(MessageMiddleware):
    @abstractmethod
    def __init__(self, host, exchange_name, routing_keys):
        pass


class MessageMiddlewareQueue(MessageMiddleware):
    @abstractmethod
    def __init__(self, host, queue_name):
        pass


class MessageMiddlewareRpcClient(ABC):
    @abstractmethod
    def __init__(self, host, request_queue_name):
        pass

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def call(self, message, timeout=30):
        pass


class MessageMiddlewareRpcServer(ABC):
    @abstractmethod
    def __init__(self, host, request_queue_name):
        pass

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def start(self, on_request_callback):
        pass

    @abstractmethod
    def stop(self):
        pass
