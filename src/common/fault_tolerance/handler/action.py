from enum import Enum, auto


class Action(Enum):
    ACK = auto()                  # nothing to publish, ack RabbitMQ
    PUBLISH_THEN_COMMIT = auto()  # publish the outputs, wait confirms, then commit
