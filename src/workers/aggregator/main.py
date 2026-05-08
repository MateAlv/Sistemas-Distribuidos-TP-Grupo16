import os
import logging
import signal
import bisect

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.clients_fruit_top = {}

    def _process_data(self, client_id, fruit, amount):
        logging.info("Processing data message")
        client_fruit_top = self.clients_fruit_top.setdefault(client_id, [])

        for i in range(len(client_fruit_top)):
            if client_fruit_top[i].fruit == fruit:
                updated_fruit_item = client_fruit_top[i] + fruit_item.FruitItem(
                    fruit, amount
                )
                del client_fruit_top[i]
                bisect.insort(client_fruit_top, updated_fruit_item)
                return
        bisect.insort(client_fruit_top, fruit_item.FruitItem(fruit, amount))

    def _process_eof(self, client_id):
        logging.info("Received EOF")

        client_fruit_top = self.clients_fruit_top.setdefault(client_id, [])

        fruit_chunk = list(client_fruit_top[-TOP_SIZE:])
        fruit_chunk.reverse()
        fruit_top = list(
            map(
                lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                fruit_chunk,
            )
        )
        self.output_queue.send(message_protocol.internal.serialize([client_id, fruit_top]))
        self.clients_fruit_top.pop(client_id, None)

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            self._process_eof(*fields)
        ack()

    def _handle_sigterm(self, _signum, _frame):
        self.input_exchange.stop_consuming()

    def start(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        self.input_exchange.start_consuming(self.process_messsage)

        self.input_exchange.close()
        self.output_queue.close()


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
