import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        self.partial_tops = {}
        self.received_count = {}

    def _merge_partial_tops(self, client_id):
        total_by_fruit = {}
        partial_tops = self.partial_tops.get(client_id, [])

        for partial_top in partial_tops:
            for fruit, amount in partial_top:
                new_item = fruit_item.FruitItem(fruit, int(amount))
                current_item = total_by_fruit.get(fruit, fruit_item.FruitItem(fruit, 0))
                total_by_fruit[fruit] = current_item + new_item

        return total_by_fruit

    def _build_final_top(self, total_by_fruit):
        sorted_items = sorted(total_by_fruit.values())
        sorted_items.reverse()

        top_items = []
        for item in sorted_items[:TOP_SIZE]:
            top_items.append((item.fruit, item.amount))

        return top_items

    def process_messsage(self, message, ack, nack):
        logging.info("Received top")

        client_id, fruit_top = message_protocol.internal.deserialize(message)
        self.partial_tops.setdefault(client_id, []).append(fruit_top)
        self.received_count[client_id] = self.received_count.get(client_id, 0) + 1

        if self.received_count[client_id] < AGGREGATION_AMOUNT:
            ack()
            return

        total_by_fruit = self._merge_partial_tops(client_id)
        final_top = self._build_final_top(total_by_fruit)

        self.output_queue.send(
            message_protocol.internal.serialize([client_id, final_top])
        )

        self.partial_tops.pop(client_id, None)
        self.received_count.pop(client_id, None)

        ack()

    def _handle_sigterm(self, _signum, _frame):
        self.input_queue.stop_consuming()

    def start(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        self.input_queue.start_consuming(self.process_messsage)
        self.input_queue.close()
        self.output_queue.close()


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
