import os
import logging
import signal
import threading
import zlib

from common import middleware, message_protocol, fruit_item

# ENV variables:
ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = f"{SUM_PREFIX}_control"
SUM_RESPONSE_QUEUE_PREFIX = f"{SUM_PREFIX}_response"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

# Positions for client list of items:
FRUITS_POS = 0
MESSAGE_COUNT_POS = 1

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self._control_sender = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_CONTROL_EXCHANGE]
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            self.data_output_exchanges.append(
                middleware.MessageMiddlewareExchangeRabbitMQ(
                    MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
                )
            )

        self.lock = threading.Lock()
        self.clients = {}

        self.pending_eof = {}

        self.leader_totals = {}
        self.expected_totals = {}

        self.my_response_queue_name = f"{SUM_RESPONSE_QUEUE_PREFIX}_{ID}"

        self._control_thread_exchange = None
        self._response_thread_queue = None
        self._eof_control_thread = None
        self._response_thread = None

    def _new_data_output_exchanges(self):
        exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            exchanges.append(
                middleware.MessageMiddlewareExchangeRabbitMQ(
                    MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
                )
            )
        return exchanges

    def _aggregation_index(self, fruit):
        return zlib.crc32(fruit.encode("utf-8")) % AGGREGATION_AMOUNT

    def _forward_data_to_aggs(self, client_id, fruit, amount, exchanges):
        index = self._aggregation_index(fruit)
        exchanges[index].send(
            message_protocol.internal.serialize([client_id, fruit, int(amount)])
        )

    def _handle_eof_signal(self, message, ack, nack, output_exchanges):
        try:
            fields = message_protocol.internal.deserialize(message)
            if len(fields) != 3:
                raise ValueError(f"Unexpected control message format: {fields}")

            client_id, expected_total, leader_id = fields

            with self.lock:
                current_count = self.clients.get(client_id, [{}, 0])[MESSAGE_COUNT_POS]
                current_fruits = dict(self.clients.get(client_id, [{}, 0])[FRUITS_POS])
                self.pending_eof[client_id] = (int(expected_total), int(leader_id))

            for fi in current_fruits.values():
                self._forward_data_to_aggs(client_id, fi.fruit, fi.amount, output_exchanges)

            self._report_increment_to_leader(client_id, leader_id, increment=current_count)
            ack()
        except Exception:
            logging.exception("Error processing control message")
            nack()

    def _leader_broadcast_eof(self, client_id, expected_count):
        self._control_sender.send(
            message_protocol.internal.serialize([client_id, int(expected_count), ID])
        )

    def _leader_handle_count_report(self, message, ack, nack, output_exchanges):
        try:
            client_id, count = message_protocol.internal.deserialize(message)
            count = int(count)
            should_send_eof = False

            with self.lock:
                self.leader_totals[client_id] = self.leader_totals.get(client_id, 0) + count
                expected = self.expected_totals.get(client_id)
                if expected is not None and self.leader_totals[client_id] == expected:
                    should_send_eof = True
                    del self.leader_totals[client_id]
                    del self.expected_totals[client_id]
                    self.clients.pop(client_id, None)
                    self.pending_eof.pop(client_id, None)

            if should_send_eof:
                for output_exchange in output_exchanges:
                    output_exchange.send(message_protocol.internal.serialize([client_id]))

            ack()
        except Exception:
            logging.exception("Error processing leader count report")
            nack()

    def _report_increment_to_leader(self, client_id, leader_id, increment=1):
        response_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, f"{SUM_RESPONSE_QUEUE_PREFIX}_{leader_id}"
        )
        response_queue.send(
            message_protocol.internal.serialize([client_id, int(increment)])
        )
        response_queue.close()

    def _handle_data_record(self, client_id, fruit, amount):
        with self.lock:
            self._process_data(client_id, fruit, amount)
            pending = self.pending_eof.get(client_id)

        if pending is None:
            return

        _, leader_id = pending
        self._forward_data_to_aggs(client_id, fruit, amount, self.data_output_exchanges)
        self._report_increment_to_leader(client_id, leader_id, 1)

    def _leader_handle_gateway_eof(self, client_id, expected_count):
        with self.lock:
            self.expected_totals[client_id] = int(expected_count)

        self._leader_broadcast_eof(client_id, expected_count)

    def _process_data(self, client_id, fruit, amount):
        logging.info("Processing data for client %s", client_id)

        client = self.clients.setdefault(client_id, [{}, 0])

        client[MESSAGE_COUNT_POS] += 1
        client[FRUITS_POS][fruit] = client[FRUITS_POS].get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def process_messsage(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)

            if len(fields) == 3:
                self._handle_data_record(*fields)
            elif len(fields) == 2:
                self._leader_handle_gateway_eof(*fields)
            else:
                raise ValueError(f"Unexpected message format: {fields}")

            ack()
        except Exception:
            logging.exception("Error processing data message")
            nack()

    def _start_eof_control_thread(self):
        exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_CONTROL_EXCHANGE]
        )
        control_thread_exchanges = self._new_data_output_exchanges()
        self._control_thread_exchange = exchange

        def on_eof_signal(message, ack, nack):
            self._handle_eof_signal(message, ack, nack, control_thread_exchanges)

        exchange.start_consuming(on_eof_signal)

        for ex in control_thread_exchanges:
            ex.close()
        exchange.close()

    def _start_response_thread(self):
        queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, self.my_response_queue_name
        )
        response_thread_exchanges = self._new_data_output_exchanges()
        self._response_thread_queue = queue

        def on_count_report(message, ack, nack):
            self._leader_handle_count_report(message, ack, nack, response_thread_exchanges)

        queue.start_consuming(on_count_report)

        for ex in response_thread_exchanges:
            ex.close()
        queue.close()

    def _handle_sigterm(self, _signum, _frame):
        self.input_queue.stop_consuming()
        if self._control_thread_exchange:
            self._control_thread_exchange.connection.add_callback_threadsafe(
                self._control_thread_exchange.stop_consuming
            )
        if self._response_thread_queue:
            self._response_thread_queue.connection.add_callback_threadsafe(
                self._response_thread_queue.stop_consuming
            )

    def start(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)

        self._eof_control_thread = threading.Thread(target=self._start_eof_control_thread)
        self._response_thread = threading.Thread(target=self._start_response_thread)
        self._eof_control_thread.start()
        self._response_thread.start()

        self.input_queue.start_consuming(self.process_messsage)
        
        self._eof_control_thread.join(timeout=5)
        self._response_thread.join(timeout=5)
        self.input_queue.close()
        self._control_sender.close()
        for exchange in self.data_output_exchanges:
            exchange.close()

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
