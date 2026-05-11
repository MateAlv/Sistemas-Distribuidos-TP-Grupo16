from asyncio import protocols
import logging
import json
import time
import signal
import os
import pika
import threading
from src.common.rates.rates_manager import RatesManager

REQUEST_QUEUE = "rates_requests"



class ExchangeRateService:
    def __init__(self, rabbit_host: str, cache_path: str, start_date: str, end_date: str):
        self._manager = RatesManager(cache_path)
        self._rabbit_host = rabbit_host
        self._start_date = start_date
        self._end_date = end_date
        self._stop_event = threading.Event()
        self._connection: pika.BlockingConnection | None = None
        self._channel: pika.channel.Channel | None = None

    def _setup_signals(self):
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

    def _handle_signal(self, signum, frame):
        logging.info("rates_service | signal=%s", signum)
        self._stop_event.set()

    def run(self) -> None:
        if not self._bootstrap_rates():
            logging.error("rates_service | bootstrap_failed")
            return

        self._connect()
        self._setup_signals()

        try: 
            self._channel.basic_qos(prefetch_count=1)
            self._channel.basic_consume(
                queue=REQUEST_QUEUE,
                on_message_callback=self._on_request,
            )
            logging.info("rates_service | ready | queue=%s", REQUEST_QUEUE)
            while not self._stop_event.is_set():
                self._connection.process_data_events(time_limit=1)
        finally:
            self._close()

    def _bootstrap_rates(self) -> bool:
        if self._manager.load_cache():
            return True
        logging.info("rates_service | cache_miss | fetching")

        if not self._manager.fetch_period(self._start_date, self._end_date):
            return False
        self._manager.save_cache()
        return True

    def _connect(self) -> None:
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._rabbit_host))
        self._channel = self._connection.channel()
        self._channel.queue_declare(queue=REQUEST_QUEUE, durable=True)


    def _on_request(self, ch, method, properties, body):
        reply_queue = properties.reply_to if properties else None
        correlation_id = properties.correlation_id if properties else None
        if not reply_queue:
            logging.warning("rates_service | request_without_reply_to | discarding")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        payload = json.dumps(self._manager.rates).encode("utf-8")
        ch.basic_publish(
            exchange="",
            routing_key=reply_queue,
            body=payload,
            properties=pika.BasicProperties(correlation_id=correlation_id)
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logging.info("rates_service | replied | to=%s | bytes=%d", reply_queue, len(payload))

    def close(self):
        logging.info("rates_service | action=closing_resources")
        try:
            if self._channel and self._channel.is_open: 
                self._channel.close()
            if self._connection and self._connection.is_open:
                self._connection.close()
        except Exception as e:
            logging.error(f"rates_service | action=close_error | error={e}")

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)-8s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    RABBIT_HOST = os.getenv("RABBIT_HOST", "rabbitmq")
    CACHE_PATH = os.getenv("RATES_CACHE_PATH", "/data/rates/cache.json")
    
    service = ExchangeRateService(rabbit_host=RABBIT_HOST, cache_path=CACHE_PATH)
    service.run()
