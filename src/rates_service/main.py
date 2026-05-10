import logging
import json
import time
import signal
import os
from src.common.rates.rates_manager import RatesManager
from src.common.middleware.middleware_rabbitmq import RabbitMQMiddleware
from src.common.message_protocol.internal import MessageType, InternalProtocol

class ExchangeRateService:
    def __init__(self, rabbit_host: str, cache_path: str):
        self._manager = RatesManager(cache_path)
        self._cache_path = cache_path
        
        self._middleware = RabbitMQMiddleware(rabbit_host)
        self._exchange_name = "exchange_rates"
        
        self._running = True
        self._setup_signals()

    def _setup_signals(self):
        signal.signal(signal.SIGTERM, self._handle_exit)
        signal.signal(signal.SIGINT, self._handle_exit)

    def _handle_exit(self, signum, frame):
        logging.info(f"rates_service | action=signal_received | signal={signum}")
        self._running = False

    def run(self):
        try:
            logging.info("rates_service | action=starting")
            
            if not self._manager.load_cache():
                logging.info("rates_service | action=fetching_from_api")
                if self._manager.fetch_period("2022-09-01", "2022-09-15"):
                    self._manager.save_cache()
                else:
                    logging.error("rates_service | action=bootstrap_failed")
                    return

            rates_payload = json.dumps(self._manager.rates).encode('utf-8')
            packet = InternalProtocol.create_packet(
                msg_type=MessageType.CONTROL,
                client_id_bytes=b'\x00' * 16,
                payload=rates_payload
            )

            logging.info("rates_service | action=broadcasting_rates")
            self._middleware.declare_exchange(self._exchange_name, exchange_type='fanout')
            self._middleware.send_to_exchange(self._exchange_name, packet)
            logging.info("rates_service | action=broadcast_complete")

            while self._running:
                time.sleep(1)

        except Exception as e:
            logging.error(f"rates_service | action=unexpected_error | error={e}")
        finally:
            self.close()

    def close(self):
        logging.info("rates_service | action=closing_resources")
        try:
            if self._middleware:
                self._middleware.close()
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
