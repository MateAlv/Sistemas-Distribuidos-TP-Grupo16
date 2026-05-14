import logging
import json
import os
from src.common.middleware import MessageMiddlewareRpcServerRabbitMQ
from src.common.rates import RatesManager

REQUEST_QUEUE = "rates_requests"



class ExchangeRateService:
    def __init__(self, rabbit_host: str, cache_path: str, start_date: str = "2022-09-01", end_date: str = "2022-09-30"):
        self._manager = RatesManager(cache_path)
        self._rabbit_host = rabbit_host
        self._start_date = start_date
        self._end_date = end_date
        self._rpc_server = MessageMiddlewareRpcServerRabbitMQ(rabbit_host, REQUEST_QUEUE)

    def run(self) -> None:
        if not self._bootstrap_rates():
            logging.error("rates_service | bootstrap_failed")
            return

        with self._rpc_server:
            self._rpc_server.connect()
            logging.info("rates_service | ready | queue=%s", REQUEST_QUEUE)
            self._rpc_server.start(self._on_request)

    def _bootstrap_rates(self) -> bool:
        if self._manager.load_cache():
            return True
        logging.info("rates_service | cache_miss | fetching")

        if not self._manager.fetch_period(self._start_date, self._end_date):
            return False
        self._manager.save_cache()
        return True

    def _on_request(self, body, reply):
        payload = json.dumps(self._manager.rates).encode("utf-8")
        reply(payload)
        logging.info("rates_service | replied | bytes=%d", len(payload))

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)-8s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    RABBIT_HOST = os.getenv("RABBIT_HOST", "rabbitmq")
    CACHE_PATH = os.getenv("RATES_CACHE_PATH", "/data/rates/cache.json")
    START_DATE = os.getenv("START_DATE", "2022-09-01")
    END_DATE = os.getenv("END_DATE", "2022-09-30")
    
    service = ExchangeRateService(rabbit_host=RABBIT_HOST, cache_path=CACHE_PATH, start_date=START_DATE, end_date=END_DATE)
    service.run()

