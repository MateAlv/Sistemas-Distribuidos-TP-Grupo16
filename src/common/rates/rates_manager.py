import requests
import json
import os
import logging

BASE_URL = "https://api.frankfurter.app"
MAX_RETRIES = 3

class RatesManager:
    def __init__(self, cache_path: str, base_url=BASE_URL):
        self.cache_path = cache_path
        self._rates = {}
        self._base_url = base_url,

    def fetch_period(self, start_date: str, end_date: str, max_retries=MAX_RETRIES) -> bool:
        url = f"{self._base_url}/{start_date}..{end_date}?base=USD"
        logging.info(f"fetch_rates | url={url} | result=requesting")
        for attempt in range(1, max_retries + 1):
            try:
                logging.info("fetch_rates | url=%s | atempt=%d", url, attempt)
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                self._rates = response.json().get("rates", {})
                logging.info("fetch_rates | result=success | entries=%d", len(self._rates))
                return True
            except Exception as e:
                logging.error("fetch_rates | attempt=%d | error=%s", attempt, e)
        logging.error("fetch_rates | result=error | max_retries=%d", max_retries)
        return False



    def load_cache(self) -> bool:
        if not os.path.exists(self.cache_path):
            logging.warning(f"load_cache | path={self.cache_path} | result=not_found")
            return False
            
        try:
            with open(self.cache_path, 'r') as f:
                self._rates = json.load(f)
            logging.info(f"load_cache | result=success | entries={len(self._rates)}")
            return True
        except Exception as e:
            logging.error(f"load_cache | result=error | error={e}")
            return False

    def save_cache(self) -> None:
        directory = os.path.dirname(self.cache_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        try:
            with open(self.cache_path, 'w') as f:
                json.dump(self._rates, f)
            logging.info(f"save_cache | path={self.cache_path} | result=success")
        except Exception as e:
            logging.error(f"save_cache | result=error | error={e}")

    def get_rate(self, date: str, currency: str) -> float:
        if currency == "USD":
            return 1.0
            
        day_rates = self._rates.get(date)
        if not day_rates:
            raise ValueError(f"No exchange rates available for date: {date}")
            
        rate_to_currency = day_rates.get(currency)
        if rate_to_currency is None:
            raise ValueError(f"No exchange rate for currency {currency} on {date}")
            
        return 1.0 / float(rate_to_currency)

    @property
    def rates(self) -> dict:
        return self._rates

    def load_from_payload(self, payload: bytes) -> None:
        self._rates = json.loads(payload.decode('utf-8'))