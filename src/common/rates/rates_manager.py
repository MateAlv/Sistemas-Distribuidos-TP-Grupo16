import requests
import json
import os
import logging

class RatesManager:
    def __init__(self, cache_path: str):
        self.cache_path = cache_path
        self._rates = {}
        self._base_url = "https://api.frankfurter.app"

    def fetch_period(self, start_date: str, end_date: str) -> bool:
        url = f"{self._base_url}/{start_date}..{end_date}?base=USD"
        logging.info(f"fetch_rates | url={url} | result=requesting")
        
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            self._rates = data.get('rates', {})
            logging.info(f"fetch_rates | result=success | days={len(self._rates)}")
            return True
        except Exception as e:
            logging.error(f"fetch_rates | result=error | error={e}")
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
        try:
            os.makedirs(os.path.dirname(self.cache_path), exist_ok=True)
            with open(self.cache_path, 'w') as f:
                json.dump(self._rates, f, indent=2)
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
