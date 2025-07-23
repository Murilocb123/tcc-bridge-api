import yfinance as yf

from src.constants.TimeConstants import ONE_DAY, THIRTY_MINUTES_SECONDS
from src.domain.mongo_data import MongoData
from src.services.mongo_service import MongoService
from datetime import date, datetime
import logging as log

from src.services.redis_service import RedisService

HISTORY_COLLECTION_MDB = 'yfinance-history'
LATEST_PRICE_COLLECTION_RD = 'yfinance-latest-price'

class YFinanceService:
    """
    Service for interacting with the Yahoo Finance API and storing data in MongoDB.
    """

    def __init__(self):
        self.mongo_service = MongoService()
        self.redis_service = RedisService()

    def get_history_max(self, ticker: str):
        history = self.mongo_service.find_one(HISTORY_COLLECTION_MDB, {'_id': ticker})
        if self.is_expired(history, True, ONE_DAY):
            log.info(f"Data for {ticker} expired or not found, fetching from Yahoo Finance")
            yf_history = yf.Ticker(ticker).history(period="max", interval="1d")
            if not yf_history.empty:
                history_data = yf_history.reset_index().to_dict(orient='records')
                mongo_data = MongoData(_id=ticker, data=history_data).to_dict()
                self.mongo_service.delete(HISTORY_COLLECTION_MDB, {'_id': ticker})
                self.mongo_service.save(HISTORY_COLLECTION_MDB, mongo_data)
                log.info(f"Data for {ticker} saved to MongoDB")
                return mongo_data['data']
            else:
                log.error(f"No data found for ticker {ticker}")
                return None
        return history['data'] if history['data'] else None

    def get_latest_price(self, ticker: str):
        "Retorna a cotação de um ticker, armazenando em Redis durante 30minutos."
        key = LATEST_PRICE_COLLECTION_RD + ticker
        latest_price = self.redis_service.find(key)
        if latest_price is None:
            log.info(f"Latest price for {ticker} expired or not found, fetching from Yahoo Finance")
            yf_ticker = yf.Ticker(ticker)
            price = yf_ticker.fast_info['lastPrice']
            if price is not None or price != 0:
                self.redis_service.save(key, str(price), THIRTY_MINUTES_SECONDS)
                log.info(f"Latest price for {ticker} saved to Redis")
                return price
            else:
                log.error(f"No latest price found for ticker {ticker}")
                return None
        log.info(f"Latest price for {ticker} retrieved from Redis")
        return float(latest_price)

    @staticmethod
    def is_expired(data: dict, only_days: bool = True, lifetime: int = 3600) -> bool:
        if not data or 'created_at' not in data:
            log.error("Data not provided or missing 'created_at' attribute for expiration check.")
            return True
        created_at = date.fromisoformat(data['created_at']) if isinstance(data['created_at'], str) else data['created_at']
        if only_days:
            days = (date.today() - created_at).days
            expired = days > lifetime // 86400
            log.info(f"Checking expiration by days: expired={expired}, days={days}, limit={lifetime // 86400}")
            return expired
        else:
            now = datetime.now()
            seconds = (now - created_at).total_seconds()
            expired = seconds > lifetime
            log.info(f"Checking expiration by seconds: expired={expired}, seconds={seconds}, limit={lifetime}")
            return expired