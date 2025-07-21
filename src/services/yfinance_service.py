import yfinance as yf
from src.domain.mongo_data import MongoData
from src.services.mongo_service import MongoService
from datetime import date, datetime
import logging as log

HISTORY_COLLECTION = 'yfinance-history'

class YFinanceService:
    """
    Service for interacting with the Yahoo Finance API and storing data in MongoDB.
    """

    def __init__(self):
        self.mongo_service = MongoService()

    def get_history_max(self, ticker: str):
        history = self.mongo_service.find_one(HISTORY_COLLECTION, {'_id': ticker})
        if self.is_expired(history, True, 1):
            log.info(f"Data for {ticker} expired or not found, fetching from Yahoo Finance")
            yf_history = yf.Ticker(ticker).history(period="max", interval="1d")
            if not yf_history.empty:
                history_data = yf_history.reset_index().to_dict(orient='records')
                mongo_data = MongoData(_id=ticker, data=history_data).to_dict()
                self.mongo_service.delete(HISTORY_COLLECTION, {'_id': ticker})
                self.mongo_service.save(HISTORY_COLLECTION, mongo_data)
                log.info(f"Data for {ticker} saved to MongoDB")
                return mongo_data['data']
            else:
                log.error(f"No data found for ticker {ticker}")
                return None
        return history['data'] if history['data'] else None

    def is_expired(self, data: dict, only_days: bool = True, lifetime: int = 3600) -> bool:
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