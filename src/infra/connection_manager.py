from pymongo import MongoClient
import redis
import os
from dotenv import load_dotenv

class ConnectionManager:
    def __init__(self):
        load_dotenv()
        self.mongo_client = None
        self.redis_client = None
        self.get_mongo_client()
        self.get_redis_client()

    def get_mongo_client(self):
        if self.mongo_client is None:
            mongo_url= os.getenv('MONGO_URL')
            mongo_db = os.getenv('MONGO_DB', 'default_db')
            mongo_user = os.getenv('MONGO_USER')
            mongo_password = os.getenv('MONGO_PASSWORD')
            if not mongo_url or not mongo_user or not mongo_password:
                raise ValueError("MONGO_URL, MONGO_USER, and MONGO_PASSWORD environment variables must be set.")
            mongo_uri = f"mongodb://{mongo_user}:{mongo_password}@{mongo_url}"
            print(f"Connecting to MongoDB at {mongo_uri}")
            self.mongo_client = MongoClient(mongo_uri)[mongo_db]
        return self.mongo_client

    def get_redis_client(self):
        if self.redis_client is None:
            redis_host = os.getenv('REDIS_HOST', 'localhost')
            redis_port = int(os.getenv('REDIS_PORT', 6379))
            redis_password = os.getenv('REDIS_PASSWORD', None)
            redis_db = int(os.getenv('REDIS_DB', 0))
            print(f"Connecting to Redis at {redis_host}:{redis_port}, DB: {redis_db}")
            self.redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db, password=redis_password,
                                            decode_responses=True)
        return self.redis_client

connection_manager = ConnectionManager()
