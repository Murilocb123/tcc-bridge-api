from src.domain.mongo_data import MongoData
from src.infra.connection_manager import connection_manager

class MongoService:
    def __init__(self):
        self.client = connection_manager.get_mongo_client()

    def save(self, collection: str, document: dict):
        return self.client[collection].insert_one(document)

    def find_one(self, collection: str, filter: dict):
        return self.client[collection].find_one(filter)

    def update(self, collection: str, filter: dict, update: dict):
        return self.client[collection].update_one(filter, {'$set': update}, upsert=True)

    def delete(self, collection: str, filter: dict):
        return self.client[collection].delete_one(filter)