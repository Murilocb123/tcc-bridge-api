from src.infra.connection_manager import connection_manager


class RedisService:
    def __init__(self):
        self.client = connection_manager.get_redis_client()

    def save(self, key: str, value: str, ttl: int = 3600):
        self.client.set(key, value, ex=ttl)

    def find(self, key: str):
        return self.client.get(key)

    def exists(self, key: str):
        return self.client.exists(key) == 1

    def delete(self, key: str):
        self.client.delete(key)