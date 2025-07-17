import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from src.services.mongo_service import MongoService
from src.services.redis_service import RedisService

app = FastAPI()
mongo_service = MongoService()
redis_service = RedisService()

class MongoInsertRequest(BaseModel):
    collection: str
    document: dict

class RedisSaveRequest(BaseModel):
    key: str
    value: str
    ttl: int = 3600

@app.post("/mongo/insert")
def insert_mongo(req: MongoInsertRequest):
    result = mongo_service.save(req.collection, req.document)
    return {"inserted_id": str(result.inserted_id)}

@app.post("/redis/save")
def save_redis(req: RedisSaveRequest):
    redis_service.save(req.key, req.value, req.ttl)
    return {"status": "success"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)