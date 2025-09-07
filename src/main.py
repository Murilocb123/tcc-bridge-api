import logging

import uvicorn
from fastapi import FastAPI

from src.controllers.yfinance_controller import router as yfinance_controller

app = FastAPI()
app.include_router(yfinance_controller)

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
