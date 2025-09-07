# src/main.py
from __future__ import annotations
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI

from src.infra.config import load_settings
from src.infra.db import init_engine
from src.infra.logging import configure_logging
from src.controllers.app_controller import router as app_router
from src.controllers.scheduler_controller import router as scheduler_router
from src.services.scheduler_service import start_scheduler, shutdown_scheduler

settings = load_settings()

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        configure_logging(settings.log_level, service_name=settings.app.service_name, create_log_file=settings.create_log_file)
        init_engine(settings.db_url, logging_sql=settings.logging_sql)
        start_scheduler()
        yield
    finally:
        try:
            shutdown_scheduler()
        except Exception as e:
            logging.getLogger(__name__).exception("Falha ao parar scheduler: %s", e)

app = FastAPI(title="YF Price Fetcher API", version="1.0.0", lifespan=lifespan)
app.include_router(app_router)
app.include_router(scheduler_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
