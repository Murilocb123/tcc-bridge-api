from __future__ import annotations
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from sqlalchemy.orm import Session

from src.infra.config import load_settings
from src.infra.db import get_session
from src.services.fetcher_service import fetch_and_persist

log = logging.getLogger(__name__)
settings = load_settings()

_scheduler: BackgroundScheduler | None = None

def _job_wrapper():
    with get_session() as s:  # type: Session
        processed, inserted = fetch_and_persist(s)
        log.info(f"Job finalizado. processados={processed} inseridos={inserted}")

def start_scheduler():
    global _scheduler
    if _scheduler and _scheduler.running:
        return _scheduler
    _scheduler = BackgroundScheduler(timezone=settings.app.timezone)
    _scheduler.add_job(_job_wrapper, IntervalTrigger(minutes=settings.app.schedule_minutes), id="yf_fetch_job", replace_existing=True)
    _scheduler.start()
    log.info(f"Scheduler iniciado (Background). Intervalo: {settings.app.schedule_minutes} min.")
    return _scheduler

def shutdown_scheduler():
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        log.info("Scheduler parado.")

def run_once_now() -> dict:
    with get_session() as s:  # type: Session
        processed, inserted = fetch_and_persist(s)
        return {"processed": processed, "inserted": inserted}