from __future__ import annotations
from fastapi import APIRouter

from src.services.scheduler_service import run_once_now

router = APIRouter(prefix="/scheduler", tags=["scheduler"])

@router.post("/run-once")
def run_once():
    """Dispara a execução imediata do fetch (sem esperar o agendamento)."""
    result = run_once_now()
    return {"message": "ok", **result}