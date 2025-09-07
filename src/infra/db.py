from __future__ import annotations

import logging as log
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool

_engine: Engine | None = None
_Session = None

def init_engine(db_url: str,  logging_sql: bool = False) -> Engine:
    global _engine, _Session
    log.info("Inicializando engine com URL: %s", db_url)
    if _engine is None:
        _engine = create_engine(
            db_url,
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=5,
            pool_timeout=30,
            pool_pre_ping=True,
            future=True,
            echo=logging_sql
        )
        _Session = sessionmaker(bind=_engine, autoflush=False, autocommit=False, future=True)
    return _engine

def get_session():
    if _Session is None:
        raise RuntimeError("Engine not initialized. Call init_engine first.")
    return _Session()
