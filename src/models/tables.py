from __future__ import annotations
from sqlalchemy import Table, Column, MetaData, String, Date, BigInteger, Numeric, TIMESTAMP, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

metadata = MetaData()

asset = Table(
    "asset",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, nullable=False),
    Column("ticker", String(255)),
    Column("name", String(255), nullable=False),
    Column("type", String(255), nullable=False),
    Column("currency", String(3), nullable=False),
    Column("exchange", String(255)),
    Column("created_at", TIMESTAMP(timezone=False), nullable=False),
    Column("updated_at", TIMESTAMP(timezone=False)),
    Column("created_by", UUID(as_uuid=True)),
    Column("updated_by", UUID(as_uuid=True)),
)

asset_history = Table(
    "asset_history",
    metadata,
    Column("asset", UUID(as_uuid=True), primary_key=True, nullable=False),
    Column("price_date", Date, primary_key=True, nullable=False),
    Column("close_price", Numeric(18, 6), nullable=False),
    Column("open_price", Numeric(18, 6)),
    Column("high_price", Numeric(18, 6)),
    Column("low_price", Numeric(18, 6)),
    Column("volume", BigInteger),
    Column("dividends", Numeric(18, 6)),
    Column("splits", Numeric(18, 6)),
)

asset_log = Table(
    "asset_log",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, nullable=False),
    Column("ticker", String(255)),
    Column("title", String(255), nullable=False),
    Column("message", String, nullable=False),
    Column("service", String(255), nullable=False),
    Column("level", String(50), nullable=False),
    Column("created_at", TIMESTAMP(timezone=False), nullable=False, server_default=func.now()),
    Column("updated_at", TIMESTAMP(timezone=False)),
    Column("created_by", UUID(as_uuid=True)),
    Column("updated_by", UUID(as_uuid=True)),
)
