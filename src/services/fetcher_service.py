from __future__ import annotations
import logging
from typing import Iterable, Dict, Tuple, List, DefaultDict, Optional
from collections import defaultdict
import uuid
import datetime as dt

import pandas as pd
import yfinance as yf
from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from src.infra.config import load_settings
from src.models.tables import asset, asset_history, asset_log
from src.constants.common import SERVICE_NAME_ASSET_FETCHER, LOG_LEVEL_ERROR, LOG_LEVEL_WARNING

log = logging.getLogger(__name__)
settings = load_settings()

# ----------------- Helpers -----------------

def _chunk(it: Iterable, size: int):
    it = list(it)
    for i in range(0, len(it), size):
        yield it[i:i + size]

def _yahoo_symbol(ticker: str) -> str:
    """Adiciona .SA ao ticker caso ainda não tenha."""
    t = (ticker or "").strip().upper()
    return t if t.endswith(".SA") else f"{t}.SA"

def _base_ticker(symbol: str) -> str:
    """Remove o sufixo .SA, mantendo o ticker base (como salvo no banco)."""
    s = (symbol or "").strip().upper()
    return s[:-3] if s.endswith(".SA") else s

def _normalize_prices(df: pd.DataFrame, tickers_base: List[str]) -> pd.DataFrame:
    """
    Converte o DataFrame do yfinance para formato longo.
    Garante que a coluna 'ticker' fique sem .SA para casar com o banco.
    """
    if isinstance(df.columns, pd.MultiIndex):
        wide = df.copy()
    else:
        # yfinance retornou colunas simples (caso de 1 ticker)
        fields = df.columns.tolist()
        arrays = [[f for f in fields], [tickers_base[0] for _ in fields]]
        wide = df.copy()
        wide.columns = pd.MultiIndex.from_arrays(arrays)

    parts = []
    mapping = [
        ("Open", "open_price"),
        ("High", "high_price"),
        ("Low", "low_price"),
        ("Close", "close_price"),
        ("Volume", "volume"),
        ("Dividends", "dividends"),
        ("Stock Splits", "splits"),
    ]
    for field, out_col in mapping:
        cols = [c for c in wide.columns if c[0] == field]
        if cols:
            tmp = wide[cols].copy()
            # colunas são o 2º nível do MultiIndex (tickers com sufixo do yahoo)
            tmp.columns = [_base_ticker(c[1]) for c in cols]  # <-- remove .SA aqui
            tmp = tmp.stack().rename(out_col)
            parts.append(tmp)
        else:
            idx = pd.MultiIndex.from_product([wide.index, tickers_base], names=[wide.index.name or "Date", "Ticker"])
            parts.append(pd.Series(index=idx, name=out_col, dtype="float64"))

    df_long = pd.concat(parts, axis=1).reset_index()
    df_long.rename(columns={df_long.columns[0]: "price_date", df_long.columns[1]: "ticker"}, inplace=True)
    df_long["price_date"] = pd.to_datetime(df_long["price_date"]).dt.date

    if "volume" in df_long.columns:
        # preserva inteiros nulos de forma segura
        df_long["volume"] = df_long["volume"].astype("Int64").astype("float").astype("Int64")
    for c in ["dividends", "splits"]:
        if c in df_long.columns:
            df_long[c] = df_long[c].fillna(0.0)
    return df_long

# ----------------- Núcleo -----------------

def fetch_and_persist(session: Session) -> Tuple[int, int]:
    """
    Estratégia:
      1) Carrega (asset.id, asset.ticker) e, via LEFT JOIN, o MAX(asset_history.price_date) por asset.
      2) Monta um dict: last_date_by_ticker[ticker] = date (ou None).
      3) Agrupa tickers por essa 'last_date'.
      4) Para cada grupo:
           - Determina start_date:
               * se last_date is None -> usa cfg.start_date (ou None) / cfg.period.
               * se last_date < hoje -> start = last_date + 1 dia (para pegar apenas o que falta).
               * se last_date == hoje -> start = hoje (força atualização do dia).
           - Faz download em chunks, normaliza, e insere ignorando duplicatas (asset, price_date).
    """
    cfg = settings.app

    # Hoje (timezone do projeto: America/Sao_Paulo). Se tiver tz no settings, ajuste aqui.
    today = dt.date.today()

    # 1) Buscar assets + max(price_date) (ou None)
    subq_max = (
        select(
            asset_history.c.asset.label("asset_id"),
            func.max(asset_history.c.price_date).label("last_date")
        )
        .group_by(asset_history.c.asset)
        .subquery()
    )

    # LEFT JOIN com asset para pegar last_date (pode ser nulo se ainda sem histórico)
    q = (
        select(
            asset.c.id,
            asset.c.ticker,
            subq_max.c.last_date
        )
        .select_from(
            asset.outerjoin(subq_max, subq_max.c.asset_id == asset.c.id)
        )
        .where(asset.c.ticker.isnot(None))
    )

    rows = session.execute(q).fetchall()

    id_by_ticker: Dict[str, uuid.UUID] = {}
    last_date_by_ticker: Dict[str, Optional[dt.date]] = {}
    for a_id, t, last_dt in rows:
        if not t:
            continue
        tk = t.strip().upper()
        id_by_ticker[tk] = a_id
        # last_dt pode ser None
        last_date_by_ticker[tk] = last_dt

    all_tickers = list(id_by_ticker.keys())
    if not all_tickers:
        log.warning("Nenhum ticker encontrado em asset.")
        return (0, 0)

    # 2) Agrupar por última data
    grouped: DefaultDict[Optional[dt.date], List[str]] = defaultdict(list)
    for tk in all_tickers:
        grouped[last_date_by_ticker.get(tk)].append(tk)

    total_processed = 0
    total_inserted = 0

    # 3) Processar grupo a grupo (cada grupo compartilha a mesma 'last_date')
    for last_date, tickers_in_group in grouped.items():
        # Determinar start_date/end_date ou period conforme sua config
        # Regra:
        #   - None         -> baixar tudo conforme cfg (start_date/period)
        #   - < hoje       -> start = last_date + 1 dia (incremental)
        #   - == hoje      -> start = hoje (força atualizar os de hoje)
        if last_date is None:
            start_for_group = cfg.start_date if getattr(cfg, "use_start_end", False) else None
            end_for_group = cfg.end_date if getattr(cfg, "use_start_end", False) else None
            period_for_group = None if getattr(cfg, "use_start_end", False) else cfg.period
        else:
            if last_date >= today:
                # já tem hoje -> força atualizar hoje
                start_for_group = today
            else:
                start_for_group = last_date + dt.timedelta(days=1)
            end_for_group = None  # deixa em aberto para pegar até o último disponível
            period_for_group = None  # quando usar start/end, não usar period

        log.info(
            f"Processando grupo com last_date={last_date} "
            f"({len(tickers_in_group)} tickers) | start={start_for_group}, end={end_for_group}, period={period_for_group}"
        )

        # 4) Download em chunks dentro do grupo
        for tick_chunk in _chunk(tickers_in_group, cfg.chunk_size):
            try:
                yahoo_tickers = [_yahoo_symbol(tk) for tk in tick_chunk]
                log.info(f"Baixando chunk com {len(tick_chunk)} tickers: {tick_chunk}")

                df = yf.download(
                    yahoo_tickers,
                    period=period_for_group,
                    interval=cfg.interval,
                    start=start_for_group,
                    end=end_for_group,
                    auto_adjust=cfg.auto_adjust,
                    actions=cfg.actions,
                    group_by="column",
                    threads=True,
                    progress=False,
                )
                if df is None or len(df) == 0:
                    log.warning(f"Nenhum dado retornado para chunk: {tick_chunk}")
                    for tk in tick_chunk:
                        _log_asset_issue(
                            session, tk, "Sem Dados",
                            "Nenhum dado retornado pelo yfinance para o ticker.",
                            level=LOG_LEVEL_WARNING
                        )
                    session.commit()
                    continue

                # Normaliza SEM sufixo (.SA) para casar com o banco
                df_long = _normalize_prices(df, tick_chunk)

                # Mapear para asset id
                df_long["asset"] = df_long["ticker"].str.upper().map(id_by_ticker)
                # Filtra linhas válidas
                df_long = df_long.dropna(subset=["asset", "price_date", "close_price"])

                insert_rows = []
                for _, r in df_long.iterrows():
                    insert_rows.append({
                        "asset": r["asset"],
                        "price_date": r["price_date"],
                        "close_price": float(r["close_price"]) if pd.notna(r["close_price"]) else None,
                        "open_price": float(r["open_price"]) if pd.notna(r["open_price"]) else None,
                        "high_price": float(r["high_price"]) if pd.notna(r["high_price"]) else None,
                        "low_price": float(r["low_price"]) if pd.notna(r["low_price"]) else None,
                        "volume": int(r["volume"]) if pd.notna(r["volume"]) else None,
                        "dividends": float(r["dividends"]) if pd.notna(r["dividends"]) else 0.0,
                        "splits": float(r["splits"]) if pd.notna(r["splits"]) else 0.0,
                    })

                total_processed += len(insert_rows)

                if insert_rows:
                    stmt = pg_insert(asset_history).values(insert_rows)
                    stmt = stmt.on_conflict_do_nothing(index_elements=["asset", "price_date"])
                    result = session.execute(stmt)
                    session.commit()
                    inserted = result.rowcount if result.rowcount is not None else 0
                    total_inserted += inserted
                    log.info(f"Persistidos {inserted} novos registros de {len(insert_rows)} processados.")
            except Exception as e:
                msg = str(e)
                log.exception(f"Falha ao processar chunk {tick_chunk}: {msg}")
                for tk in tick_chunk:
                    _log_asset_issue(session, tk, "Erro no Download", msg)
                session.commit()
                continue

    return total_processed, total_inserted

# ----------------- Log de Issues -----------------

def _log_asset_issue(session: Session, ticker: str, title: str, message: str, level: str = LOG_LEVEL_ERROR):
    ins = asset_log.insert().values(
        id=uuid.uuid4(),
        ticker=ticker,
        title=title,
        message=(message or "")[:10000],
        service=SERVICE_NAME_ASSET_FETCHER,
        level=level,
        created_at=pd.Timestamp.now(),
        updated_at=pd.Timestamp.now(),
    )
    session.execute(ins)
