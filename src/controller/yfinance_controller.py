from src.services.yfinance_service import YFinanceService
from fastapi import FastAPI, HTTPException, APIRouter
import logging as log

yfinance_service =  YFinanceService()
router = APIRouter()

@router.get("/yfinance/history/max/{ticker}")
def get_history_max(ticker: str):
    """
    Obtém o histórico máximo de um ticker usando o serviço YFinance.
    :param ticker: O ticker da ação ou ativo financeiro.
    :return: Dados do histórico máximo ou None se não encontrado.
    """
    try:
        log.info(f"ticker {ticker}")
        history = yfinance_service.get_history_max(ticker)
        if history is None:
            raise HTTPException(status_code=404, detail=f"Dados para o ticker {ticker} não encontrados.")
        return history
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/yfinance/latest-price/{ticker}")
def get_latest_price(ticker: str):
    """
    Obtém o preço mais recente de um ticker usando o serviço YFinance.
    :param ticker: O ticker da ação ou ativo financeiro.
    :return: Preço mais recente ou None se não encontrado.
    """
    try:
        log.info(f"ticker {ticker}")
        latest_price = yfinance_service.get_latest_price(ticker)
        if latest_price is None:
            raise HTTPException(status_code=404, detail=f"Preço mais recente para o ticker {ticker} não encontrado.")
        return {"ticker": ticker, "latest_price": latest_price}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))