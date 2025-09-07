from __future__ import annotations

import os
import yaml
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

@dataclass(frozen=True)
class AppConfig:
    service_name: str
    schedule_minutes: int
    chunk_size: int
    interval: str
    period: str | None
    use_start_end: bool
    start_date: str | None
    end_date: str | None
    auto_adjust: bool
    actions: bool
    timezone: str

@dataclass(frozen=True)
class Settings:
    db_url: str
    log_level: str
    app: AppConfig
    logging_sql: bool = False
    create_log_file: bool = False

def _load_yaml_config() -> dict:
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.yml")
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def load_settings() -> Settings:
    """
    Carrega as configurações da aplicação combinando um arquivo YAML e variáveis de ambiente.

    Esta função lê as configurações do arquivo YAML localizado no mesmo diretório do script,
    obtém variáveis de ambiente para configurar a conexão com o banco de dados e monta um
    objeto Settings com todas as informações necessárias para a aplicação.

    Retorna:
        Settings: Objeto contendo as configurações completas da aplicação.
    """
    y = _load_yaml_config()
    app = y.get("app", {})
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "postgres")
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD", "postgres")
    db_url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    return Settings(
        db_url=db_url,
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        logging_sql=bool(os.getenv("LOGGING_SQL", False)),
        create_log_file=bool(os.getenv("CREATE_LOG_FILE", False)),
        app=AppConfig(
            service_name=str(app.get("service_name", "yf_price_fetcher")),
            schedule_minutes=int(app.get("schedule_minutes", 15)),
            chunk_size=int(app.get("chunk_size", 16)),
            interval=str(app.get("interval", "1d")),
            period=str(app.get("period", "1y")),
            use_start_end=bool(app.get("use_start_end", False)),
            start_date=app.get("start_date"),
            end_date=app.get("end_date"),
            auto_adjust=bool(app.get("auto_adjust", True)),
            actions=bool(app.get("actions", True)),
            timezone=str(app.get("timezone", "America/Sao_Paulo")),
        ),
    )
