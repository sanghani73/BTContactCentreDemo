from datetime import datetime, timedelta, timezone
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- MongoDB -----------------------------------------------------------
    mongo_uri: Optional[str] = None
    mongo_db: str = "bt_contact_centre"

    # --- LLM via Grove -----------------------------------------------------
    # Full Grove endpoint, e.g.
    #   https://ai-gateway.corp.mongodb.com/anthropic/v1/messages
    llm_base_url: Optional[str] = None
    llm_api_key: Optional[str] = None
    llm_model: str = "claude-sonnet-4-5"

    # --- Misc --------------------------------------------------------------
    recent_window_days: int = 7
    host: str = "0.0.0.0"
    port: int = 8080

    @property
    def mock_mode(self) -> bool:
        return not self.mongo_uri

    @property
    def mock_llm(self) -> bool:
        return not (self.llm_base_url and self.llm_api_key)

    def recent_window_start(self) -> datetime:
        return now_utc() - timedelta(days=self.recent_window_days)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def seven_days_ago() -> datetime:
    return now_utc() - timedelta(days=7)


def one_hour_ago() -> datetime:
    return now_utc() - timedelta(hours=1)


settings = Settings()
