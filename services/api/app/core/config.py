from __future__ import annotations

from functools import lru_cache

from pydantic import Field, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime settings loaded from environment variables or a local .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    database_url: str = Field(repr=False)
    app_env: str = "development"
    log_level: str = "INFO"
    cors_origins: str = ""

    @property
    def allowed_origins(self) -> list[str]:
        return [origin.strip() for origin in self.cors_origins.split(",") if origin.strip()]

    def model_post_init(self, __context: object) -> None:
        if not self.database_url.startswith("postgresql+psycopg://"):
            raise ValueError("DATABASE_URL must use the postgresql+psycopg driver URL format")


@lru_cache
def load_settings() -> Settings:
    """Return validated settings without leaking raw environment values on failure."""

    try:
        return Settings()
    except (ValidationError, ValueError) as exc:
        raise RuntimeError(
            "Invalid application configuration. Set required environment variable names and a valid DATABASE_URL."
        ) from None
