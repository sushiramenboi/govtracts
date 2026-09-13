from __future__ import annotations

from sqlalchemy import Engine, create_engine

from app.core.config import Settings


class Database:
    """Lazily connecting SQLAlchemy database wrapper."""

    def __init__(self, settings: Settings) -> None:
        self.engine: Engine = create_engine(
            settings.database_url,
            pool_pre_ping=True,
            pool_recycle=1800,
        )

    def dispose(self) -> None:
        self.engine.dispose()
