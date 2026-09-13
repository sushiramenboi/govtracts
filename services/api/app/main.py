from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes.health import router as health_router
from app.core.config import Settings, load_settings
from app.db.session import Database


def create_app(settings: Settings | None = None) -> FastAPI:
    """Create the Govtracts API without opening a database connection at startup."""

    resolved_settings = settings or load_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.database = Database(resolved_settings)
        try:
            yield
        finally:
            app.state.database.dispose()

    app = FastAPI(title="Govtracts API", version="0.1.0", lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=resolved_settings.allowed_origins,
        allow_credentials=False,
        allow_methods=["GET"],
        allow_headers=[],
    )
    app.include_router(health_router)
    return app
