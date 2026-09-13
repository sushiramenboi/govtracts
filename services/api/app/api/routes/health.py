from datetime import datetime, timedelta, timezone
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy import func, select, text
from sqlalchemy.exc import SQLAlchemyError

from app.api.dependencies import get_database
from app.db.session import Database
from app.db.models import IngestionRun

router = APIRouter(prefix="/health", tags=["health"])


@router.get("/live")
def liveness() -> dict[str, str]:
    """Report whether the API process is serving requests."""

    return {"status": "ok"}


@router.get("/ready")
def readiness(database: Annotated[Database, Depends(get_database)]) -> dict[str, str]:
    """Report database readiness without exposing connection details."""

    try:
        with database.engine.connect() as connection:
            connection.execute(text("SELECT 1"))
    except (OSError, SQLAlchemyError):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"status": "not_ready"},
        ) from None

    return {"status": "ready"}


@router.get("/datasets/usaspending")
def usaspending_freshness(request: Request, database: Annotated[Database, Depends(get_database)]) -> dict[str, object]:
    """Report USAspending freshness without exposing connection or upstream details."""
    try:
        with database.engine.connect() as connection:
            latest = connection.scalar(select(func.max(IngestionRun.finished_at)).where(IngestionRun.source == "usaspending", IngestionRun.status == "succeeded"))
    except (OSError, SQLAlchemyError):
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail={"status": "not_ready"}) from None
    if latest is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail={"status": "unavailable"})
    if latest < datetime.now(timezone.utc) - timedelta(hours=request.app.state.settings.usaspending_dataset_stale_after_hours):
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail={"status": "stale"})
    return {"status": "fresh", "last_successful_refresh": latest}
