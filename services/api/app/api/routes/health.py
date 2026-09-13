from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from app.api.dependencies import get_database
from app.db.session import Database

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
