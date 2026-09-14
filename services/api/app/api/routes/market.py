from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import Select, and_, desc, func, select
from sqlalchemy.exc import SQLAlchemyError

from app.api.dependencies import get_database
from app.db.models import Agency, Award, IngestionRun, Vendor
from app.db.session import Database
from app.usaspending.preset import as_public_dict


router = APIRouter(prefix="/v1", tags=["market"])


def _award_query(start_date: date | None, end_date: date | None) -> Select[tuple[Award]]:
    query = select(Award)
    if start_date:
        query = query.where(Award.base_obligation_date >= start_date)
    if end_date:
        query = query.where(Award.base_obligation_date <= end_date)
    return query


def _freshness(connection: object) -> datetime | None:
    return connection.scalar(  # type: ignore[attr-defined]
        select(func.max(IngestionRun.finished_at)).where(and_(IngestionRun.source == "usaspending", IngestionRun.status == "succeeded"))
    )


@router.get("/market-overview")
def market_overview(
    database: Annotated[Database, Depends(get_database)],
    start_date: date | None = None,
    end_date: date | None = None,
    granularity: Literal["month", "year"] = "year",
) -> dict[str, object]:
    try:
        if start_date and end_date and end_date < start_date:
            raise HTTPException(status_code=422, detail="end_date must not be before start_date")
        query = _award_query(start_date, end_date).subquery()
        with database.engine.connect() as connection:
            total, count, min_date, max_date = connection.execute(
                select(func.coalesce(func.sum(query.c.obligation_amount), Decimal("0")), func.count(), func.min(query.c.base_obligation_date), func.max(query.c.base_obligation_date))
            ).one()
            period = func.date_trunc(granularity, query.c.base_obligation_date).label("period")
            trend = connection.execute(select(period, func.coalesce(func.sum(query.c.obligation_amount), Decimal("0"))).where(query.c.base_obligation_date.is_not(None)).group_by(period).order_by(period)).all()
            agencies = connection.execute(select(Agency.name, Agency.external_code, func.coalesce(func.sum(Award.obligation_amount), Decimal("0")).label("amount"), func.count(Award.id).label("count")).join(Award, Award.awarding_agency_id == Agency.id).where(Award.id.in_(select(query.c.id))).group_by(Agency.id, Agency.name, Agency.external_code).order_by(desc("amount"), Agency.name).limit(10)).all()
            vendors = connection.execute(select(Vendor.canonical_name, func.coalesce(func.sum(Award.obligation_amount), Decimal("0")).label("amount"), func.count(Award.id).label("count")).join(Award, Award.vendor_id == Vendor.id).where(Award.id.in_(select(query.c.id))).group_by(Vendor.id, Vendor.canonical_name).order_by(desc("amount"), Vendor.canonical_name).limit(10)).all()
            recent = connection.execute(select(Award, Agency.name.label("agency_name"), Vendor.canonical_name.label("vendor_name")).outerjoin(Agency, Award.awarding_agency_id == Agency.id).outerjoin(Vendor, Award.vendor_id == Vendor.id).where(Award.id.in_(select(query.c.id))).order_by(Award.base_obligation_date.desc().nullslast(), Award.usa_generated_id).limit(10)).all()
            freshness = _freshness(connection)
    except SQLAlchemyError:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail={"status": "not_ready"}) from None

    return {
        "preset": as_public_dict(), "range": {"start_date": min_date, "end_date": max_date},
        "total_obligations": total, "award_count": count,
        "trend": [{"period": row[0], "amount": row[1]} for row in trend],
        "top_agencies": [{"name": row.name, "code": row.external_code, "amount": row.amount, "award_count": row.count} for row in agencies],
        "top_vendors": [{"name": row.canonical_name, "amount": row.amount, "award_count": row.count} for row in vendors],
        "recent_awards": [_award_item(row, row.agency_name, row.vendor_name) for row in recent],
        "source": {"name": "USAspending.gov", "url": "https://api.usaspending.gov/docs/endpoints", "last_successful_refresh": freshness},
    }


@router.get("/awards")
def list_awards(
    database: Annotated[Database, Depends(get_database)],
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 25,
    start_date: date | None = None,
    end_date: date | None = None,
    agency_code: str | None = None,
    naics_code: Annotated[str | None, Query(pattern=r"^\d{6}$")] = None,
    vendor_id: UUID | None = None,
    award_id: str | None = None,
    minimum_amount: Decimal | None = None,
    maximum_amount: Decimal | None = None,
) -> dict[str, object]:
    if end_date and start_date and end_date < start_date:
        raise HTTPException(status_code=422, detail="end_date must not be before start_date")
    if minimum_amount is not None and maximum_amount is not None and maximum_amount < minimum_amount:
        raise HTTPException(status_code=422, detail="maximum_amount must not be below minimum_amount")
    conditions = []
    if start_date: conditions.append(Award.base_obligation_date >= start_date)
    if end_date: conditions.append(Award.base_obligation_date <= end_date)
    if agency_code: conditions.append(Agency.external_code == agency_code)
    if naics_code: conditions.append(Award.naics_code == naics_code)
    if vendor_id: conditions.append(Vendor.id == vendor_id)
    if award_id: conditions.append(Award.award_id == award_id)
    if minimum_amount is not None: conditions.append(Award.obligation_amount >= minimum_amount)
    if maximum_amount is not None: conditions.append(Award.obligation_amount <= maximum_amount)
    try:
        base = select(Award, Agency.name.label("agency_name"), Vendor.canonical_name.label("vendor_name")).outerjoin(Agency, Award.awarding_agency_id == Agency.id).outerjoin(Vendor, Award.vendor_id == Vendor.id)
        if conditions: base = base.where(*conditions)
        with database.engine.connect() as connection:
            total = connection.scalar(select(func.count()).select_from(base.subquery())) or 0
            rows = connection.execute(base.order_by(Award.base_obligation_date.desc().nullslast(), Award.usa_generated_id).offset((page - 1) * page_size).limit(page_size)).all()
    except SQLAlchemyError:
        raise HTTPException(status_code=503, detail={"status": "not_ready"}) from None
    return {"items": [_award_item(row, row.agency_name, row.vendor_name) for row in rows], "page": page, "page_size": page_size, "total": total}


@router.get("/awards/{award_id}")
def get_award(award_id: str, database: Annotated[Database, Depends(get_database)]) -> dict[str, object]:
    try:
        with database.engine.connect() as connection:
            row = connection.execute(select(Award, Agency.name.label("agency_name"), Vendor.canonical_name.label("vendor_name")).outerjoin(Agency, Award.awarding_agency_id == Agency.id).outerjoin(Vendor, Award.vendor_id == Vendor.id).where(Award.usa_generated_id == award_id)).first()
    except SQLAlchemyError:
        raise HTTPException(status_code=503, detail={"status": "not_ready"}) from None
    if row is None:
        raise HTTPException(status_code=404, detail={"status": "not_found"})
    item = _award_item(row, row.agency_name, row.vendor_name)
    item["description"] = _award_value(row, "description")
    item["raw_payload"] = _award_value(row, "raw_payload")
    return item


def _award_value(award: object, name: str) -> object:
    mapping = getattr(award, "_mapping", None)
    return mapping[name] if mapping is not None else getattr(award, name)


def _award_item(award: object, agency_name: str | None, vendor_name: str | None) -> dict[str, object]:
    return {
        "id": _award_value(award, "usa_generated_id"),
        "award_id": _award_value(award, "award_id"),
        "agency": agency_name,
        "vendor": vendor_name,
        "naics_code": _award_value(award, "naics_code"),
        "psc_code": _award_value(award, "psc_code"),
        "obligation_amount": _award_value(award, "obligation_amount"),
        "base_obligation_date": _award_value(award, "base_obligation_date"),
        "award_type": _award_value(award, "award_type"),
        "source_url": _award_value(award, "source_url"),
    }
