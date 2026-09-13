from sqlalchemy import UniqueConstraint

from app.db.base import Base
from app.db import models  # noqa: F401 - registers ORM tables


def test_foundation_metadata_contains_only_planned_tables() -> None:
    assert set(Base.metadata.tables) == {
        "agencies",
        "vendors",
        "awards",
        "opportunities",
        "upstream_cache",
        "ingestion_runs",
    }


def test_vendor_identifier_indexes_are_partial_and_unique() -> None:
    vendor_indexes = {index.name: index for index in Base.metadata.tables["vendors"].indexes}

    assert vendor_indexes["uq_vendors_uei_not_null"].unique is True
    assert vendor_indexes["uq_vendors_duns_not_null"].unique is True
    assert "postgresql" in vendor_indexes["uq_vendors_uei_not_null"].dialect_options


def test_award_and_cache_uniqueness_constraints_are_declared() -> None:
    award_constraints = {constraint.name for constraint in Base.metadata.tables["awards"].constraints if isinstance(constraint, UniqueConstraint)}
    cache_constraints = {
        constraint.name for constraint in Base.metadata.tables["upstream_cache"].constraints if isinstance(constraint, UniqueConstraint)
    }

    assert "uq_awards_usa_generated_id" in award_constraints
    assert "uq_upstream_cache_request" in cache_constraints


def test_planned_foreign_keys_are_declared() -> None:
    award_foreign_keys = {foreign_key.target_fullname for foreign_key in Base.metadata.tables["awards"].foreign_keys}
    opportunity_foreign_keys = {
        foreign_key.target_fullname for foreign_key in Base.metadata.tables["opportunities"].foreign_keys
    }

    assert award_foreign_keys == {"agencies.id", "vendors.id"}
    assert opportunity_foreign_keys == {"agencies.id"}


def test_planned_query_indexes_exist() -> None:
    award_indexes = {index.name for index in Base.metadata.tables["awards"].indexes}
    opportunity_indexes = {index.name for index in Base.metadata.tables["opportunities"].indexes}

    assert {"ix_awards_agency_obligation_date", "ix_awards_vendor_obligation_date", "ix_awards_naics_obligation_date", "ix_awards_obligation_amount"} <= award_indexes
    assert {"ix_opportunities_active_deadline", "ix_opportunities_naics_code", "ix_opportunities_set_aside_code", "ix_opportunities_state", "ix_opportunities_organization_path"} <= opportunity_indexes
