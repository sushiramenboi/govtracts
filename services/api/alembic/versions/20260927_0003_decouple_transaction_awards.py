"""Decouple canonical USAspending transactions from legacy awards.

Revision ID: 20260927_0003
Revises: 20260927_0002
Create Date: 2026-09-27
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "20260927_0003"
down_revision: Union[str, Sequence[str], None] = "20260927_0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_constraint(
        "fk_usaspending_tx_award",
        "usaspending_transactions",
        type_="foreignkey",
    )


def _has_orphan_transaction_awards() -> bool:
    orphan_exists = op.get_bind().scalar(
        sa.text(
            """
            SELECT EXISTS (
                SELECT 1
                FROM usaspending_transactions AS tx
                LEFT JOIN awards AS award
                    ON award.usa_generated_id = tx.generated_award_id
                WHERE award.usa_generated_id IS NULL
            )
            """
        )
    )
    return bool(orphan_exists)


def downgrade() -> None:
    if _has_orphan_transaction_awards():
        raise RuntimeError(
            "cannot restore fk_usaspending_tx_award: "
            "usaspending_transactions contains generated_award_id values absent from awards"
        )

    op.create_foreign_key(
        "fk_usaspending_tx_award",
        "usaspending_transactions",
        "awards",
        ["generated_award_id"],
        ["usa_generated_id"],
        ondelete="RESTRICT",
    )
