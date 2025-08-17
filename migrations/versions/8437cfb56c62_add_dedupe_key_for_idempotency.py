"""add_dedupe_key_for_idempotency

Revision ID: 8437cfb56c62
Revises: 3f1c72b095a3
Create Date: 2025-08-17 10:09:18.424905

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8437cfb56c62'
down_revision: Union[str, Sequence[str], None] = '3f1c72b095a3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
