"""add dedupe_key for idempotency

Revision ID: 886e79fe406d
Revises: 8437cfb56c62
Create Date: 2025-08-17 10:10:10.106542

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '886e79fe406d'
down_revision: Union[str, Sequence[str], None] = '8437cfb56c62'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
