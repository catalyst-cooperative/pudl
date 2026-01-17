"""merge heads

Revision ID: 0173ef466285
Revises: 6c84ecc06d2e, e825be7a3ebe
Create Date: 2026-01-16 20:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '0173ef466285'
down_revision = ('6c84ecc06d2e', 'e825be7a3ebe')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
