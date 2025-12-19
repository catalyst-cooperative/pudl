"""merge heads

Revision ID: c48d8beff7e0
Revises: f4e23f78709a, 91b851ca4e39
Create Date: 2025-12-19 15:22:24.163078

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c48d8beff7e0'
down_revision = ('f4e23f78709a', '91b851ca4e39')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
