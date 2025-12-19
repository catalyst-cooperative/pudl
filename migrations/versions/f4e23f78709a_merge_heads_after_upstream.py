"""merge heads after upstream

Revision ID: f4e23f78709a
Revises: 7d9ec620e804, d2a79341dd16
Create Date: 2025-12-16 13:13:19.831007

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'f4e23f78709a'
down_revision = ('7d9ec620e804', 'd2a79341dd16')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
