"""Merge migrations

Revision ID: 9ab4befcc33a
Revises: 91358c4810b5, e878ec83ceb5
Create Date: 2023-05-22 11:08:29.071908

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '9ab4befcc33a'
down_revision = ('91358c4810b5', 'e878ec83ceb5')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
