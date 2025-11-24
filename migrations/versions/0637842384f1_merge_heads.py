"""merge heads

Revision ID: 0637842384f1
Revises: 0f7ef503eef7, a9b37124d6c8
Create Date: 2025-11-14 19:25:13.177672

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '0637842384f1'
down_revision = ('0f7ef503eef7', 'a9b37124d6c8')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
