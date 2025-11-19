"""merge heads

Revision ID: 5ba8ad3ad527
Revises: 0637842384f1, a08df6e8711c
Create Date: 2025-11-19 16:36:31.318364

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '5ba8ad3ad527'
down_revision = ('0637842384f1', 'a08df6e8711c')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
