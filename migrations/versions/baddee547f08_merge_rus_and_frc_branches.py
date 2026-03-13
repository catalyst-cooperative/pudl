"""merge rus and frc branches

Revision ID: baddee547f08
Revises: 35d9f1376640, 758a4b398eeb
Create Date: 2026-03-09 16:27:44.043196

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'baddee547f08'
down_revision = ('35d9f1376640', '758a4b398eeb')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
