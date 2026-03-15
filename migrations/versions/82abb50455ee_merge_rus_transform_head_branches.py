"""merge rus transform head branches

Revision ID: 82abb50455ee
Revises: 4c3808642f32, 562d67ae1d84
Create Date: 2026-03-06 11:53:35.029122

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '82abb50455ee'
down_revision = ('4c3808642f32', '562d67ae1d84')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
