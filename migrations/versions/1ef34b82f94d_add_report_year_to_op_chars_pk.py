"""add report_year to op-chars primary key for already-migrated databases

An earlier commit on this branch edited e7c433fdf3cb (which adds
out_epacems__yearly_operational_characteristics) in place to make report_year
NOT NULL and part of the primary key, instead of adding a new migration. That
edit only affects fresh installs: databases that already applied the original
e7c433fdf3cb still have report_year nullable and excluded from the primary
key. This migration brings those already-migrated databases in line with what
a fresh build now produces.

Revision ID: 1ef34b82f94d
Revises: c191da5fd3ff
Create Date: 2026-08-26 12:10:53.222208

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '1ef34b82f94d'
down_revision = 'c191da5fd3ff'
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table('out_epacems__yearly_operational_characteristics', schema=None) as batch_op:
        batch_op.alter_column('report_year',
               existing_type=sa.Integer(),
               nullable=False)
        batch_op.drop_constraint('pk_out_epacems__yearly_operational_characteristics', type_='primary')
        batch_op.create_primary_key(
            'pk_out_epacems__yearly_operational_characteristics',
            ['report_year', 'plant_id_epa', 'emissions_unit_id_epa'],
        )


def downgrade() -> None:
    with op.batch_alter_table('out_epacems__yearly_operational_characteristics', schema=None) as batch_op:
        batch_op.drop_constraint('pk_out_epacems__yearly_operational_characteristics', type_='primary')
        batch_op.create_primary_key(
            'pk_out_epacems__yearly_operational_characteristics',
            ['plant_id_epa', 'emissions_unit_id_epa'],
        )
        batch_op.alter_column('report_year',
               existing_type=sa.Integer(),
               nullable=True)
