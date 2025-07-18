"""Consolidate operating_swtich and can_switch_when_operating columns

Revision ID: d9eb718658b0
Revises: d0c5180c12f0
Create Date: 2025-07-09 22:40:14.024782

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd9eb718658b0'
down_revision = 'd0c5180c12f0'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('core_eia__entity_generators', schema=None) as batch_op:
        batch_op.add_column(sa.Column('can_switch_when_operating', sa.Boolean(), nullable=True, comment='Indicates whether a fuel switching generator can switch fuels while operating.'))
        batch_op.drop_column('operating_switch')

    with op.batch_alter_table('out_eia__monthly_generators', schema=None) as batch_op:
        batch_op.add_column(sa.Column('can_switch_when_operating', sa.Boolean(), nullable=True, comment='Indicates whether a fuel switching generator can switch fuels while operating.'))
        batch_op.drop_column('operating_switch')

    with op.batch_alter_table('out_eia__yearly_generators', schema=None) as batch_op:
        batch_op.add_column(sa.Column('can_switch_when_operating', sa.Boolean(), nullable=True, comment='Indicates whether a fuel switching generator can switch fuels while operating.'))
        batch_op.drop_column('operating_switch')

    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('out_eia__yearly_generators', schema=None) as batch_op:
        batch_op.add_column(sa.Column('operating_switch', sa.TEXT(), nullable=True))
        batch_op.drop_column('can_switch_when_operating')

    with op.batch_alter_table('out_eia__monthly_generators', schema=None) as batch_op:
        batch_op.add_column(sa.Column('operating_switch', sa.TEXT(), nullable=True))
        batch_op.drop_column('can_switch_when_operating')

    with op.batch_alter_table('core_eia__entity_generators', schema=None) as batch_op:
        batch_op.add_column(sa.Column('operating_switch', sa.TEXT(), nullable=True))
        batch_op.drop_column('can_switch_when_operating')

    # ### end Alembic commands ###
