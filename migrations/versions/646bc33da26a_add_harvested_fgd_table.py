"""Add harvested FGD table

Revision ID: 646bc33da26a
Revises: 9dfb4295511e
Create Date: 2024-02-19 17:20:17.933839

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '646bc33da26a'
down_revision = '98836fb06355'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('core_eia__yearly_so2_control_equipment',
    sa.Column('report_year', sa.Integer(), nullable=False, comment='Four-digit year in which the data was reported.'),
    sa.Column('plant_id_eia', sa.Integer(), nullable=False, comment='The unique six-digit facility identification number, also called an ORISPL, assigned by the Energy Information Administration.'),
    sa.Column('so2_control_id_eia', sa.Text(), nullable=False, comment='Sulfur dioxide control identification number. This ID is not a unique identifier.'),
    sa.Column('opex_fgd_feed_materials_chemical', sa.Integer(), nullable=True, comment='Annual operation and maintenance expenditures for feed materials and chemicals for FGD equipment, excluding electricity.'),
    sa.Column('opex_fgd_labor_supervision', sa.Integer(), nullable=True, comment='Annual operation and maintenance expenditures for labor and supervision of FGD equipment, excluding electricity.'),
    sa.Column('opex_fgd_land_acquisition', sa.Integer(), nullable=True, comment='Annual operation and maintenance expenditures for land acquisition for FGD equipment, excluding electricity.'),
    sa.Column('opex_fgd_waste_disposal', sa.Integer(), nullable=True, comment='Annual operation and maintenance expenditures for waste disposal, excluding electricity.'),
    sa.Column('opex_fgd_maintenance_material_other', sa.Integer(), nullable=True, comment='Annual operation and maintenance expenditures for maintenance, materials and all other costs of FGD equipment, excluding electricity'),
    sa.Column('opex_fgd_total_cost', sa.Integer(), nullable=True, comment='Annual total cost of operation and maintenance expenditures on FGD equipment, excluding electricity'),
    sa.Column('fgd_control_flag', sa.Boolean(), nullable=True, comment='Indicatates whether or not a plant has a FGD control unit.'),
    sa.Column('fgd_operational_status', sa.Text(), nullable=True, comment='Operating status code for FGD equipment.'),
    sa.Column('fgd_hours_in_service', sa.Integer(), nullable=True, comment='Number of hours the FGD equipment was in operation during the year.'),
    sa.Column('fgd_sorbent_consumption_1000_tons', sa.Float(), nullable=True, comment='Quantity of FGD sorbent used, to the nearest 0.1 thousand tons.'),
    sa.Column('fgd_electricity_consumption_mwh', sa.Float(), nullable=True, comment='Electric power consumed by the FGD unit (in MWh).'),
    sa.Column('so2_removal_efficiency_annual', sa.Float(), nullable=True, comment='Removal efficiency for sulfur dioxide (to the nearest 0.1 percent by weight) at annual operating factor.'),
    sa.Column('so2_removal_efficiency_100pct_load', sa.Float(), nullable=True, comment='Removal efficiency for sulfur dioxide (to the nearest 0.1 percent by weight) at tested rate.'),
    sa.Column('so2_test_date', sa.Date(), nullable=True, comment='Date of most recent test for sulfur dioxide removal efficiency.'),
    sa.Column('data_maturity', sa.Text(), nullable=True, comment='Level of maturity of the data record. Some data sources report less-than-final data. PUDL sometimes includes this data, but use at your own risk.'),
    sa.ForeignKeyConstraint(['data_maturity'], ['core_pudl__codes_data_maturities.code'], name=op.f('fk_core_eia__yearly_so2_control_equipment_data_maturity_core_pudl__codes_data_maturities')),
    sa.ForeignKeyConstraint(['plant_id_eia'], ['core_eia__entity_plants.plant_id_eia'], name=op.f('fk_core_eia__yearly_so2_control_equipment_plant_id_eia_core_eia__entity_plants')),
    sa.PrimaryKeyConstraint('plant_id_eia', 'so2_control_id_eia', 'report_year', name=op.f('pk_core_eia__yearly_so2_control_equipment'))
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('core_eia__yearly_so2_control_equipment')
    # ### end Alembic commands ###