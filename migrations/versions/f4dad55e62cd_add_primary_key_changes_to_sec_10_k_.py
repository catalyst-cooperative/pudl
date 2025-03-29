"""Add primary key changes to SEC 10-K tables

Revision ID: f4dad55e62cd
Revises: f01cc3a84cc6
Create Date: 2025-03-28 19:16:29.214402

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = 'f4dad55e62cd'
down_revision = 'f01cc3a84cc6'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Apply the schema changes for the SEC 10-K tables."""
    # Remove the primary key constraint from core_sec10k__changelog_company_name
    with op.batch_alter_table("core_sec10k__changelog_company_name", schema=None) as batch_op:
        batch_op.drop_constraint("pk_core_sec10k__changelog_company_name", type_="primary")

    # Add a composite primary key to core_sec10k__quarterly_company_information
    with op.batch_alter_table("core_sec10k__quarterly_company_information", schema=None) as batch_op:
        batch_op.create_primary_key("pk_core_sec10k__quarterly_company_information", ["filename_sec10k", "filer_count"])

    # Change the primary key for out_sec10k__quarterly_company_information
    with op.batch_alter_table("out_sec10k__quarterly_company_information", schema=None) as batch_op:
        batch_op.drop_constraint("pk_out_sec10k__quarterly_company_information", type_="primary")
        batch_op.create_primary_key("pk_out_sec10k__quarterly_company_information", ["filename_sec10k", "filer_count"])

def downgrade() -> None:
    """Revert the schema changes for the SEC 10-K tables."""
    # Re-add the primary key constraint to core_sec10k__changelog_company_name
    with op.batch_alter_table("core_sec10k__changelog_company_name", schema=None) as batch_op:
        batch_op.create_primary_key("pk_core_sec10k__changelog_company_name", ["central_index_key"])

    # Remove the composite primary key from core_sec10k__quarterly_company_information
    with op.batch_alter_table("core_sec10k__quarterly_company_information", schema=None) as batch_op:
        batch_op.drop_constraint("pk_core_sec10k__quarterly_company_information", type_="primary")

    # Revert the primary key for out_sec10k__quarterly_company_information
    with op.batch_alter_table("out_sec10k__quarterly_company_information", schema=None) as batch_op:
        batch_op.drop_constraint("pk_out_sec10k__quarterly_company_information", type_="primary")
        batch_op.create_primary_key("pk_out_sec10k__quarterly_company_information", ["central_index_key", "report_date"])
