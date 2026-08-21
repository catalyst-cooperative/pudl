"""PyTest based testing of the FERC DBF Extraction logic."""

import sqlalchemy as sa


def test_ferc1_dbf2sqlite(ferc1_engine_dbf):
    """Attempt to access the DBF based FERC 1 SQLite DB fixture."""
    assert isinstance(ferc1_engine_dbf, sa.Engine)
    assert "f1_respondent_id" in sa.inspect(ferc1_engine_dbf).get_table_names()
