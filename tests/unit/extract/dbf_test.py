"""Tests for DBF extraction helpers."""

import json
import sqlite3

from click.testing import CliRunner

from pudl.extract.dbf import audit_dbf_datapackage_types
from pudl.scripts.audit_dbf_datapackage_types import main


def _make_dbf_audit_inputs(tmp_path):
    """Create a tiny SQLite database and matching synthetic datapackage."""
    sqlite_path = tmp_path / "ferc_test.sqlite"
    with sqlite3.connect(sqlite_path) as conn:
        conn.execute(
            """
            CREATE TABLE sample (
                id INTEGER,
                amount FLOAT,
                wrong_type VARCHAR(20),
                mixed INTEGER,
                report_date DATE
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO sample
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                (1, 1.5, "one", 1, "2020-01-01"),
                (2, 2.5, "two", "not an int", "not a date"),
            ],
        )

    datapackage_path = tmp_path / "ferc_test_datapackage.json"
    datapackage_path.write_text(
        json.dumps(
            {
                "resources": [
                    {
                        "name": "sample",
                        "schema": {
                            "fields": [
                                {"name": "id", "type": "integer"},
                                {"name": "amount", "type": "number"},
                                {"name": "wrong_type", "type": "number"},
                                {"name": "mixed", "type": "integer"},
                                {"name": "report_date", "type": "date"},
                            ]
                        },
                    }
                ]
            }
        )
    )
    return sqlite_path, datapackage_path


def test_audit_dbf_datapackage_types_flags_mismatches(tmp_path):
    """The audit reports declared, datapackage, and observed storage types."""
    sqlite_path, datapackage_path = _make_dbf_audit_inputs(tmp_path)

    audit = audit_dbf_datapackage_types(sqlite_path, datapackage_path)
    rows = audit.set_index("column").to_dict("index")

    assert rows["amount"]["datapackage_type"] == "number"
    assert rows["amount"]["sqlite_declared_type"] == "FLOAT"
    assert rows["amount"]["storage_type_counts"] == "real:2"
    assert not rows["amount"]["mismatch"]

    assert rows["wrong_type"]["normalized_sqlite_type"] == "string"
    assert rows["wrong_type"]["type_mismatch"]
    assert rows["wrong_type"]["mismatch"]

    assert rows["mixed"]["storage_type_counts"] == "integer:1,text:1"
    assert rows["mixed"]["mixed_storage_types"]
    assert rows["mixed"]["mismatch"]

    assert rows["report_date"]["unparseable_date_values"] == 1
    assert rows["report_date"]["mismatch"]


def test_audit_dbf_datapackage_types_cli_outputs_csv(tmp_path):
    """The CLI emits the audit rows with expected user-facing columns."""
    sqlite_path, datapackage_path = _make_dbf_audit_inputs(tmp_path)

    result = CliRunner().invoke(main, [str(sqlite_path), str(datapackage_path)])

    assert result.exit_code == 0
    lines = result.output.splitlines()
    assert lines[0].split(",") == [
        "table",
        "column",
        "datapackage_type",
        "sqlite_declared_type",
        "normalized_sqlite_type",
        "storage_type_counts",
        "mixed_storage_types",
        "type_mismatch",
        "unparseable_date_values",
        "mismatch",
    ]
    assert "sample,amount,number,FLOAT,number,real:2,False,False,0,False" in lines
