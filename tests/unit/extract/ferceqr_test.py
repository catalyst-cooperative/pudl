"""Unit tests for pudl.extract.ferceqr."""

from pathlib import Path

from pudl.extract.ferceqr import (
    _csvs_to_parquet,
    _extract_other_table,
)


def _touch(path: Path) -> Path:
    path.write_text("")
    return path


def _warning_messages(mock_logger) -> str:
    """Join all logger.warning() call messages into one string for substring checks.

    Asserting on the mocked logger directly rather than pytest's ``caplog``
    fixture: caplog's capture of this project's loggers is unreliable when run
    as part of the full suite (see the ``# TODO`` in
    ``tests/unit/dagster/provenance_test.py`` -- a known, pre-existing issue,
    not specific to these tests).
    """
    return " ".join(str(call.args[0]) for call in mock_logger.warning.call_args_list)


def test_extract_other_table_uses_null_company_identifier_when_cid_is_none(mocker):
    """cid=None should produce a real SQL NULL, not the literal string "None"."""
    duckdb_connection = mocker.MagicMock()
    mocker.patch("pudl.extract.ferceqr.persist_table_as_parquet")

    _extract_other_table(
        table_type="contracts",
        csv_path="some.csv",
        year_quarter="2024q1",
        cid=None,
        filing_name="filing",
        duckdb_connection=duckdb_connection,
    )

    select_arg = duckdb_connection.read_csv.return_value.select.call_args[0][0]
    assert "NULL as company_identifier" in select_arg
    assert "None" not in select_arg


def test_extract_other_table_quotes_cid_when_present(mocker):
    """A real cid should be quoted as a SQL string literal."""
    duckdb_connection = mocker.MagicMock()
    mocker.patch("pudl.extract.ferceqr.persist_table_as_parquet")

    _extract_other_table(
        table_type="contracts",
        csv_path="some.csv",
        year_quarter="2024q1",
        cid="C012345",
        filing_name="filing",
        duckdb_connection=duckdb_connection,
    )

    select_arg = duckdb_connection.read_csv.return_value.select.call_args[0][0]
    assert "'C012345' as company_identifier" in select_arg


def test_csvs_to_parquet_processes_remaining_tables_when_ident_missing(
    tmp_path, mocker
):
    """A filing missing its identity CSV should still extract the other tables.

    Regression test: this filing shape (contracts + transactions, but no ident)
    is real -- see ferceqr-2024q1.zip's filing CSV_2024_Q1_6529361_1746231.ZIP,
    a Public Service Company of New Mexico filing that only included contracts
    and transactions CSVs. This used to crash extraction for the entire quarter
    with ``ValueError: not enough values to unpack (expected 1, got 0)`` from the
    old ``[ident_path] = [...]`` unpacking assignment, and an intermediate fix
    skipped the whole filing outright -- neither drops the still-useful
    contracts/transactions data on the floor.
    """
    _touch(tmp_path / "202403_Some_Company_contracts.CSV")
    _touch(tmp_path / "202403_Some_Company_transactions.CSV")
    extract_ident = mocker.patch("pudl.extract.ferceqr._extract_ident")
    extract_other = mocker.patch("pudl.extract.ferceqr._extract_other_table")
    mock_logger = mocker.patch("pudl.extract.ferceqr.logger")

    _csvs_to_parquet(
        csv_path=tmp_path,
        year_quarter="2024q1",
        filing_name="CSV_2024_Q1_6529361_1746231",
        duckdb_connection=mocker.MagicMock(),
    )

    extract_ident.assert_not_called()
    assert extract_other.call_count == 2
    table_types = {call.kwargs["table_type"] for call in extract_other.call_args_list}
    assert table_types == {"contracts", "transactions"}
    assert all(call.kwargs["cid"] is None for call in extract_other.call_args_list)

    messages = _warning_messages(mock_logger)
    assert "no identity CSV" in messages
    assert "contracts.CSV" in messages
    assert "transactions.CSV" in messages


def test_csvs_to_parquet_processes_remaining_tables_when_ident_unparseable(
    tmp_path, mocker
):
    """A filing whose identity CSV fails to parse should still extract the rest."""
    _touch(tmp_path / "202403_Some_Company_ident.CSV")
    _touch(tmp_path / "202403_Some_Company_contracts.CSV")
    mocker.patch("pudl.extract.ferceqr._extract_ident", side_effect=TypeError)
    extract_other = mocker.patch("pudl.extract.ferceqr._extract_other_table")
    mock_logger = mocker.patch("pudl.extract.ferceqr.logger")

    _csvs_to_parquet(
        csv_path=tmp_path,
        year_quarter="2024q1",
        filing_name="CSV_2024_Q1_test",
        duckdb_connection=mocker.MagicMock(),
    )

    extract_other.assert_called_once()
    assert extract_other.call_args.kwargs["table_type"] == "contracts"
    assert extract_other.call_args.kwargs["cid"] is None
    assert "processing remaining tables with a null company_identifier" in (
        _warning_messages(mock_logger)
    )


def test_csvs_to_parquet_skips_unrecognized_csv(tmp_path, mocker):
    """A CSV that doesn't match any known table type is skipped, not fatal.

    Regression test for the same class of bug as the missing-ident case above,
    but for the second, symmetrical ``[table_type] = [...]`` unpacking
    assignment in the per-file loop.
    """
    _touch(tmp_path / "202403_Some_Company_ident.CSV")
    _touch(tmp_path / "202403_Some_Company_mystery.CSV")
    mocker.patch("pudl.extract.ferceqr._extract_ident", return_value="12345")
    extract_other = mocker.patch("pudl.extract.ferceqr._extract_other_table")
    mock_logger = mocker.patch("pudl.extract.ferceqr.logger")

    _csvs_to_parquet(
        csv_path=tmp_path,
        year_quarter="2024q1",
        filing_name="CSV_2024_Q1_test",
        duckdb_connection=mocker.MagicMock(),
    )

    extract_other.assert_not_called()
    messages = _warning_messages(mock_logger)
    assert "unrecognized CSV" in messages
    assert "mystery.CSV" in messages


def test_csvs_to_parquet_warns_on_multiple_ident_matches(tmp_path, mocker):
    """Multiple identity-CSV matches log a warning and use the first, not crash."""
    _touch(tmp_path / "202403_Some_Company_ident.CSV")
    _touch(tmp_path / "202403_Some_Company_old_ident.CSV")
    mocker.patch("pudl.extract.ferceqr._extract_ident", return_value="12345")
    mock_logger = mocker.patch("pudl.extract.ferceqr.logger")

    _csvs_to_parquet(
        csv_path=tmp_path,
        year_quarter="2024q1",
        filing_name="CSV_2024_Q1_test",
        duckdb_connection=mocker.MagicMock(),
    )

    assert "2 identity CSVs, expected 1" in _warning_messages(mock_logger)
