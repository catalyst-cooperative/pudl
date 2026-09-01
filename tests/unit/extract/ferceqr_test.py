"""Unit tests for pudl.extract.ferceqr."""

import io
import zipfile
from pathlib import Path

import dagster as dg
import duckdb
import pytest

from pudl.dagster.resources import FercEqrArchiveResource
from pudl.extract.ferceqr import (
    _clean_csv_name,
    _clear_raw_table_partition,
    _csvs_to_parquet,
    _extract_other_table,
    _get_rejected_record_counts,
    extract_ferceqr,
)
from pudl.helpers import ParquetData


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


@pytest.mark.parametrize(
    ("dirty_name", "expected_clean_name"),
    [
        ("normal_file.csv", "normal_file.csv"),
        ("file's_ident.CSV", "files_ident.CSV"),
        ('file"with"doublequote.CSV', "filewithdoublequote.CSV"),
        ("file*with*star.CSV", "filewithstar.CSV"),
        ("file?with?question.CSV", "filewithquestion.CSV"),
        ("file[with]brackets.CSV", "filewithbrackets.CSV"),
        ("Palmer's_[weird]*file?.CSV", "Palmers_weirdfile.CSV"),
        # Real characters observed in a scan of all 52 locally cached FERC EQR
        # quarters (2013q3-2026q2) -- parens, '#', and mangled accented letters --
        # none of which are unsafe here, so they're left untouched.
        ("Just_Energy_(U.S.)_Corp._ident.CSV", "Just_Energy_(U.S.)_Corp._ident.CSV"),
        ("Nevada_Cogeneration_#1_ident.CSV", "Nevada_Cogeneration_#1_ident.CSV"),
        ("Double_ôCö_Limited_ident.CSV", "Double_ôCö_Limited_ident.CSV"),
    ],
    ids=[
        "already-clean",
        "apostrophe",
        "double-quote",
        "star",
        "question-mark",
        "brackets",
        "multiple-unsafe-chars",
        "parens-left-alone",
        "hash-left-alone",
        "accented-letters-left-alone",
    ],
)
def test_clean_csv_name_strips_only_unsafe_characters(
    tmp_path, dirty_name, expected_clean_name
):
    """Quote and glob-metacharacters are stripped; everything else survives untouched."""
    dirty_path = _touch(tmp_path / dirty_name)

    cleaned_path = _clean_csv_name(dirty_path)

    assert cleaned_path.name == expected_clean_name
    assert cleaned_path.exists()


def test_clean_csv_name_does_not_rename_an_already_clean_file(tmp_path, mocker):
    """A filename with nothing to strip is returned as-is, with no filesystem rename.

    Guards against a regression that would call ``.rename()`` unconditionally --
    harmless for the file itself, but wasteful and a hint the no-op check broke.
    """
    clean_path = _touch(tmp_path / "already_clean_ident.CSV")
    rename_spy = mocker.patch.object(Path, "rename")

    result = _clean_csv_name(clean_path)

    assert result == clean_path
    rename_spy.assert_not_called()


def test_clean_csv_name_prevents_duckdb_glob_misdirection(tmp_path):
    """Without cleaning, a bracket-containing filename gets duckdb to read the wrong file.

    duckdb's ``read_csv`` interprets ``[``/``]`` as a glob character class even when
    given a single literal path string (not a SQL string) -- ``[ab].csv`` matches
    ``a.csv`` as a *pattern*, so if a sibling ``a.csv`` happens to exist, duckdb
    silently reads its contents instead, with no error at all. This reproduces that
    failure directly against a real duckdb connection, then confirms
    ``_clean_csv_name`` closes it.
    """
    _touch(tmp_path / "a.csv").write_text("company_identifier\nWRONG\n")
    bracket_path = tmp_path / "[ab].csv"
    bracket_path.write_text("company_identifier\nCORRECT\n")

    conn = duckdb.connect()

    # Uncleaned: duckdb's glob expansion silently reads the wrong file.
    misdirected = conn.read_csv(
        str(bracket_path), all_varchar=True, store_rejects=True, ignore_errors=True
    )
    assert misdirected.select("company_identifier").fetchall() == [("WRONG",)]

    # Cleaned: the literal, intended file is read.
    cleaned_path = _clean_csv_name(bracket_path)
    fixed = conn.read_csv(
        str(cleaned_path), all_varchar=True, store_rejects=True, ignore_errors=True
    )
    assert fixed.select("company_identifier").fetchall() == [("CORRECT",)]


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

    found_table_types = _csvs_to_parquet(
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
    assert found_table_types == {"contracts", "transactions"}

    messages = _warning_messages(mock_logger)
    assert "no identity CSV" in messages
    assert "contracts.CSV" in messages
    assert "transactions.CSV" in messages


def test_csvs_to_parquet_processes_remaining_tables_when_ident_has_no_rows(
    tmp_path, mocker
):
    """A filing whose identity CSV parses but has no rows should still extract the rest.

    ``_extract_ident`` returns ``None`` (rather than raising) when the CSV parses
    cleanly but has zero data rows -- a genuinely unparseable CSV instead raises
    ``duckdb.Error``, covered separately below.
    """
    _touch(tmp_path / "202403_Some_Company_ident.CSV")
    _touch(tmp_path / "202403_Some_Company_contracts.CSV")
    mocker.patch("pudl.extract.ferceqr._extract_ident", return_value=None)
    extract_other = mocker.patch("pudl.extract.ferceqr._extract_other_table")
    mock_logger = mocker.patch("pudl.extract.ferceqr.logger")

    found_table_types = _csvs_to_parquet(
        csv_path=tmp_path,
        year_quarter="2024q1",
        filing_name="CSV_2024_Q1_test",
        duckdb_connection=mocker.MagicMock(),
    )

    extract_other.assert_called_once()
    assert extract_other.call_args.kwargs["table_type"] == "contracts"
    assert extract_other.call_args.kwargs["cid"] is None
    assert "contains no rows" in _warning_messages(mock_logger)
    # The ident CSV was present (even though it had no rows), so it still
    # counts as "found".
    assert found_table_types == {"ident", "contracts"}


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

    found_table_types = _csvs_to_parquet(
        csv_path=tmp_path,
        year_quarter="2024q1",
        filing_name="CSV_2024_Q1_test",
        duckdb_connection=mocker.MagicMock(),
    )

    extract_other.assert_not_called()
    messages = _warning_messages(mock_logger)
    assert "unrecognized CSV" in messages
    assert "mystery.CSV" in messages
    assert found_table_types == {"ident"}


def test_csvs_to_parquet_processes_remaining_tables_when_one_other_table_missing(
    tmp_path, mocker
):
    """A filing missing one of contracts/transactions/indexPub still processes the rest.

    Generalizes the missing-ident handling: any subset of the expected CSVs
    being present and parsable should be enough to extract that subset. Unlike
    a missing ident CSV, a missing contracts/transactions/indexPub CSV is
    routine -- thousands of filings in a real quarter lack one or more of
    these -- so it's reflected only in the return value, not a per-filing
    warning (that used to log "missing the expected contracts CSV", which
    would flood the logs at that volume).
    """
    _touch(tmp_path / "202403_Some_Company_ident.CSV")
    _touch(tmp_path / "202403_Some_Company_transactions.CSV")
    _touch(tmp_path / "202403_Some_Company_indexPub.CSV")
    mocker.patch("pudl.extract.ferceqr._extract_ident", return_value="12345")
    extract_other = mocker.patch("pudl.extract.ferceqr._extract_other_table")
    mock_logger = mocker.patch("pudl.extract.ferceqr.logger")

    found_table_types = _csvs_to_parquet(
        csv_path=tmp_path,
        year_quarter="2024q1",
        filing_name="CSV_2024_Q1_test",
        duckdb_connection=mocker.MagicMock(),
    )

    assert extract_other.call_count == 2
    table_types = {call.kwargs["table_type"] for call in extract_other.call_args_list}
    assert table_types == {"transactions", "indexPub"}
    assert all(call.kwargs["cid"] == "12345" for call in extract_other.call_args_list)
    assert found_table_types == {"ident", "transactions", "indexPub"}
    mock_logger.warning.assert_not_called()


@pytest.mark.parametrize(
    ("files", "expected_match"),
    [
        (
            ["202403_Some_Company_ident.CSV", "202403_Some_Company_old_ident.CSV"],
            "2 identity CSVs, expected at most 1",
        ),
        (
            [
                "202403_Some_Company_ident.CSV",
                "202403_Some_Company_contracts.CSV",
                "202403_Some_Company_old_contracts.CSV",
            ],
            "2 contracts CSVs, expected at most 1",
        ),
    ],
    ids=["ident", "other-table-type"],
)
def test_csvs_to_parquet_raises_on_multiple_matches(
    tmp_path, mocker, files, expected_match
):
    """Two CSVs matching the same table type raise rather than silently picking one.

    Covers both the ident branch and the (structurally separate) non-ident branch of
    ``_csvs_to_parquet`` -- this filing shape has never been observed in the wild for
    either, so there's no established handling strategy -- a warning would be easy to
    miss among the routine ones logged for expected cases, so it raises instead.
    """
    for name in files:
        _touch(tmp_path / name)
    mocker.patch("pudl.extract.ferceqr._extract_ident", return_value="12345")
    mocker.patch("pudl.extract.ferceqr._extract_other_table")

    with pytest.raises(ValueError, match=expected_match):
        _csvs_to_parquet(
            csv_path=tmp_path,
            year_quarter="2024q1",
            filing_name="CSV_2024_Q1_test",
            duckdb_connection=mocker.MagicMock(),
        )


def test_csvs_to_parquet_skips_other_table_that_fails_to_parse(tmp_path, mocker):
    """A present but corrupt/unparsable non-ident CSV is skipped, not fatal.

    Simulates a file that duckdb can't parse at all (e.g. due to quoting
    issues or corruption) rather than one with merely malformed rows, which
    ``ignore_errors=True`` already tolerates at the row level.
    """
    _touch(tmp_path / "202403_Some_Company_ident.CSV")
    _touch(tmp_path / "202403_Some_Company_contracts.CSV")
    _touch(tmp_path / "202403_Some_Company_transactions.CSV")
    mocker.patch("pudl.extract.ferceqr._extract_ident", return_value="12345")
    mocker.patch(
        "pudl.extract.ferceqr._extract_other_table",
        side_effect=duckdb.Error("malformed CSV"),
    )
    mock_logger = mocker.patch("pudl.extract.ferceqr.logger")

    found_table_types = _csvs_to_parquet(
        csv_path=tmp_path,
        year_quarter="2024q1",
        filing_name="CSV_2024_Q1_test",
        duckdb_connection=mocker.MagicMock(),
    )

    messages = _warning_messages(mock_logger)
    assert "Failed to parse contracts table" in messages
    assert "Failed to parse transactions table" in messages
    # Both CSVs were present (even though parsing failed), so both count as found.
    assert found_table_types == {"ident", "contracts", "transactions"}


def test_csvs_to_parquet_processes_remaining_tables_when_ident_fails_with_duckdb_error(
    tmp_path, mocker
):
    """A duckdb.Error parsing ident degrades gracefully."""
    _touch(tmp_path / "202403_Some_Company_ident.CSV")
    _touch(tmp_path / "202403_Some_Company_contracts.CSV")
    mocker.patch(
        "pudl.extract.ferceqr._extract_ident",
        side_effect=duckdb.Error("no company_identifier column"),
    )
    extract_other = mocker.patch("pudl.extract.ferceqr._extract_other_table")
    mock_logger = mocker.patch("pudl.extract.ferceqr.logger")

    found_table_types = _csvs_to_parquet(
        csv_path=tmp_path,
        year_quarter="2024q1",
        filing_name="CSV_2024_Q1_test",
        duckdb_connection=mocker.MagicMock(),
    )

    assert extract_other.call_args.kwargs["cid"] is None
    assert "Failed to parse ident table" in _warning_messages(mock_logger)
    assert found_table_types == {"ident", "contracts"}


def test_csvs_to_parquet_does_not_warn_when_all_other_tables_missing(tmp_path, mocker):
    """A filing with only an ident CSV logs no warning at all.

    Real quarters contain many thousands of filings that only report a subset
    of contracts/transactions/indexPub (or none of them) -- this must stay
    silent, not log per filing, or the logs would be flooded.
    """
    _touch(tmp_path / "202403_Some_Company_ident.CSV")
    mocker.patch("pudl.extract.ferceqr._extract_ident", return_value="12345")
    mock_logger = mocker.patch("pudl.extract.ferceqr.logger")

    found_table_types = _csvs_to_parquet(
        csv_path=tmp_path,
        year_quarter="2024q1",
        filing_name="CSV_2024_Q1_test",
        duckdb_connection=mocker.MagicMock(),
    )

    mock_logger.warning.assert_not_called()
    assert found_table_types == {"ident"}


def test_get_rejected_record_counts(tmp_path):
    """Rejected records are counted by DuckDB's error_type, using a real connection.

    A MagicMock can't validate real DuckDB SQL or the ``error_type`` enum's
    actual values, so this uses a real in-memory connection and a CSV with two
    genuinely malformed rows (wrong column count and invalid UTF-8).
    """
    csv_path = tmp_path / "malformed.csv"
    csv_path.write_bytes(b"a,b,c\n1,2,3\n1,2\n" + bytes([0xFF, 0xFE]) + b",2,3\n")
    conn = duckdb.connect()
    conn.read_csv(
        str(csv_path), all_varchar=True, store_rejects=True, ignore_errors=True
    ).execute()

    counts = _get_rejected_record_counts(conn)

    assert counts == {"MISSING COLUMNS": 1, "INVALID ENCODING": 1}


def _make_filing_zip(files: dict[str, str]) -> bytes:
    """Build an in-memory zip file containing the given {filename: text content}."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buf.getvalue()


def test_extract_ferceqr_collects_and_attaches_summary_stats(tmp_path):
    """extract_ferceqr tallies filings, table presence, corrupt zips, and rejects.

    Uses a real archive directory, a real DuckDB connection, and a real Dagster
    asset context (rather than mocks) since the thing under test is precisely
    the aggregation across multiple real filings and the attachment of
    real Dagster output metadata -- both hard to fake convincingly with mocks.
    """
    year_quarter = "2024q1"
    archive_dir = tmp_path / "archive"
    archive_dir.mkdir()
    with zipfile.ZipFile(archive_dir / f"ferceqr-{year_quarter}.zip", "w") as outer:
        # Well-formed filing with ident + contracts.
        outer.writestr(
            "filing1.zip",
            _make_filing_zip(
                {
                    "ident.CSV": "company_identifier\nC001\n",
                    "contracts.CSV": "a,b\n1,2\n",
                }
            ),
        )
        # Routine filing with only transactions -- no ident, should not warn.
        outer.writestr(
            "filing2.zip",
            _make_filing_zip({"transactions.CSV": "a,b\n1,2\n"}),
        )
        # Filing whose ident CSV has one malformed row (wrong column count).
        outer.writestr(
            "filing3.zip",
            _make_filing_zip({"ident.CSV": "company_identifier\nC002\nC003,extra\n"}),
        )
        # Corrupt filing archive (not a real zip).
        outer.writestr("filing4.zip", b"not a real zip")

    with dg.build_asset_context(partition_key=year_quarter) as context:
        extract_ferceqr(
            context=context,
            ferceqr_archive=FercEqrArchiveResource(path=str(archive_dir)),
        )
        stats = context.get_output_metadata("raw_ferceqr__extract_errors")[
            "extraction_stats"
        ].data

    assert stats["total_filings"] == 4
    assert stats["corrupt_filings"] == 1
    assert stats["table_file_counts"] == {
        "ident": 2,
        "contracts": 1,
        "transactions": 1,
        "indexPub": 0,
    }
    assert stats["rejected_record_counts"] == {"TOO MANY COLUMNS": 1}


def test_clear_raw_table_partition_removes_existing_output():
    """Clearing a raw table+quarter partition deletes any files already there.

    Simulates a stale per-filing parquet file left behind by a previous
    extraction run under a filing ID that no longer exists in the current
    archive (e.g. because the filing was since amended and re-filed under a
    new ID) -- nothing else would ever clean this up on its own.
    """
    directory = ParquetData(table_name="raw_ferceqr__ident_2024q1").parquet_directory
    stale_file = directory / "CSV_2024_Q1_0000000_0000000.parquet"
    stale_file.write_text("stale leftover from a previous run")
    assert stale_file.exists()

    _clear_raw_table_partition("ident", "2024q1")

    assert not stale_file.exists()


def test_clear_raw_table_partition_is_a_noop_when_nothing_exists():
    """Clearing a partition that was never written to should not raise."""
    _clear_raw_table_partition("contracts", "2099q1")


def test_extract_ferceqr_removes_stale_output_from_a_previous_run(tmp_path):
    """A leftover file from a filing ID no longer in the archive doesn't survive.

    Regression test for the raw-output accumulation bug: without clearing each
    table+quarter's output directory before extraction, a company's filing
    from a prior archive pull would stick around under its old filing ID even
    after being superseded by a resubmission under a new ID, silently
    duplicating that company's records once both are read back together.
    """
    year_quarter = "2024q1"
    archive_dir = tmp_path / "archive"
    archive_dir.mkdir()
    with zipfile.ZipFile(archive_dir / f"ferceqr-{year_quarter}.zip", "w") as outer:
        outer.writestr(
            "CSV_2024_Q1_1111111_1111111.zip",
            _make_filing_zip({"ident.CSV": "company_identifier\nC001\n"}),
        )

    # Simulate leftover output from a previous run under a filing ID that is
    # no longer present in the archive above (e.g. company C001 resubmitted).
    stale_directory = ParquetData(
        table_name="raw_ferceqr__ident_2024q1"
    ).parquet_directory
    stale_file = stale_directory / "CSV_2024_Q1_0000000_0000000.parquet"
    stale_file.write_text("stale leftover from a previous run")

    with dg.build_asset_context(partition_key=year_quarter) as context:
        extract_ferceqr(
            context=context,
            ferceqr_archive=FercEqrArchiveResource(path=str(archive_dir)),
        )

    assert not stale_file.exists()
    assert (stale_directory / "CSV_2024_Q1_1111111_1111111.parquet").exists()
