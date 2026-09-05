"""Tests for metadata not covered elsewhere."""

import json
import re
from datetime import date
from typing import Any

import duckdb.sqltypes
import frictionless
import geopandas as gpd  # noqa: ICN002
import pandas as pd
import pandera.pandas as pr_pandas
import pandera.polars as pr_polars
import polars as pl
import pyarrow as pa
import pydantic
import pytest
import sqlalchemy as sa
from pandera.errors import SchemaErrors
from shapely import Point

from pudl.metadata.classes import (
    PUDL_PACKAGE,
    DataSource,
    Field,
    FieldConstraints,
    Package,
    PudlResourceDescriptor,
    Resource,
)
from pudl.metadata.descriptions import (
    PARTITION_OFFSETS,
    ResourceDescriptionBuilder,
    ResourceTrait,
)
from pudl.metadata.dtypes import (
    PudlDtypeBackend,
    apply_pudl_dtypes,
    apply_pudl_dtypes_polars,
    get_pudl_dtypes,
)
from pudl.metadata.fields import (
    FIELD_METADATA,
)
from pudl.metadata.helpers import format_errors
from pudl.metadata.resource_helpers import merge_descriptions
from pudl.metadata.resources import RESOURCE_METADATA
from pudl.metadata.sources import SOURCES
from pudl.metadata.units import PUDL_UNIT_REGISTRY

PUDL_RESOURCES = {r.name: r for r in PUDL_PACKAGE.resources}
PUDL_ENCODERS = PUDL_PACKAGE.encoders


def test_all_resources_valid() -> None:
    """All resources in metadata pass validation tests."""
    _ = PUDL_PACKAGE


def test_all_data_sources_valid() -> None:
    """All stored DataSource definitions are valid."""
    failures = []
    for src in SOURCES:
        try:
            DataSource.from_id(src)
        except Exception as exc:
            failures.append(f"  {src}: {exc}")
    if failures:
        raise AssertionError(
            f"{len(failures)} data source(s) are invalid:\n" + "\n".join(failures)
        )


def test_all_excluded_resources_exist() -> None:
    """All resources excluded from foreign key rules exist."""
    errors = []
    for name, meta in RESOURCE_METADATA.items():
        rule = meta.get("schema", {}).get("foreign_key_rules")
        if rule:
            missing = [x for x in rule.get("exclude", []) if x not in RESOURCE_METADATA]
            if missing:
                errors.append(f"{name}: {missing}")
    if errors:
        raise ValueError(
            format_errors(
                *errors, title="Invalid resources in foreign_key_rules.exclude"
            )
        )


def test_get_etl_group_tables() -> None:
    """Test that a Value error is raised for non existent etl group."""
    with pytest.raises(ValueError):
        Package.get_etl_group_tables("not_an_etl_group")


def test_pyarrow_schemas() -> None:
    """All defined Resources can produce pyarrow schemas."""
    failures = []
    for resource_name in sorted(PUDL_RESOURCES.keys()):
        try:
            PUDL_RESOURCES[resource_name].to_pyarrow()
        except Exception as exc:
            failures.append(f"  {resource_name}: {exc}")
    if failures:
        raise AssertionError(
            f"{len(failures)} resource(s) failed to produce pyarrow schemas:\n"
            + "\n".join(failures)
        )


@pytest.mark.parametrize(
    ("value", "expected_valid"),
    [
        ("2019-04-23 10:00:00.000000", True),  # whole seconds, correctly formatted
        ("2019-04-23 10:00:00.123456", True),  # real microsecond precision
        ("2019-13-45 25:99:99.123456", False),  # correctly formatted, but not a real
        # calendar date/time (month 13, day 45, hour 25, ...)
        ("2019-04-23 10:00:00.12", False),  # fractional part is the wrong width
        ("2019-04-23 10:00:00", False),  # missing fractional part entirely
        ("not-a-real-datetime", False),  # not a datetime string at all
    ],
)
def test_datetime_field_sql_check_constraint_accepts_real_values(
    value: str, expected_valid: bool
) -> None:
    """A ``datetime``-typed field's own CHECK constraint must accept real values.

    Regression test: ``Field.to_sql()`` validates ``datetime`` columns with a
    CHECK constraint that has to accommodate PUDL's microsecond-precision
    datetime strings (matching every other backend) while still rejecting
    malformed values. This can't be done with a plain
    ``col IS DATETIME(col)`` check, since SQLite's own ``DATETIME()``
    function only round-trips whole-second precision (even its ``'subsec'``
    modifier only adds milliseconds, never microseconds) -- so a naive fix
    either breaks on any real value (the bug this guards against) or silently
    truncates precision. This is something only an actual insert against a
    real SQLite connection can catch; a bad CHECK expression is otherwise
    perfectly valid Python/SQL and passes every other unit test.

    Each ``value`` is inserted as a literal string via raw SQL, bypassing
    SQLAlchemy's Python-side type coercion (which would reject a non-datetime
    Python object before it ever reaches SQLite), so this exercises the CHECK
    constraint itself rather than the ``bind_processor``.
    """
    field = Field(name="some_datetime", type="datetime", description="A timestamp.")
    metadata = sa.MetaData()
    sa.Table("t", metadata, field.to_sql())
    engine = sa.create_engine("sqlite:///:memory:")
    try:
        metadata.create_all(engine)

        insert = sa.text("INSERT INTO t (some_datetime) VALUES (:value)")
        if expected_valid:
            with engine.begin() as conn:
                conn.execute(insert, {"value": value})
        else:
            with (
                engine.begin() as conn,
                pytest.raises(sa.exc.IntegrityError, match="CHECK constraint failed"),
            ):
                conn.execute(insert, {"value": value})
    finally:
        engine.dispose()


def test_field_to_sql_duckdb_pattern_uses_regexp_full_match() -> None:
    """DuckDB has no bare REGEXP keyword; Field.to_sql() must emit regexp_full_match().

    Regression test: unlike SQLite, DuckDB's parser rejects ``col REGEXP pattern``
    outright (a ``Parser Error``, not a runtime failure), so this has to be caught
    at CHECK-constraint-construction time, not just when a bad value is inserted.
    """
    field = Field(
        name="code",
        type="string",
        description="A code.",
        constraints=FieldConstraints(pattern="^[A-Z]{2}$"),
    )
    metadata = sa.MetaData()
    sa.Table("t", metadata, field.to_sql(dialect="duckdb"))
    engine = sa.create_engine("duckdb:///:memory:")
    try:
        metadata.create_all(engine)  # would raise a Parser Error for bare REGEXP
        with engine.begin() as conn:
            conn.execute(sa.text("INSERT INTO t (code) VALUES ('AB')"))
            with pytest.raises(sa.exc.DBAPIError, match="CHECK constraint failed"):
                conn.execute(sa.text("INSERT INTO t (code) VALUES ('abc')"))
    finally:
        engine.dispose()


def test_field_to_sql_duckdb_integer_primary_key_has_no_autoincrement() -> None:
    """An integer primary key should not become SERIAL under the duckdb dialect.

    Regression test: SQLAlchemy's postgres-derived DDL compiler (which
    duckdb-engine's dialect is built on) upgrades an Integer primary-key column to
    SERIAL unless autoincrement is explicitly disabled -- DuckDB has no SERIAL
    keyword, so this would otherwise break schema creation for every table with a
    single-column integer primary key.
    """
    resource = Resource(
        name="widgets",
        schema={
            "fields": [{"name": "id", "type": "integer", "description": "id"}],
            "primary_key": ["id"],
        },
        description="Widgets",
    )
    metadata = sa.MetaData()
    resource.to_sql(metadata, dialect="duckdb")
    engine = sa.create_engine("duckdb:///:memory:")
    try:
        metadata.create_all(engine)  # would raise if SERIAL were emitted
    finally:
        engine.dispose()


def test_resource_to_sql_duckdb_excludes_foreign_keys_when_requested() -> None:
    """include_foreign_keys=False should omit FK constraints under any dialect."""
    parent = Resource(
        name="parent",
        schema={
            "fields": [{"name": "id", "type": "integer", "description": "id"}],
            "primary_key": ["id"],
        },
        description="Parent",
    )
    child = Resource(
        name="child",
        schema={
            "fields": [
                {"name": "id", "type": "integer", "description": "id"},
                {"name": "parent_id", "type": "integer", "description": "parent_id"},
            ],
            "primary_key": ["id"],
            "foreign_keys": [
                {
                    "fields": ["parent_id"],
                    "reference": {"resource": "parent", "fields": ["id"]},
                }
            ],
        },
        description="Child",
    )

    metadata = sa.MetaData()
    parent.to_sql(metadata, dialect="duckdb", include_foreign_keys=False)
    table = child.to_sql(metadata, dialect="duckdb", include_foreign_keys=False)
    assert list(table.foreign_keys) == []

    metadata_with_fk = sa.MetaData()
    parent.to_sql(metadata_with_fk, dialect="duckdb")
    table_with_fk = child.to_sql(metadata_with_fk, dialect="duckdb")
    assert list(table_with_fk.foreign_keys) != []


def test_package_rejects_foreign_key_type_mismatch() -> None:
    """A FK column and its referenced PK column must declare the same type.

    Regression test for PUDL issue #5552: a dozen FK relationships had an integer
    child column referencing a string parent column (a "code" column storing
    integers-as-strings), which DuckDB can't join without a type coercion. Package
    construction should catch this kind of mismatch instead of letting it surface
    only when a database schema is actually built.
    """
    parent = Resource(
        name="parent",
        schema={
            "fields": [{"name": "code", "type": "string", "description": "code"}],
            "primary_key": ["code"],
        },
        description="Parent",
    )
    child = Resource(
        name="child",
        schema={
            "fields": [
                {"name": "id", "type": "integer", "description": "id"},
                {"name": "code", "type": "integer", "description": "code"},
            ],
            "primary_key": ["id"],
            "foreign_keys": [
                {
                    "fields": ["code"],
                    "reference": {"resource": "parent", "fields": ["code"]},
                }
            ],
        },
        description="Child",
    )

    with pytest.raises(pydantic.ValidationError, match="incompatible types"):
        Package(name="ab", resources=[parent, child])


def test_field_to_sql_duckdb_same_name_different_enum_values_get_distinct_types() -> (
    None
):
    """Two fields sharing a name but not their enum values must not share a type.

    Regression test: PUDL reuses field names like "plant_type" across many
    resources with resource-specific enum overrides (see
    FIELD_METADATA_BY_RESOURCE) that don't all share the same allowed values.
    Naming the DuckDB ENUM type after the field name alone caused whichever
    resource's column got created first to "win" -- every other table with the
    same field name but different values then failed to load, since its real
    values weren't members of the first table's enum type. Caught via a real
    Dagster pudl_duckdb run (e.g. "combined_cycle" is a valid "plant_type" value
    in some FERC1 resources but not in the RUS12 resources that also use that
    field name, whichever one happened to run first).
    """
    hydro_plant_type = Field(
        name="plant_type",
        type="string",
        description="Type of plant.",
        constraints=FieldConstraints(enum=["hydro", "storage"]),
    )
    fossil_plant_type = Field(
        name="plant_type",
        type="string",
        description="Type of plant.",
        constraints=FieldConstraints(enum=["combined_cycle", "steam"]),
    )

    metadata = sa.MetaData()
    sa.Table("hydro_plants", metadata, hydro_plant_type.to_sql(dialect="duckdb"))
    sa.Table("fossil_plants", metadata, fossil_plant_type.to_sql(dialect="duckdb"))

    engine = sa.create_engine("duckdb:///:memory:")
    try:
        metadata.create_all(engine)
        with engine.begin() as conn:
            conn.execute(sa.text("INSERT INTO hydro_plants VALUES ('hydro')"))
            # This is exactly the failure mode from the real bug: a value that's
            # valid for fossil_plants' enum, but not hydro_plants', would have
            # silently been checked against the wrong (first-created) type.
            conn.execute(sa.text("INSERT INTO fossil_plants VALUES ('combined_cycle')"))
            with pytest.raises(sa.exc.DBAPIError):
                conn.execute(sa.text("INSERT INTO hydro_plants VALUES ('steam')"))
    finally:
        engine.dispose()


def test_encoders() -> None:
    """All Encoders work on the kinds of values they're supposed to."""
    failures = []
    for encoder_name in sorted(PUDL_ENCODERS.keys()):
        try:
            encoder = PUDL_ENCODERS[encoder_name]
            test_data = encoder.generate_encodable_data(size=100)
            encoder.encode(test_data)
        except Exception as exc:
            failures.append(f"  {encoder_name}: {exc}")
    if failures:
        raise AssertionError(
            f"{len(failures)} encoder(s) failed:\n" + "\n".join(failures)
        )


def test_field_definitions() -> None:
    """All defined fields are valid."""
    failures = []
    for field_name in sorted(FIELD_METADATA.keys()):
        try:
            Field(name=field_name, **FIELD_METADATA[field_name])
        except Exception as exc:
            failures.append(f"  {field_name}: {exc}")
    if failures:
        raise AssertionError(
            f"{len(failures)} field(s) are invalid:\n" + "\n".join(failures)
        )


def test_enum_constraint_order_is_deterministic() -> None:
    """An enum constraint's value order must not depend on set-iteration order.

    Some enum constraints (e.g. ``EPACEMS_STATES`` in ``pudl.metadata.enums``) are
    built from a Python ``set``, whose iteration order depends on per-process hash
    randomization rather than the values themselves -- so ``list(some_set)`` can
    differ between two runs of the same code. ``FieldConstraints``' deterministic-
    sort validator makes the order a pure function of the values themselves,
    independent of the input list/set's construction order or process.
    """
    field = Field(
        name="_test_field",
        type="string",
        description="Test field.",
        constraints={"enum": {"z", "a", "m", "b"}},
    )
    assert field.constraints.enum == ["a", "b", "m", "z"]


@pytest.mark.parametrize(
    ("constraints", "expected"),
    [
        ({}, False),
        ({"required": True}, True),
        ({"unique": True}, True),
        ({"minimum": 0}, True),
        ({"maximum": 100}, True),
        ({"min_length": 1}, True),
        ({"max_length": 10}, True),
        ({"pattern": r"^[a-z]+$"}, True),
        ({"enum": ["a", "b"]}, True),
    ],
    ids=[
        "no_constraints",
        "required",
        "unique",
        "minimum",
        "maximum",
        "min_length",
        "max_length",
        "pattern",
        "enum",
    ],
)
def test_field_constraints_requires_content_validation(constraints, expected) -> None:
    """Each individual content constraint should independently flip the result.

    ``requires_content_validation`` compares each constraint field against its
    default; asserting only on combinations that set several at once wouldn't
    catch a future edit that broke the comparison for one particular field,
    since the others would still make the check pass. Each constraint is
    exercised alone, plus the all-defaults case, so every field is
    independently load-bearing.
    """
    assert FieldConstraints(**constraints).requires_content_validation() == expected


def test_field_constraints_requires_content_validation_covers_new_fields() -> None:
    """A newly added constraint field is automatically treated as content-requiring.

    ``requires_content_validation`` derives the set of fields it checks from
    ``FieldConstraints.model_fields`` rather than a hand-maintained list, so a
    constraint type added to ``FieldConstraints`` is picked up automatically
    instead of being silently ignored. This subclass stands in for that future
    field.
    """

    class _FieldConstraintsWithNewField(FieldConstraints):
        new_constraint: str | None = None

    defaults = _FieldConstraintsWithNewField()
    assert defaults.requires_content_validation() is False

    with_new_field_set = _FieldConstraintsWithNewField(new_constraint="x")
    assert with_new_field_set.requires_content_validation() is True


def _pk_violation_resource() -> Resource:
    return Resource(
        name="_test__check_primary_key",
        description="Synthetic resource for check_primary_key tests.",
        schema={
            "fields": [
                {"name": "id", "type": "integer", "description": "Primary key."}
            ],
            "primary_key": ["id"],
        },
    )


def _pandas_pk_violation_data() -> pd.DataFrame:
    """A single-column PK with both a duplicate (1, 1) and a null value."""
    return pd.DataFrame({"id": pd.array([1, 1, None], dtype="Int64")})


def _polars_pk_violation_data() -> pl.LazyFrame:
    """A single-column PK with both a duplicate (1, 1) and a null value."""
    return pl.LazyFrame({"id": [1, 1, None]})


@pytest.mark.parametrize(
    "make_data",
    [_pandas_pk_violation_data, _polars_pk_violation_data],
    ids=["pandas", "polars"],
)
def test_check_primary_key_reports_duplicates_and_nulls(make_data) -> None:
    """check_primary_key should report every violation type, for either backend.

    Regression test: the pandas path used to raise on the first problem it
    found (duplicates), so a caller fixing that would only discover the
    null-value problem on a second run.
    """
    errors = _pk_violation_resource().check_primary_key(make_data())
    messages = " ".join(str(error).lower() for error in errors)
    assert "duplicate" in messages
    assert "null" in messages


def test_enforce_schema_raises_combining_primary_key_violations() -> None:
    """enforce_schema is the caller that wants a hard failure on any PK violation.

    It combines every SchemaError ``check_primary_key`` returns into one
    ``ValueError`` rather than raising on the first, so both duplicate and null
    violations are visible in a single run instead of requiring a fix-and-rerun
    cycle to discover the second one.
    """
    resource = _pk_violation_resource()
    df = _pandas_pk_violation_data()
    with pytest.raises(ValueError, match=r"(?s)duplicate primary keys.*[Nn]ull") as exc:
        resource.enforce_schema(df)
    assert "duplicate primary keys" in str(exc.value)
    assert "null" in str(exc.value).lower()


@pytest.mark.parametrize(
    "make_data",
    [_pandas_pk_violation_data, _polars_pk_violation_data],
    ids=["pandas", "polars"],
)
def test_check_primary_key_errors_are_schema_errors_compatible(make_data) -> None:
    """Every backend's SchemaErrors must survive being wrapped in a real SchemaErrors.

    Regression test: pandera's backends unconditionally read attributes like
    ``schema``/``check_output`` off of a SchemaError the moment it's collected
    into a SchemaErrors, not just when displayed later (see
    ``failure_cases_metadata`` in ``pandera/backends/{pandas,polars}/base.py``).
    A manually-built duplicate-PK SchemaError once left both of those ``None``
    for the polars backend, crashing with an opaque ``AttributeError`` -- as
    first surfaced by a real duplicate-primary-key partition of
    ``core_ferceqr__quarterly_index_pub``. Constructing a real SchemaErrors
    here -- not just checking ``len(errors) > 0`` -- is what catches that; a
    bare list of errors is not enough, since the crash only happens once
    they're collected into a SchemaErrors.
    """
    data = make_data()
    errors = _pk_violation_resource().check_primary_key(data)
    schema_errors = SchemaErrors(
        schema=errors[0].schema, schema_errors=errors, data=data
    )
    message = str(schema_errors).lower()
    assert "duplicate" in message
    assert "null" in message


# ---------------------------------------------------------------------------
# Tests for chunked primary-key uniqueness checking (Resource.check_primary_key's
# polars path)
# ---------------------------------------------------------------------------
#
# The polars path never uses pandera's built-in composite-uniqueness check
# (too memory-hungry, see polars-comment.md); it always uses its own
# group-by/count-based `Resource._find_duplicate_primary_keys`, optionally run
# once per chunk instead of once on the whole table via the resource's declared
# `schema.pk_check_chunk_field` (which tables need this, if any, is decided
# per-resource -- see `pudl.metadata.resources.epacems`/`vcerare` for PUDL's few
# oversized tables that set it). Chunking is only correct because the chunking
# column is itself part of the primary key -- two rows with an identical
# composite key necessarily share the same value of it, so duplicates can never
# span chunk boundaries, whether the column is date/datetime (chunked by
# calendar year -- always annual, no other granularity is supported) or
# anything else (chunked by exact distinct value). These tests go through the
# public `check_primary_key` dispatcher rather than the private
# `_check_primary_key_polars`/`_chunk_filters` helpers directly, with synthetic
# resources, independent of which real tables are currently enumerated.


def _temporal_pk_resource(chunked: bool = False) -> Resource:
    return Resource(
        name="_test__temporal_pk_chunking",
        description="Synthetic resource with a temporal primary key.",
        schema={
            "fields": [
                {"name": "event_date", "type": "date", "description": "Event date."},
                {"name": "unit_id", "type": "integer", "description": "Unit ID."},
            ],
            "primary_key": ["event_date", "unit_id"],
            "pk_check_chunk_field": "event_date" if chunked else None,
        },
    )


def _categorical_pk_resource(chunked: bool = False) -> Resource:
    return Resource(
        name="_test__categorical_pk_chunking",
        description="Synthetic resource with a string primary-key column.",
        schema={
            "fields": [
                {"name": "filer_id", "type": "string", "description": "Filer ID."},
                {
                    "name": "record_id",
                    "type": "integer",
                    "description": "Record ID.",
                },
            ],
            "primary_key": ["filer_id", "record_id"],
            "pk_check_chunk_field": "filer_id" if chunked else None,
        },
    )


@pytest.mark.parametrize(
    ("resource_fn", "data", "expect_violation"),
    [
        (
            _temporal_pk_resource,
            {
                "event_date": [
                    date(2020, 1, 1),
                    date(2020, 1, 2),
                    date(2021, 1, 1),
                    date(2021, 1, 2),
                ],
                # unit_id repeats across years -- fine, since event_date differs.
                "unit_id": [1, 2, 1, 2],
            },
            False,
        ),
        (
            _temporal_pk_resource,
            {
                "event_date": [date(2020, 1, 1), date(2020, 1, 1), date(2021, 1, 1)],
                "unit_id": [1, 1, 1],
            },
            True,
        ),
        (
            _categorical_pk_resource,
            {
                "filer_id": ["A", "A", "B", "B"],
                # record_id repeats across filers -- fine, since filer_id differs.
                "record_id": [1, 2, 1, 2],
            },
            False,
        ),
        (
            _categorical_pk_resource,
            {
                "filer_id": ["A", "A", "B"],
                "record_id": [1, 1, 1],
            },
            True,
        ),
    ],
    ids=[
        "temporal_valid",
        "temporal_duplicate",
        "categorical_valid",
        "categorical_duplicate",
    ],
)
@pytest.mark.parametrize("chunked", [False, True], ids=["unchunked", "chunked"])
def test_check_primary_key_polars_detects_violations(
    resource_fn, data, expect_violation, chunked
) -> None:
    """Repeated non-key values are not false positives; true duplicates are caught.

    Parametrized over ``chunked`` so that every case runs both with and without
    ``schema.pk_check_chunk_field`` set on the synthetic resource, on the exact
    same input data -- chunking must never change the answer
    ``check_primary_key`` gives, only how it gets there. Goes through the
    public ``check_primary_key`` dispatcher, the same entry point every real
    caller uses, rather than the private ``_check_primary_key_polars``.
    """
    errors = resource_fn(chunked=chunked).check_primary_key(pl.LazyFrame(data))
    assert bool(errors) == expect_violation


def test_check_primary_key_chunks_temporal_field_by_calendar_year(mocker) -> None:
    """A date/datetime ``pk_check_chunk_field`` partitions rows by calendar year.

    Chunking a temporal field is always annual -- no other granularity is
    supported (see ``Resource._chunk_filters``). Spies on the private
    ``Resource._chunk_filters`` rather than calling it directly, so the test
    still goes through the public ``check_primary_key`` entry point (chunking
    is otherwise invisible from the outside, since it's only an internal
    performance detail -- the spy is what makes "we're peeking at an
    implementation detail here" explicit rather than incidental).
    """
    spy = mocker.spy(Resource, "_chunk_filters")
    lf = pl.LazyFrame(
        {
            "event_date": [date(2020, 1, 1), date(2020, 6, 1), date(2021, 3, 1)],
            "unit_id": [1, 2, 1],
        }
    )
    _temporal_pk_resource(chunked=True).check_primary_key(lf)

    spy.assert_called_once_with(mocker.ANY, "event_date")
    filters = spy.spy_return
    assert len(filters) == 2  # 2020 and 2021
    row_counts = sorted(lf.filter(f).select(pl.len()).collect().item() for f in filters)
    assert row_counts == [1, 2]


def test_check_primary_key_chunks_categorical_field_by_value(mocker) -> None:
    """A non-temporal ``pk_check_chunk_field`` partitions rows by exact value.

    See ``test_check_primary_key_chunks_temporal_field_by_calendar_year`` for
    why this spies on ``Resource._chunk_filters`` rather than calling it
    directly.
    """
    spy = mocker.spy(Resource, "_chunk_filters")
    lf = pl.LazyFrame({"filer_id": ["A", "A", "B"], "record_id": [1, 2, 1]})
    _categorical_pk_resource(chunked=True).check_primary_key(lf)

    spy.assert_called_once_with(mocker.ANY, "filer_id")
    filters = spy.spy_return
    assert len(filters) == 2  # "A" and "B"
    counts_by_value = {}
    for f in filters:
        chunk = lf.filter(f).collect()
        counts_by_value[chunk["filer_id"][0]] = chunk.height
    assert counts_by_value == {"A": 2, "B": 1}


def test_check_primary_key_polars_no_primary_key() -> None:
    """Resources without a primary key are trivially valid.

    Goes through the public ``check_primary_key`` dispatcher rather than
    ``_check_primary_key_polars`` directly: the "no primary key" guard lives
    only in the dispatcher (``_check_primary_key_polars`` assumes a non-empty
    ``primary_key`` -- an empty one breaks ``pl.any_horizontal`` in the null
    check), so calling the private method directly here would test a
    combination that never happens in practice.
    """
    resource = Resource(
        name="_test__no_pk",
        description="Synthetic resource without a primary key.",
        schema={"fields": [{"name": "x", "type": "integer", "description": "X."}]},
    )
    lf = pl.LazyFrame({"x": [1, 1, 1]})
    assert resource.check_primary_key(lf) == []


def test_field_unit_strings() -> None:
    """Check that all unit strings in FIELD_METADATA parse against PUDL_UNIT_REGISTRY.

    Collects every failure before raising so a single run reveals all bad strings.
    """
    failures = []
    for field_name, meta in FIELD_METADATA.items():
        unit = meta.get("unit")
        if unit is None:
            continue
        try:
            PUDL_UNIT_REGISTRY.parse_units(unit)
        except Exception as exc:
            failures.append(f"  {field_name}: unit={unit!r} — {exc}")
    if failures:
        raise AssertionError(
            f"{len(failures)} field(s) have unparseable unit strings:\n"
            + "\n".join(failures)
        )


def test_defined_fields_are_used():
    """Check that all fields which are defined are actually used."""
    used_fields = set()
    for resource in PUDL_RESOURCES.values():
        used_fields |= {f.name for f in resource.schema.fields}
    defined_fields = set(FIELD_METADATA.keys())
    unused_fields = sorted(defined_fields - used_fields)
    if len(unused_fields) > 0:
        raise AssertionError(
            f"Found {len(unused_fields)} unused fields: {unused_fields}"
        )


def test_get_sorted_resources() -> None:
    """Test that resources are returned in this order (out, core, _core)."""
    resource_ids = (
        "_core_eia860__fgd_equipment",
        "core_eia__entity_boilers",
        "out_eia__yearly_boilers",
    )
    resources = Package.from_resource_ids(
        resource_ids=resource_ids, resolve_foreign_keys=True
    ).get_sorted_resources()

    first_resource_name = resources[0].name
    last_resource_name = resources[-1].name
    assert first_resource_name.startswith("out"), (
        f"{first_resource_name} is the first resource. Expected a resource with the prefix 'out'"
    )
    assert last_resource_name.startswith("_core"), (
        f"{last_resource_name} is the last resource. Expected a resource with the prefix '_core'"
    )


def test_resource_descriptors_valid():
    # just make sure these validate properly
    descriptors = {
        name: PudlResourceDescriptor.model_validate(desc)
        for name, desc in RESOURCE_METADATA.items()
    }
    assert len(descriptors) > 0


@pytest.fixture()
def dummy_resource_dict():
    return {
        "description": "test resource based on core_eia__entity_plants",
        "schema": {
            "fields": ["plant_id_eia", "city", "capacity_mw"],
            "primary_key": ["plant_id_eia"],
        },
        "sources": ["eia860", "eia923"],
        "etl_group": "entity_eia",
        "field_namespace": "eia",
    }


@pytest.fixture()
def dummy_resource_dict_w_geometry():
    return {
        "description": "test resource based on core_eia__entity_plants with added geometry field",
        "schema": {
            "fields": ["plant_id_eia", "city", "capacity_mw", "geometry"],
            "primary_key": ["plant_id_eia"],
        },
        "sources": ["eia860", "eia923"],
        "etl_group": "entity_eia",
        "field_namespace": "eia",
    }


@pytest.fixture()
def dummy_pandera_schema(dummy_resource_dict):
    resource_descriptor = PudlResourceDescriptor.model_validate(dummy_resource_dict)
    resource = Resource.model_validate(
        Resource.dict_from_resource_descriptor(
            "test_eia__entity_plants", resource_descriptor
        )
    )
    return resource.schema.to_pandera()


@pytest.fixture()
def dummy_pandera_schema_w_geometry(dummy_resource_dict_w_geometry):
    resource_descriptor = PudlResourceDescriptor.model_validate(
        dummy_resource_dict_w_geometry
    )
    resource = Resource.model_validate(
        Resource.dict_from_resource_descriptor(
            "test_eia__entity_plants_w_geometry", resource_descriptor
        )
    )
    return resource.schema.to_pandera()


@pytest.mark.parametrize(
    "data,backend",
    [
        (
            pl.DataFrame(
                {
                    "plant_id_eia": [12345, 12346],
                    "city": ["Bloomington", "Springfield"],
                    "capacity_mw": [1.3, 1.0],
                }
            ),
            "polars",
        ),
        (
            gpd.GeoDataFrame(
                {
                    "plant_id_eia": [12345, 12346],
                    "city": pd.Series(["Bloomington", "Springfield"], dtype="string"),
                    "capacity_mw": [1.3, 1.0],
                    "geometry": [Point(0, 0), Point(1, 0)],
                }
            ),
            "pandas",
        ),
    ],
)
def test_resource_descriptors_can_encode_schemas(
    data, backend, dummy_pandera_schema, dummy_pandera_schema_w_geometry
):
    if backend == "polars":
        schema = dummy_pandera_schema
        assert not schema.validate(data).is_empty()
    else:
        schema = dummy_pandera_schema_w_geometry
        assert not schema.validate(data).empty


@pytest.mark.parametrize(
    "error_msg,data",
    [
        pytest.param(
            "column 'plant_id_eia' not in dataframe",
            pl.DataFrame([]),
            id="empty dataframe",
        ),
        pytest.param(
            "expected column 'plant_id_eia' to have type Int64, got String",
            pl.DataFrame(
                {
                    "plant_id_eia": ["non_number"],
                    "city": ["Bloomington"],
                    "capacity_mw": ["1.3"],
                }
            ),
            id="bad dtype",
        ),
        pytest.param(
            "columns .* not unique",
            pl.DataFrame(
                {
                    "plant_id_eia": [12345, 12345],
                    "city": ["Bloomington", "Springfield"],
                    "capacity_mw": [1.3, 1.0],
                }
            ),
            id="duplicate PK",
        ),
    ],
)
def test_resource_descriptor_schema_failures(error_msg, data, dummy_pandera_schema):
    with pytest.raises(pr_polars.errors.SchemaError, match=error_msg):
        dummy_pandera_schema.validate(data)


@pytest.mark.parametrize(
    "error_msg,data",
    [
        pytest.param(
            "column 'plant_id_eia' not in dataframe",
            gpd.GeoDataFrame([]),
            id="empty dataframe",
        ),
        pytest.param(
            "expected series 'plant_id_eia' to have type Int64",
            gpd.GeoDataFrame(
                {
                    "plant_id_eia": ["non_number"],
                    "city": ["Bloomington"],
                    "capacity_mw": ["1.3"],
                    "geometry": [Point(0, 0)],
                }
            ).astype(str),
            id="bad dtype",
        ),
        pytest.param(
            "columns .* not unique",
            gpd.GeoDataFrame(
                {
                    "plant_id_eia": [12345, 12345],
                    "city": pd.Series(["Bloomington", "Springfield"], dtype="string"),
                    "capacity_mw": [1.3, 1.0],
                    "geometry": [Point(0, 0), Point(1, 0)],
                }
            ),
            id="duplicate PK",
        ),
    ],
)
def test_resource_descriptor_schema_failures_w_geometry(
    error_msg, data, dummy_pandera_schema_w_geometry
):
    with pytest.raises(pr_pandas.errors.SchemaError, match=error_msg):
        dummy_pandera_schema_w_geometry.validate(data)


def test_frictionless_data_package_non_empty(tmp_path):
    datapackage = PUDL_PACKAGE.to_frictionless()
    assert len(datapackage.resources) == len(RESOURCE_METADATA)


METADATA_OVERRIDE_KEYS = [
    "layer_code",
    "table_type_code",
    "timeseries_resolution_code",
    "additional_summary_text",
    "additional_layer_text",
    "additional_source_text",
    "additional_primary_key_text",
    "additional_details_text",
]


def test_frictionless_data_package_resources_populated(tmp_path):
    datapackage = PUDL_PACKAGE.to_frictionless()
    for resource in datapackage.resources:
        assert resource.name in RESOURCE_METADATA
        expected_resource = RESOURCE_METADATA[resource.name]
        # TODO: remove str option after metadata migration
        strings_to_find = []
        if isinstance(expected_resource["description"], str):
            strings_to_find.append(expected_resource["description"])
        else:
            for k in METADATA_OVERRIDE_KEYS:
                if k in expected_resource["description"]:
                    strings_to_find.append(expected_resource["description"][k])
        assert any(
            resource.description.find(candidate) >= 0 for candidate in strings_to_find
        )
        assert expected_resource["schema"]["fields"] == [
            f.name for f in resource.schema.fields
        ]
        assert (
            expected_resource["schema"].get("primary_key", [])
            == resource.schema.primary_key
        )


def test_merge_descriptions():
    "Ensure descriptions are merged properly."
    left = {
        "additional_summary_text": "one",
        "additional_details_text": "two",
        "usage_warnings": ["red"],
    }
    right = {
        "additional_summary_text": "fish",
        "additional_details_text": "poisson",
        "usage_warnings": ["blue"],
    }
    result = {
        "additional_summary_text": "one fish",
        "additional_details_text": "two\n\npoisson",
        "usage_warnings": ["red", "blue"],
    }
    assert merge_descriptions(left, right) == result
    # make sure we didn't accidentally modify the source list during the merge
    assert left["usage_warnings"] == ["red"]


def test_multiple_path_resources(dummy_resource_dict):
    """Test that resources paths are output to json as expected after converting to frictionless."""
    default_path_resource = Resource(
        **Resource.dict_from_resource_descriptor(
            "out_pudl__default_path_resource",
            PudlResourceDescriptor.model_validate(dummy_resource_dict),
        )
    )
    frictionless_resource = frictionless.Resource(
        json.loads(default_path_resource.to_frictionless().to_json())
    )
    assert frictionless_resource.path == "out_pudl__default_path_resource.parquet"

    override_single_path_resource = Resource(
        **Resource.dict_from_resource_descriptor(
            "out_pudl__override_single_path_resource",
            PudlResourceDescriptor.model_validate(
                dummy_resource_dict | {"path": "fake_path.parquet"}
            ),
        )
    )
    frictionless_resource = frictionless.Resource(
        json.loads(override_single_path_resource.to_frictionless().to_json())
    )
    assert frictionless_resource.path == "fake_path.parquet"

    paths = [f"fake_path{i}" for i in range(1, 5)]
    multiple_path_resource = Resource(
        **Resource.dict_from_resource_descriptor(
            "out_pudl__multiple_path_resource",
            PudlResourceDescriptor.model_validate(
                dummy_resource_dict
                | {
                    "path": paths[0],
                    "extrapaths": paths[1:],
                }
            ),
        )
    )
    frictionless_resource = frictionless.Resource(
        json.loads(multiple_path_resource.to_frictionless().to_json())
    )
    assert frictionless_resource.path == paths[0]
    assert set(frictionless_resource.extrapaths) == set(paths[1:])
    assert set(frictionless_resource.normpaths) == set(paths)


def test_frictionless_data_package_filter_resources():
    """Test that filtering resources when converting Package to frictionless works as expected."""
    eqr_pattern = r"core_ferceqr.*"
    expected_num_eqr_resources = 4
    all_resources = PUDL_PACKAGE.to_frictionless().resources
    no_eqr_resources = PUDL_PACKAGE.to_frictionless(
        exclude_pattern=eqr_pattern
    ).resources
    only_eqr_resources = PUDL_PACKAGE.to_frictionless(
        include_pattern=eqr_pattern
    ).resources

    assert len(no_eqr_resources) == (len(all_resources) - expected_num_eqr_resources)
    assert len(only_eqr_resources) == expected_num_eqr_resources
    assert not any("eqr" in r.name for r in no_eqr_resources)
    assert all("eqr" in r.name for r in only_eqr_resources)


@pytest.mark.parametrize(
    "partition_key,period,offset,expected",
    [
        ("years", "2026", 0, "2026"),
        ("years", "2026", 1, "2027"),
        ("years", "2026", -1, "2025"),
        ("year_quarters", "2026q1", 0, "2026q1"),
        ("year_quarters", "2026q1", 1, "2026q2"),
        ("year_quarters", "2026q1", -1, "2025q4"),
        ("half_years", "2026half1", 0, "2026half1"),
        ("half_years", "2026half1", 1, "2026half2"),
        ("half_years", "2026half1", -1, "2025half2"),
        ("year_months", "2026-01", 0, "2026-01"),
        ("year_months", "2026-01", 1, "2026-02"),
        ("year_months", "2026-01", -1, "2025-12"),
    ],
)
def test_availability_offsets(partition_key, period, offset, expected):
    """Check edge cases for temporal partition arithmetic."""
    assert PARTITION_OFFSETS[partition_key](period, offset) == expected


def test_source_availability() -> None:
    """All sources have a reasonable temporal availability.

    Sources with only non-temporal partitions will evaluate to None; all others
    should show after 1990.

    We check this because if you get the data types wrong in pd.Timestamp, it
    spits out 1970 instead of the proper year.
    """
    # Checking the lexical ordering of strings using > is a bit brittle, but has
    # the bonus of handling years, year quarters, half years, and year months.
    # If this breaks it probably means we're running on a machine that orders
    # strings by weird criteria -- we can revisit this at that time.
    failures = []
    for source_id in sorted(SOURCES):
        try:
            src = DataSource.from_id(source_id)
            availability = ResourceDescriptionBuilder.offset_source_availability(src, 0)
            if not ((availability is None) or (availability > "1990")):
                failures.append(
                    f"  {source_id}: availability {availability!r} is before 1990"
                )
        except Exception as exc:
            failures.append(f"  {source_id}: {exc}")
    if failures:
        raise AssertionError(
            f"{len(failures)} source(s) have invalid availability:\n"
            + "\n".join(failures)
        )


@pytest.mark.parametrize(
    "given_name,given_settings,given_rowcounts,given_source,expected",
    [
        (  # manually specified
            "test_eia__entity_test",
            {"availability_text": "manual", "sources": []},
            [None],
            [],
            ResourceTrait(type="True", description="manual"),
        ),
        (  # using row counts
            "test_eia__entity_test",
            {"sources": []},
            ["row counts"],
            [],
            ResourceTrait(type="True", description="row counts"),
        ),
        (  # using source: unambiguous
            "test_eia__entity_test",
            {"sources": ["1"]},
            [None],
            ["1 source"],
            ResourceTrait(type="True", description="1 source"),
        ),
        (  # using source: atemporal
            "test_eia__entity_test",
            {"sources": ["1"]},
            [None],
            [None],
            ResourceTrait(type="False", description="Unknown"),
        ),
        (  # using source: ambiguous
            "test_eia__entity_test",
            {"sources": ["1", "x", "2"]},
            [None],
            ["1 source", None, "2 source"],
            ResourceTrait(type="False", description="Unknown"),
        ),
        (  # nobody knows
            "test_eia__entity_test",
            {"sources": []},
            [None],
            [],
            ResourceTrait(type="False", description="Unknown"),
        ),
    ],
)
def test_resolve_resource_availability(
    mocker, given_name, given_settings, given_rowcounts, given_source, expected
):
    """Verify fallback flow is correct for resolving most recently available data."""
    mocker.patch(
        "pudl.metadata.descriptions.ResourceDescriptionBuilder.compute_rowcounts_availability",
        mocker.Mock(side_effect=given_rowcounts, __is_component=False),
    )
    mocker.patch(
        "pudl.metadata.descriptions.ResourceDescriptionBuilder.offset_source_availability",
        mocker.Mock(side_effect=given_source, __is_component=False),
    )
    resolved = ResourceDescriptionBuilder(
        resource_id=given_name, settings=given_settings
    ).build()

    assert (resolved.availability.type == expected.type) and (
        resolved.availability.description == expected.description
    )


# TODO: flip this to true after we do the second pass to set description_primary_key
# everywhere that needs it
CHECK_DESCRIPTION_PRIMARY_KEYS = False

RE_CAPS = re.compile("[A-Z]")

EXPECT_NO_AVAILABILITY = {
    "core_eia__codes_cooling_system_types",
    "core_eia__codes_energy_sources",
    "core_eia__codes_fuel_transportation_modes",
    "core_eia__codes_operational_status",
    "core_eia__codes_prime_movers",
    "core_eia__codes_sector_consolidated",
    "core_eia__entity_boilers",
    "core_eia__entity_generators",
    "core_eia__entity_plants",
    "core_eia__entity_utilities",
    "core_ferc__entity_companies",
    "core_gridpathratoolkit__assn_generator_aggregation_group",
    "core_pudl__assn_eia_pudl_plants",
    "core_pudl__assn_eia_pudl_utilities",
    "core_pudl__assn_ferc714_csv_pudl_respondents",
    "core_pudl__assn_ferc714_pudl_respondents",
    "core_pudl__assn_ferc714_xbrl_pudl_respondents",
    "core_pudl__assn_utilities_plants",
    "core_pudl__codes_data_maturities",
    "core_pudl__codes_datasources",
    "core_pudl__codes_imputation_reasons",
    "core_pudl__codes_subdivisions",
    "core_pudl__entity_plants_pudl",
    "core_pudl__entity_utilities_pudl",
    "core_rus__codes_fuel_types",
    "core_rus__codes_investment_types",
    "_core_eia__forensics_entity_resolution_boilers",
    "_core_eia__forensics_entity_resolution_plants",
    "_core_eia__forensics_entity_resolution_generators",
    "_core_eia__forensics_entity_resolution_utilities",
}


def test_description_compliance() -> None:
    """Migrated resource descriptions comply with all formatting and availability rules.

    Only checks resources where ``description`` has been converted from a string to a
    dict (i.e. migrated tables).
    """
    # todo: back this off to sorted(PUDL_RESOURCES.keys()) after the migration.
    # only check migrated tables. a table is migrated if "description" has been converted from a string to a dict.
    resource_ids = sorted(
        r
        for r in PUDL_RESOURCES
        if isinstance(RESOURCE_METADATA[r]["description"], dict)
    )
    failures = []
    for resource_id in resource_ids:
        try:
            resource_dict = RESOURCE_METADATA[resource_id]
            description_dict = resource_dict["description"]
            assert isinstance(description_dict, dict), (
                f"""Table {resource_id} must have a dictionary under the "description" key, but instead I found a {type(description_dict)}"""
            )
            resolved = ResourceDescriptionBuilder(
                resource_id=resource_id,
                settings=Resource._resolve_references_from_resource_descriptor(
                    resource_id, PudlResourceDescriptor.model_validate(resource_dict)
                ),
            ).build()
            name_parse = {
                "layer_code": resolved.layer.type,
                "source_code": resolved.source.type,
                "table_type_code": (
                    (resolved.summary.type.split("[")[0] != "None")
                    or (
                        (len(resolved.summary.description) > 0)
                        and RE_CAPS.match(resolved.summary.description[0])
                    )
                ),
                "timeseries_resolution_code": (
                    (not resolved.summary.type.startswith("timeseries"))
                    or (len(resolved.summary.type.split("[")[1]) > 1)
                    or RE_CAPS.match(resolved.summary.description[0])
                ),
            }
            fix_with_summary = f"""Ensure RESOURCE_METADATA["{resource_id}"]["description"]["additional_summary_text"] is a complete sentence starting with a capital letter"""
            for key, has_value in name_parse.items():
                assert has_value, f"""Table {resource_id} could not be parsed as layer_source__tabletype_slug and insufficient hints were set in the table metadata. Repair using one of the following:
\t1. Rename {resource_id}
\t2. Set the following keys in RESOURCE_METADATA["{resource_id}"]["description"]: {key}{("\n\t3. " + fix_with_summary) if key in {"table_type_code", "timeseries_resolution_code"} else ""}"""
            # todo: layer-based checks
            # todo: asset_type-based checks
            # pk-based checks
            has_pk = resolved.primary_key.type == "True"
            if CHECK_DESCRIPTION_PRIMARY_KEYS and not has_pk:  # pragma: no cover
                assert "additional_primary_key_text" in description_dict, (
                    f"""Table {resource_id} has no primary key, but the table metadata does not include an explanation in the required format. We expect the key "additional_primary_key_text" to briefly describe what each record represents and, if needed, why no primary key is possible."""
                )
            # availability-based checks
            assert ("availability_text" not in description_dict) or (
                "availability_offset" not in description_dict
            ), (
                f"Table {resource_id} has set both availability_text and availability_offset; you can't have both."
            )
            if resource_id not in EXPECT_NO_AVAILABILITY:
                assert resolved.availability.type == "True", (
                    f"Missing availability for {resource_id}"
                )
        except AssertionError as exc:
            failures.append(f"  {exc}")
    if failures:
        raise AssertionError(
            f"{len(failures)} resource(s) failed description compliance:\n"
            + "\n".join(failures)
        )


# ---------------------------------------------------------------------------
# Tests for get_pudl_dtypes / apply_pudl_dtypes / apply_pudl_dtypes_polars
# ---------------------------------------------------------------------------

# Use the real FIELD_METADATA_BY_RESOURCE override that exists for
# core_eia861__yearly_reliability: globally "customers" is type "integer"
# (→ Int64), but that table stores weighted averages so the override changes
# it back to "number" (→ float64).
_RELIABILITY_RESOURCE = "core_eia861__yearly_reliability"
_OVERRIDE_FIELD = "customers"
_GEOMETRY_RESOURCE = "out_censusdp1tract__states"

_BACKEND_GEOMETRY_SUPPORT: list[tuple[PudlDtypeBackend, bool]] = [
    ("pandas", True),
    ("polars", False),
    ("sqlite", False),
    ("duckdb", False),
    ("pyarrow", True),
]


def test_get_pudl_dtypes_global_type() -> None:
    """Without a resource, customers maps to the global integer dtype."""
    dtypes = get_pudl_dtypes()
    assert dtypes[_OVERRIDE_FIELD] == "Int64"


@pytest.mark.parametrize(
    ("dtype_backend", "expected_dtype"),
    [
        ("pandas", "Int64"),
        ("polars", pl.Int64),
        ("sqlite", sa.BigInteger),
        ("duckdb", duckdb.sqltypes.BIGINT),
        ("pyarrow", pa.int64()),
    ],
)
def test_get_pudl_dtypes_named_backend(
    dtype_backend: PudlDtypeBackend, expected_dtype: Any
) -> None:
    """Named dtype backends should select the expected canonical mapping."""
    dtypes = get_pudl_dtypes(dtype_backend=dtype_backend)
    assert dtypes[_OVERRIDE_FIELD] == expected_dtype


def test_get_pudl_dtypes_polars_skips_unsupported_types() -> None:
    """Polars dtype selection should skip fields whose canonical type is unsupported."""
    dtypes = get_pudl_dtypes(dtype_backend="polars")
    assert "geometry" not in dtypes


@pytest.mark.parametrize(
    ("dtype_backend", "includes_geometry"), _BACKEND_GEOMETRY_SUPPORT
)
def test_get_pudl_dtypes_geometry_field_support(
    dtype_backend: PudlDtypeBackend, includes_geometry: bool
) -> None:
    """Each backend should explicitly include or omit global geometry fields."""
    dtypes = get_pudl_dtypes(dtype_backend=dtype_backend)
    assert ("geometry" in dtypes) is includes_geometry


@pytest.mark.parametrize(
    ("dtype_backend", "includes_geometry"), _BACKEND_GEOMETRY_SUPPORT
)
def test_get_pudl_dtypes_resource_geometry_field_support(
    dtype_backend: PudlDtypeBackend, includes_geometry: bool
) -> None:
    """Geometry inclusion should also be explicit for concrete resources."""
    dtypes = get_pudl_dtypes(
        resource=_GEOMETRY_RESOURCE,
        dtype_backend=dtype_backend,
    )
    assert ("geometry" in dtypes) is includes_geometry


def test_get_pudl_dtypes_resource_override() -> None:
    """With the reliability resource name, customers maps to float64."""
    dtypes = get_pudl_dtypes(resource=_RELIABILITY_RESOURCE)
    assert dtypes[_OVERRIDE_FIELD] == "float64"


def test_get_pudl_dtypes_resource_uses_package_schema() -> None:
    """Default resource lookups should use the already-defined resource dtypes."""
    dtypes = get_pudl_dtypes(resource=_RELIABILITY_RESOURCE)
    assert dtypes == PUDL_PACKAGE.get_resource(_RELIABILITY_RESOURCE).to_pandas_dtypes()


def test_get_pudl_dtypes_resource_overrides_group() -> None:
    """Resource-level override takes precedence over group-level override."""
    dtypes_no_resource = get_pudl_dtypes(field_namespace="eia")
    dtypes_with_resource = get_pudl_dtypes(resource=_RELIABILITY_RESOURCE)
    assert dtypes_no_resource[_OVERRIDE_FIELD] == "Int64"
    assert dtypes_with_resource[_OVERRIDE_FIELD] == "float64"


def test_get_pudl_dtypes_invalid_field_namespace() -> None:
    """Unknown field namespaces should fail with a clear error."""
    with pytest.raises(ValueError, match="Unknown PUDL field namespace"):
        get_pudl_dtypes(field_namespace="not_a_real_group")


def test_get_pudl_dtypes_rejects_field_namespace_and_resource() -> None:
    """field_namespace and resource should be mutually exclusive selectors."""
    with pytest.raises(ValueError, match="mutually exclusive"):
        get_pudl_dtypes(field_namespace="eia", resource=_RELIABILITY_RESOURCE)


def test_get_pudl_dtypes_invalid_resource() -> None:
    """Unknown resources should fail with a clear error."""
    with pytest.raises(ValueError, match="Unknown resource"):
        get_pudl_dtypes(resource="not_a_real_resource")


def test_apply_pudl_dtypes_global_type() -> None:
    """Without a resource, customers column becomes Int64."""
    df = pd.DataFrame({_OVERRIDE_FIELD: [1.0, 2.0, 3.0]})
    result = apply_pudl_dtypes(df)
    assert str(result[_OVERRIDE_FIELD].dtype) == "Int64"


def test_apply_pudl_dtypes_resource_override() -> None:
    """With the reliability resource, float customers values are preserved."""
    df = pd.DataFrame({_OVERRIDE_FIELD: [1.5, 2.3, 3.7]})
    result = apply_pudl_dtypes(df, resource=_RELIABILITY_RESOURCE)
    assert str(result[_OVERRIDE_FIELD].dtype) == "float64"
    assert result[_OVERRIDE_FIELD].tolist() == [1.5, 2.3, 3.7]


def test_apply_pudl_dtypes_resource_override_prevents_cast_failure() -> None:
    """Resource override must prevent the float→Int64 cast that would raise TypeError."""
    df = pd.DataFrame({_OVERRIDE_FIELD: [1.5, 2.3, 3.7]})
    # Without the resource override, casting float values to Int64 raises TypeError
    with pytest.raises(TypeError):
        apply_pudl_dtypes(df)

    # With the override it succeeds silently
    result = apply_pudl_dtypes(df, resource=_RELIABILITY_RESOURCE)
    assert result[_OVERRIDE_FIELD].tolist() == [1.5, 2.3, 3.7]


def test_apply_pudl_dtypes_polars_global_type() -> None:
    """Without a resource, customers column becomes Int64 in a polars LazyFrame."""
    lf = pl.LazyFrame({_OVERRIDE_FIELD: [1, 2, 3]})
    result = apply_pudl_dtypes_polars(lf).collect()
    assert result[_OVERRIDE_FIELD].dtype == pl.Int64


def test_apply_pudl_dtypes_polars_resource_override() -> None:
    """With the reliability resource, customers stays float in polars."""
    lf = pl.LazyFrame({_OVERRIDE_FIELD: [1.5, 2.3, 3.7]})
    result = apply_pudl_dtypes_polars(lf, resource=_RELIABILITY_RESOURCE).collect()
    assert result[_OVERRIDE_FIELD].dtype == pl.Float64
