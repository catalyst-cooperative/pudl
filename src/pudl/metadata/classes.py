"""Metadata data classes."""

import copy
import datetime
import json
import re
import sys
import warnings
from collections.abc import Callable, Iterable
from functools import cached_property, lru_cache
from hashlib import sha1
from pathlib import Path
from typing import Annotated, Any, Literal, Self, TypeVar

import frictionless
import jinja2
import numpy as np
import pandas as pd
import pandera.pandas as pr
import pyarrow as pa
import pydantic
import sqlalchemy as sa
from pandas._libs.missing import NAType
from pydantic import (
    AnyHttpUrl,
    BaseModel,
    ConfigDict,
    DirectoryPath,
    EmailStr,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
    StringConstraints,
    ValidationInfo,
    field_validator,
    model_validator,
)

import pudl.logging_helpers
from pudl.metadata import descriptions
from pudl.metadata.codes import CODE_METADATA
from pudl.metadata.constants import (
    CONSTRAINT_DTYPES,
    CONTRIBUTORS,
    FIELD_DTYPES_PANDAS,
    FIELD_DTYPES_PYARROW,
    FIELD_DTYPES_SQL,
    LICENSES,
    PERIODS,
)
from pudl.metadata.fields import (
    FIELD_METADATA,
    FIELD_METADATA_BY_GROUP,
    FIELD_METADATA_BY_RESOURCE,
)
from pudl.metadata.helpers import (
    expand_periodic_column_names,
    format_errors,
    groupby_aggregate,
    most_and_more_frequent,
    split_period,
)
from pudl.metadata.resources import FOREIGN_KEYS, RESOURCE_METADATA
from pudl.metadata.sources import SOURCES
from pudl.workspace.datastore import Datastore, ZenodoDoi
from pudl.workspace.setup import PudlPaths

logger = pudl.logging_helpers.get_logger(__name__)

# The BaseModel.schema attribute is deprecated and we are shadowing it to avoid needing
# to define an inconvenient alias for it.
warnings.filterwarnings(
    action="ignore",
    message='Field name "schema" in "Resource" shadows an attribute in parent "PudlMeta"',
    category=UserWarning,
    module="pydantic._internal._fields",
)

# ---- Helpers ---- #


def _unique(*args: Iterable) -> list:
    """Return a list of all unique values, in order of first appearance.

    Args:
        args: Iterables of values.

    Examples:
        >>> _unique([0, 2], (2, 1))
        [0, 2, 1]
        >>> _unique([{'x': 0, 'y': 1}, {'y': 1, 'x': 0}], [{'z': 2}])
        [{'x': 0, 'y': 1}, {'z': 2}]
    """
    values = []
    for parent in args:
        for child in parent:
            if child not in values:
                values.append(child)
    return values


def _format_for_sql(x: Any, identifier: bool = False) -> str:  # noqa: C901
    """Format value for use in raw SQL(ite).

    Args:
        x: Value to format.
        identifier: Whether `x` represents an identifier
            (e.g. table, column) name.

    Examples:
        >>> _format_for_sql('table_name', identifier=True)
        '"table_name"'
        >>> _format_for_sql('any string')
        "'any string'"
        >>> _format_for_sql("Single's quote")
        "'Single''s quote'"
        >>> _format_for_sql(None)
        'null'
        >>> _format_for_sql(1)
        '1'
        >>> _format_for_sql(True)
        'True'
        >>> _format_for_sql(False)
        'False'
        >>> _format_for_sql(re.compile("^[^']*$"))
        "'^[^'']*$'"
        >>> _format_for_sql(datetime.date(2020, 1, 2))
        "'2020-01-02'"
        >>> _format_for_sql(datetime.datetime(2020, 1, 2, 3, 4, 5, 6))
        "'2020-01-02 03:04:05'"
    """
    if identifier:
        if isinstance(x, str):
            # Table and column names are escaped with double quotes (")
            return f'"{x}"'
        raise ValueError("Identifier must be a string")
    if x is None:
        return "null"
    if isinstance(x, int | float):
        # NOTE: nan and (-)inf are TEXT in sqlite but numeric in postgresSQL
        return str(x)
    if x is True:
        return "TRUE"
    if x is False:
        return "FALSE"
    if isinstance(x, re.Pattern):
        x = x.pattern
    elif isinstance(x, datetime.datetime):
        # Check datetime.datetime first, since also datetime.date
        x = x.strftime("%Y-%m-%d %H:%M:%S")
    elif isinstance(x, datetime.date):
        x = x.strftime("%Y-%m-%d")
    if not isinstance(x, str):
        raise ValueError(f"Cannot format type {type(x)} for SQL")
    # Single quotes (') are escaped by doubling them ('')
    x = x.replace("'", "''")
    return f"'{x}'"


def _get_jinja_environment(template_dir: DirectoryPath = None):
    if template_dir:
        path = template_dir / "templates"
    else:
        path = Path(__file__).parent.resolve() / "templates"
    environment = jinja2.Environment(
        loader=jinja2.FileSystemLoader(path),
        autoescape=True,
    )
    return environment


# ---- Class attribute types ---- #

# NOTE: Using regex=r"^\S(.*\S)*$" to fail on whitespace is too slow
String = Annotated[
    str, StringConstraints(min_length=1, strict=True, pattern=r"^\S+(\s+\S+)*$")
]
"""Non-empty :class:`str` with no trailing or leading whitespace."""

SnakeCase = Annotated[
    str,
    StringConstraints(
        min_length=1, strict=True, pattern=r"^[a-z_][a-z0-9_]*(_[a-z0-9]+)*$"
    ),
]
"""Snake-case variable name :class:`str` (e.g. 'pudl', 'entity_eia860')."""

PositiveInt = Annotated[int, pydantic.Field(ge=0, strict=True)]
"""Positive :class:`int`."""

PositiveFloat = Annotated[float, pydantic.Field(ge=0, strict=True)]
"""Positive :class:`float`."""


T = TypeVar("T")
StrictList = Annotated[list[T], pydantic.Field(min_length=1)]


"""Non-empty :class:`list`.

Allows :class:`list`, :class:`tuple`, :class:`set`, :class:`frozenset`,
:class:`collections.deque`, or generators and casts to a :class:`list`.
"""


# ---- Class attribute validators ---- #


def _check_unique(value: list = None) -> list | None:
    """Check that input list has unique values."""
    if value:
        for i in range(len(value)):
            if value[i] in value[:i]:
                raise ValueError(f"contains duplicate {value[i]}")
    return value


def _validator(*names, fn: Callable) -> Callable:
    """Construct reusable Pydantic validator.

    Args:
        names: Names of attributes to validate.
        fn: Validation function (see :meth:`pydantic.field_validator`).

    Examples:
        >>> class Class(BaseModel):
        ...     x: list = None
        ...     _check_unique = _validator("x", fn=_check_unique)
        >>> Class(x=[0, 0])
        Traceback (most recent call last):
        ValidationError: ...
    """
    return field_validator(*names)(fn)


########################################################################################
# PUDL Metadata Classes
########################################################################################
class PudlMeta(BaseModel):
    """A base model that configures some options for PUDL metadata classes."""

    model_config = ConfigDict(
        extra="forbid",
        validate_default=True,
        validate_assignment=True,
    )


class FieldConstraints(PudlMeta):
    """Field constraints (`resource.schema.fields[...].constraints`).

    See https://specs.frictionlessdata.io/table-schema/#constraints.
    """

    required: StrictBool = False
    unique: StrictBool = False
    min_length: PositiveInt | None = None
    max_length: PositiveInt | None = None
    minimum: StrictInt | StrictFloat | datetime.date | datetime.datetime | None = None
    maximum: StrictInt | StrictFloat | datetime.date | datetime.datetime | None = None
    pattern: re.Pattern | None = None
    enum: (
        StrictList[
            String
            | StrictInt
            | StrictFloat
            | StrictBool
            | datetime.date
            | datetime.datetime
        ]
        | None
    ) = None

    _check_unique = _validator("enum", fn=_check_unique)

    @field_validator("max_length")
    @classmethod
    def _check_max_length(cls, value, info: ValidationInfo):
        minimum, maximum = info.data.get("min_length"), value
        if minimum is not None and maximum is not None:
            if type(minimum) is not type(maximum):
                raise ValueError("must be same type as min_length")
            if maximum < minimum:
                raise ValueError("must be greater or equal to min_length")
        return value

    @field_validator("maximum")
    @classmethod
    def _check_max(cls, value, info: ValidationInfo):
        minimum, maximum = info.data.get("minimum"), value
        if minimum is not None and maximum is not None:
            if type(minimum) is not type(maximum):
                raise ValueError("must be same type as minimum")
            if maximum < minimum:
                raise ValueError("must be greater or equal to minimum")
        return value

    def to_pandera_checks(self) -> list[pr.Check]:
        """Convert these constraints to pandera Column checks."""
        checks = []
        if self.min_length is not None:
            checks.append(pr.Check.str_length(min_value=self.min_length))
        if self.max_length is not None:
            checks.append(pr.Check.str_length(max_value=self.max_length))
        if self.minimum is not None:
            checks.append(pr.Check.ge(self.minimum))
        if self.maximum is not None:
            checks.append(pr.Check.le(self.maximum))
        if self.pattern is not None:
            checks.append(pr.Check.str_matches(self.pattern))
        if self.enum:
            checks.append(pr.Check.isin(self.enum))

        return checks


class FieldHarvest(PudlMeta):
    """Field harvest parameters (`resource.schema.fields[...].harvest`)."""

    # NOTE: Callables with defaults must use pydantic.Field() to not bind to self
    aggregate: Callable[[pd.Series], pd.Series] = pydantic.Field(
        default=lambda x: most_and_more_frequent(x, min_frequency=0.7)
    )
    """Computes a single value from all field values in a group."""

    tolerance: PositiveFloat = 0.0
    """Fraction of invalid groups above which result is considered invalid."""


class Encoder(PudlMeta):
    """A class that allows us to standardize reported categorical codes.

    Often the original data we are integrating uses short codes to indicate a
    categorical value, like ``ST`` in place of "steam turbine" or ``LIG`` in place of
    "lignite coal". Many of these coded fields contain non-standard codes due to
    data-entry errors. The codes have also evolved over the years.

    In order to allow easy comparison of records across all years and tables, we define
    a standard set of codes, a mapping from non-standard codes to standard codes (where
    possible), and a set of known but unfixable codes which will be ignored and replaced
    with NA values. These definitions can be found in :mod:`pudl.metadata.codes` and we
    refer to these as coding tables.

    In our metadata structures, each coding table is defined just like any other DB
    table, with the addition of an associated ``Encoder`` object defining the standard,
    fixable, and ignored codes.

    In addition, a :class:`Package` class that has been instantiated using the
    :meth:`Package.from_resource_ids` method will associate an `Encoder` object with any
    column that has a foreign key constraint referring to a coding table (This
    column-level encoder is same as the encoder associated with the referenced table).
    This `Encoder` can be used to standardize the codes found within the column.

    :class:`Field` and :class:`Resource` objects have ``encode()`` methods that will
    use the column-level encoders to recode the original values, either for a single
    column or for all coded columns within a Resource, given either a corresponding
    :class:`pandas.Series` or :class:`pandas.DataFrame` containing actual values.

    If any unrecognized values are encountered, an exception will be raised, alerting
    us that a new code has been identified, and needs to be classified as fixable or
    to be ignored.
    """

    df: pd.DataFrame
    """A table associating short codes with long descriptions and other information.

    Each coding table contains at least a ``code`` column containing the standard codes
    and a ``description`` column with a human readable explanation of what the code
    stands for. Additional metadata pertaining to the codes and their categories may
    also appear in this dataframe, which will be loaded into the PUDL DB as a static
    table. The ``code`` column is a natural primary key and must contain no duplicate
    values.
    """

    ignored_codes: list[StrictInt | str] = []
    """A list of non-standard codes which appear in the data, and will be set to NA.

    These codes may be the result of data entry errors, and we are unable to map them to
    the appropriate canonical code. They are discarded from the raw input data.
    """

    code_fixes: dict[StrictInt | String, StrictInt | String] = {}
    """A dictionary mapping non-standard codes to canonical, standardized codes.

    The intended meanings of some non-standard codes are clear, and therefore they can
    be mapped to the standardized, canonical codes with confidence. Sometimes these are
    the result of data entry errors or changes in the standard codes over time.
    """

    name: String | None = None
    """The name of the code."""

    # Required to allow DataFrame
    model_config = ConfigDict(arbitrary_types_allowed=True)

    @field_validator("df")
    @classmethod
    def _df_is_encoding_table(cls, df: pd.DataFrame):
        """Verify that the coding table provides both codes and descriptions."""
        errors = []
        if "code" not in df.columns or "description" not in df.columns:
            errors.append(
                "Encoding tables must contain both 'code' & 'description' columns."
            )
        if len(df.code) != len(df.code.unique()):
            dupes = df[df.duplicated("code")].code.to_list()
            errors.append(f"Duplicate codes {dupes} found in coding table")
        if errors:
            raise ValueError(format_errors(*errors, pydantic=True))
        return df

    @field_validator("ignored_codes")
    @classmethod
    def _good_and_ignored_codes_are_disjoint(cls, ignored_codes, info: ValidationInfo):
        """Check that there's no overlap between good and ignored codes."""
        if "df" not in info.data:
            return ignored_codes
        errors = []
        overlap = set(info.data["df"]["code"]).intersection(ignored_codes)
        if overlap:
            errors.append(f"Overlap found between good and ignored codes: {overlap}.")
        if errors:
            raise ValueError(format_errors(*errors, pydantic=True))
        return ignored_codes

    @field_validator("code_fixes")
    @classmethod
    def _good_and_fixable_codes_are_disjoint(cls, code_fixes, info: ValidationInfo):
        """Check that there's no overlap between the good and fixable codes."""
        if "df" not in info.data:
            return code_fixes
        errors = []
        overlap = set(info.data["df"]["code"]).intersection(code_fixes)
        if overlap:
            errors.append(f"Overlap found between good and fixable codes: {overlap}")
        if errors:
            raise ValueError(format_errors(*errors, pydantic=True))
        return code_fixes

    @field_validator("code_fixes")
    @classmethod
    def _fixable_and_ignored_codes_are_disjoint(cls, code_fixes, info: ValidationInfo):
        """Check that there's no overlap between the ignored and fixable codes."""
        if "ignored_codes" not in info.data:
            return code_fixes
        errors = []
        overlap = set(code_fixes).intersection(info.data["ignored_codes"])
        if overlap:
            errors.append(f"Overlap found between fixable and ignored codes: {overlap}")
        if errors:
            raise ValueError(format_errors(*errors, pydantic=True))
        return code_fixes

    @field_validator("code_fixes")
    @classmethod
    def _check_fixed_codes_are_good_codes(cls, code_fixes, info: ValidationInfo):
        """Check that every every fixed code is also one of the good codes."""
        if "df" not in info.data:
            return code_fixes
        errors = []
        bad_codes = set(code_fixes.values()).difference(info.data["df"]["code"])
        if bad_codes:
            errors.append(
                f"Some fixed codes aren't in the list of good codes: {bad_codes}"
            )
        if errors:
            raise ValueError(format_errors(*errors, pydantic=True))
        return code_fixes

    @property
    def code_map(self) -> dict[str, str | NAType]:
        """A mapping of all known codes to their standardized values, or NA."""
        code_map = {code: code for code in self.df["code"]}
        code_map.update(self.code_fixes)
        code_map.update(dict.fromkeys(self.ignored_codes, pd.NA))
        return code_map

    def encode(
        self,
        col: pd.Series,
        dtype: type | None = None,
    ) -> pd.Series:
        """Apply the stored code mapping to an input Series."""
        # Every value in the Series should appear in the map. If that's not the
        # case we want to hear about it so we don't wipe out data unknowingly.
        logger.info(f"Encoding {col.name}")
        unknown_codes = set(col.dropna()).difference(self.code_map)
        if unknown_codes:
            raise ValueError(
                f"Found unknown codes while encoding {col.name}: {unknown_codes=}"
            )
        col = col.map(self.code_map)
        if dtype:
            col = col.astype(dtype)

        return col

    @staticmethod
    def dict_from_id(x: str) -> dict:
        """Look up the encoder by coding table name in the metadata."""
        return copy.deepcopy(RESOURCE_METADATA[x]).get("encoder", None)

    @classmethod
    def from_id(cls, x: str) -> "Encoder":
        """Construct an Encoder based on `Resource.name` of a coding table."""
        return cls(**cls.dict_from_id(x))

    @classmethod
    def from_code_id(cls, x: str) -> "Encoder":
        """Construct an Encoder by looking up name of coding table in codes metadata."""
        return cls(**copy.deepcopy(CODE_METADATA[x]), name=x)

    def to_rst(
        self, top_dir: DirectoryPath, csv_subdir: DirectoryPath, is_header: StrictBool
    ) -> String:
        """Output dataframe to a csv for use in jinja template.

        Then output to an RST file.
        """
        self.df.to_csv(Path(top_dir) / csv_subdir / f"{self.name}.csv", index=False)
        template = _get_jinja_environment(top_dir).get_template(
            "codemetadata.rst.jinja"
        )
        rendered = template.render(
            Encoder=self,
            description=RESOURCE_METADATA[self.name]["description"],
            csv_filepath=(Path("/") / csv_subdir / f"{self.name}.csv"),
            is_header=is_header,
        )
        return rendered

    def generate_encodable_data(self: Self, size: int = 10) -> pd.Series:
        """Produce a series of data which can be encoded by this encoder.

        Selects values randomly from valid, ignored, and fixable codes.
        """
        rng = np.random.default_rng()

        return pd.Series(
            rng.choice(
                list(self.df["code"])
                + list(self.ignored_codes)
                + list(self.code_fixes),
                size=size,
            )
        )


class Field(PudlMeta):
    """Field (`resource.schema.fields[...]`).

    See https://specs.frictionlessdata.io/table-schema/#field-descriptors.

    Examples:
        >>> field = Field(name='x', type='string', description='X', constraints={'enum': ['x', 'y']})
        >>> field.to_pandas_dtype()
        CategoricalDtype(categories=['x', 'y'], ordered=False, categories_dtype=object)
        >>> field.to_sql()
        Column('x', Enum('x', 'y'), CheckConstraint(...), table=None, comment='X')
        >>> field = Field.from_id('utility_id_eia')
        >>> field.name
        'utility_id_eia'
    """

    name: SnakeCase
    # Shadows built-in type.
    type: Literal["string", "number", "integer", "boolean", "date", "datetime", "year"]
    title: String | None = None
    # Alias required to avoid shadowing Python built-in format()
    format_: Literal["default"] = pydantic.Field(alias="format", default="default")
    description: String
    unit: String | None = None
    constraints: FieldConstraints = FieldConstraints()
    harvest: FieldHarvest = FieldHarvest()
    encoder: Encoder | None = None

    @field_validator("constraints")
    @classmethod
    def _check_constraints(cls, value, info: ValidationInfo):  # noqa: C901
        if "type" not in info.data:
            return value
        dtype = info.data["type"]
        errors = []
        for key in ("min_length", "max_length", "pattern"):
            if getattr(value, key) is not None and dtype != "string":
                errors.append(f"{key} not supported by {dtype} field")
        for key in ("minimum", "maximum"):
            x = getattr(value, key)
            if x is not None:
                if dtype in ("string", "boolean"):
                    errors.append(f"{key} not supported by {dtype} field")
                elif not isinstance(x, CONSTRAINT_DTYPES[dtype]):
                    errors.append(f"{key} not {dtype}")
        if value.enum:
            for x in value.enum:
                if not isinstance(x, CONSTRAINT_DTYPES[dtype]):
                    errors.append(f"enum value {x} not {dtype}")
        if errors:
            raise ValueError(format_errors(*errors, pydantic=True))
        return value

    @field_validator("encoder")
    @classmethod
    def _check_encoder(cls, value, info: ValidationInfo):
        if "type" not in info.data or value is None:
            return value
        errors = []
        dtype = info.data["type"]
        if dtype not in ["string", "integer"]:
            errors.append(
                f"Encoding only supported for string and integer fields, found {dtype}"
            )
        if errors:
            raise ValueError(format_errors(*errors, pydantic=True))
        return value

    @staticmethod
    def dict_from_id(x: str) -> dict:
        """Construct dictionary from PUDL identifier (`Field.name`)."""
        return {"name": x, **copy.deepcopy(FIELD_METADATA[x])}

    @classmethod
    def from_id(cls, x: str) -> "Field":
        """Construct from PUDL identifier (`Field.name`)."""
        return cls(**cls.dict_from_id(x))

    def to_pandas_dtype(self, compact: bool = False) -> str | pd.CategoricalDtype:
        """Return Pandas data type.

        Args:
            compact: Whether to return a low-memory data type (32-bit integer or float).
        """
        if self.constraints.enum:
            return pd.CategoricalDtype(self.constraints.enum)
        if compact:
            if self.type == "integer":
                return "Int32"
            if self.type == "number":
                return "float32"
        return FIELD_DTYPES_PANDAS[self.type]

    def to_sql_dtype(self) -> type:  # noqa: A003
        """Return SQLAlchemy data type."""
        if self.constraints.enum and self.type == "string":
            return sa.Enum(*self.constraints.enum)
        return FIELD_DTYPES_SQL[self.type]

    def to_pyarrow_dtype(self) -> pa.lib.DataType:
        """Return PyArrow data type."""
        if self.constraints.enum and self.type == "string":
            return pa.dictionary(pa.int32(), pa.string(), ordered=False)
        return FIELD_DTYPES_PYARROW[self.type]

    def to_pyarrow(self) -> pa.Field:
        """Return a PyArrow Field appropriate to the field."""
        return pa.field(
            name=self.name,
            type=self.to_pyarrow_dtype(),
            nullable=(not self.constraints.required),
            metadata={"description": self.description},
        )

    def to_sql(  # noqa: C901
        self,
        dialect: Literal["sqlite"] = "sqlite",
        check_types: bool = True,
        check_values: bool = True,
    ) -> sa.Column:
        """Return equivalent SQL column."""
        if dialect != "sqlite":
            raise NotImplementedError(f"Dialect {dialect} is not supported")
        checks = []
        name = _format_for_sql(self.name, identifier=True)
        if check_types:
            # Required with TYPEOF since TYPEOF(NULL) = 'null'
            prefix = "" if self.constraints.required else f"{name} IS NULL OR "
            # Field type
            if self.type == "string":
                checks.append(f"{prefix}TYPEOF({name}) = 'text'")
            elif self.type in ("integer", "year"):
                checks.append(f"{prefix}TYPEOF({name}) = 'integer'")
            elif self.type == "number":
                checks.append(f"{prefix}TYPEOF({name}) = 'real'")
            elif self.type == "boolean":
                # Just IN (0, 1) accepts floats equal to 0, 1 (0.0, 1.0)
                checks.append(
                    f"{prefix}(TYPEOF({name}) = 'integer' AND {name} IN (0, 1))"
                )
            elif self.type == "date":
                checks.append(f"{name} IS DATE({name})")
            elif self.type == "datetime":
                checks.append(f"{name} IS DATETIME({name})")
        if check_values:
            # Field constraints
            if self.constraints.min_length is not None:
                checks.append(f"LENGTH({name}) >= {self.constraints.min_length}")
            if self.constraints.max_length is not None:
                checks.append(f"LENGTH({name}) <= {self.constraints.max_length}")
            if self.constraints.minimum is not None:
                minimum = _format_for_sql(self.constraints.minimum)
                checks.append(f"{name} >= {minimum}")
            if self.constraints.maximum is not None:
                maximum = _format_for_sql(self.constraints.maximum)
                checks.append(f"{name} <= {maximum}")
            if self.constraints.pattern:
                pattern = _format_for_sql(self.constraints.pattern)
                # Need to escape colons in regex to avoid this issue:
                # https://github.com/sqlalchemy/sqlalchemy/discussions/12498
                checks.append(f"{name} REGEXP {pattern.replace(':', r'\:')}")
            if self.constraints.enum:
                enum = [_format_for_sql(x) for x in self.constraints.enum]
                checks.append(f"{name} IN ({', '.join(enum)})")
        return sa.Column(
            self.name,
            self.to_sql_dtype(),
            *[
                sa.CheckConstraint(
                    check,
                    name=sha1(check.encode("utf-8")).hexdigest()[:8],  # noqa: S324
                )
                for check in checks
            ],
            nullable=not self.constraints.required,
            unique=self.constraints.unique,
            comment=self.description,
        )

    def encode(self, col: pd.Series, dtype: type | None = None) -> pd.Series:  # noqa: A003
        """Recode the Field if it has an associated encoder."""
        return self.encoder.encode(col, dtype=dtype) if self.encoder else col

    def to_pandera_column(self) -> pr.Column:
        """Encode this field def as a Pandera column."""
        constraints = self.constraints
        checks = constraints.to_pandera_checks()
        column_type = "category" if constraints.enum else FIELD_DTYPES_PANDAS[self.type]

        return pr.Column(
            column_type,
            checks=checks,
            nullable=not constraints.required,
            unique=constraints.unique,
        )


# ---- Classes: Resource ---- #


class ForeignKeyReference(PudlMeta):
    """Foreign key reference (`resource.schema.foreign_keys[...].reference`).

    See https://specs.frictionlessdata.io/table-schema/#foreign-keys.
    """

    resource: SnakeCase
    fields: StrictList[SnakeCase]

    _check_unique = _validator("fields", fn=_check_unique)


class ForeignKey(PudlMeta):
    """Foreign key (`resource.schema.foreign_keys[...]`).

    See https://specs.frictionlessdata.io/table-schema/#foreign-keys.
    """

    fields: StrictList[SnakeCase]
    reference: ForeignKeyReference

    _check_unique = _validator("fields", fn=_check_unique)

    @field_validator("reference")
    @classmethod
    def _check_fields_equal_length(cls, value, info: ValidationInfo):
        if "fields" in info.data and len(value.fields) != len(info.data["fields"]):
            raise ValueError("fields and reference.fields are not equal length")
        return value

    def is_simple(self) -> bool:
        """Indicate whether the FK relationship contains a single column."""
        return len(self.fields) == 1

    def to_sql(self) -> sa.ForeignKeyConstraint:
        """Return equivalent SQL Foreign Key."""
        return sa.ForeignKeyConstraint(
            self.fields,
            [f"{self.reference.resource}.{field}" for field in self.reference.fields],
        )


class Schema(PudlMeta):
    """Table schema (`resource.schema`).

    See https://specs.frictionlessdata.io/table-schema.
    """

    fields: StrictList[Field]
    missing_values: list[StrictStr] = [""]
    primary_key: list[SnakeCase] = []
    foreign_keys: list[ForeignKey] = []

    _check_unique = _validator(
        "missing_values", "primary_key", "foreign_keys", fn=_check_unique
    )

    @field_validator("fields")
    @classmethod
    def _check_field_names_unique(cls, fields: list[Field]):
        _check_unique([f.name for f in fields])
        return fields

    @field_validator("primary_key")
    @classmethod
    def _check_primary_key_in_fields(cls, pk, info: ValidationInfo):
        """Verify that all primary key elements also appear in the schema fields."""
        if pk is not None and "fields" in info.data:
            missing = []
            names = [f.name for f in info.data["fields"]]
            for name in pk:
                if name in names:
                    # Flag primary key fields as required
                    field = info.data["fields"][names.index(name)]
                    field.constraints.required = True
                else:
                    missing.append(name)
            if missing:
                raise ValueError(f"names {missing} missing from fields")
        return pk

    @model_validator(mode="after")
    def _check_foreign_key_in_fields(self: Self):
        """Verify that all foreign key elements also appear in the schema fields."""
        if self.foreign_keys:
            schema_field_names = [field.name for field in self.fields]
            for fk in self.foreign_keys:
                missing_field_names = set(fk.fields).difference(schema_field_names)
                if missing_field_names:
                    raise ValueError(
                        f"Foreign key fields {missing_field_names} not found in schema."
                    )
        return self

    def to_pandera(self: Self) -> pr.DataFrameSchema:
        """Turn PUDL Schema into Pandera schema, so dagster can understand it."""
        # 2024-02-09: pr.Check doesn't have interop with Pydantic type system
        # yet, so we encode as Callable, then cast.

        return pr.DataFrameSchema(
            {field.name: field.to_pandera_column() for field in self.fields},
            unique=self.primary_key,
        )


class License(PudlMeta):
    """Data license (`package|resource.licenses[...]`).

    See https://specs.frictionlessdata.io/data-package/#licenses.
    """

    name: String
    title: String
    path: AnyHttpUrl

    @staticmethod
    def dict_from_id(x: str) -> dict:
        """Construct dictionary from PUDL identifier."""
        return copy.deepcopy(LICENSES[x])

    @classmethod
    def from_id(cls, x: str) -> "License":
        """Construct from PUDL identifier."""
        return cls(**cls.dict_from_id(x))


class Contributor(PudlMeta):
    """Data contributor (`package.contributors[...]`).

    See https://specs.frictionlessdata.io/data-package/#contributors.
    """

    title: String
    path: AnyHttpUrl | None = None
    email: EmailStr | None = None
    role: Literal["author", "contributor", "maintainer", "publisher", "wrangler"] = (
        "contributor"
    )
    zenodo_role: Literal[
        "contact person",
        "data collector",
        "data curator",
        "data manager",
        "distributor",
        "editor",
        "hosting institution",
        "other",
        "producer",
        "project leader",
        "project member",
        "registration agency",
        "registration authority",
        "related person",
        "researcher",
        "rights holder",
        "sponsor",
        "supervisor",
        "work package leader",
    ] = "project member"
    organization: String | None = None
    orcid: String | None = None

    @staticmethod
    def dict_from_id(x: str) -> dict:
        """Construct dictionary from PUDL identifier."""
        return copy.deepcopy(CONTRIBUTORS[x])

    @classmethod
    def from_id(cls, x: str) -> "Contributor":
        """Construct from PUDL identifier."""
        return cls(**cls.dict_from_id(x))

    def __hash__(self):
        """Implements simple hash method.

        Allows use of `set()` on a list of Contributor
        """
        return hash(str(self))


class DataSource(PudlMeta):
    """A data source that has been integrated into PUDL.

    This metadata is used for:

    * Generating PUDL documentation.
    * Annotating long-term archives of the raw input data on Zenodo.
    * Defining what data partitions can be processed using PUDL.

    It can also be used to populate the "source" fields of frictionless
    data packages and data resources (`package|resource.sources[...]`).

    See https://specs.frictionlessdata.io/data-package/#sources.
    """

    name: SnakeCase
    title: String | None = None
    description: String | None = None
    field_namespace: String | None = None
    keywords: list[str] = []
    path: AnyHttpUrl | None = None
    contributors: list[Contributor] = []
    license_raw: License
    license_pudl: License
    concept_doi: ZenodoDoi | None = None
    working_partitions: dict[SnakeCase, Any] = {}
    source_file_dict: dict[SnakeCase, Any] = {}
    # agency: Agency  # needs to be defined
    email: EmailStr | None = None

    def get_resource_ids(self) -> list[str]:
        """Compile list of resource IDs associated with this data source."""
        resources = RESOURCE_METADATA

        return sorted(
            name
            for name, value in resources.items()
            if value.get("etl_group") == self.name
        )

    def get_temporal_coverage(self, partitions: dict = None) -> str:
        """Return a string describing the time span covered by the data source."""
        if partitions is None:
            partitions = self.working_partitions
        if "years" in partitions:
            temporal_coverage = f"{min(partitions['years'])}-{max(partitions['years'])}"
        elif "half_years" in partitions:
            temporal_coverage = (
                f"{min(partitions['half_years'])}-{max(partitions['half_years'])}"
            )
        elif "year_quarters" in partitions:
            temporal_coverage = (
                f"{min(partitions['year_quarters'])}-{max(partitions['year_quarters'])}"
            )
        elif "year_month" in partitions:
            temporal_coverage = f"through {partitions['year_month']}"
        else:
            temporal_coverage = ""
        return temporal_coverage

    def add_datastore_metadata(self) -> None:
        """Get source file metadata from the datastore."""
        dp_desc = Datastore(
            local_cache_path=PudlPaths().data_dir,
            gcs_cache_path="gs://zenodo-cache.catalyst.coop",
        ).get_datapackage_descriptor(self.name)
        partitions = dp_desc.get_partitions()
        if "year" in partitions:
            partitions["years"] = partitions["year"]
        elif "year_month" in partitions:
            partitions["year_month"] = max(partitions["year_month"])
        self.source_file_dict["download_size"] = dp_desc.get_download_size()

    def to_rst(
        self,
        docs_dir: DirectoryPath,
        source_resources: list,
        extra_resources: list,
        output_path: str = None,
    ) -> None:
        """Output a representation of the data source in RST for documentation."""
        self.add_datastore_metadata()
        template = _get_jinja_environment(docs_dir).get_template(
            f"{self.name}_child.rst.jinja"
        )
        data_source_dir = docs_dir / "data_sources"
        download_paths = [
            path.relative_to(data_source_dir)
            for path in (
                list((data_source_dir / self.name).glob("*.pdf"))
                + list((data_source_dir / self.name).glob("*.html"))
            )
            if path.is_file()
        ]
        # If PHMSA, also include .txt files in documentation
        if self.name == "phmsagas":
            download_paths += [
                path.relative_to(data_source_dir)
                for path in (list((data_source_dir / self.name).glob("*.txt")))
                if path.is_file()
            ]
        download_paths = sorted(download_paths)
        rendered = template.render(
            source=self,
            source_resources=source_resources,
            extra_resources=extra_resources,
            download_paths=download_paths,
        )
        if output_path:
            Path(output_path).write_text(rendered)
        else:
            sys.stdout.write(rendered)

    @classmethod
    def from_field_namespace(
        cls, x: str, sources: dict[str, Any] = SOURCES
    ) -> list["DataSource"]:
        """Return list of DataSource objects by field namespace."""
        return [
            cls(**cls.dict_from_id(name, sources))
            for name, val in sources.items()
            if val.get("field_namespace") == x
        ]

    @staticmethod
    def dict_from_id(x: str, sources: dict[str, Any]) -> dict:
        """Look up the source by source name in the metadata."""
        # If ID ends with _xbrl strip end to find data source
        lookup_id = x.replace("_xbrl", "")
        return {"name": x, **copy.deepcopy(sources[lookup_id])}

    @classmethod
    def from_id(cls, x: str, sources: dict[str, Any] = SOURCES) -> "DataSource":
        """Construct Source by source name in the metadata."""
        return cls(**cls.dict_from_id(x, sources=sources))


class ResourceHarvest(PudlMeta):
    """Resource harvest parameters (`resource.harvest`)."""

    harvest: StrictBool = False
    """Whether to harvest from dataframes based on field names.

    If `False`, the dataframe with the same name is used and the process is limited to
    dropping unwanted fields.
    """

    tolerance: PositiveFloat = 0.0
    """Fraction of invalid fields above which result is considered invalid."""


class PudlResourceDescriptor(PudlMeta):
    """The form we expect the RESOURCE_METADATA elements to take.

    This differs from :class:`Resource` and :class:`Schema`, etc., in that we represent
    many complex types (:class:`Field`, :class:`DataSource`, etc.) with string IDs that
    we then turn into instances of those types with lookups. We also use
    ``foreign_key_rules`` to generate the actual ``foreign_key`` relationships that are
    represented in a :class:`Schema`.

    This is all very useful in that we can describe the resources more concisely!

    TODO: In the future, we could convert from a :class:`PudlResourceDescriptor` to
    various standard formats, such as a Frictionless resource or a :mod:`pandera`
    schema. This would require some of the logic currently in :class:`Resource` to move
    into this class.
    """

    class PudlSchemaDescriptor(PudlMeta):
        """Container to hold the schema shape."""

        class PudlForeignKeyRules(PudlMeta):
            """Container to describe what foreign key rules look like."""

            field_id_lists: list[list[str]] = pydantic.Field(alias="fields", default=[])
            exclude_ids: list[str] = pydantic.Field(alias="exclude", default=[])

        field_ids: list[str] = pydantic.Field(alias="fields", default=[])
        primary_key_ids: list[str] = pydantic.Field(alias="primary_key", default=[])
        foreign_key_rules: PudlForeignKeyRules = PudlForeignKeyRules()

    class PudlCodeMetadata(PudlMeta):
        """Describes a bunch of codes."""

        class CodeDataFrame(pr.DataFrameModel):
            """The DF we use to represent code/label/description associations."""

            # TODO (daz) 2024-02-09: each of these | Nones are one-offs. Fix
            # the frickin data instead.
            code: pr.typing.Series[Any]
            label: pr.typing.Series[str] | None
            description: pr.typing.Series[str]
            operational_status: pr.typing.Series[str] | None

        df: pr.typing.DataFrame[CodeDataFrame] = pd.DataFrame(
            {"code": [], "label": [], "description": []}
        )
        code_fixes: dict = {}
        ignored_codes: list = []

    class PudlDescriptionComponents(PudlMeta):
        """Container to hold description configuration information.

        All of these parameters have reasonable defaults for most resources if left unset.
        You must specify :attr:`PudlResourceDescriptor.description` as a dictionary, but you do not have to put anything in it so long as the resource id follows the standard pattern.
        """

        table_type_code: (
            Literal["assn", "codes", "entity", "scd", "timeseries"] | None
        ) = None
        """Indicates the type of asset stored in this resource.
        If None or otherwise left unset, will be filled in with a default type parsed from the resource id string."""
        timeseries_resolution_code: (
            Literal[
                "quarterly",
                "yearly",
                "monthly",
                "hourly",
            ]
            | None
        ) = None
        """If this resource has :attr:`~PudlResourceDescriptor.PudlDescriptionComponents.table_type_code` timeseries, indicates the temporal resolution, otherwise None.
        If :attr:`~PudlResourceDescriptor.PudlDescriptionComponents.table_type_code` is timeseries and this value is None or otherwise left unset, will be filled in with a default resolution parsed from the resource id string."""
        layer_code: Literal["raw", "_core", "core", "out", "test"] | None = None
        """Indicates the degree of processing applied to the data in this resource.
        If None or otherwise left unset, will be filled in with a default layer parsed from the resource id string."""
        source_code: str | None = None
        """Indicates the source we wish to display for this resource; distinct from :attr:`PudlResourceDescriptor.source_ids` because here we want the majority source (or grouped source if truly mixed) and not a complete list of all sources used for this resource.
        If set, should be a known data source shortcode like "eia923" or one of the grouped shortcodes from :data:`~pudl.metadata.descriptions.source_descriptions`.
        If None or otherwise left unset, will be filled in with a default source parsed from the resource id string."""
        usage_warnings: list[str | dict] | None = None
        """List of string keys (for common warnings; see :mod:`warnings`) and dicts (for custom warnings) stating necessary precautions for using this resource.

        Usage Warnings are a way for us to quickly and skim-ably tell users about analysis hazards when using a particular table.
        It has two goals:

        1. help users quickly reach a point of success in their use of our data, and
        2. reduce the incidence of repeated questions and bug-like reports due to these inescapable hazards.

        Reserve this field for severe and/or frequent problems an unfamiliar user may encounter, and list lighter or edge-case problems in :attr:`~PudlResourceDescriptor.PudlDescriptionComponents.additional_details_text`.

        The list can contain two kinds of entries:

        * a string, which should match one of the keys in :data:`~pudl.metadata.warnings.USAGE_WARNINGS`
        * a dict, which should contain two keys:

          * "type" - a short code for the warning, which doesn't need to be unique and will only appear in preview & debugging tooling, not to users
          * "description" - the one-to-two-sentence summary of a warning used only on this particular resource

        The system will automatically detect and include the following warnings based on the resource id string and schema information (see :meth:`~pudl.metadata.descriptions.ResourceDescriptionBuilder._assemble_usage_warnings`):

        * multiple_inputs
        * ferc_is_hard

        Any items provided here will be listed before the automatically detected warnings.

        If None or otherwise left unset, will be filled in with auto warnings only. If no auto warnings apply, hides the Usage Warnings section entirely."""
        additional_summary_text: str | None = None
        """A brief (~one-line) description of the contents of this resource.
        If None or otherwise left unset, will be left blank.

        If filled, should support whichever of the following scenarios is most appropriate for this resource:

        * the :attr:`~PudlResourceDescriptor.PudlDescriptionComponents.table_type_code` is set or can be automatically detected: this value should complete the sentence corresponding to :data:`~pudl.metadata.descriptions.table_type_fragments` for this resource's :attr:`~PudlResourceDescriptor.PudlDescriptionComponents.table_type_code`
        * the :attr:`~PudlResourceDescriptor.PudlDescriptionComponents.table_type_code` is None/unset *and* the resource is not named according to a standard table type listed in :data:`~pudl.metadata.descriptions.table_type_fragments`: this value should be a complete sentence summarizing the contents of this resource at a similar level of detail.
        """
        additional_layer_text: str | None = None
        """Unusual details about this resource's level of processing that don't fall into the normal definition of raw/core/_core/out/etc.
        If None or otherwise left unset, will be left blank.
        This should only be set in truly obscure situations. If set, should be a complete sentence."""
        additional_source_text: str | None = None
        """A brief refinement on the source data for this table, such as indicating the Schedule or other section number.
        If None or otherwise left unset, will be left blank.
        If set, should make sense when displayed directly after the title of a datasource (see :data:`~pudl.metadata.descriptions.source_descriptions`); parentheticals work best here."""
        additional_primary_key_text: str | None = None
        """For resources with no primary key, a brief summary of what each row contains, and perhaps why a primary key doesn't make sense for this table.
        If None or otherwise left unset, will be left blank.
        If set, should be a complete sentence or two.

        This is generally not set when there is a primary key for the table.
        If a primary key is available, :attr:`~PudlResourceDescriptor.PudlDescriptionComponents.additional_primary_key_text` will appear after the comma-delimited list of primary key columns."""
        additional_details_text: str | None = None
        """All other information about this resource's construction and intended use, including guidelines and recommendations for best results.
        If None or otherwise left unset, will be left blank; hides the Additional Details section entirely.

        Q3 2025 Migration Mode variance: if :attr:`PudlResourceDescriptor.description` is a string, it gets moved here so you can see the old description content in the Additional Details section of the preview.

        May also include more-detailed explanations of listed usage warnings."""  # TODO: drop migration mode variance after migration is complete

    # TODO (daz) 2024-02-09: with a name like "title" you might imagine all
    # resources would have one...
    title: str | None = None
    # TODO: the str type is legacy support and should be removed once we get all the metadata migrated
    description: PudlDescriptionComponents | str
    schema_: PudlSchemaDescriptor = pydantic.Field(alias="schema")
    encoder: PudlCodeMetadata | None = None
    source_ids: list[str] = pydantic.Field(alias="sources")
    etl_group_id: str = pydantic.Field(alias="etl_group")
    field_namespace_id: str = pydantic.Field(alias="field_namespace")
    create_database_schema: bool = True


class Resource(PudlMeta):
    """Tabular data resource (`package.resources[...]`).

    See https://specs.frictionlessdata.io/tabular-data-resource.

    Examples:
        A simple example illustrates the conversion to SQLAlchemy objects.

        >>> fields = [{'name': 'x', 'type': 'year', 'description': 'X'}, {'name': 'y', 'type': 'string', 'description': 'Y'}]
        >>> fkeys = [{'fields': ['x', 'y'], 'reference': {'resource': 'b', 'fields': ['x', 'y']}}]
        >>> schema = {'fields': fields, 'primary_key': ['x'], 'foreign_keys': fkeys}
        >>> resource = Resource(name='a', schema=schema, description='A')
        >>> table = resource.to_sql()
        >>> table.columns.x
        Column('x', Integer(), ForeignKey('b.x'), CheckConstraint(...), table=<a>, primary_key=True, nullable=False, comment='X')
        >>> table.columns.y
        Column('y', Text(), ForeignKey('b.y'), CheckConstraint(...), table=<a>, comment='Y')

        To illustrate harvesting operations,
        say we have a resource with two fields - a primary key (`id`) and a data field -
        which we want to harvest from two different dataframes.

        >>> from pudl.metadata.helpers import unique, as_dict
        >>> fields = [
        ...     {'name': 'id', 'type': 'integer', 'description': 'ID'},
        ...     {'name': 'x', 'type': 'integer', 'harvest': {'aggregate': unique, 'tolerance': 0.25}, 'description': 'X'}
        ... ]
        >>> resource = Resource(**{
        ...     'name': 'a',
        ...     'harvest': {'harvest': True},
        ...     'schema': {'fields': fields, 'primary_key': ['id']},
        ...     'description': 'A',
        ... })
        >>> dfs = {
        ...     'a': pd.DataFrame({'id': [1, 1, 2, 2], 'x': [1, 1, 2, 2]}),
        ...     'b': pd.DataFrame({'id': [2, 3, 3], 'x': [3, 4, 4]})
        ... }

        Skip aggregation to access all the rows concatenated from the input dataframes.
        The names of the input dataframes are used as the index.

        >>> df, _ = resource.harvest_dfs(dfs, aggregate=False)
        >>> df
            id  x
        df
        a    1  1
        a    1  1
        a    2  2
        a    2  2
        b    2  3
        b    3  4
        b    3  4

        Field names and data types are enforced.

        >>> resource.to_pandas_dtypes() == df.dtypes.apply(str).to_dict()
        True

        Alternatively, aggregate by primary key
        (the default when :attr:`harvest`. `harvest=True`)
        and report aggregation errors.

        >>> df, report = resource.harvest_dfs(dfs)
        >>> df
               x
        id
        1      1
        2   <NA>
        3      4
        >>> report['stats']
        {'all': 2, 'invalid': 1, 'tolerance': 0.0, 'actual': 0.5}
        >>> report['fields']['x']['stats']
        {'all': 3, 'invalid': 1, 'tolerance': 0.25, 'actual': 0.33...}
        >>> report['fields']['x']['errors']
        id
        2    Not unique.
        Name: x, dtype: object

        Customize the error values in the error report.

        >>> error = lambda x, e: as_dict(x)
        >>> df, report = resource.harvest_dfs(
        ...    dfs, aggregate_kwargs={'raised': False, 'error': error}
        ... )
        >>> report['fields']['x']['errors']
        id
        2    {'a': [2, 2], 'b': [3]}
        Name: x, dtype: object

        Limit harvesting to the input dataframe of the same name
        by setting :attr:`harvest`. `harvest=False`.

        >>> resource.harvest.harvest = False
        >>> df, _ = resource.harvest_dfs(dfs, aggregate_kwargs={'raised': False})
        >>> df
            id  x
        df
        a    1  1
        a    1  1
        a    2  2
        a    2  2

        Harvesting can also handle conversion to longer time periods.
        Period harvesting requires primary key fields with a `datetime` data type,
        except for `year` fields which can be integer.

        >>> fields = [{'name': 'report_year', 'type': 'year', 'description': 'Report year'}]
        >>> resource = Resource(**{
        ...     'name': 'table', 'harvest': {'harvest': True},
        ...     'schema': {'fields': fields, 'primary_key': ['report_year']},
        ...     'description': 'Table',
        ... })
        >>> df = pd.DataFrame({'report_date': ['2000-02-02', '2000-03-03']})
        >>> resource.format_df(df)
          report_year
        0  2000-01-01
        1  2000-01-01
        >>> df = pd.DataFrame({'report_year': [2000, 2000]})
        >>> resource.format_df(df)
          report_year
        0  2000-01-01
        1  2000-01-01
    """

    name: SnakeCase
    title: String | None = None
    description: String
    harvest: ResourceHarvest = ResourceHarvest()
    schema: Schema
    # Alias required to avoid shadowing Python built-in format()
    format_: String | None = pydantic.Field(alias="format", default=None)
    mediatype: String | None = None
    path: String | None = None
    dialect: dict[str, str] | None = None
    profile: String = "tabular-data-resource"
    contributors: list[Contributor] = []
    licenses: list[License] = []
    sources: list[DataSource] = []
    keywords: list[String] = []
    encoder: Encoder | None = None
    field_namespace: (
        Literal[
            "eia",
            "eiaaeo",
            "eiaapi",
            "epacems",
            "ferc1",
            "ferc714",
            "glue",
            "gridpathratoolkit",
            "ppe",
            "pudl",
            "nrelatb",
            "vcerare",
            "sec",
        ]
        | None
    ) = None
    etl_group: (
        Literal[
            "eia860",
            "eia861",
            "eia861_disabled",
            "eia923",
            "eia930",
            "eiaaeo",
            "entity_eia",
            "epacems",
            "ferc1",
            "ferc1_disabled",
            "ferc714",
            "glue",
            "gridpathratoolkit",
            "outputs",
            "static_ferc1",
            "static_eia",
            "static_eia_disabled",
            "eiaapi",
            "state_demand",
            "static_pudl",
            "service_territories",
            "nrelatb",
            "vcerare",
            "sec10k",
        ]
        | None
    ) = None
    create_database_schema: bool = True

    _check_unique = _validator(
        "contributors", "keywords", "licenses", "sources", fn=_check_unique
    )

    @property
    def sphinx_ref_name(self):
        """Get legal Sphinx ref name.

        Sphinx throws an error when creating a cross ref target for
        a resource that has a preceding underscore. It is
        also possible for resources to have identical names
        when the preceding underscore is removed. This function
        adds a preceding 'i' to cross ref targets for resources
        with preceding underscores. The 'i' will not be rendered
        in the docs, only in the .rst files the hyperlinks.
        """
        if self.name.startswith("_"):
            return f"i{self.name}"
        return self.name

    @field_validator("schema")
    @classmethod
    def _check_harvest_primary_key(cls, value, info: ValidationInfo):
        if info.data["harvest"].harvest and not value.primary_key:
            raise ValueError("Harvesting requires a primary key")
        return value

    @staticmethod
    def dict_from_id(resource_id: str) -> dict:
        """Construct dictionary from PUDL identifier (`resource.name`)."""
        descriptor = PudlResourceDescriptor.model_validate(
            RESOURCE_METADATA[resource_id]
        )
        return Resource.dict_from_resource_descriptor(resource_id, descriptor)

    @staticmethod
    def dict_from_resource_descriptor(  # noqa: C901
        resource_id: str,
        descriptor: PudlResourceDescriptor,
    ) -> dict:
        """Get a Resource-shaped dict from a PudlResourceDescriptor.

        * `schema.fields`

          * Field names are expanded (:meth:`Field.from_id`).
          * Field attributes are replaced with any specific to the
            `resource.group` and `field.name`.

        * `sources`: Source ids are expanded (:meth:`Source.from_id`).
        * `licenses`: License ids are expanded (:meth:`License.from_id`).
        * `contributors`: Contributor ids are fetched by source ids,
          then expanded (:meth:`Contributor.from_id`).
        * `keywords`: Keywords are fetched by source ids.
        * `schema.foreign_keys`: Foreign keys are fetched by resource name.
        * `description`: Full description text block is rendered from its component parts.
        """
        obj = descriptor.model_dump(by_alias=True)
        obj["name"] = resource_id
        schema = obj["schema"]
        # Expand fields
        if "fields" in schema:
            fields = []
            for name in schema["fields"]:
                # Lookup field by name
                value = Field.dict_from_id(name)
                # Update with any custom group-level metadata
                namespace = obj.get("field_namespace")
                if name in FIELD_METADATA_BY_GROUP.get(namespace, {}):
                    value = {**value, **FIELD_METADATA_BY_GROUP[namespace][name]}
                # Update with any custom resource-level metadata
                if name in FIELD_METADATA_BY_RESOURCE.get(resource_id, {}):
                    value = {**value, **FIELD_METADATA_BY_RESOURCE[resource_id][name]}
                fields.append(value)
            schema["fields"] = fields
        # Expand sources
        sources = obj.get("sources", [])
        obj["sources"] = [
            DataSource.from_id(value) for value in sources if value in SOURCES
        ]
        # Expand licenses (assign CC-BY-4.0 by default)
        licenses = obj.get("licenses", ["cc-by-4.0"])
        obj["licenses"] = [License.dict_from_id(value) for value in licenses]
        # Lookup and insert contributors
        if "contributors" in schema:
            raise ValueError("Resource metadata contains explicit contributors")
        contributors = []
        for source in sources:
            if source in SOURCES:
                contributors.extend(DataSource.from_id(source).contributors)
        obj["contributors"] = set(contributors)
        # Lookup and insert keywords
        if "keywords" in schema:
            raise ValueError("Resource metadata contains explicit keywords")
        keywords = []
        for source in sources:
            if source in SOURCES:
                keywords.extend(DataSource.from_id(source).keywords)
        obj["keywords"] = sorted(set(keywords))
        # Insert foreign keys
        schema["foreign_keys"] = FOREIGN_KEYS.get(resource_id, [])
        # Delete foreign key rules
        if "foreign_key_rules" in schema:
            del schema["foreign_key_rules"]

        # overwrite description components with rendered text block
        obj["description"] = descriptions.ResourceDescriptionBuilder(
            resource_id,
            obj,
        ).build(_get_jinja_environment())

        # Add encoders to columns as appropriate, based on FKs.
        # Foreign key relationships determine the set of codes to use
        for fk in obj["schema"]["foreign_keys"]:
            # Only referenced tables with an associated encoder indicate
            # that the column we're looking at should have an encoder
            # attached to it. All of these FK relationships must have simple
            # single-column keys.
            encoder = Encoder.dict_from_id(fk["reference"]["resource"])
            if len(fk["fields"]) != 1 and encoder:
                raise ValueError(
                    "Encoder for table with a composite primary key: "
                    f"{fk['reference']['resource']}"
                )
            if len(fk["fields"]) == 1 and encoder:
                # fk["fields"] is a one element list, get the one element:
                field = fk["fields"][0]
                for f in obj["schema"]["fields"]:
                    if f["name"] == field:
                        f["encoder"] = encoder
                        break

        return obj

    @classmethod
    def from_id(cls, x: str) -> "Resource":
        """Construct from PUDL identifier (`resource.name`)."""
        return cls(**cls.dict_from_id(x))

    def get_field(self, name: str) -> Field:
        """Return field with the given name if it's part of the Resources."""
        names = [field.name for field in self.schema.fields]
        if name not in names:
            raise KeyError(f"The field {name} is not part of the {self.name} schema.")
        return self.schema.fields[names.index(name)]

    def get_field_names(self) -> list[str]:
        """Return a list of all the field names in the resource schema."""
        return [field.name for field in self.schema.fields]

    def to_sql(
        self,
        metadata: sa.MetaData = None,
        check_types: bool = True,
        check_values: bool = True,
    ) -> sa.Table:
        """Return equivalent SQL Table."""
        if metadata is None:
            metadata = sa.MetaData()
        columns = [
            f.to_sql(
                check_types=check_types,
                check_values=check_values,
            )
            for f in self.schema.fields
        ]
        constraints = []
        if self.schema.primary_key:
            constraints.append(sa.PrimaryKeyConstraint(*self.schema.primary_key))
        for key in self.schema.foreign_keys:
            constraints.append(key.to_sql())
        return sa.Table(self.name, metadata, *columns, *constraints)

    def to_frictionless(self) -> frictionless.Resource:
        """Convert to a Frictionless Resource."""
        schema = frictionless.Schema(
            fields=[
                frictionless.Field(name=f.name, description=f.description)
                for f in self.schema.fields
            ],
            primary_key=self.schema.primary_key,
        )
        return frictionless.Resource(
            name=self.name,
            description=self.description,
            schema=schema,
            path=f"{self.name}.parquet",
        )

    def to_pyarrow(self) -> pa.Schema:
        """Construct a PyArrow schema for the resource."""
        fields = [field.to_pyarrow() for field in self.schema.fields]
        metadata = {"description": self.description}
        if self.schema.primary_key is not None:
            metadata |= {"primary_key": ",".join(self.schema.primary_key)}
        return pa.schema(fields=fields, metadata=metadata)

    def to_pandas_dtypes(self, **kwargs: Any) -> dict[str, str | pd.CategoricalDtype]:
        """Return Pandas data type of each field by field name.

        Args:
            kwargs: Arguments to :meth:`Field.to_pandas_dtype`.
        """
        return {f.name: f.to_pandas_dtype(**kwargs) for f in self.schema.fields}

    def match_primary_key(self, names: Iterable[str]) -> dict[str, str] | None:
        """Match primary key fields to input field names.

        An exact match is required unless :attr:`harvest` .`harvest=True`,
        in which case periodic names may also match a basename with a smaller period.

        Args:
            names: Field names.

        Raises:
            ValueError: Field names are not unique.
            ValueError: Multiple field names match primary key field.

        Returns:
            The name matching each primary key field (if any) as a :class:`dict`,
            or `None` if not all primary key fields have a match.

        Examples:
            >>> fields = [{'name': 'x_year', 'type': 'year', 'description': 'Year'}]
            >>> schema = {'fields': fields, 'primary_key': ['x_year']}
            >>> resource = Resource(name='r', schema=schema, description='R')

            By default, when :attr:`harvest` .`harvest=False`,
            exact matches are required.

            >>> resource.harvest.harvest
            False
            >>> resource.match_primary_key(['x_month']) is None
            True
            >>> resource.match_primary_key(['x_year', 'x_month'])
            {'x_year': 'x_year'}

            When :attr:`harvest` .`harvest=True`,
            in the absence of an exact match,
            periodic names may also match a basename with a smaller period.

            >>> resource.harvest.harvest = True
            >>> resource.match_primary_key(['x_year', 'x_month'])
            {'x_year': 'x_year'}
            >>> resource.match_primary_key(['x_month'])
            {'x_month': 'x_year'}
            >>> resource.match_primary_key(['x_month', 'x_date'])
            Traceback (most recent call last):
            ValueError: ... {'x_month', 'x_date'} match primary key field 'x_year'
        """
        if len(names) != len(set(names)):
            raise ValueError("Field names are not unique")
        keys = self.schema.primary_key or []
        if self.harvest.harvest:
            remaining = set(names)
            matches = {}
            for key in keys:
                match = None
                if key in remaining:
                    # Use exact match if present
                    match = key
                elif split_period(key)[1]:
                    # Try periodic alternatives
                    periods = expand_periodic_column_names([key])
                    matching = remaining.intersection(periods)
                    if len(matching) > 1:
                        raise ValueError(
                            f"Multiple field names {matching} "
                            f"match primary key field '{key}'"
                        )
                    if len(matching) == 1:
                        match = list(matching)[0]
                if match:
                    matches[match] = key
                    remaining.remove(match)
        else:
            matches = {key: key for key in keys if key in names}
        return matches if len(matches) == len(keys) else None

    def format_df(self, df: pd.DataFrame | None = None, **kwargs: Any) -> pd.DataFrame:
        """Format a dataframe according to the resources's table schema.

        * DataFrame columns not in the schema are dropped.
        * Any columns missing from the DataFrame are added with the right dtype, but
          will be empty.
        * All columns are cast to their specified pandas dtypes.
        * Primary key columns must be present and non-null.
        * Periodic primary key fields are snapped to the start of the desired period.
        * If the primary key fields could not be matched to columns in `df`
          (:meth:`match_primary_key`) or if `df=None`, an empty dataframe is returned.

        Args:
            df: Dataframe to format.
            kwargs: Arguments to :meth:`Field.to_pandas_dtypes`.

        Returns:
            Dataframe with column names and data types matching the resource fields.
        """
        dtypes = self.to_pandas_dtypes(**kwargs)
        if df is None:
            return pd.DataFrame({n: pd.Series(dtype=d) for n, d in dtypes.items()})
        matches = self.match_primary_key(df.columns)
        if matches is None:
            # Primary key present but no matches were found
            return self.format_df()
        df = df.copy()
        # Rename periodic key columns (if any) to the requested period
        df = df.rename(columns=matches)
        # Cast integer year fields to datetime
        for field in self.schema.fields:
            if (
                field.type == "year"
                and field.name in df
                and pd.api.types.is_integer_dtype(df[field.name])
            ):
                df[field.name] = pd.to_datetime(df[field.name], format="%Y")
            if isinstance(dtypes[field.name], pd.CategoricalDtype):
                uncategorized = [
                    value
                    for value in df[field.name].dropna().unique()
                    if value not in dtypes[field.name].categories
                ]
                if uncategorized:
                    logger.warning(
                        f"Values in {field.name} column are not included in "
                        "categorical values in field enum constraint "
                        f"and will be converted to nulls ({uncategorized})."
                    )
        df = (
            # Reorder columns and insert missing columns
            df.reindex(columns=dtypes.keys(), copy=False)
            # Coerce columns to correct data type
            .astype(dtypes, copy=False)
        )
        # Convert periodic key columns to the requested period
        for df_key, key in matches.items():
            _, period = split_period(key)
            if period and df_key != key:
                df[key] = PERIODS[period](df[key])
        return df

    def enforce_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop columns not in the DB schema and enforce specified types."""
        expected_cols = pd.Index(self.get_field_names())
        missing_cols = list(expected_cols.difference(df.columns))
        if missing_cols:
            raise ValueError(
                f"{self.name}: Missing columns found when enforcing table "
                f"schema: {missing_cols}"
            )

        # Log warning if columns in dataframe are getting dropped in write
        dropped_columns = list(df.columns.difference(expected_cols))
        if dropped_columns:
            logger.info(
                "The following columns are getting dropped when the table is written:"
                f"{dropped_columns}. This is often the intended behavior. If you want "
                "to keep any of these columns, add them to the metadata.resources "
                "fields and update alembic."
            )

        df = self.format_df(df)
        pk = self.schema.primary_key
        if pk and not (dupes := df[df.duplicated(subset=pk)]).empty:
            raise ValueError(
                f"{self.name} {len(dupes)}/{len(df)} duplicate primary keys ({pk=}) when enforcing schema:\n{dupes.head()}{'\n...' if len(dupes) > 5 else ''}"
            )
        if pk and not (nulls := df[df[pk].isna().any(axis=1)]).empty:
            raise ValueError(
                f"{self.name} Null values found in primary key columns.\n{nulls}"
            )
        return df

    def aggregate_df(
        self, df: pd.DataFrame, raised: bool = False, error: Callable = None
    ) -> tuple[pd.DataFrame, dict]:
        """Aggregate dataframe by primary key.

        The dataframe is grouped by primary key fields
        and aggregated with the aggregate function of each field
        (:attr:`schema_`. `fields[*].harvest.aggregate`).

        The report is formatted as follows:

        * `valid` (bool): Whether resource is valid.
        * `stats` (dict): Error statistics for resource fields.
        * `fields` (dict):

          * `<field_name>` (str)

            * `valid` (bool): Whether field is valid.
            * `stats` (dict): Error statistics for field groups.
            * `errors` (:class:`pandas.Series`): Error values indexed by primary key.

          * ...

        Each `stats` (dict) contains the following:

        * `all` (int): Number of entities (field or field group).
        * `invalid` (int): Invalid number of entities.
        * `tolerance` (float): Fraction of invalid entities below which
          parent entity is considered valid.
        * `actual` (float): Actual fraction of invalid entities.

        Args:
            df: Dataframe to aggregate. It is assumed to have column names and
              data types matching the resource fields.
            raised: Whether aggregation errors are raised or
               replaced with :obj:`np.nan` and returned in an error report.
            error: A function with signature `f(x, e) -> Any`,
              where `x` are the original field values as a :class:`pandas.Series`
              and `e` is the original error.
              If provided, the returned value is reported instead of `e`.

        Raises:
            ValueError: A primary key is required for aggregating.

        Returns:
            The aggregated dataframe indexed by primary key fields,
            and an aggregation report (descripted above)
            that includes all aggregation errors and whether the result
            meets the resource's and fields' tolerance.
        """
        if not self.schema.primary_key:
            raise ValueError("A primary key is required for aggregating")
        aggfuncs = {
            f.name: f.harvest.aggregate
            for f in self.schema.fields
            if f.name not in self.schema.primary_key
        }
        df, report = groupby_aggregate(
            df,
            by=self.schema.primary_key,
            aggfuncs=aggfuncs,
            raised=raised,
            error=error,
        )
        report = self._build_aggregation_report(df, report)
        return df, report

    def _build_aggregation_report(self, df: pd.DataFrame, errors: dict) -> dict:
        """Build report from aggregation errors.

        Args:
            df: Harvested dataframe (see :meth:`harvest_dfs`).
            errors: Aggregation errors (see :func:`groupby_aggregate`).

        Returns:
            Aggregation report, as described in :meth:`aggregate_df`.
        """
        nrows, ncols = df.reset_index().shape
        freports = {}
        for field in self.schema.fields:
            nerrors = errors[field.name].size if field.name in errors else 0
            stats = {
                "all": nrows,
                "invalid": nerrors,
                "tolerance": field.harvest.tolerance,
                "actual": nerrors / nrows if nrows else 0,
            }
            freports[field.name] = {
                "valid": stats["actual"] <= stats["tolerance"],
                "stats": stats,
                "errors": errors.get(field.name, None),
            }
        nerrors = sum(not f["valid"] for f in freports.values())
        stats = {
            "all": ncols,
            "invalid": nerrors,
            "tolerance": self.harvest.tolerance,
            "actual": nerrors / ncols,
        }
        return {
            "valid": stats["actual"] <= stats["tolerance"],
            "stats": stats,
            "fields": freports,
        }

    def harvest_dfs(
        self,
        dfs: dict[str, pd.DataFrame],
        aggregate: bool = None,
        aggregate_kwargs: dict[str, Any] = {},
        format_kwargs: dict[str, Any] = {},
    ) -> tuple[pd.DataFrame, dict]:
        """Harvest from named dataframes.

        For standard resources (:attr:`harvest`. `harvest=False`), the columns
        matching all primary key fields and any data fields are extracted from
        the input dataframe of the same name.

        For harvested resources (:attr:`harvest`. `harvest=True`), the columns
        matching all primary key fields and any data fields are extracted from
        each compatible input dataframe, and concatenated into a single
        dataframe.  Periodic key fields (e.g. 'report_month') are matched to any
        column of the same name with an equal or smaller period (e.g.
        'report_day') and snapped to the start of the desired period.

        If `aggregate=False`, rows are indexed by the name of the input dataframe.
        If `aggregate=True`, rows are indexed by primary key fields.

        Args:
            dfs: Dataframes to harvest.
            aggregate: Whether to aggregate the harvested rows by their primary key.
                By default, this is `True` if `self.harvest.harvest=True` and
                `False` otherwise.
            aggregate_kwargs: Optional arguments to :meth:`aggregate_df`.
            format_kwargs: Optional arguments to :meth:`format_df`.

        Returns:
            A dataframe harvested from the dataframes, with column names and
            data types matching the resource fields, alongside an aggregation
            report.
        """
        if aggregate is None:
            aggregate = self.harvest.harvest
        if self.harvest.harvest:
            # Harvest resource from all inputs where all primary key fields are present
            samples = {}
            for name, df in dfs.items():
                samples[name] = self.format_df(df, **format_kwargs)
                # Pass input names to aggregate via the index
                samples[name].index = pd.Index([name] * len(samples[name]), name="df")
            df = pd.concat(samples.values())
        elif self.name in dfs:
            # Subset resource from input of same name
            df = self.format_df(dfs[self.name], **format_kwargs)
            # Pass input names to aggregate via the index
            df.index = pd.Index([self.name] * df.shape[0], name="df")
        else:
            return self.format_df(df=None, **format_kwargs), {}
        if aggregate:
            return self.aggregate_df(df, **aggregate_kwargs)
        return df, {}

    def to_rst(self, docs_dir: DirectoryPath, path: str) -> None:
        """Output to an RST file."""
        template = _get_jinja_environment(docs_dir).get_template("resource.rst.jinja")
        rendered = template.render(resource=self)
        Path(path).write_text(rendered)

    def encode(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize coded columns using the foreign column they refer to."""
        for field in self.schema.fields:
            if field.encoder and field.name in df.columns:
                logger.info(f"Recoding {self.name}.{field.name}")
                df[field.name] = field.encoder.encode(
                    col=df[field.name], dtype=field.to_pandas_dtype()
                )
        return df


# ---- Package ---- #


class Package(PudlMeta):
    """Tabular data package.

    See https://specs.frictionlessdata.io/data-package.

    Examples:
        Foreign keys between resources are checked for completeness and consistency.

        >>> fields = [{'name': 'x', 'type': 'year', 'description': 'X'}, {'name': 'y', 'type': 'string', 'description': 'Y'}]
        >>> fkey = {'fields': ['x', 'y'], 'reference': {'resource': 'b', 'fields': ['x', 'y']}}
        >>> schema = {'fields': fields, 'primary_key': ['x'], 'foreign_keys': [fkey]}
        >>> a = Resource(name='a', schema=schema, description='A')
        >>> b = Resource(name='b', schema=Schema(fields=fields, primary_key=['x']), description='B')
        >>> Package(name='ab', resources=[a, b])
        Traceback (most recent call last):
        ValidationError: ...
        >>> b.schema.primary_key = ['x', 'y']
        >>> package = Package(name='ab', resources=[a, b])

        SQL Alchemy can sort tables, based on foreign keys,
        in the order in which they need to be loaded into a database.

        >>> metadata = package.to_sql()
        >>> [table.name for table in metadata.sorted_tables]
        ['b', 'a']
    """

    name: String
    title: String | None = None
    description: String | None = None
    keywords: list[String] = []
    homepage: AnyHttpUrl = AnyHttpUrl("https://catalyst.coop/pudl")
    created: datetime.datetime = datetime.datetime.now(datetime.UTC)
    contributors: list[Contributor] = []
    sources: list[DataSource] = []
    licenses: list[License] = []
    resources: StrictList[Resource]
    profile: String = "tabular-data-package"
    model_config = ConfigDict(validate_assignment=False)

    @field_validator("resources")
    @classmethod
    def _check_foreign_keys(cls, resources: list[Resource]):
        rnames = [resource.name for resource in resources]
        errors = []
        for resource in resources:
            for foreign_key in resource.schema.foreign_keys:
                rname = foreign_key.reference.resource
                tag = f"[{resource.name} -> {rname}]"
                if rname not in rnames:
                    errors.append(f"{tag}: Reference not found")
                    continue
                reference = resources[rnames.index(rname)]
                if not reference.schema.primary_key:
                    errors.append(f"{tag}: Reference missing primary key")
                    continue
                missing = [
                    x
                    for x in foreign_key.reference.fields
                    if x not in reference.schema.primary_key
                ]
                if missing:
                    errors.append(f"{tag}: Reference primary key missing {missing}")
        if errors:
            raise ValueError(
                format_errors(*errors, title="Foreign keys", pydantic=True)
            )
        return resources

    @model_validator(mode="after")
    def _populate_from_resources(self: Self):
        """Populate Package attributes from similar deduplicated Resource attributes.

        Resources and Packages share some descriptive attributes. When building a
        Package out of a collection of Resources, we want the Package to reflect the
        union of all the analogous values found in the Resources, but we don't want
        any duplicates. We may also get values directly from the Package inputs.
        """
        for key in ("keywords", "contributors", "sources", "licenses"):
            package_value = getattr(self, key)
            resource_values = [getattr(resource, key) for resource in self.resources]
            deduped_values = _unique(package_value, *resource_values)
            setattr(self, key, deduped_values)
        return self

    @classmethod
    @lru_cache
    def from_resource_ids(
        cls,
        resource_ids: tuple[str] = tuple(sorted(RESOURCE_METADATA)),
        resolve_foreign_keys: bool = False,
        excluded_etl_groups: tuple[str] = (),
    ) -> "Package":
        """Construct a collection of Resources from PUDL identifiers (`resource.name`).

        Identify any fields that have foreign key relationships referencing the
        coding tables defined in :mod:`pudl.metadata.codes` and if so, associate the
        coding table's encoder with those columns for later use cleaning them up.

        The result is cached, since we so often need to generate the metadata for
        the full collection of PUDL tables.

        Args:
            resource_ids: Resource PUDL identifiers (`resource.name`). Needs to
                be a Tuple so that the set of identifiers is hashable, allowing
                return value caching through lru_cache.
            resolve_foreign_keys: Whether to add resources as needed based on
                foreign keys.
            excluded_etl_groups: Collection of ETL groups used to filter resources
                out of Package.
        """
        resources = [Resource.dict_from_id(x) for x in resource_ids]
        if resolve_foreign_keys:
            # Add missing resources based on foreign keys
            names = list(resource_ids)
            i = 0
            while i < len(resources):
                for resource in resources[i:]:
                    for key in resource["schema"].get("foreign_keys", []):
                        name = key.get("reference", {}).get("resource")
                        if name and name not in names:
                            names.append(name)
                i = len(resources)
                if len(names) > i:
                    resources += [Resource.dict_from_id(x) for x in names[i:]]

        if excluded_etl_groups:
            resources = [
                resource
                for resource in resources
                if resource["etl_group"] not in excluded_etl_groups
            ]

        return cls(name="pudl", resources=resources)

    @staticmethod
    def get_etl_group_tables(
        etl_group: str,
    ) -> tuple[str]:
        """Get a sorted tuple of table names for an etl_group.

        Args:
            etl_group: the etl_group key.

        Returns:
            A sorted tuple of table names for the etl_group.
        """
        resource_ids = tuple(
            sorted(
                (k for k, v in RESOURCE_METADATA.items() if v["etl_group"] == etl_group)
            )
        )
        if not resource_ids:
            raise ValueError(f"There are no resources for ETL group: {etl_group}.")

        return resource_ids

    def get_resource(self, name: str) -> Resource:
        """Return the resource with the given name if it is in the Package."""
        names = [resource.name for resource in self.resources]
        return self.resources[names.index(name)]

    def to_rst(self, docs_dir: DirectoryPath, path: str) -> None:
        """Output to an RST file."""
        template = _get_jinja_environment(docs_dir).get_template("package.rst.jinja")
        rendered = template.render(package=self)
        if path:
            Path(path).write_text(rendered)
        else:
            sys.stdout.write(rendered)

    def to_sql(
        self,
        check_types: bool = True,
        check_values: bool = True,
    ) -> sa.MetaData:
        """Return equivalent SQL MetaData."""
        metadata = sa.MetaData(
            naming_convention={
                "ix": "ix_%(column_0_label)s",
                "uq": "uq_%(table_name)s_%(column_0_name)s",
                "ck": "ck_%(table_name)s_%(constraint_name)s",
                "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
                "pk": "pk_%(table_name)s",
            }
        )
        for resource in self.resources:
            if resource.create_database_schema:
                _ = resource.to_sql(
                    metadata,
                    check_types=check_types,
                    check_values=check_values,
                )
        return metadata

    def get_sorted_resources(self) -> StrictList[Resource]:
        """Get a list of sorted Resources.

        Currently Resources are listed in reverse alphabetical order based
        on their name which results in the following order to promote output
        tables to users and push intermediate tables to the bottom of the
        docs: output, core, intermediate.
        In the future we might want to have more fine grain control over how
        Resources are sorted.

        Returns:
            A sorted list of resources.
        """
        resources = self.resources

        def sort_resource_names(resource: Resource):
            pattern = re.compile(r"^(_out_|out_|core_)")

            matches = pattern.findall(resource.name)
            prefix = matches[0] if matches else ""

            prefix_order = {"out_": 1, "core_": 2, "_out_": 3}
            return prefix_order.get(prefix, float("inf"))

        return sorted(resources, key=sort_resource_names, reverse=False)

    @cached_property
    def encoders(self) -> dict[SnakeCase, Encoder]:
        """Compile a mapping of field names to their encoders, if they exist.

        This dictionary will be used many times, so it makes sense to build it once
        when the Package is instantiated so it can be reused.
        """
        encoded_fields = [
            field
            for res in self.resources
            for field in res.schema.fields
            if field.encoder
        ]
        encoders: dict[SnakeCase, Encoder] = {}
        for field in encoded_fields:
            if field.name not in encoders:
                encoders[field.name] = field.encoder
            else:
                # We have a field which shows up in multiple tables, and we want to
                # verify that the encoders are the same across all of them.
                pd.testing.assert_frame_equal(encoders[field.name].df, field.encoder.df)
                assert encoders[field.name].code_fixes == field.encoder.code_fixes
                assert encoders[field.name].ignored_codes == field.encoder.ignored_codes
        return encoders

    def encode(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean up all coded columns in a dataframe based on PUDL coding tables.

        Returns:
            A modified copy of the input dataframe.
        """
        encoded_df = df.copy()
        for col in encoded_df.columns:
            if col in self.encoders:
                encoded_df[col] = self.encoders[col].encode(
                    encoded_df[col], dtype=Field.from_id(col).to_pandas_dtype()
                )
        return encoded_df

    def to_frictionless(self) -> frictionless.Package:
        """Convert to a Frictionless Datapackage."""
        resources = [r.to_frictionless() for r in self.resources]
        package = frictionless.Package(name=self.name, resources=resources)
        return package


PUDL_PACKAGE = Package.from_resource_ids()
"""Define a global PUDL package object for use across the entire codebase.

This needs to happen after the definition of the Package class above, and it is used in
some of the class definitions below, but having it defined in the middle of this module
is kind of obscure, so it is imported in the __init__.py for this subpackage and then
imported in other modules from that more prominent location.
"""


class CodeMetadata(PudlMeta):
    """A list of Encoders for standardizing and documenting categorical codes.

    Used to export static coding metadata to PUDL documentation automatically
    """

    encoder_list: list[Encoder] = []

    @classmethod
    def from_code_ids(cls, code_ids: Iterable[str]) -> "CodeMetadata":
        """Construct a list of encoders from code dictionaries.

        Args:
            code_ids: A list of Code PUDL identifiers, keys to entries in the
                CODE_METADATA dictionary.
        """
        encoder_list = []
        for name in code_ids:
            if name in CODE_METADATA:
                encoder_list.append(Encoder.from_code_id(name))
        return cls(encoder_list=encoder_list)

    def to_rst(
        self, top_dir: DirectoryPath, csv_subdir: DirectoryPath, rst_path: str
    ) -> None:
        """Iterate through encoders and output to an RST file."""
        with Path(rst_path).open("w") as f:
            for idx, encoder in enumerate(self.encoder_list):
                header = idx == 0
                rendered = encoder.to_rst(
                    top_dir=top_dir, csv_subdir=csv_subdir, is_header=header
                )
                f.write(rendered)


class DatasetteMetadata(PudlMeta):
    """A collection of Data Sources and Resources for metadata export.

    Used to create metadata YAML file to accompany Datasette.
    """

    data_sources: list[DataSource]
    resources: list[Resource] = PUDL_PACKAGE.resources
    xbrl_resources: dict[str, list[Resource]] = {}
    label_columns: dict[str, str] = {
        "core_eia__entity_plants": "plant_name_eia",
        "core_pudl__assn_ferc1_pudl_plants": "plant_name_ferc1",
        "core_pudl__entity_plants_pudl": "plant_name_pudl",
        "core_eia__entity_utilities": "utility_name_eia",
        "core_pudl__assn_ferc1_pudl_utilities": "utility_name_ferc1",
        "core_pudl__entity_utilities_pudl": "utility_name_pudl",
    }

    @classmethod
    def from_data_source_ids(
        cls,
        output_path: Path,
        data_source_ids: list[str] = [
            "pudl",
            "eia860",
            "eia860m",
            "eia861",
            "eia923",
            "ferc1",
            "ferc2",
            "ferc6",
            "ferc60",
            "ferc714",
        ],
        xbrl_ids: list[str] = [
            "ferc1_xbrl",
            "ferc2_xbrl",
            "ferc6_xbrl",
            "ferc60_xbrl",
            "ferc714_xbrl",
        ],
    ) -> "DatasetteMetadata":
        """Construct a dictionary of DataSources from data source names.

        Create dictionary of first and last year or year-month for each source.

        Args:
            output_path: PUDL_OUTPUT path.
            data_source_ids: ids of data sources currently included in Datasette
            xbrl_ids: ids of data converted XBRL data to be included in Datasette
        """
        # Compile a list of DataSource objects for use in the template
        data_sources = [DataSource.from_id(ds_id) for ds_id in data_source_ids]

        # Instantiate all possible resources in a Package:
        resources = PUDL_PACKAGE.resources

        # Get XBRL based resources
        xbrl_resources = {}
        for xbrl_id in xbrl_ids:
            # Read JSON Package descriptor from file
            with Path.open(Path(output_path) / f"{xbrl_id}_datapackage.json") as f:
                descriptor = json.load(f)

            # Use descriptor to create Package object
            xbrl_package = Package(**descriptor)

            # Add list of resources to dict
            xbrl_resources[xbrl_id] = xbrl_package.resources

        return cls(
            data_sources=data_sources,
            resources=resources,
            xbrl_resources=xbrl_resources,
        )

    def to_yaml(self) -> str:
        """Output database, table, and column metadata to YAML file."""
        template = _get_jinja_environment().get_template("datasette-metadata.yml.jinja")

        rendered = template.render(
            license=LICENSES["cc-by-4.0"],
            data_sources=self.data_sources,
            resources=self.resources,
            xbrl_resources=self.xbrl_resources,
            label_columns=self.label_columns,
        )
        return rendered
