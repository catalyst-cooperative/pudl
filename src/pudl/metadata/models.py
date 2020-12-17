"""Metadata data models."""
import datetime
import pathlib
import uuid
from typing import Any, Callable, List, Literal, Union

import pandas as pd
import pydantic

from ..transform.harvest import most_and_more_frequent
from .constants import CONTRIBUTORS, FIELD_DTYPES, LICENSES, SOURCES
from .fields import FIELDS
from .resources import RESOURCES

# ---- Base ---- #


# def remove_falsy(obj: Any) -> dict:
#     if type(obj) is dict:
#         return {k: remove_falsy(v) for k, v in obj.items() if v and remove_falsy(v)}
#     # NOTE: Drops missingValues: ['']
#     # if type(obj) is list:
#     #     return [remove_falsy(v) for v in obj if v and remove_falsy(v)]
#     return obj


class BaseModel(pydantic.BaseModel):
    """Custom Pydantic base model."""

    class Config:
        """Custom Pydantic configuration."""

        validate_all = True
        validate_assignment = True
        extra = 'forbid'

    def dict(self, *args, by_alias=True, **kwargs) -> dict:  # noqa: A003
        """Return as a dictionary."""
        return super().dict(*args, by_alias=by_alias, **kwargs)

    def json(self, *args, by_alias=True, **kwargs) -> str:
        """Return as JSON."""
        return super().json(*args, by_alias=by_alias, **kwargs)

    def __getattribute__(self, name: str) -> Any:
        """Get attribute."""
        if name in ("fields", "schema") and f"{name}_" in self.__dict__:
            name = f"{name}_"
        return super().__getattribute__(name)

    def __setattr__(self, name, value) -> None:
        """Set attribute."""
        if name in ("fields", "schema") and f"{name}_" in self.__dict__:
            name = f"{name}_"
        super().__setattr__(name, value)

    # def __init__(self, *args, **kwargs) -> None:
    #     positional_kwargs = {k: v for k, v in zip(self.__fields__.keys(), args)}
    #     super().__init__(**positional_kwargs, **kwargs)

    # def dict(self, *args, exclude_falsy=False, by_alias=True, **kwargs):
    #     obj = super().dict(*args, by_alias=by_alias, **kwargs)
    #     if exclude_falsy:
    #         obj = remove_falsy(obj)
    #     return obj


# ---- Field ---- #


class FieldConstraints(BaseModel):
    """
    Field constraints (`resource.schema.fields[...].constraints`).

    See https://specs.frictionlessdata.io/table-schema/#constraints.
    """

    required: bool = False
    unique: bool = False
    enum: List = None


class Field(BaseModel):
    """
    Field (`resource.schema.fields[...]`).

    See https://specs.frictionlessdata.io/table-schema/#field-descriptors.
    """

    name: str
    type: Literal["string", "number", "integer", "boolean", "date", "datetime", "year"]  # noqa: A003
    format: str = "default"  # noqa: A003
    title: str = None
    description: str = None
    aggregate: Callable[[pd.Series],
                        pd.Series] = lambda x: most_and_more_frequent(x, 0.7)
    tolerance: float = 0.0
    constraints: FieldConstraints = {}

    # def __init__(self, name: str, **kwargs: Any) -> None:
    #     kwargs['name'] = name
    #     if len(kwargs) == 1 and isinstance(name, str) and name.lower() in FIELDS_DICT:
    #         kwargs = FIELDS_DICT[name.lower()]
    #     super().__init__(**kwargs)

    # @pydantic.validator('type')
    # def check_type_supported(cls, value):
    #     if value not in FIELD_DTYPES:
    #         raise ValueError(f"must be one of {list(FIELD_DTYPES.keys())}")
    #     return value

    @classmethod
    def from_id(cls, x: str) -> 'Field':
        """Construct from PUDL identifier (`field.name`)."""
        return cls(**FIELDS[x.lower()])

    @property
    def dtype(self) -> Union[str, pd.CategoricalDtype]:
        """Pandas data dtype."""
        if self.constraints.enum:
            return pd.CategoricalDtype(self.constraints.enum)
        return FIELD_DTYPES[self.type]

    # def to_dpkg(self) -> Dict[str, Any]:
    #     exclude = {"aggregate", "tolerance"}
    #     return self.dict(exclude=exclude, exclude_falsy=True)


# ---- Models: Resource ---- #


class ForeignKeyReference(BaseModel):
    """
    Foreign key reference (`resource.schema.foreignKeys[...].reference`).

    See https://specs.frictionlessdata.io/table-schema/#foreign-keys.
    """

    resource: str
    fields_: List[str] = pydantic.Field(alias="fields")


class ForeignKey(BaseModel):
    """
    Foreign key (`resource.schema.foreignKeys[...]`).

    See https://specs.frictionlessdata.io/table-schema/#foreign-keys.
    """

    fields_: List[str] = pydantic.Field(alias="fields")
    reference: ForeignKeyReference


class Schema(BaseModel):
    """
    Table schema (`resource.schema`).

    See https://specs.frictionlessdata.io/table-schema.
    """

    fields_: List[Field] = pydantic.Field(alias="fields")
    missingValues: List[str] = [""]  # noqa: N815
    primaryKey: List[str] = None  # noqa: N815
    foreignKeys: List[ForeignKey] = []  # noqa: N815

    # @pydantic.validator("fields_", each_item=True, pre=True)
    # def _expand_field_name(cls, value):
    #     if isinstance(value, str):
    #         return {'name': value}
    #     return value

    @pydantic.validator("primaryKey")
    def _check_primary_key_in_fields(cls, value, values):  # noqa: N805
        if value:
            missing = set(value).difference([f.name for f in values['fields_']])
            if missing:
                raise ValueError(f"keys {missing} missing from fields")
        return value


class Dialect(BaseModel):
    """
    CSV dialect (`resource.dialect`).

    See https://specs.frictionlessdata.io/csv-dialect.
    """

    delimiter: str = ","
    header: bool = True
    quoteChar: str = "\""  # noqa: N815
    doubleQuote: bool = True  # noqa: N815
    lineTerminator: str = "\r\n"  # noqa: N815
    skipInitialSpace: bool = True  # noqa: N815
    caseSensitiveHeader: bool = False  # noqa: N815


class License(BaseModel):
    """
    Data license (`package|resource.licenses[...]`).

    See https://specs.frictionlessdata.io/data-package/#licenses.
    """

    name: str
    title: str
    path: pydantic.HttpUrl

    @classmethod
    def from_id(cls, x: str) -> "License":
        """Construct from PUDL identifier."""
        return cls(**LICENSES[x.lower()])


class Source(BaseModel):
    """
    Data source (`package|resource.sources[...]`).

    See https://specs.frictionlessdata.io/data-package/#sources.
    """

    title: str
    path: pydantic.AnyUrl

    @classmethod
    def from_id(cls, x: str) -> "Source":
        """Construct from PUDL identifier."""
        return cls(**SOURCES[x.lower()])


class Resource(BaseModel):
    """
    Tabular data resource (`package.resources[...]`).

    See https://specs.frictionlessdata.io/tabular-data-resource.
    """

    name: str
    path: pathlib.Path = None
    title: str = None
    description: str = None
    profile: Literal["tabular-data-resource"] = "tabular-data-resource"
    encoding: Literal["utf-8"] = "utf-8"
    mediatype: Literal["text/csv"] = "text/csv"
    format: Literal["csv"] = "csv"  # noqa: A003
    dialect: Dialect = {}
    schema_: Schema = pydantic.Field({}, alias='schema')
    licenses: List[License] = []
    sources: List[Source] = []

    @pydantic.validator('path')
    def _set_path_from_name(cls, value, values):  # noqa: N805
        return value or f"data/{values['name']}.csv"

    @classmethod
    def from_id(cls, x: str) -> "Resource":
        """Construct from PUDL identifier (`resource.name`)."""
        obj = RESOURCES[x.lower()].copy()
        # Expand fields
        if "schema" in obj and "fields" in obj["schema"]:
            obj["schema"]["fields"] = [
                Field.from_id(value) if isinstance(value, str) else value
                for value in obj["schema"]["fields"]
            ]
        # Expand sources
        if "sources" in obj:
            obj["sources"] = [
                Source.from_id(value) if isinstance(value, str) else value
                for value in obj["sources"]
            ]
        # Expand licences
        if "licenses" in obj:
            obj["licenses"] = [
                License.from_id(value) if isinstance(value, str) else value
                for value in obj["licenses"]
            ]
        return cls(**obj)


# ---- Package ---- #


class Contributor(BaseModel):
    """
    Data contributor (`package.contributors[...]`).

    See https://specs.frictionlessdata.io/data-package/#contributors.
    """

    title: str
    path: pydantic.HttpUrl = None
    email: pydantic.EmailStr = None
    role: Literal["author", "contributor", "maintainer",
                  "publisher", "wrangler"] = "contributor"
    organization: str = None

    @classmethod
    def from_id(cls, x: str) -> "Contributor":
        """Construct from PUDL identifier."""
        return cls(**CONTRIBUTORS[x.lower()])


class Package(BaseModel):
    """
    Tabular data package.

    See https://specs.frictionlessdata.io/data-package.
    """

    name: str
    # version: str = datapkg_settings["version"]
    # TODO: Validate id as UUID4, then cast to string
    id: pydantic.UUID4 = str(uuid.uuid4())  # noqa: A003
    profile: Literal["tabular-data-package"] = "tabular-data-package"
    title: str
    description: str
    keywords: List[str] = []
    homepage: pydantic.HttpUrl = "https://catalyst.coop/pudl"
    created: datetime.datetime = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    contributors: List[Contributor] = []
    sources: List[Source] = []
    licenses: List[License] = []
    # TODO: Move autoincrement to resource
    # autoincrement: Dict[str, str] = get_autoincrement_columns(datapkg_tables)
    # python-package-name: str = "catalystcoop.pudl"
    # python-package-version: str = pkg_resources.get_distribution('catalystcoop.pudl').version
    resources: List[Resource]
