"""Metadata and operational constants."""
import datetime
from typing import Callable, Dict, List, Type

import pandas as pd
import sqlalchemy as sa

FIELD_DTYPES: Dict[str, str] = {
    "string": "string",
    "number": "float64",
    "integer": "Int64",
    "boolean": "boolean",
    "date": "datetime64[ns]",
    "datetime": "datetime64[ns]",
    "year": "datetime64[ns]",
}
"""
Pandas data type by PUDL field type (Data Package `field.type`).
"""

FIELD_DTYPES_SQL: Dict[str, sa.sql.visitors.VisitableType] = {
    "boolean": sa.Boolean,
    "date": sa.Date,
    "datetime": sa.DateTime,
    "integer": sa.Integer,
    "number": sa.Float,
    "string": sa.Text,
    "year": sa.Integer,
}
"""
SQLAlchemy column types by PUDL field type (Data Package `field.type`).
"""

CONSTRAINT_DTYPES: Dict[str, Type] = {
    'string': str,
    'integer': int,
    'year': int,
    'number': float,
    'boolean': bool,
    'date': datetime.date,
    'datetime': datetime.datetime
}
"""
Python types for field constraints by PUDL field type (Data Package `field.type`).
"""

LICENSES: Dict[str, Dict[str, str]] = {
    "cc-by-4.0": {
        "name": "CC-BY-4.0",
        "title": "Creative Commons Attribution 4.0",
        "path": "https://creativecommons.org/licenses/by/4.0",
    },
    "us-govt": {
        "name": "other-pd",
        "title": "U.S. Government Works",
        "path": "https://www.usa.gov/government-works",
    },
}
"""
License attributes by PUDL identifier.
"""

SOURCES: Dict[str, Dict[str, str]] = {
    "eia860": {
        "title": "EIA Form 860",
        "path": "https://www.eia.gov/electricity/data/eia860",
    },
    "eia861": {
        "title": "EIA Form 861: Annual Electric Power Industry Report",
        "path": "https://www.eia.gov/electricity/data/eia861",
    },
    "eia923": {
        "title": "EIA Form 923",
        "path": "https://www.eia.gov/electricity/data/eia923",
    },
    "eiawater": {
        "title": "EIA Thermoelectric cooling water data",
        "path": "https://www.eia.gov/electricity/data/water",
    },
    "epacems": {
        "title": "EPA Air Markets Program Data: Hourly Continuous Emission Monitoring System(CEMS)",
        "path": "https://ampd.epa.gov/ampd",
    },
    "ferc1": {
        "title": "FERC Form 1: Electric Utility Annual Report",
        "path": "https://www.ferc.gov/industries-data/electric/general-information/electric-industry-forms/form-1-electric-utility-annual",
    },
    "ferc714": {
        "title": "FERC Form 714: Annual Electric Balancing Authority Area and Planning Area Report",
        "path": "https://www.ferc.gov/industries-data/electric/general-information/electric-industry-forms/form-no-714-annual-electric",
    },
    "ferceqr": {
        "title": "FERC Form 920: Electric Quarterly Report (EQR)",
        "path": "https://www.ferc.gov/industries-data/electric/power-sales-and-markets/electric-quarterly-reports-eqr",
    },
    "msha": {
        "title": "Mine Safety and Health Administration (MSHA)",
        "path": "https://arlweb.msha.gov/OpenGovernmentData/OGIMSHA.asp",
    },
    "phmsa": {
        "title": "Pipelines and Hazardous Materials Safety Administration (PHMSA)",
        "path": "https://www.phmsa.dot.gov/data-and-statistics/pipeline/data-and-statistics-overview",
    },
    "pudl": {
        "title": "The Public Utility Data Liberation (PUDL) Project",
        "path": "https://catalyst.coop/pudl",
        "email": "pudl@catalyst.coop",
    },
}
"""
Source attributes by PUDL identifier.
"""

CONTRIBUTORS: Dict[str, Dict[str, str]] = {
    "catalyst-cooperative": {
        "title": "Catalyst Cooperative",
        "email": "pudl@catalyst.coop",
        "path": "https://catalyst.coop",
        "role": "publisher",
        "organization": "Catalyst Cooperative",
    },
    "zane-selvans": {
        "title": "Zane Selvans",
        "email": "zane.selvans@catalyst.coop",
        "path": "https://amateurearthling.org",
        "role": "wrangler",
        "organization": "Catalyst Cooperative",
    },
    "christina-gosnell": {
        "title": "Christina Gosnell",
        "email": "christina.gosnell@catalyst.coop",
        "role": "contributor",
        "organization": "Catalyst Cooperative",
    },
    "steven-winter": {
        "title": "Steven Winter",
        "email": "steven.winter@catalyst.coop",
        "role": "contributor",
        "organization": "Catalyst Cooperative",
    },
    "alana-wilson": {
        "title": "Alana Wilson",
        "email": "alana.wilson@catalyst.coop",
        "role": "contributor",
        "organization": "Catalyst Cooperative",
    },
    "karl-dunkle-werner": {
        "title": "Karl Dunkle Werner",
        "email": "karldw@berkeley.edu",
        "path": "https://karldw.org",
        "role": "contributor",
        "organization": "UC Berkeley",
    },
    "greg-schivley": {
        "title": "Greg Schivley",
        "path": "https://gschivley.github.io",
        "role": "contributor",
        "organization": "Carbon Impact Consulting",
    },
}
"""
Contributor attributes by PUDL identifier.
"""

CONTRIBUTORS_BY_SOURCE: Dict[str, List[str]] = {
    "pudl": [
        "catalyst-cooperative",
        "zane-selvans",
        "christina-gosnell",
        "steven-winter",
        "alana-wilson",
        "karl-dunkle-werner",
    ],
    "eia923": [
        "catalyst-cooperative",
        "zane-selvans",
        "christina-gosnell",
        "steven-winter",
    ],
    "eia860": [
        "catalyst-cooperative",
        "zane-selvans",
        "christina-gosnell",
        "steven-winter",
        "alana-wilson",
    ],
    "ferc1": [
        "catalyst-cooperative",
        "zane-selvans",
        "christina-gosnell",
        "steven-winter",
        "alana-wilson",
    ],
    "epacems": [
        "catalyst-cooperative",
        "karl-dunkle-werner",
        "zane-selvans",
    ],
}
"""
Contributors (PUDL identifiers) by source (PUDL identifier).
"""

KEYWORDS_BY_SOURCE: Dict[str, List[str]] = {
    "pudl": ["us", "electricity"],
    "eia860": [
        "electricity",
        "electric",
        "boiler",
        "generator",
        "plant",
        "utility",
        "fuel",
        "coal",
        "natural gas",
        "prime mover",
        "eia860",
        "retirement",
        "capacity",
        "planned",
        "proposed",
        "energy",
        "hydro",
        "solar",
        "wind",
        "nuclear",
        "form 860",
        "eia",
        "annual",
        "gas",
        "ownership",
        "steam",
        "turbine",
        "combustion",
        "combined cycle",
        "eia",
        "energy information administration",
    ],
    "eia923": [
        "fuel",
        "boiler",
        "generator",
        "plant",
        "utility",
        "cost",
        "price",
        "natural gas",
        "coal",
        "eia923",
        "energy",
        "electricity",
        "form 923",
        "receipts",
        "generation",
        "net generation",
        "monthly",
        "annual",
        "gas",
        "fuel consumption",
        "MWh",
        "energy information administration",
        "eia",
        "mercury",
        "sulfur",
        "ash",
        "lignite",
        "bituminous",
        "subbituminous",
        "heat content",
    ],
    "epacems": [
        "epa",
        "us",
        "emissions",
        "pollution",
        "ghg",
        "so2",
        "co2",
        "sox",
        "nox",
        "load",
        "utility",
        "electricity",
        "plant",
        "generator",
        "unit",
        "generation",
        "capacity",
        "output",
        "power",
        "heat content",
        "mmbtu",
        "steam",
        "cems",
        "continuous emissions monitoring system",
        "hourly",
        "environmental protection agency",
        "ampd",
        "air markets program data",
    ],
    "ferc1": [
        "electricity",
        "electric",
        "utility",
        "plant",
        "steam",
        "generation",
        "cost",
        "expense",
        "price",
        "heat content",
        "ferc",
        "form 1",
        "federal energy regulatory commission",
        "capital",
        "accounting",
        "depreciation",
        "finance",
        "plant in service",
        "hydro",
        "coal",
        "natural gas",
        "gas",
        "opex",
        "capex",
        "accounts",
        "investment",
        "capacity",
    ],
}
"""
Keywords by source (PUDL identifier).
"""

PERIODS: Dict[str, Callable[[pd.Series], pd.Series]] = {
    "year": lambda x: x.astype("datetime64[Y]"),
    "quarter": lambda x: x.apply(
        pd.tseries.offsets.QuarterBegin(startingMonth=1).rollback
    ),
    "month": lambda x: x.astype("datetime64[M]"),
    "date": lambda x: x.astype("datetime64[D]"),
}
"""
Functions converting datetimes to period start times, by time period.
"""
