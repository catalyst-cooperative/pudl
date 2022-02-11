"""Metadata and operational constants."""
import datetime
from typing import Callable, Dict, List, Type

import pandas as pd
import pyarrow as pa
import sqlalchemy as sa


FIELD_DTYPES_PANDAS: Dict[str, str] = {
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

FIELD_DTYPES_PYARROW: Dict[str, pa.lib.DataType] = {
    "boolean": pa.bool_(),
    "date": pa.date32(),
    # We'll probably need to make the TZ dynamic rather than hard coded...
    "datetime": pa.timestamp("s", tz="UTC"),
    "integer": pa.int32(),
    "number": pa.float32(),
    "string": pa.string(),
    "year": pa.int32(),
}

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
License attributes.
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
        "orcid": "0000-0002-9961-7208"
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
    "austen-sharpe": {
        "title": "Austen Sharpe",
        "email": "austen.sharpe@catalyst.coop",
        "role": "contributor",
        "organization": "Catalyst Cooperative",
    },
    "katherine-lamb": {
        "title": "Katherine Lamb",
        "email": "katherine.lamb@catalyst.coop",
        "role": "contributor",
        "organization": "Catalyst Cooperative",
    },
    "bennett-norman": {
        "title": "Bennett Norman",
        "email": "bennett.norman@catalyst.coop",
        "role": "contributor",
        "organization": "Catalyst Cooperative",
    },
    "trenton-bush": {
        "title": "Trenton Bush",
        "email": "trenton.bush@catalyst.coop",
        "role": "contributor",
        "organization": "Catalyst Cooperative",
    },
    "ethan-welty": {
        "title": "Ethan Welty",
        "email": "ethan.welty@gmail.com",
        "role": "contributor",
        "organization": "Catalyst Cooperative",
    },
}
"""
PUDL Contributors for attribution.
"""

KEYWORDS: Dict[str, List[str]] = {
    "electricity": [
        "electricity",
        "electric",
        "generation",
        "energy",
        "utility",
        "transmission",
        "distribution",
        "kWh",
        "MWh",
        "kW",
        "MW",
        "kilowatt hours",
        "kilowatts",
        "megawatts",
        "megawatt hours",
        "power",
    ],
    "fuels": [
        "fuel",
        "coal",
        "bituminous",
        "lignite",
        "nagural gas",
        "solar",
        "wind",
        "hydro",
        "nuclear"
        "subbituminous",
        "heat content",
        "mmbtu",
        "fuel cost",
        "fuel price",
    ],
    "plants": [
        "plant",
        "boilers",
        "generators",
        "steam",
        "turbine",
        "combined_cycle",
        "retirement",
        "planned",
        "proposed",
        "combustion",
        "prime mover",
        "capacity",
        "heat rate",
    ],
    "finance": [
        "finance",
        "debt",
        "accounting",
        "capital",
        "cost",
        "contract",
        "price",
        "receipts",
        "ownership",
        "depreciation",
        "plant in service",
        "capex",
        "opex",
        "operating expenses",
        "capital expenses",
    ],
    "environment": [
        "emissions",
        "pollution",
        "ash",
        "sulfur",
        "mercury",
        "chlorine",
        "sox",
        "so2",
        "nox",
        "ghg",
        "co2",
        "carbon dioxide",
        "particulate",
        "pm2.5",
    ],
    "eia": [
        "eia",
        "energy information administration",
    ],
    "ferc": [
        "ferc",
        "federal energy regulatory commission",
    ],
    "epa": [
        "epa",
        "environmental protection agency",
    ],
    "us_govt": [
        "united states",
        "us",
        "usa",
        "government",
        "federal",
    ]
}
