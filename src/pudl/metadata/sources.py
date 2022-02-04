"""Metadata and operational constants."""
from typing import Any, Dict

from pudl.metadata.enums import EPACEMS_STATES
from pudl.metadata.constants import KEYWORDS

SOURCES: Dict[str, Any] = {
    "censusdp1tract": {
        "title": "Census DP1",
        "path": "",
        "description": (
            "US Census Demographic Profile 1 (DP1) County and Tract "
            "GeoDatabase."
        ),
        "working_partitions": {},  # Census DP1 is monolithic.
    },
    "eia860": {
        "title": "EIA Form 860",
        "path": "https://www.eia.gov/electricity/data/eia860",
        "description": (
            "US Energy Information Administration (EIA) Form 860 data for "
            "electric power plants with 1 megawatt or greater combined nameplate "
            "capacity."
        ),
        "contributors": [
            "catalyst-cooperative",
            "zane-selvans",
            "christina-gosnell",
            "steven-winter",
            "alana-wilson",
        ],
        "working_partitions": {
            "years": sorted(set(range(2001, 2021))),
        },
        "keywords": sorted(set(
            [
                "eia860",
                "form 860",
                "ownership",
                "annual",
                "yearly",
            ]
            + KEYWORDS["eia"]
            + KEYWORDS["us_govt"]
            + KEYWORDS["electricity"]
            + KEYWORDS["fuels"]
            + KEYWORDS["plants"]
            + KEYWORDS["environment"]
        )),
    },
    "eia860m": {
        "title": "EIA Form 860m",
        "path": "",
        "description": (
            "US Energy Information Administration (EIA) Form 860 M data for "
            "electric power plants with 1 megawatt or greater combined nameplate "
            "capacity. Preliminary Monthly Electric Generator Inventory (based on "
            "Form EIA-860M as a supplement to Form EIA-860)"
        ),
        "contributors": [],
        "working_partitions": {
            "year_month": "2021-08",
        },
        "keywords": sorted(set(
            [
                "eia860m",
                "form 860m"
                "monthly"
            ]
        ))
    },
    "eia861": {
        "title": "EIA Form 861: Annual Electric Power Industry Report",
        "path": "https://www.eia.gov/electricity/data/eia861",
        "description": (
            "EIA Form 861 Annual Electric Power Industry Report, detailed "
            "data files."
        ),
        "working_partitions": {
            "years": sorted(set(range(2001, 2021))),
        },
        "contributors": [],
        "keywords": sorted(set(
            [
                "eia861",
                "form 861",
                "balancing authority",
                "advanced metering infrastructure",
                "customer class",
                "green pricing",
                "energy efficiency",
                "demand side management",
                "demand response",
                "net metering",
                "business model",
                "service territory",
                "annual",
                "yearly"
            ]
            + KEYWORDS["eia"]
            + KEYWORDS["us_govt"]
            + KEYWORDS["electricity"]
        ))
    },
    "eia923": {
        "title": "EIA Form 923",
        "path": "https://www.eia.gov/electricity/data/eia923",
        "description": (
            "The EIA Form 923 collects detailed monthly and annual electric "
            "power data on electricity generation, fuel consumption, fossil fuel "
            "stocks, and receipts at the power plant and prime mover level."
        ),
        "working_partitions": {
            "years": sorted(set(range(2001, 2021))),
        },
        "contributors": [
            "catalyst-cooperative",
            "zane-selvans",
            "christina-gosnell",
            "steven-winter",
            "katherine-lamb",
        ],
        "keywords": sorted(set(
            [
                "eia923",
                "form 923",
                "monthly",
            ]
            + KEYWORDS["eia"]
            + KEYWORDS["us_govt"]
            + KEYWORDS["electricity"]
            + KEYWORDS["fuels"]
            + KEYWORDS["plants"]
            + KEYWORDS["environment"]
        )),
    },
    "eiawater": {
        "title": "EIA Thermoelectric Cooling Water",
        "path": "https://www.eia.gov/electricity/data/water",
        "description": "",
    },
    "epacems": {
        "title": "EPA Hourly Continuous Emission Monitoring System (CEMS)",
        "path": "https://ampd.epa.gov/ampd",
        "description": (
            "US EPA hourly Continuous Emissions Monitoring System (CEMS) data."
        ),
        "working_partitions": {
            "years": sorted(set(range(1995, 2021))),
            "states": sorted(set(EPACEMS_STATES)),
        },
        "contributors": [
            "catalyst-cooperative",
            "karl-dunkle-werner",
            "zane-selvans",
        ],
        "keywords": sorted(set(
            [
                "continuous emissions monitoring system",
                "cems",
                "air markets program data",
                "ampd",
                "hourly"
            ]
            + KEYWORDS["epa"]
            + KEYWORDS["us_govt"]
            + KEYWORDS["electricity"]
            + KEYWORDS["environment"]
        )),
    },
    "ferc1": {
        "title": "FERC Form 1",
        "path": "https://www.ferc.gov/industries-data/electric/general-information/electric-industry-forms/form-1-electric-utility-annual",
        "description": (
            "The Federal Energy Regulatory Commission (FERC) Form 1 is a "
            "comprehensive financial and operating report submitted annually for "
            "electric rate regulation, market oversight analysis, and financial audits "
            "by Major electric utilities, licensees and others."
        ),
        "working_partitions": {
            "years": sorted(set(range(1994, 2021))),
        },
        "contributors": [
            "catalyst-cooperative",
            "zane-selvans",
            "christina-gosnell",
            "steven-winter",
            "alana-wilson",
            "austen-sharpe",
        ],
        "keywords": sorted(set(
            [
                "form 1",
                "ferc1",
            ]
            + KEYWORDS["ferc1"]
            + KEYWORDS["us_govt"]
            + KEYWORDS["finance"]
            + KEYWORDS["electricity"]
            + KEYWORDS["plants"]
            + KEYWORDS["fuels"]
        )),
    },
    "ferc2": {
        "title": "FERC Form 2",
        "path": "",
        "description": (
            "The Federal Energy Regulatory Commission (FERC) Form 2 is a "
            "comprehensive financial and operating report submitted for natural gas "
            "pipelines rate regulation and financial audits."
        ),
        "working_partitions": {
            "years": [],  # Not yet working!
        }
    },
    "ferc714": {
        "title": "FERC Form 714: Annual Electric Balancing Authority Area and Planning Area Report",
        "path": "https://www.ferc.gov/industries-data/electric/general-information/electric-industry-forms/form-no-714-annual-electric",
        "description": (
            "Electric transmitting utilities operating balancing authority "
            "areas and planning areas with annual peak demand over 200MW are required "
            "to file Form 714 with the Federal Energy Regulatory Commission (FERC), "
            "reporting balancing authority area generation, actual and scheduled "
            "inter-balancing authority area power transfers, and net energy for load, "
            "summer-winter generation peaks and system lambda."
        ),
        "working_partitions": {},  # Data is monolitic, one file with all years.
    },
    "ferceqr": {
        "title": "FERC Form 920: Electric Quarterly Report (EQR)",
        "path": "https://www.ferc.gov/industries-data/electric/power-sales-and-markets/electric-quarterly-reports-eqr",
        "description": "",
    },
    "msha": {
        "title": "Mine Safety and Health Administration (MSHA)",
        "path": "https://arlweb.msha.gov/OpenGovernmentData/OGIMSHA.asp",
        "description": "",
    },
    "phmsa": {
        "title": "Pipelines and Hazardous Materials Safety Administration (PHMSA)",
        "path": "https://www.phmsa.dot.gov/data-and-statistics/pipeline/data-and-statistics-overview",
        "description": "",
    },
    "pudl": {
        "title": "The Public Utility Data Liberation (PUDL) Project",
        "path": "https://catalyst.coop/pudl",
        "description": "",
        "email": "pudl@catalyst.coop",
        "contributors": [
            "catalyst-cooperative",
            "zane-selvans",
            "christina-gosnell",
            "steven-winter",
        ],
        "keywords": ["us", "electricity", "open data", "open source"],
    },
}
"""
Data source attributes by PUDL identifier.
"""
