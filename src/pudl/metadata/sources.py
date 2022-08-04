"""Metadata and operational constants."""
from typing import Any

from pudl.metadata.constants import CONTRIBUTORS, KEYWORDS, LICENSES
from pudl.metadata.enums import EPACEMS_STATES

SOURCES: dict[str, Any] = {
    "censusdp1tract": {
        "title": "Census DP1",
        "path": "https://www.census.gov/geographies/mapping-files/2010/geo/tiger-data.html",
        "description": (
            "US Census Demographic Profile 1 (DP1) County and Tract GeoDatabase."
        ),
        "working_partitions": {},  # Census DP1 is monolithic.
        "keywords": sorted(
            {
                "censusdp1tract",
                "census",
            }
        ),
        "license_raw": LICENSES["us-govt"],
        "license_pudl": LICENSES["cc-by-4.0"],
    },
    "eia860": {
        "title": "EIA Form 860",
        "path": "https://www.eia.gov/electricity/data/eia860",
        "description": (
            "US Energy Information Administration (EIA) Form 860 data for "
            "electric power plants with 1 megawatt or greater combined nameplate "
            "capacity."
        ),
        "source_file_dict": {
            "respondents": "Utilities",
            "records_liberated": "~1 million",
            "source_format": "Microsoft Excel (.xls/.xlsx)",
        },
        "field_namespace": "eia",
        "contributors": [
            CONTRIBUTORS["catalyst-cooperative"],
            CONTRIBUTORS["zane-selvans"],
            CONTRIBUTORS["christina-gosnell"],
            CONTRIBUTORS["steven-winter"],
            CONTRIBUTORS["alana-wilson"],
        ],
        "working_partitions": {
            "years": sorted(set(range(2001, 2022))),
        },
        "keywords": sorted(
            set(
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
            )
        ),
        "license_raw": LICENSES["us-govt"],
        "license_pudl": LICENSES["cc-by-4.0"],
    },
    "eia860m": {
        "title": "EIA Form 860m",
        "path": "https://www.eia.gov/electricity/data/eia860m",
        "description": (
            "US Energy Information Administration (EIA) Form 860 M data for "
            "electric power plants with 1 megawatt or greater combined nameplate "
            "capacity. Preliminary Monthly Electric Generator Inventory (based on "
            "Form EIA-860M as a supplement to Form EIA-860)"
        ),
        "field_namespace": "eia",
        "contributors": [],
        "working_partitions": {
            "year_month": "2021-12",
        },
        "keywords": sorted(
            set(
                [
                    "eia860m",
                    "form 860m",
                    "monthly",
                ]
                + KEYWORDS["eia"]
                + KEYWORDS["us_govt"]
                + KEYWORDS["electricity"]
                + KEYWORDS["fuels"]
                + KEYWORDS["plants"]
                + KEYWORDS["environment"]
            )
        ),
        "license_raw": LICENSES["us-govt"],
        "license_pudl": LICENSES["cc-by-4.0"],
    },
    "eia861": {
        "title": "EIA Form 861: Annual Electric Power Industry Report",
        "path": "https://www.eia.gov/electricity/data/eia861",
        "description": (
            "EIA Form 861 Annual Electric Power Industry Report, detailed "
            "data files."
        ),
        "field_namespace": "eia",
        "working_partitions": {
            "years": sorted(set(range(2001, 2021))),
        },
        "contributors": [],
        "keywords": sorted(
            set(
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
                    "yearly",
                ]
                + KEYWORDS["eia"]
                + KEYWORDS["us_govt"]
                + KEYWORDS["electricity"]
            )
        ),
        "license_raw": LICENSES["us-govt"],
        "license_pudl": LICENSES["cc-by-4.0"],
    },
    "eia923": {
        "title": "EIA Form 923",
        "path": "https://www.eia.gov/electricity/data/eia923",
        "description": (
            "The EIA Form 923 collects detailed monthly and annual electric "
            "power data on electricity generation, fuel consumption, fossil fuel "
            "stocks, and receipts at the power plant and prime mover level."
        ),
        "source_file_dict": {
            "respondents": (
                "Electric, CHP plants, and sometimes fuel transfer termianls with "
                "either 1MW+ or the ability to receive and deliver power to the grid."
            ),
            "records_liberated": "~5 million",
            "source_format": "Microsoft Excel (.xls/.xlsx)",
        },
        "field_namespace": "eia",
        "working_partitions": {
            "years": sorted(set(range(2001, 2022))),
        },
        "contributors": [
            CONTRIBUTORS["catalyst-cooperative"],
            CONTRIBUTORS["zane-selvans"],
            CONTRIBUTORS["christina-gosnell"],
            CONTRIBUTORS["steven-winter"],
            CONTRIBUTORS["katherine-lamb"],
        ],
        "keywords": sorted(
            set(
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
            )
        ),
        "license_raw": LICENSES["us-govt"],
        "license_pudl": LICENSES["cc-by-4.0"],
    },
    "eiawater": {
        "title": "EIA Thermoelectric Cooling Water",
        "path": "https://www.eia.gov/electricity/data/water",
        "description": (
            "Monthly cooling water usage by generator and boiler. Data "
            "collected in conjunction with the EIA-860 and EIA-923."
        ),
        "license_raw": LICENSES["us-govt"],
        "license_pudl": LICENSES["cc-by-4.0"],
    },
    "epacems": {
        "title": "EPA Hourly Continuous Emission Monitoring System (CEMS)",
        "path": "https://ampd.epa.gov/ampd",
        "description": (
            "US EPA hourly Continuous Emissions Monitoring System (CEMS) data."
            "Hourly CO2, SO2, NOx emissions and gross load."
        ),
        "source_file_dict": {
            "respondents": "Coal and high-sulfur fueled plants",
            "records_liberated": "~800 million",
            "source_format": "Comma Separated Value (.csv)",
        },
        "field_namespace": "epacems",
        "working_partitions": {
            "years": sorted(set(range(1995, 2022))),
            "states": sorted(set(EPACEMS_STATES)),
        },
        "contributors": [
            CONTRIBUTORS["catalyst-cooperative"],
            CONTRIBUTORS["karl-dunkle-werner"],
            CONTRIBUTORS["zane-selvans"],
        ],
        "keywords": sorted(
            set(
                [
                    "continuous emissions monitoring system",
                    "cems",
                    "air markets program data",
                    "ampd",
                    "hourly",
                ]
                + KEYWORDS["epa"]
                + KEYWORDS["us_govt"]
                + KEYWORDS["electricity"]
                + KEYWORDS["environment"]
            )
        ),
        "license_raw": LICENSES["us-govt"],
        "license_pudl": LICENSES["cc-by-4.0"],
    },
    "epacems_unitid_eia_plant_crosswalk": {
        "title": "EPA CEMS unitid to EIA Plant Crosswalk",
        "path": "https://github.com/USEPA/camd-eia-crosswalk",
        "description": (
            "A file created collaboratively by EPA and EIA that connects EPA CEMS "
            "smokestacks (unitids) with cooresponding EIA plant part ids reported in "
            "EIA Forms 860 and 923 (plant_id_eia, boiler_id, generator_id). This "
            "one-to-many connection is necessary because pollutants from various plant "
            "parts are collecitvely emitted and measured from one point-source."
        ),
        "source_file_dict": {
            "records_liberated": "~7000",
            "source_format": "Microsoft Excel (.xlsx)",
        },
        "field_namespace": "glue",
        "working_partitions": {},
        "contributors": [
            CONTRIBUTORS["catalyst-cooperative"],
            CONTRIBUTORS["austen-sharpe"],
        ],
        "keywords": sorted(
            set(
                [
                    "continuous emissions monitoring system",
                    "cems",
                    "air markets program data",
                    "ampd",
                    "hourly",
                    "eia",
                    "crosswalk",
                ]
                + KEYWORDS["epa"]
                + KEYWORDS["us_govt"]
                + KEYWORDS["electricity"]
                + KEYWORDS["environment"]
                + KEYWORDS["eia"]
                + KEYWORDS["plants"]
            )
        ),
        "license_raw": LICENSES["us-govt"],
        "license_pudl": LICENSES["cc-by-4.0"],
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
        "source_file_dict": {
            "respondents": "Major electric utilities and licenses.",
            "records_liberated": "~13.2 million (116 raw tables), ~307,000 (7 clean tables)",
            "source_format": "XBRL (.XBRL) and Visual FoxPro Database (.DBC/.DBF)",
        },
        "field_namespace": "ferc1",
        "working_partitions": {
            "years": sorted(set(range(1994, 2021))),
        },
        "contributors": [
            CONTRIBUTORS["catalyst-cooperative"],
            CONTRIBUTORS["zane-selvans"],
            CONTRIBUTORS["christina-gosnell"],
            CONTRIBUTORS["steven-winter"],
            CONTRIBUTORS["alana-wilson"],
            CONTRIBUTORS["austen-sharpe"],
        ],
        "keywords": sorted(
            set(
                [
                    "form 1",
                    "ferc1",
                ]
                + KEYWORDS["ferc"]
                + KEYWORDS["us_govt"]
                + KEYWORDS["finance"]
                + KEYWORDS["electricity"]
                + KEYWORDS["plants"]
                + KEYWORDS["fuels"]
            )
        ),
        "license_raw": LICENSES["us-govt"],
        "license_pudl": LICENSES["cc-by-4.0"],
    },
    "ferc2": {
        "title": "FERC Form 2",
        "path": "https://www.ferc.gov/industries-data/natural-gas/industry-forms/form-2-2a-3-q-gas-historical-vfp-data",
        "description": (
            "The Federal Energy Regulatory Commission (FERC) Form 2 is a "
            "comprehensive financial and operating report submitted for natural gas "
            "pipelines rate regulation and financial audits."
        ),
        "field_namespace": "ferc2",
        "working_partitions": {
            "years": [],  # Not yet working!
        },
        "license_raw": LICENSES["us-govt"],
        "license_pudl": LICENSES["cc-by-4.0"],
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
        "field_namespace": "ferc714",
        "working_partitions": {},  # Data is monolitic, one file with all years.
        "keywords": sorted(
            set(
                [
                    "form 714",
                    "ferc714",
                ]
                + KEYWORDS["ferc"]
                + KEYWORDS["us_govt"]
                + KEYWORDS["electricity"]
            )
        ),
        "license_raw": LICENSES["us-govt"],
        "license_pudl": LICENSES["cc-by-4.0"],
    },
    "ferceqr": {
        "title": "FERC Form 920: Electric Quarterly Report (EQR)",
        "path": "https://www.ferc.gov/industries-data/electric/power-sales-and-markets/electric-quarterly-reports-eqr",
        "description": (
            "The EQR contains Seller-provided data summarizing contractual terms and "
            "conditions in agreements for all jurisdictional services, including "
            "cost-based sales, market-based rate sales, and transmission service, "
            "as well as transaction information for short-term and long-term "
            "market-based power sales and cost-based power sales."
        ),
        "license_raw": LICENSES["us-govt"],
        "license_pudl": LICENSES["cc-by-4.0"],
    },
    "mshamines": {
        "title": "Mine Safety and Health Administration (MSHA) Mines",
        "path": "https://arlweb.msha.gov/OpenGovernmentData/OGIMSHA.asp",
        "description": (
            "The Mine dataset lists all Coal and Metal/Non-Metal mines under MSHA's "
            "jurisdiction. It includes such information as the current status of each "
            "mine (Active, Abandoned, NonProducing, etc.), the current owner and "
            "operating company, commodity codes and physical attributes of the mine."
        ),
        "license_raw": LICENSES["us-govt"],
        "license_pudl": LICENSES["cc-by-4.0"],
    },
    "phmsagas": {
        "title": "Pipelines and Hazardous Materials Safety Administration (PHMSA) Annual Natural Gas Report",
        "path": "https://www.phmsa.dot.gov/data-and-statistics/pipeline/gas-distribution-gas-gathering-gas-transmission-hazardous-liquids",
        "description": (
            "Annual reports submitted to PHMSA from gas distribution, gas gathering, "
            "gas transmission, liquefied natural gas, and underground gas storage "
            "system operators. Annual reports include information such as total "
            "pipeline mileage, facilities, commodities transported, miles by material, "
            "and installation dates."
        ),
        "license_raw": LICENSES["us-govt"],
        "license_pudl": LICENSES["cc-by-4.0"],
    },
    "pudl": {
        "title": "The Public Utility Data Liberation (PUDL) Project",
        "path": "https://catalyst.coop/pudl",
        "description": (
            "PUDL is a data processing pipeline created by Catalyst Cooperative that "
            "cleans, integrates, and standardizes some of the most widely used public "
            "energy datasets in the US. The data serve researchers, activists, "
            "journalists, and policy makers that might not have the technical expertise "
            "to access it in its raw form, the time to clean and prepare the data for "
            "bulk analysis, or the means to purchase it from existing commercial "
            "providers."
        ),
        "contributors": [
            CONTRIBUTORS["catalyst-cooperative"],
            CONTRIBUTORS["zane-selvans"],
            CONTRIBUTORS["christina-gosnell"],
            CONTRIBUTORS["steven-winter"],
        ],
        "field_namespace": "pudl",
        "keywords": ["us", "electricity", "open data", "open source"],
        "license_raw": LICENSES["cc-by-4.0"],
        "license_pudl": LICENSES["cc-by-4.0"],
        "email": "pudl@catalyst.coop",
    },
}
"""
Data source attributes by PUDL identifier.
"""
