"""Metadata and operational constants."""
from typing import Any

from pudl.metadata.constants import CONTRIBUTORS, KEYWORDS, LICENSES
from pudl.metadata.enums import EPACEMS_STATES

SOURCES: dict[str, Any] = {
    "censusdp1tract": {
        "title": "Census DP1 -- Profile of General Demographic Characteristics",
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
    "eia176": {
        "title": "EIA Form 176 -- Annual Report of Natural and Supplemental Gas Supply and Disposition",
        "path": "https://www.eia.gov/naturalgas/ngqs/",
        "description": (
            "The EIA Form 176, also known as the Annual Report of Natural and "
            "Supplemental Gas Supply and Disposition, describes the origins, suppliers, "
            "and disposition of natural gas on a yearly and state by state basis."
        ),
        "source_file_dict": {
            "respondents": (
                "Interstate natural gas pipeline companies; intrastate natural gas "
                "pipeline companies; natural gas distribution companies; underground "
                "natural gas storage (UNG) operators; synthetic natural gas plant "
                "operators; field, well, or processing plant operators that deliver "
                "natural gas directly to consumers (including their own industrial "
                "facilities) other than for lease or plant use or processing; "
                "field, well, or processing plant operators that transport gas to, "
                "across, or from a State border through field or gathering facilities; "
                "liquefied natural gas (LNG) storage operators."
            ),
            "source_format": "Comma Separated Value (.csv)",
        },
        "field_namespace": "eia",
        "contributors": [CONTRIBUTORS["catalyst-cooperative"]],
        "keywords": sorted(
            set(
                [
                    "eia176",
                    "form 176",
                ]
                + KEYWORDS["eia"]
                + KEYWORDS["us_govt"]
                + KEYWORDS["fuels"]
                + KEYWORDS["environment"]
            )
        ),
        "license_raw": LICENSES["us-govt"],
        "license_pudl": LICENSES["cc-by-4.0"],
    },
    "eia860": {
        "title": "EIA Form 860 -- Annual Electric Generator Report",
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
        "title": "EIA Form 860M -- Monthly Update to the Annual Electric Generator Report",
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
            "year_month": "2022-09",
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
        "title": "EIA Form 861 -- Annual Electric Power Industry Report",
        "path": "https://www.eia.gov/electricity/data/eia861",
        "description": (
            "EIA Form 861 Annual Electric Power Industry Report, detailed "
            "data files."
        ),
        "field_namespace": "eia",
        "working_partitions": {
            "years": sorted(set(range(2001, 2022))),
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
        "title": "EIA Form 923 -- Power Plant Operations Report",
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
    "eia_bulk_elec": {
        "title": "EIA Bulk Electricity API Data",
        "path": "https://www.eia.gov/opendata/bulkfiles.php",
        "description": (
            "Aggregate national, state, and plant-level electricity generation "
            "statistics, including fuel quality and consumption, for grid-connected "
            "plants with nameplate capacity of 1 megawatt or greater."
            "\n"
            "At present, PUDL integrates only a few specific data series related to "
            "fuel receipts and costs figures."
        ),
        "source_file_dict": {
            "respondents": (
                "Electric, CHP plants, and sometimes fuel transfer termianls with "
                "either 1MW+ or the ability to receive and deliver power to the grid."
            ),
            "source_format": "JSON",
        },
        "field_namespace": "eia",
        "working_partitions": {},
        "contributors": [
            CONTRIBUTORS["catalyst-cooperative"],
            CONTRIBUTORS["zane-selvans"],
            CONTRIBUTORS["trenton-bush"],
        ],
        "keywords": sorted(
            set(
                KEYWORDS["eia"]
                + KEYWORDS["us_govt"]
                + KEYWORDS["electricity"]
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
        "path": "https://campd.epa.gov/",
        "description": (
            "US EPA hourly Continuous Emissions Monitoring System (CEMS) data."
            "Hourly CO2, SO2, NOx emissions and gross load."
        ),
        "source_file_dict": {
            "respondents": "Coal and high-sulfur fueled plants over 25MW",
            "records_liberated": "~800 million",
            "source_format": "Comma Separated Value (.csv)",
        },
        "field_namespace": "epacems",
        "working_partitions": {
            "years": sorted(set(range(1995, 2022))),
            "states": sorted(EPACEMS_STATES),
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
    "epacamd_eia": {
        "title": "EPA CAMD to EIA Power Sector Data Crosswalk",
        "path": "https://github.com/USEPA/camd-eia-crosswalk",
        "description": (
            "A file created collaboratively by EPA and EIA that connects EPA CAMD "
            "smokestacks (units) with cooresponding EIA plant part ids reported in "
            "EIA Forms 860 and 923 (plant_id_eia, boiler_id, generator_id). This "
            "one-to-many connection is necessary because pollutants from various plant "
            "parts are collecitvely emitted and measured from one point-source."
        ),
        "source_file_dict": {
            "records_liberated": "~7000",
            "source_format": "Comma Separated Value (.csv)",
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
        "title": "FERC Form 1 -- Annual Report of Major Electric Utilities",
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
            "years": sorted(set(range(1994, 2022))),
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
        "title": "FERC Form 2 -- Annual Report of Major Natural Gas Companies",
        "path": "https://www.ferc.gov/industries-data/natural-gas/industry-forms/form-2-2a-3-q-gas-historical-vfp-data",
        "description": (
            "The Federal Energy Regulatory Commission (FERC) Form 2 is a "
            "comprehensive financial and operating report submitted for natural gas "
            "pipelines rate regulation and financial audits."
        ),
        "field_namespace": "ferc2",
        "working_partitions": {
            "years": sorted(set(range(2021, 2022))),  # XBRL only
        },
        "contributors": [
            CONTRIBUTORS["catalyst-cooperative"],
        ],
        "keywords": sorted(
            set(
                [
                    "form 2",
                    "ferc2",
                ]
                + KEYWORDS["ferc"]
                + KEYWORDS["finance"]
            )
        ),
        "license_raw": LICENSES["us-govt"],
        "license_pudl": LICENSES["cc-by-4.0"],
    },
    "ferc6": {
        "title": "FERC Form 6 -- Annual Report of Oil Pipeline Companies",
        "path": "https://www.ferc.gov/industries-data/electric/general-information/electric-industry-forms/form-66-q-overview-orders",
        "description": (
            "The Federal Energy Regulatory Commission (FERC) Form 6 is a "
            "comprehensive financial and operating report submitted for oil "
            "pipelines rate regulation and financial audits."
        ),
        "field_namespace": "ferc6",
        "working_partitions": {
            "years": sorted(set(range(2021, 2022))),  # XBRL only
        },
        "contributors": [
            CONTRIBUTORS["catalyst-cooperative"],
        ],
        "keywords": sorted(
            set(
                [
                    "form 6",
                    "ferc6",
                ]
                + KEYWORDS["ferc"]
                + KEYWORDS["finance"]
            )
        ),
        "license_raw": LICENSES["us-govt"],
        "license_pudl": LICENSES["cc-by-4.0"],
    },
    "ferc60": {
        "title": "FERC Form 60 -- Annual Report of Centralized Service Companies",
        "path": "https://www.ferc.gov/ferc-online/ferc-online/filing-forms/service-companies-filing-forms/form-60-annual-report",
        "description": (
            "The Federal Energy Regulatory Commission (FERC) Form 60 is a "
            "comprehensive financial and operating report submitted for centralized "
            "service companies."
        ),
        "field_namespace": "ferc60",
        "working_partitions": {
            "years": sorted(set(range(2021, 2022))),  # XBRL only
        },
        "contributors": [
            CONTRIBUTORS["catalyst-cooperative"],
        ],
        "keywords": sorted(
            set(
                [
                    "form 60",
                    "ferc60",
                ]
                + KEYWORDS["ferc"]
                + KEYWORDS["finance"]
            )
        ),
        "license_raw": LICENSES["us-govt"],
        "license_pudl": LICENSES["cc-by-4.0"],
    },
    "ferc714": {
        "title": "FERC Form 714 -- Annual Electric Balancing Authority Area and Planning Area Report",
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
        "working_partitions": {
            # 2021 and later data is in XBRL and not yet supported.
            # 2006-2020 data is in monolithic CSV files, so any year means all years.
            "years": sorted(set(range(2006, 2021))),
        },
        "contributors": [
            CONTRIBUTORS["catalyst-cooperative"],
        ],
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
        "title": "FERC Form 920 -- Electric Quarterly Report (EQR)",
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
"""Data source attributes by PUDL identifier."""
