"""Metadata and operational constants."""

from typing import Any

import pandas as pd

from pudl.metadata.constants import CONTRIBUTORS, KEYWORDS, LICENSES

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
        "contributors": [CONTRIBUTORS["catalyst-cooperative"]],
    },
    "censuspep": {
        "title": "Population Estimates Program's (PEP) Federal Information Processing Series (FIPS) Codes",
        "path": "https://www.census.gov/geographies/reference-files/2023/demo/popest/2023-fips.html",
        "description": (
            "Reference files for Federal Information Processing Series (FIPS) Geographic Codes. "
            "These FIPS Codes are a subset of a broader Population Estimates dataset."
        ),
        "working_partitions": {"years": [2023, 2015, 2009]},
        "keywords": sorted(
            {"fips", "census", "county", "state", "geography", "geocodes"}
        ),
        "license_raw": LICENSES["us-govt"],
        "license_pudl": LICENSES["cc-by-4.0"],
        "contributors": [CONTRIBUTORS["catalyst-cooperative"]],
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
        "working_partitions": {"years": sorted(set(range(1997, 2024)))},
        "contributors": [CONTRIBUTORS["catalyst-cooperative"]],
        "keywords": sorted(
            set(
                [
                    "eia176",
                    "form 176",
                    "natural gas",
                ]
                + KEYWORDS["eia"]
                + KEYWORDS["us_govt"]
            )
        ),
        "license_raw": LICENSES["us-govt"],
        "license_pudl": LICENSES["cc-by-4.0"],
    },
    "eia191": {
        "title": "EIA Form 191 -- Monthly Underground Natural Gas Storage Report",
        "path": "https://www.eia.gov/naturalgas/ngqs/",
        "description": (
            "The EIA Form 191, also known as the Monthly Underground Natural Gas "
            "Storage Report, describes the working and base gas in reservoirs, "
            "injections, withdrawals, and location of reservoirs by field monthly."
        ),
        "source_file_dict": {
            "respondents": (
                "All companies that operate underground natural gas storage fields in "
                "the United States."
            ),
            "source_format": "JSON",
        },
        "field_namespace": "eia",
        "working_partitions": {"years": sorted(set(range(2014, 2024)))},
        "contributors": [CONTRIBUTORS["catalyst-cooperative"]],
        "keywords": sorted(
            set(
                [
                    "eia191",
                    "form 191",
                    "natural gas",
                ]
                + KEYWORDS["eia"]
                + KEYWORDS["us_govt"]
            )
        ),
        "license_raw": LICENSES["us-govt"],
        "license_pudl": LICENSES["cc-by-4.0"],
    },
    "eia757a": {
        "title": "EIA Form 757A -- Natural Gas Processing Plant Survey",
        "path": "https://www.eia.gov/naturalgas/ngqs/",
        "description": (
            "The EIA Form 757A, also known as the Natural Gas Processing Plant Survey "
            "Schedule A provides detailed plant-level information on the capacity, "
            "status, operations and connecting infrastructure of natural gas processing "
            "plants. The form is completed tri-annually."
        ),
        "source_file_dict": {
            "respondents": ("Natural gas processing plants."),
            "source_format": "JSON",
        },
        "field_namespace": "eia",
        "working_partitions": {"years": sorted({2012, 2014, 2017})},
        "contributors": [CONTRIBUTORS["catalyst-cooperative"]],
        "keywords": sorted(
            set(
                [
                    "eia757",
                    "eia757a",
                    "form 757",
                    "form 757 schedule a",
                    "natural gas",
                ]
                + KEYWORDS["eia"]
                + KEYWORDS["us_govt"]
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
        ],
        "working_partitions": {
            "years": sorted(set(range(2001, 2025))),
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
            "year_months": [
                str(q).lower()
                for q in pd.period_range(start="2015-07", end="2025-04", freq="M")
            ],
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
            "EIA Form 861 Annual Electric Power Industry Report, detailed data files."
        ),
        "field_namespace": "eia",
        "working_partitions": {
            "years": sorted(set(range(2001, 2024))),
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
                "Electric, CHP plants, and sometimes fuel transfer terminals with "
                "either 1MW+ or the ability to receive and deliver power to the grid."
            ),
            "records_liberated": "~5 million",
            "source_format": "Microsoft Excel (.xls/.xlsx)",
        },
        "field_namespace": "eia",
        "working_partitions": {
            "years": sorted(set(range(2001, 2026))),
        },
        "contributors": [
            CONTRIBUTORS["catalyst-cooperative"],
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
    "eia930": {
        "title": "EIA Form 930 -- Hourly and Daily Balancing Authority Operations Report",
        "path": "https://www.eia.gov/electricity/gridmonitor/",
        "description": (
            "The EIA Form 930 provides hourly demand and generation statistics by "
            "balancing area (or sub-balancing area in the case of some larger areas). "
            "These statistics include a breakdown by energy source (coal, gas, hydro, "
            "wind, solar, etc.) as well as interchange between the balancing areas, "
            "including international exchanges with Canada and Mexico."
        ),
        "source_file_dict": {
            "respondents": (
                "All entities in the contiguous United States that are listed in the "
                "North American Electric Reliability Corporation’s (NERC) Compliance "
                "Registry as a balancing authority."
            ),
            "source_format": "Comma separated values (CSV)",
        },
        "field_namespace": "eia",
        "working_partitions": {
            "half_years": [
                f"{year}half{half}" for year in range(2015, 2026) for half in [1, 2]
            ][1:-1]  # Begins in H2 of 2015 and currently ends in H1 of 2025
        },
        "contributors": [
            CONTRIBUTORS["catalyst-cooperative"],
        ],
        "keywords": sorted(
            set(
                [
                    "eia930",
                    "form 930",
                    "hourly",
                    "interchange",
                    "grid operations",
                    "balancing authorities",
                    "net generation",
                ]
                + KEYWORDS["eia"]
                + KEYWORDS["us_govt"]
                + KEYWORDS["electricity"]
            )
        ),
        "license_raw": LICENSES["us-govt"],
        "license_pudl": LICENSES["cc-by-4.0"],
    },
    "eiaaeo": {
        "title": "EIA Annual Energy Outlook (AEO)",
        "path": "https://www.eia.gov/outlooks/aeo/",
        "description": (
            "The EIA Annual Energy Outlook provides projections of future fuel prices, "
            "energy supply and consumption, and carbon dioxide emissions by sector and "
            "region."
        ),
        "source_file_dict": {
            "source_format": "JSON",
        },
        "field_namespace": "eia",
        "working_partitions": {
            "years": [2023],
        },
        "contributors": [
            CONTRIBUTORS["catalyst-cooperative"],
        ],
        "keywords": sorted(
            set(
                [
                    "eia aeo",
                    "annual energy outlook",
                    "nems",
                    "fuel projections",
                    "energy supply",
                    "energy consumption",
                ]
                + KEYWORDS["eia"]
                + KEYWORDS["us_govt"]
                + KEYWORDS["electricity"]
            )
        ),
        "license_raw": LICENSES["us-govt"],
        "license_pudl": LICENSES["cc-by-4.0"],
    },
    "eiaapi": {
        "title": "EIA Bulk API Data",
        "path": "https://www.eia.gov/opendata/bulkfiles.php",
        "description": (
            "All data made available in bulk through the EIA Open Data API, "
            "including:\n"
            "* the Annual Energy Outlook, the International Energy Outlook and the Short "
            "Term Energy Outlook;\n"
            "* aggregate national, state, and mine-level coal production statistics, "
            "including imports and exports, receipts of coal at electric power plants, "
            "consumption and quality, market sales, reserves, and productive capacity; "
            "* U.S. electric system operating data;\n"
            "* aggregate national, state, and plant-level electricity generation "
            "statistics, including fuel quality and consumption, for grid-connected"
            "plants with nameplate capacity of 1 megawatt or greater;\n"
            "* CO2 emissions aggregates, CO2 emissions and carbon coefficients by fuel, "
            "state, and sector;\n"
            "* International Energy System (IES) data containing production, reserves, "
            "consumption, capacity, storage, imports, exports, and emissions time "
            "series by country for electricity, petroleum, natural gas, coal, nuclear, "
            "and renewable energy;\n"
            "* statistics of U.S. natural gas production, imports, exploration, pipelines, "
            "exports, prices, consumption, stocks, and reserves;\n"
            "* statistics of U.S. petroleum and other liquid fuel production, imports, "
            "refining, exports, prices, consumption, stocks, and reserves;\n"
            "* aggregate national, PADD, state, city, port, and refinery petroleum imports "
            "data for various grades of crude oil and country of origin;\n"
            "* state and national energy production and consumption, using survey and "
            "estimates to create comprehensive state energy statistics and flows;\n"
            "* U.S. total energy production, prices, carbon dioxide emissions, and "
            "consumption of energy from all sources by sector.\n\n"
            "At present, PUDL integrates only a few specific data series related to "
            "fuel receipts and costs figures from the Bulk Electricity API."
        ),
        "source_file_dict": {
            "source_format": "JSON",
        },
        "field_namespace": "eia",
        "working_partitions": {"data_set": "electricity"},
        "contributors": [
            CONTRIBUTORS["catalyst-cooperative"],
        ],
        "keywords": sorted(
            set(
                KEYWORDS["eia"]
                + KEYWORDS["us_govt"]
                + KEYWORDS["electricity"]
                + KEYWORDS["environment"]
                + [
                    "eia aeo",
                    "annual energy outlook",
                    "eia seo",
                    "short-term energy outlook",
                    "eia seds",
                    "state energy data system",
                    "eia ieo",
                    "international energy outlook",
                    "nems",
                    "fuel projections",
                    "energy supply",
                    "energy consumption",
                ]
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
        "keywords": sorted(
            set(KEYWORDS["eia"] + KEYWORDS["us_govt"] + KEYWORDS["eia_water"])
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
            "records_liberated": "~1 billion",
            "source_format": "Comma Separated Value (.csv)",
        },
        "field_namespace": "epacems",
        "working_partitions": {
            "year_quarters": [
                str(q).lower()
                for q in pd.period_range(start="1995q1", end="2025q1", freq="Q")
            ]
        },
        "contributors": [
            CONTRIBUTORS["catalyst-cooperative"],
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
            "smokestacks (units) with corresponding EIA plant part ids reported in "
            "EIA Forms 860 and 923 (plant_id_eia, boiler_id, generator_id). This "
            "one-to-many connection is necessary because pollutants from various plant "
            "parts are collecitvely emitted and measured from one point-source."
        ),
        "source_file_dict": {
            "records_liberated": "~7000",
            "source_format": "Comma Separated Value (.csv)",
        },
        "field_namespace": "glue",
        "working_partitions": {"year": sorted(set(range(2018, 2024)))},
        "contributors": [
            CONTRIBUTORS["catalyst-cooperative"],
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
            "records_liberated": "~13.2 million (116 raw tables), ~4.7 million (23 clean tables)",
            "source_format": "XBRL (.XBRL) and Visual FoxPro Database (.DBC/.DBF)",
        },
        "field_namespace": "ferc1",
        "working_partitions": {
            "years": sorted(set(range(1994, 2025))),
        },
        "contributors": [
            CONTRIBUTORS["catalyst-cooperative"],
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
            # Years 1991-1995 use strange formats that need to be investigated further.
            # Years 1996-1999 come in split archives and full archives and we are going
            # to be using the aggregated archives (part=None).
            "years": sorted(set(range(1996, 2024))),
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
            # Years 2000-2020 are backed by DBF format.
            # Years 2021-present are backed by XBRL.
            "years": sorted(set(range(2000, 2024))),
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
            "years": sorted(set(range(2006, 2024))),
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
            # 2021 and later data is in XBRL.
            # 2006-2020 data is in monolithic CSV files, so any year means all years.
            "years": sorted(set(range(2006, 2024))),
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
        "keywords": sorted(
            set(
                [
                    "ferceqr",
                    "electric quarterly report",
                ]
                + KEYWORDS["ferc"]
                + KEYWORDS["us_govt"]
                + KEYWORDS["electricity"]
            )
        ),
        "license_raw": LICENSES["us-govt"],
        "license_pudl": LICENSES["cc-by-4.0"],
    },
    "gridpathratoolkit": {
        "title": "GridPath Resource Adequacy Toolkit Data",
        "path": "https://gridlab.org/gridpathratoolkit/",
        "description": (
            "Hourly renewable generation profiles compiled for the Western United "
            "States as part of the GridPath Resource Adequacy Toolkit. Profiles are "
            "stated as a capacity factor (a fraction of nameplate capacity). There are "
            "3 different levels of processing or aggregation provided, all at hourly "
            "resolution: Individual plant (wind) or generator (solar) output, "
            "capacity-weighted averages of wind and solar output aggregated to the "
            "level of balancing authority territories (or transmission zones for "
            "larger balancing authorities), and that same aggregated output but with "
            "some problematic individual generator profiles modified such that they "
            "match the overall production curve of the balancing authority they are "
            "within. This data also contains some daily weather data from several "
            "sites across the western US and tables describing the way in which "
            "individual wind and solar projects were aggregated up to the level of "
            "balancing authority or transmission zone."
        ),
        "keywords": sorted(
            {
                "solar",
                "wind",
                "time series",
                "energy",
                "electricity",
                "generation",
                "weather",
                "capacity factor",
                "hourly",
                "united states",
                "usa",
                "resource adequacy",
                "gridpath",
            }
        ),
        "license_raw": LICENSES["cc-by-4.0"],
        "license_pudl": LICENSES["cc-by-4.0"],
        "working_partitions": {
            "parts": [
                "aggregated_extended_solar_capacity",
                "aggregated_extended_wind_capacity",
                # "aggregated_solar_capacity",
                # "aggregated_wind_capacity",
                "daily_weather",
                # "original_solar_capacity",
                # "original_wind_capacity",
                "solar_capacity_aggregations",
                "wind_capacity_aggregations",
            ]
        },
        "contributors": [
            CONTRIBUTORS["elaine-hart"],
        ],
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
        "keywords": sorted(set(KEYWORDS["msha"] + KEYWORDS["us_govt"])),
        "license_raw": LICENSES["us-govt"],
        "license_pudl": LICENSES["cc-by-4.0"],
    },
    "nrelatb": {
        "title": "NREL Annual Technology Baseline (ATB) for Electricity",
        "path": "https://atb.nrel.gov/",
        "description": (
            "The NREL Annual Technology Baseline (ATB) for Electricity publishes "
            "annual projections of operational and capital expenditures (by technology "
            "and vintage), as well as operating characteristics (by technology)."
        ),
        "source_file_dict": {
            "source_format": "Parquet",
        },
        "working_partitions": {
            "years": list(
                range(2021, 2025)
            ),  # see issue #3576 for why 2019 and 2020 are not working
        },
        "contributors": [
            CONTRIBUTORS["catalyst-cooperative"],
        ],
        "keywords": sorted(
            set(
                [
                    "nrel atb",
                    "annual technology baseline",
                    "opex",
                    "capex",
                    "technology",
                ]
                + KEYWORDS["eia"]
                + KEYWORDS["us_govt"]
                + KEYWORDS["electricity"]
            )
        ),
        "license_raw": LICENSES["cc-by-4.0"],
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
        "field_namespace": "phmsagas",
        "working_partitions": {
            # 1970 - 1989 are all in one CSV in multiple tabs with multi-column headers
            # and will need to be more extensively processed, not currently integrated.
            "years": sorted(set(range(1990, 2024))),
        },
        "keywords": sorted(set(KEYWORDS["phmsa"] + KEYWORDS["us_govt"])),
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
        ],
        "field_namespace": "pudl",
        "keywords": ["us", "electricity", "open data", "open source"],
        "license_raw": LICENSES["cc-by-4.0"],
        "license_pudl": LICENSES["cc-by-4.0"],
        "email": "pudl@catalyst.coop",
    },
    "sec10k": {
        "title": "U.S. Securities and Exchange Commission Form 10-K",
        "path": "https://www.sec.gov/search-filings/edgar-application-programming-interfaces",
        "description": (
            """The SEC Form 10-K is an annual report required by the U.S. Securities and
Exchange Commission (SEC), that gives a comprehensive summary of a company's financial
performance.

The full contents of the SEC 10-K are available through the SEC's EDGAR
database. PUDL integrates only some of the 10-K metadata and data extracted from the
unstructured Exhibit 21 attachment, which describes the ownership relationships between
the parent company and its subsidiaries. This data is used to create a linkage between
EIA utilities and SEC reporting companies, to better understand the relationships
between utilities and their affiliates, and the resulting economic and political impacts.

This data was originally downloaded from the SEC and processed using a machine learning
pipeline found here: https://github.com/catalyst-cooperative/mozilla-sec-eia"""
        ),
        "field_namespace": "sec",
        "working_partitions": {
            "tables": [
                "raw_sec10k__quarterly_filings",
                "raw_sec10k__quarterly_company_information",
                "raw_sec10k__parents_and_subsidiaries",
                "raw_sec10k__exhibit_21_company_ownership",
            ],
            "years": sorted(range(1993, 2024)),
        },
        "keywords": sorted(
            set(KEYWORDS["sec"] + KEYWORDS["us_govt"] + KEYWORDS["finance"])
        ),
        "license_raw": LICENSES["us-govt"],
        "license_pudl": LICENSES["cc-by-4.0"],
    },
    "vcerare": {
        "title": "Vibrant Clean Energy Resource Adequacy Renewable Energy (RARE) Power Dataset",
        "path": "https://vibrantcleanenergy.com/products/datasets/",
        "description": (
            "This dataset was produced by Vibrant Clean Energy and is licensed to "
            "the public under the Creative Commons Attribution 4.0 International "
            "license (CC-BY-4.0). The data consists of hourly, county-level renewable "
            "generation profiles in the continental United States and was compiled "
            "based on outputs from the NOAA HRRR weather model. Profiles are stated "
            "as a capacity factor (a percentage of nameplate capacity) and exist for "
            "onshore wind, offshore wind, and fixed-tilt solar generation types."
        ),
        "source_file_dict": {
            "source_format": "Comma Separated Value (.csv)",
        },
        "keywords": sorted(
            {
                "solar",
                "wind",
                "time series",
                "energy",
                "electricity",
                "generation",
                "weather",
                "capacity factor",
                "hourly",
                "united states",
                "usa",
                "resource adequacy",
                "gridpath",
                "vibrant clean energy",
                "county",
            }
        ),
        "license_raw": LICENSES["cc-by-4.0"],
        "license_pudl": LICENSES["cc-by-4.0"],
        "working_partitions": {"years": sorted(set(range(2014, 2024)))},
        "contributors": [CONTRIBUTORS["vibrant-clean-energy"]],
    },
}
"""Data source attributes by PUDL identifier."""
