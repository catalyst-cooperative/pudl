"""Static database tables."""
from io import StringIO

import pandas as pd

FERC_ACCOUNTS: pd.DataFrame = pd.DataFrame(
    columns=["row_number", "ferc_account_id", "ferc_account_description"],
    data=[
        # 1. Intangible Plant
        (2, "301", "Intangible: Organization"),
        (3, "302", "Intangible: Franchises and consents"),
        (4, "303", "Intangible: Miscellaneous intangible plant"),
        (5, "subtotal_intangible", "Subtotal: Intangible Plant"),
        # 2. Production Plant
        #  A. steam production
        (8, "310", "Steam production: Land and land rights"),
        (9, "311", "Steam production: Structures and improvements"),
        (10, "312", "Steam production: Boiler plant equipment"),
        (11, "313", "Steam production: Engines and engine-driven generators"),
        (12, "314", "Steam production: Turbogenerator units"),
        (13, "315", "Steam production: Accessory electric equipment"),
        (14, "316", "Steam production: Miscellaneous power plant equipment"),
        (
            15,
            "317",
            "Steam production: Asset retirement costs for steam production plant",
        ),
        (16, "subtotal_steam_production", "Subtotal: Steam Production Plant"),
        #  B. nuclear production
        (18, "320", "Nuclear production: Land and land rights (Major only)"),
        (19, "321", "Nuclear production: Structures and improvements (Major only)"),
        (20, "322", "Nuclear production: Reactor plant equipment (Major only)"),
        (21, "323", "Nuclear production: Turbogenerator units (Major only)"),
        (22, "324", "Nuclear production: Accessory electric equipment (Major only)"),
        (
            23,
            "325",
            "Nuclear production: Miscellaneous power plant equipment (Major only)",
        ),
        (
            24,
            "326",
            "Nuclear production: Asset retirement costs for nuclear production plant (Major only)",
        ),
        (25, "subtotal_nuclear_produciton", "Subtotal: Nuclear Production Plant"),
        #  C. hydraulic production
        (27, "330", "Hydraulic production: Land and land rights"),
        (28, "331", "Hydraulic production: Structures and improvements"),
        (29, "332", "Hydraulic production: Reservoirs, dams, and waterways"),
        (30, "333", "Hydraulic production: Water wheels, turbines and generators"),
        (31, "334", "Hydraulic production: Accessory electric equipment"),
        (32, "335", "Hydraulic production: Miscellaneous power plant equipment"),
        (33, "336", "Hydraulic production: Roads, railroads and bridges"),
        (
            34,
            "337",
            "Hydraulic production: Asset retirement costs for hydraulic production plant",
        ),
        (35, "subtotal_hydraulic_production", "Subtotal: Hydraulic Production Plant"),
        #  D. other production
        (37, "340", "Other production: Land and land rights"),
        (38, "341", "Other production: Structures and improvements"),
        (39, "342", "Other production: Fuel holders, producers, and accessories"),
        (40, "343", "Other production: Prime movers"),
        (41, "344", "Other production: Generators"),
        (42, "345", "Other production: Accessory electric equipment"),
        (43, "346", "Other production: Miscellaneous power plant equipment"),
        (
            44,
            "347",
            "Other production: Asset retirement costs for other production plant",
        ),
        (None, "348", "Other production: Energy Storage Equipment"),
        (45, "subtotal_other_production", "Subtotal: Other Production Plant"),
        (46, "subtotal_production", "Subtotal: Production Plant"),
        # 3. Transmission Plant,
        (48, "350", "Transmission: Land and land rights"),
        (None, "351", "Transmission: Energy Storage Equipment"),
        (49, "352", "Transmission: Structures and improvements"),
        (50, "353", "Transmission: Station equipment"),
        (51, "354", "Transmission: Towers and fixtures"),
        (52, "355", "Transmission: Poles and fixtures"),
        (53, "356", "Transmission: Overhead conductors and devices"),
        (54, "357", "Transmission: Underground conduit"),
        (55, "358", "Transmission: Underground conductors and devices"),
        (56, "359", "Transmission: Roads and trails"),
        (57, "359.1", "Transmission: Asset retirement costs for transmission plant"),
        (58, "subtotal_transmission", "Subtotal: Transmission Plant"),
        # 4. Distribution Plant
        (60, "360", "Distribution: Land and land rights"),
        (61, "361", "Distribution: Structures and improvements"),
        (62, "362", "Distribution: Station equipment"),
        (63, "363", "Distribution: Storage battery equipment"),
        (64, "364", "Distribution: Poles, towers and fixtures"),
        (65, "365", "Distribution: Overhead conductors and devices"),
        (66, "366", "Distribution: Underground conduit"),
        (67, "367", "Distribution: Underground conductors and devices"),
        (68, "368", "Distribution: Line transformers"),
        (69, "369", "Distribution: Services"),
        (70, "370", "Distribution: Meters"),
        (71, "371", "Distribution: Installations on customers' premises"),
        (72, "372", "Distribution: Leased property on customers' premises"),
        (73, "373", "Distribution: Street lighting and signal systems"),
        (74, "374", "Distribution: Asset retirement costs for distribution plant"),
        (75, "subtotal_distribution", "Subtotal: Distribution Plant"),
        # 5. Regional Transmission and Market Operation Plant
        (77, "380", "Regional transmission: Land and land rights"),
        (78, "381", "Regional transmission: Structures and improvements"),
        (79, "382", "Regional transmission: Computer hardware"),
        (80, "383", "Regional transmission: Computer software"),
        (81, "384", "Regional transmission: Communication Equipment"),
        (
            82,
            "385",
            "Regional transmission: Miscellaneous Regional Transmission and Market Operation Plant",
        ),
        (
            83,
            "386",
            "Regional transmission: Asset Retirement Costs for Regional Transmission and Market Operation Plant",
        ),
        (
            84,
            "subtotal_regional_transmission",
            "Subtotal: Transmission and Market Operation Plant",
        ),
        (None, "387", "Regional transmission: [Reserved]"),
        # 6. General Plant
        (86, "389", "General: Land and land rights"),
        (87, "390", "General: Structures and improvements"),
        (88, "391", "General: Office furniture and equipment"),
        (89, "392", "General: Transportation equipment"),
        (90, "393", "General: Stores equipment"),
        (91, "394", "General: Tools, shop and garage equipment"),
        (92, "395", "General: Laboratory equipment"),
        (93, "396", "General: Power operated equipment"),
        (94, "397", "General: Communication equipment"),
        (95, "398", "General: Miscellaneous equipment"),
        (96, "subtotal_general", "Subtotal: General Plant"),
        (97, "399", "General: Other tangible property"),
        (98, "399.1", "General: Asset retirement costs for general plant"),
        (99, "total_general", "TOTAL General Plant"),
        (100, "101_and_106", "Electric plant in service (Major only)"),
        (101, "102_purchased", "Electric plant purchased"),
        (102, "102_sold", "Electric plant sold"),
        (103, "103", "Experimental plant unclassified"),
        (104, "total_electric_plant", "TOTAL Electric Plant in Service"),
    ],
)
"""FERC electric plant account IDs with associated row numbers and descriptions.

From FERC Form 1 pages 204-207, Electric Plant in Service. Descriptions from:
https://www.law.cornell.edu/cfr/text/18/part-101
"""

FERC_DEPRECIATION_LINES: pd.DataFrame = pd.DataFrame(
    columns=["row_number", "line_id", "ferc_account_description"],
    data=[
        # Section A. Balances and Changes During Year
        (1, "balance_beginning_of_year", "Balance Beginning of Year"),
        (3, "depreciation_expense", "(403) Depreciation Expense"),
        (
            4,
            "depreciation_expense_asset_retirement",
            "(403.1) Depreciation Expense for Asset Retirement Costs",
        ),
        (
            5,
            "expense_electric_plant_leased_to_others",
            "(413) Exp. of Elec. Plt. Leas. to Others",
        ),
        (6, "transportation_expenses_clearing", "Transportation Expenses-Clearing"),
        (7, "other_clearing_accounts", "Other Clearing Accounts"),
        (
            8,
            "other_accounts_specified",
            "Other Accounts (Specify, details in footnote):",
        ),
        # blank: might also be other charges like line 17.
        (9, "other_charges", "Other Charges:"),
        (
            10,
            "total_depreciation_provision_for_year",
            "TOTAL Deprec. Prov for Year (Enter Total of lines 3 thru 9)",
        ),
        (11, "net_charges_for_plant_retired", "Net Charges for Plant Retired:"),
        (12, "book_cost_of_plant_retired", "Book Cost of Plant Retired"),
        (13, "cost_of_removal", "Cost of Removal"),
        (14, "salvage_credit", "Salvage (Credit)"),
        (
            15,
            "total_net_charges_for_plant_retired",
            "TOTAL Net Chrgs. for Plant Ret. (Enter Total of lines 12 thru 14)",
        ),
        (
            16,
            "other_debit_or_credit_items",
            "Other Debit or Cr. Items (Describe, details in footnote):",
        ),
        # blank: can be "Other Charges", e.g. in 2012 for PSCo.
        (17, "other_charges_2", "Other Charges 2"),
        (
            18,
            "book_cost_or_asset_retirement_costs_retired",
            "Book Cost or Asset Retirement Costs Retired",
        ),
        (
            19,
            "balance_end_of_year",
            "Balance End of Year (Enter Totals of lines 1, 10, 15, 16, and 18)",
        ),
        # Section B. Balances at End of Year According to Functional Classification
        (20, "steam_production_end_of_year", "Steam Production"),
        (21, "nuclear_production_end_of_year", "Nuclear Production"),
        (22, "hydraulic_production_end_of_year", "Hydraulic Production-Conventional"),
        (23, "pumped_storage_end_of_year", "Hydraulic Production-Pumped Storage"),
        (24, "other_production", "Other Production"),
        (25, "transmission", "Transmission"),
        (26, "distribution", "Distribution"),
        (
            27,
            "regional_transmission_and_market_operation",
            "Regional Transmission and Market Operation",
        ),
        (28, "general", "General"),
        (29, "total", "TOTAL (Enter Total of lines 20 thru 28)"),
    ],
)
"""Row numbers, FERC account IDs, and FERC account descriptions.

From FERC Form 1 page 219, Accumulated Provision for Depreciation of electric utility
plant (Account 108).
"""

EIA_SECTOR_AGGREGATE_ASSN = pd.read_csv(
    StringIO(
        """
sector_agg,sector_id_eia
electric_utility,1
ipp_non_cogen,2
ipp_cogen,3
commercial_non_cogen,4
commercial_cogen,5
industrial_non_cogen,6
industrial_cogen,7
all_sectors,1
all_sectors,2
all_sectors,3
all_sectors,4
all_sectors,5
all_sectors,6
all_sectors,7
all_ipp,2
all_ipp,3
all_commercial,4
all_commercial,5
all_industrial,6
all_industrial,7
all_electric_power,1
all_electric_power,2
all_electric_power,3
"""
    ),
)
"""Association table describing the many-to-many relationships between plant sectors and
various aggregates in fuel_receipts_costs_aggs_eia."""


EIA_FUEL_AGGREGATE_ASSN = pd.read_csv(
    StringIO(
        """
fuel_agg,energy_source_code_eia
bituminous_coal,BIT
sub_bituminous_coal,SUB
lignite_coal,LIG
all_coal,BIT
all_coal,SUB
all_coal,LIG
all_coal,WC
natural_gas,NG
petroleum_coke,PC
petroleum_liquids,DFO
petroleum_liquids,RFO
petroleum_liquids,JF
petroleum_liquids,KER
petroleum_liquids,WO
    """
    ),
)
"""Association table describing the many-to-many relationships between fuel types and
various aggregates in fuel_receipts_costs_aggs_eia.

Missing from these aggregates are all the "other" categories of gases: OG, BFG, SGP, SC,
PG. But those gases combine for about 0.2% of total MMBTU of reported fuel receipts.
"""


POLITICAL_SUBDIVISIONS: pd.DataFrame = pd.read_csv(
    StringIO(
        """
subdivision_code,subdivision_name,country_code,country_name,subdivision_type,timezone_approx,state_id_fips,division_name_us_census,division_code_us_census,region_name_us_census,is_epacems_state
AB,Alberta,CAN,Canada,province,America/Edmonton,,,,,0
AK,Alaska,USA,United States of America,state,America/Anchorage,"02",Pacific Noncontiguous,PCN,West,0
AL,Alabama,USA,United States of America,state,America/Chicago,"01",East South Central,ESC,South,1
AR,Arkansas,USA,United States of America,state,America/Chicago,"05",West South Central,WSC,South,1
AS,American Samoa,USA,United States of America,outlying_area,Pacific/Pago_Pago,"60",,,,0
AZ,Arizona,USA,United States of America,state,America/Phoenix,"04",Mountain,MTN,West,1
BC,British Columbia,CAN,Canada,province,America/Vancouver,,,,,0
CA,California,USA,United States of America,state,America/Los_Angeles,"06",Pacific Contiguous,PCC,West,1
CO,Colorado,USA,United States of America,state,America/Denver,"08",Mountain,MTN,West,1
CT,Connecticut,USA,United States of America,state,America/New_York,"09",New England,NEW,Northeast,1
DC,District of Columbia,USA,United States of America,district,America/New_York,"11",South Atlantic,SAT,South,1
DE,Delaware,USA,United States of America,state,America/New_York,"10",South Atlantic,SAT,South,1
FL,Florida,USA,United States of America,state,America/New_York,"12",South Atlantic,SAT,South,1
GA,Georgia,USA,United States of America,state,America/New_York,"13",South Atlantic,SAT,South,1
GU,Guam,USA,United States of America,outlying_area,Pacific/Guam,"66",,,,0
HI,Hawaii,USA,United States of America,state,Pacific/Honolulu,"15",Pacific Noncontiguous,PCN,West,0
IA,Iowa,USA,United States of America,state,America/Chicago,"19",West North Central,WNC,Midwest,1
ID,Idaho,USA,United States of America,state,America/Denver,"16",Mountain,MTN,West,1
IL,Illinois,USA,United States of America,state,America/Chicago,"17",East North Central,ENC,Midwest,1
IN,Indiana,USA,United States of America,state,America/New_York,"18",East North Central,ENC,Midwest,1
KS,Kansas,USA,United States of America,state,America/Chicago,"20",West North Central,WNC,Midwest,1
KY,Kentucky,USA,United States of America,state,America/New_York,"21",East South Central,ESC,South,1
LA,Louisiana,USA,United States of America,state,America/Chicago,"22",West South Central,WSC,South,1
MA,Massachusetts,USA,United States of America,state,America/New_York,"25",New England,NEW,Northeast,1
MB,Manitoba,CAN,Canada,province,America/Winnipeg,,,,,0
MD,Maryland,USA,United States of America,state,America/New_York,"24",South Atlantic,SAT,South,1
ME,Maine,USA,United States of America,state,America/New_York,"23",New England,NEW,Northeast,1
MI,Michigan,USA,United States of America,state,America/Detroit,"26",East North Central,ENC,Midwest,1
MN,Minnesota,USA,United States of America,state,America/Chicago,"27",West North Central,WNC,Midwest,1
MO,Missouri,USA,United States of America,state,America/Chicago,"29",West North Central,WNC,Midwest,1
MP,Northern Mariana Islands,USA,United States of America,outlying_area,Pacific/Guam,"69",,,,0
MS,Mississippi,USA,United States of America,state,America/Chicago,"28",East South Central,ESC,South,1
MT,Montana,USA,United States of America,state,America/Denver,"30",Mountain,MTN,West,1
NB,New Brunswick,CAN,Canada,province,America/Moncton,,,,,0
NC,North Carolina,USA,United States of America,state,America/New_York,"37",South Atlantic,SAT,South,1
ND,North Dakota,USA,United States of America,state,America/Chicago,"38",West North Central,WNC,Midwest,1
NE,Nebraska,USA,United States of America,state,America/Chicago,"31",West North Central,WNC,Midwest,1
NH,New Hampshire,USA,United States of America,state,America/New_York,"33",New England,NEW,Northeast,1
NJ,New Jersey,USA,United States of America,state,America/New_York,"34",Middle Atlantic,MAT,Northeast,1
NL,Newfoundland and Labrador,CAN,Canada,province,America/St_Johns,,,,,0
NM,New Mexico,USA,United States of America,state,America/Denver,"35",Mountain,MTN,West,1
NS,Nova Scotia,CAN,Canada,province,America/Halifax,,,,,0
NT,Northwest Territories,CAN,Canada,territory,America/Yellowknife,,,,,0
NU,Nunavut,CAN,Canada,territory,America/Iqaluit,,,,,0
NV,Nevada,USA,United States of America,state,America/Los_Angeles,"32",Mountain,MTN,West,1
NY,New York,USA,United States of America,state,America/New_York,"36",Middle Atlantic,MAT,Northeast,1
OH,Ohio,USA,United States of America,state,America/New_York,"39",East North Central,ENC,Midwest,1
OK,Oklahoma,USA,United States of America,state,America/Chicago,"40",West South Central,WSC,South,1
ON,Ontario,CAN,Canada,province,America/Toronto,,,,,0
OR,Oregon,USA,United States of America,state,America/Los_Angeles,"41",Pacific Contiguous,PCC,West,1
PA,Pennsylvania,USA,United States of America,state,America/New_York,"42",Middle Atlantic,MAT,Northeast,1
PE,Prince Edwards Island,CAN,Canada,province,America/Halifax,,,,,0
PR,Puerto Rico,USA,United States of America,outlying_area,America/Puerto_Rico,"72",,,,0
QC,Quebec,CAN,Canada,province,America/Montreal,,,,,0
RI,Rhode Island,USA,United States of America,state,America/New_York,"44",New England,NEW,Northeast,1
SC,South Carolina,USA,United States of America,state,America/New_York,"45",South Atlantic,SAT,South,1
SD,South Dakota,USA,United States of America,state,America/Chicago,"46",West North Central,WNC,Midwest,1
SK,Saskatchewan,CAN,Canada,province,America/Regina,,,,,0
TN,Tennessee,USA,United States of America,state,America/Chicago,"47",East South Central,ESC,South,1
TX,Texas,USA,United States of America,state,America/Chicago,"48",West South Central,WSC,South,1
UT,Utah,USA,United States of America,state,America/Denver,"49",Mountain,MTN,West,1
VA,Virginia,USA,United States of America,state,America/New_York,"51",South Atlantic,SAT,South,1
VI,Virgin Islands,USA,United States of America,outlying_area,America/Port_of_Spain,"78",,,,0
VT,Vermont,USA,United States of America,state,America/New_York,"50",New England,NEW,Northeast,1
WA,Washington,USA,United States of America,state,America/Los_Angeles,"53",Pacific Contiguous,PCC,West,1
WI,Wisconsin,USA,United States of America,state,America/Chicago,"55",East North Central,ENC,Midwest,1
WV,West Virginia,USA,United States of America,state,America/New_York,"54",South Atlantic,SAT,South,1
WY,Wyoming,USA,United States of America,state,America/Denver,"56",Mountain,MTN,West,1
YT,Yukon Territory,CAN,Canada,territory,America/Whitehorse,,,,,0
    """
    ),
    dtype={
        "subdivision_code": "string",
        "subdivision_name": "string",
        "country_code": "string",
        "country_name": "string",
        "subdivision_type": "string",
        "timezone": "string",
        "state_id_fips": "string",
        "division_name_us_census": "string",
        "division_code_us_census": "string",
        "region_name_us_census": "string",
        "is_epacems_state": bool,
    },
)
"""Static attributes of sub-national political jurisdictions.

Note AK and PR have incomplete EPA CEMS data, and so are excluded from is_epacems_state:
See https://github.com/catalyst-cooperative/pudl/issues/1264
"""
