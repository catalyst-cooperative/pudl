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
"""
FERC electric plant account IDs with associated row numbers and descriptions.

From FERC Form 1 pages 204-207, Electric Plant in Service.
Descriptions from: https://www.law.cornell.edu/cfr/text/18/part-101
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
"""
Row numbers, FERC account IDs, and FERC account descriptions.

From FERC Form 1 page 219, Accumulated Provision for Depreciation of electric
utility plant (Account 108).
"""

EIA_SECTOR_AGGREGATE_ASSOCIATION = pd.read_csv(
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
electric_power,1
electric_power,2
electric_power,3
"""
    ),
)
"""Association table describing the many-to-many relationships between plant sectors
and various aggregates in fuel_receipts_costs_aggs_eia.
"""

# PEL is defined as "Summation of all petroleum liquids (distallte fuel oil, jet fuel,
# residual fuel oil, kerosense waste oil and other petroleum liquids)"
# Missing from this list are all the "other" categories of gases: OG, BFG, SGP, SC, PG
# Those gases combine for about the same total MMBTU as DFO, about 0.2% of all reported
# fuel receipts
EIA_FUEL_AGGREGATE_ASSOCIATION = pd.read_csv(
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
"""Association table describing the many-to-many relationships between fuel types
and various aggregates in fuel_receipts_costs_aggs_eia."""


# This is NOT an association table! The bulk data has a region 'USA' that aggregates
# all states (not sure about territories)
STATES = pd.read_csv(
    StringIO(
        """
state_id_fips,state_name,state_abbrev,census_region,census_region_abbrev
"01",Alabama,AL,East South Central,ESC
"02",Alaska,AK,Pacific Noncontiguous,PCN
"04",Arizona,AZ,Mountain,MTN
"05",Arkansas,AR,West South Central,WSC
"06",California,CA,Pacific Contiguous,PCC
"08",Colorado,CO,Mountain,MTN
"09",Connecticut,CT,New England,NEW
"10",Delaware,DE,South Atlantic,SAT
"11",D.C.,DC,South Atlantic,SAT
"12",Florida,FL,South Atlantic,SAT
"13",Georgia,GA,South Atlantic,SAT
"15",Hawaii,HI,Pacific Noncontiguous,PCN
"16",Idaho,ID,Mountain,MTN
"17",Illinois,IL,East North Central,ENC
"18",Indiana,IN,East North Central,ENC
"19",Iowa,IA,West North Central,WNC
"20",Kansas,KS,West North Central,WNC
"21",Kentucky,KY,East South Central,ESC
"22",Louisiana,LA,West South Central,WSC
"23",Maine,ME,New England,NEW
"24",Maryland,MD,South Atlantic,SAT
"25",Massachusetts,MA,New England,NEW
"26",Michigan,MI,East North Central,ENC
"27",Minnesota,MN,West North Central,WNC
"28",Mississippi,MS,East South Central,ESC
"29",Missouri,MO,West North Central,WNC
"30",Montana,MT,Mountain,MTN
"31",Nebraska,NE,West North Central,WNC
"32",Nevada,NV,Mountain,MTN
"33",New Hampshire,NH,New England,NEW
"34",New Jersey,NJ,Middle Atlantic,MAT
"35",New Mexico,NM,Mountain,MTN
"36",New York,NY,Middle Atlantic,MAT
"37",North Carolina,NC,South Atlantic,SAT
"38",North Dakota,ND,West North Central,WNC
"39",Ohio,OH,East North Central,ENC
"40",Oklahoma,OK,West South Central,WSC
"41",Oregon,OR,Pacific Contiguous,PCC
"42",Pennsylvania,PA,Middle Atlantic,MAT
"44",Rhode Island,RI,New England,NEW
"45",South Carolina,SC,South Atlantic,SAT
"46",South Dakota,SD,West North Central,WNC
"47",Tennessee,TN,East South Central,ESC
"48",Texas,TX,West South Central,WSC
"49",Utah,UT,Mountain,MTN
"50",Vermont,VT,New England,NEW
"51",Virginia,VA,South Atlantic,SAT
"53",Washington,WA,Pacific Contiguous,PCC
"54",West Virginia,WV,South Atlantic,SAT
"55",Wisconsin,WI,East North Central,ENC
"56",Wyoming,WY,Mountain,MTN
"60",American Samoa,AS,,
"66",Guam,GU,,
"69",Northern Mariana Islands,MP,,
"72",Puerto Rico,PR,,
"78",Virgin Islands,VI,,
    """
    ),
)
"""Table of state information, including the census regions used in
fuel_receipts_costs_aggs_eia."""
