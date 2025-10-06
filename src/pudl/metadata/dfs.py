"""Static database tables."""

from enum import Enum, unique
from io import StringIO

import pandas as pd


@unique
class ImputationReasonCodes(Enum):
    """Defines all reasons a value might be flagged for imputation."""

    MISSING_VALUE = "Indicates that reported value was already NULL."
    ANOMALOUS_REGION = "Indicates that value is surrounded by flagged values."
    NEGATIVE_OR_ZERO = "Indicates value is negative or zero."
    IDENTICAL_RUN = "Indicates value is part of an identical run of values, excluding first value in run."
    GLOBAL_OUTLIER = (
        "Indicates value is greater or less than n times the global median."
    )
    GLOBAL_OUTLIER_NEIGHBOR = "Indicates value neighbors global outliers."
    LOCAL_OUTLIER_HIGH = "Indicates value is a local outlier on the high end."
    LOCAL_OUTLIER_LOW = "Indicates value is a local outlier on the low end."
    DOUBLE_DELTA = "Indicates value is very different from neighbors on either side."
    SINGLE_DELTA = (
        "Indicates value is significantly different from nearest unflagged value."
    )
    BAD_YEAR = "Indicates the entire year of data for a respondent was dropped due to too much missing data."
    SIMULATED = "Used for scoring imputation using simulated data. SHOULD NOT APPEAR IN PRODUCTION DATA."


IMPUTATION_REASON_CODES = pd.DataFrame(
    [
        {"code": code.name.lower(), "description": code.value}
        for code in ImputationReasonCodes
    ]
)


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
        (25, "subtotal_nuclear_production", "Subtotal: Nuclear Production Plant"),
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

BALANCING_AUTHORITY_SUBREGIONS_EIA: pd.DataFrame = pd.read_csv(
    StringIO(
        """balancing_authority_code_eia,balancing_authority_subregion_code_eia,balancing_authority_subregion_name_eia
CISO,PGAE,Pacific Gas and Electric
CISO,SCE,Southern California Edison
CISO,SDGE,San Diego Gas and Electric
CISO,VEA,Valley Electric Association
ERCO,COAS,Coast
ERCO,EAST,East
ERCO,FWES,Far West
ERCO,NCEN,North Central
ERCO,NRTH,North
ERCO,SCEN,South Central
ERCO,SOUT,South
ERCO,WEST,West
ISNE,"4001",Maine
ISNE,"4002",New Hampshire
ISNE,"4003",Vermont
ISNE,"4004",Connecticut
ISNE,"4005",Rhode Island
ISNE,"4006",Southeast Mass.
ISNE,"4007",Western/Central Mass.
ISNE,"4008",Northeast Mass.
MISO,"0001",Zone 1
MISO,"0004",Zone 4
MISO,"0006",Zone 6
MISO,"0027",Zones 2 and 7
MISO,"0035",Zones 3 and 5
MISO,"8910","Zones 8, 9, and 10"
NYIS,ZONA,West
NYIS,ZONB,Genesee
NYIS,ZONC,Central
NYIS,ZOND,North
NYIS,ZONE,Mohawk Valley
NYIS,ZONF,Capital
NYIS,ZONG,Hudson Valley
NYIS,ZONH,Millwood
NYIS,ZONI,Dunwoodie
NYIS,ZONJ,New York City
NYIS,ZONK,Long Island
PJM,AE,Atlantic Electric zone
PJM,AEP,American Electric Power zone
PJM,AP,Allegheny Power zone
PJM,ATSI,"American Transmission Systems, Inc. zone"
PJM,BC,Baltimore Gas & Electric zone
PJM,CE,Commonwealth Edison zone
PJM,DAY,Dayton Power & Light zone
PJM,DEOK,Duke Energy Ohio/Kentucky zone
PJM,DOM,Dominion Virginia Power zone
PJM,DPL,Delmarva Power & Light zone
PJM,DUQ,Duquesne Lighting Company zone
PJM,EKPC,East Kentucky Power Cooperative zone
PJM,JC,Jersey Central Power & Light zone
PJM,ME,Metropolitan Edison zone
PJM,PE,PECO Energy zone
PJM,PEP,Potomac Electric Power zone
PJM,PL,Pennsylvania Power & Light zone
PJM,PN,Pennsylvania Electric zone
PJM,PS,Public Service Electric & Gas zone
PJM,RECO,Rockland Electric (East) zone
PNM,ACMA,City of Acoma Pueblo
PNM,CYGA,City of Gallup
PNM,FREP,Freeport
PNM,JICA,Jicarilla Apache Nation
PNM,KAFB,Kirtland Air Force Base
PNM,KCEC,Kit Carson Electric Cooperative
PNM,LAC,Los Alamos County
PNM,NTUA,Navajo Tribal Utility Authority
PNM,PNM,PNM System Firm Load
PNM,TSGT,Tri-State Generation and Transmission
SWPP,CSWS,AEPW American Electric Power West
SWPP,EDE,Empire District Electric Company
SWPP,GRDA,Grand River Dam Authority
SWPP,INDN,Independence Power & Light
SWPP,KACY,Kansas City Board of Public Utilities
SWPP,KCPL,Kansas City Power & Light
SWPP,LES,Lincoln Electric System
SWPP,MPS,KCP&L Greater Missouri Operations
SWPP,NPPD,Nebraska Public Power District
SWPP,OKGE,Oklahoma Gas and Electric Co.
SWPP,OPPD,Omaha Public Power District
SWPP,SECI,Sunflower Electric
SWPP,SPRM,City of Springfield
SWPP,SPS,Southwestern Public Service Company
SWPP,WAUE,Western Area Power Upper Great Plains East
SWPP,WFEC,Western Farmers Electric Cooperative
SWPP,WR,Westar Energy"""
    ),
).convert_dtypes()

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
various aggregates in core_eia__yearly_fuel_receipts_costs_aggs."""


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
various aggregates in core_eia__yearly_fuel_receipts_costs_aggs.

Missing from these aggregates are all the "other" categories of gases: OG, BFG, SGP, SC,
PG. But those gases combine for about 0.2% of total MMBTU of reported fuel receipts.
"""


POLITICAL_SUBDIVISIONS: pd.DataFrame = pd.read_csv(
    StringIO(
        """
subdivision_code,subdivision_name,country_code,country_name,subdivision_type,timezone_approx,state_id_fips,division_name_us_census,division_code_us_census,region_name_us_census,is_epacems_state
AB,Alberta,CAN,Canada,province,America/Edmonton,,,,,0
AK,Alaska,USA,United States of America,state,America/Anchorage,"02",Pacific Noncontiguous,PCN,West,1
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
HI,Hawaii,USA,United States of America,state,Pacific/Honolulu,"15",Pacific Noncontiguous,PCN,West,1
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
PR,Puerto Rico,USA,United States of America,outlying_area,America/Puerto_Rico,"72",,,,1
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
See
https://github.com/catalyst-cooperative/pudl/issues/1264
"""

SEC_EDGAR_STATE_AND_COUNTRY_CODES = pd.DataFrame(
    {
        "al": "alabama",
        "ak": "alaska",
        "az": "arizona",
        "ar": "arkansas",
        "ca": "california",
        "co": "colorado",
        "ct": "connecticut",
        "de": "delaware",
        "dc": "district of columbia",
        "fl": "florida",
        "ga": "georgia",
        "hi": "hawaii",
        "id": "idaho",
        "il": "illinois",
        "in": "indiana",
        "ia": "iowa",
        "ks": "kansas",
        "ky": "kentucky",
        "la": "louisiana",
        "me": "maine",
        "md": "maryland",
        "ma": "massachusetts",
        "mi": "michigan",
        "mn": "minnesota",
        "ms": "mississippi",
        "mo": "missouri",
        "mt": "montana",
        "ne": "nebraska",
        "nv": "nevada",
        "nh": "new hampshire",
        "nj": "new jersey",
        "nm": "new mexico",
        "ny": "new york",
        "nc": "north carolina",
        "nd": "north dakota",
        "oh": "ohio",
        "ok": "oklahoma",
        "or": "oregon",
        "pa": "pennsylvania",
        "ri": "rhode island",
        "sc": "south carolina",
        "sd": "south dakota",
        "tn": "tennessee",
        "tx": "texas",
        "x1": "united states",
        "ut": "utah",
        "vt": "vermont",
        "va": "virginia",
        "wa": "washington",
        "wv": "west virginia",
        "wi": "wisconsin",
        "wy": "wyoming",
        "a0": "alberta, canada",
        "a1": "british columbia, canada",
        "a2": "manitoba, canada",
        "a3": "new brunswick, canada",
        "a4": "newfoundland, canada",
        "a5": "nova scotia, canada",
        "a6": "ontario, canada",
        "a7": "prince edward island, canada",
        "a8": "quebec, canada",
        "a9": "saskatchewan, canada",
        "b0": "yukon, canada",
        "z4": "canada (federal level)",
        "b2": "afghanistan",
        "y6": "aland islands",
        "b3": "albania",
        "b4": "algeria",
        "b5": "american samoa",
        "b6": "andorra",
        "b7": "angola",
        "1a": "anguilla",
        "b8": "antarctica",
        "b9": "antigua and barbuda",
        "c1": "argentina",
        "1b": "armenia",
        "1c": "aruba",
        "c3": "australia",
        "c4": "austria",
        "1d": "azerbaijan",
        "c5": "bahamas",
        "c6": "bahrain",
        "c7": "bangladesh",
        "c8": "barbados",
        "1f": "belarus",
        "c9": "belgium",
        "d1": "belize",
        "g6": "benin",
        "d0": "bermuda",
        "d2": "bhutan",
        "d3": "bolivia",
        "1e": "bosnia and herzegovina",
        "b1": "botswana",
        "d4": "bouvet island",
        "d5": "brazil",
        "d6": "british indian ocean territory",
        "d9": "brunei darussalam",
        "e0": "bulgaria",
        "x2": "burkina faso",
        "e2": "burundi",
        "e3": "cambodia",
        "e4": "cameroon",
        "e8": "cape verde",
        "e9": "cayman islands",
        "f0": "central african republic",
        "f2": "chad",
        "f3": "chile",
        "f4": "china",
        "f6": "christmas island",
        "f7": "cocos (keeling) islands",
        "f8": "colombia",
        "f9": "comoros",
        "g0": "congo",
        "y3": "congo, the democratic republic of the",
        "g1": "cook islands",
        "g2": "costa rica",
        "l7": "cote d'ivoire",
        "1m": "croatia",
        "g3": "cuba",
        "g4": "cyprus",
        "2n": "czech republic",
        "g7": "denmark",
        "1g": "djibouti",
        "g9": "dominica",
        "g8": "dominican republic",
        "h1": "ecuador",
        "h2": "egypt",
        "h3": "el salvador",
        "h4": "equatorial guinea",
        "1j": "eritrea",
        "1h": "estonia",
        "h5": "ethiopia",
        "h7": "falkland islands (malvinas)",
        "h6": "faroe islands",
        "h8": "fiji",
        "h9": "finland",
        "i0": "france",
        "i3": "french guiana",
        "i4": "french polynesia",
        "2c": "french southern territories",
        "i5": "gabon",
        "i6": "gambia",
        "2q": "georgia",
        "2m": "germany",
        "j0": "ghana",
        "j1": "gibraltar",
        "j3": "greece",
        "j4": "greenland",
        "j5": "grenada",
        "j6": "guadeloupe",
        "gu": "guam",
        "j8": "guatemala",
        "y7": "guernsey",
        "j9": "guinea",
        "s0": "guinea-bissau",
        "k0": "guyana",
        "k1": "haiti",
        "k4": "heard island and mcdonald islands",
        "x4": "holy see (vatican city state)",
        "k2": "honduras",
        "k3": "hong kong",
        "k5": "hungary",
        "k6": "iceland",
        "k7": "india",
        "k8": "indonesia",
        "k9": "iran, islamic republic of",
        "l0": "iraq",
        "l2": "ireland",
        "y8": "isle of man",
        "l3": "israel",
        "l6": "italy",
        "l8": "jamaica",
        "m0": "japan",
        "y9": "jersey",
        "m2": "jordan",
        "1p": "kazakhstan",
        "m3": "kenya",
        "j2": "kiribati",
        "m4": "korea, democratic people's republic of",
        "m5": "korea, republic of",
        "m6": "kuwait",
        "1n": "kyrgyzstan",
        "m7": "lao people's democratic republic",
        "1r": "latvia",
        "m8": "lebanon",
        "m9": "lesotho",
        "n0": "liberia",
        "n1": "libyan arab jamahiriya",
        "n2": "liechtenstein",
        "1q": "lithuania",
        "n4": "luxembourg",
        "n5": "macau",
        "1u": "macedonia, the former yugoslav republic of",
        "n6": "madagascar",
        "n7": "malawi",
        "n8": "malaysia",
        "n9": "maldives",
        "o0": "mali",
        "o1": "malta",
        "1t": "marshall islands",
        "o2": "martinique",
        "o3": "mauritania",
        "o4": "mauritius",
        "2p": "mayotte",
        "o5": "mexico",
        "1k": "micronesia, federated states of",
        "1s": "moldova, republic of",
        "o9": "monaco",
        "p0": "mongolia",
        "z5": "montenegro",
        "p1": "montserrat",
        "p2": "morocco",
        "p3": "mozambique",
        "e1": "myanmar",
        "t6": "namibia",
        "p5": "nauru",
        "p6": "nepal",
        "p7": "netherlands",
        "p8": "netherlands antilles",
        "1w": "new caledonia",
        "q2": "new zealand",
        "q3": "nicaragua",
        "q4": "niger",
        "q5": "nigeria",
        "q6": "niue",
        "q7": "norfolk island",
        "1v": "northern mariana islands",
        "q8": "norway",
        "p4": "oman",
        "r0": "pakistan",
        "1y": "palau",
        "1x": "palestinian territory, occupied",
        "r1": "panama",
        "r2": "papua new guinea",
        "r4": "paraguay",
        "r5": "peru",
        "r6": "philippines",
        "r8": "pitcairn",
        "r9": "poland",
        "s1": "portugal",
        "pr": "puerto rico",
        "s3": "qatar",
        "s4": "reunion",
        "s5": "romania",
        "1z": "russian federation",
        "s6": "rwanda",
        "z0": "saint barthelemy",
        "u8": "saint helena",
        "u7": "saint kitts and nevis",
        "u9": "saint lucia",
        "z1": "saint martin",
        "v0": "saint pierre and miquelon",
        "v1": "saint vincent and the grenadines",
        "y0": "samoa",
        "s8": "san marino",
        "s9": "sao tome and principe",
        "t0": "saudi arabia",
        "t1": "senegal",
        "z2": "serbia",
        "t2": "seychelles",
        "t8": "sierra leone",
        "u0": "singapore",
        "2b": "slovakia",
        "2a": "slovenia",
        "d7": "solomon islands",
        "u1": "somalia",
        "t3": "south africa",
        "1l": "south georgia and the south sandwich islands",
        "u3": "spain",
        "f1": "sri lanka",
        "v2": "sudan",
        "v3": "suriname",
        "l9": "svalbard and jan mayen",
        "v6": "swaziland",
        "v7": "sweden",
        "v8": "switzerland",
        "v9": "syrian arab republic",
        "f5": "taiwan, province of china",
        "2d": "tajikistan",
        "w0": "tanzania, united republic of",
        "w1": "thailand",
        "z3": "timor-leste",
        "w2": "togo",
        "w3": "tokelau",
        "w4": "tonga",
        "w5": "trinidad and tobago",
        "w6": "tunisia",
        "w8": "turkey",
        "2e": "turkmenistan",
        "w7": "turks and caicos islands",
        "2g": "tuvalu",
        "w9": "uganda",
        "2h": "ukraine",
        "c0": "united arab emirates",
        "x0": "united kingdom",
        "2j": "united states minor outlying islands",
        "x3": "uruguay",
        "2k": "uzbekistan",
        "2l": "vanuatu",
        "x5": "venezuela",
        "q1": "vietnam",
        "d8": "virgin islands, british",
        "vi": "virgin islands, u.s.",
        "x8": "wallis and futuna",
        "u5": "western sahara",
        "t7": "yemen",
        "y4": "zambia",
        "y5": "zimbabwe",
        "xx": "unknown",
    }.items(),
    columns=["state_or_country_code", "state_or_country_name"],
)
"""State and country codes and their names as are reported to SEC's EDGAR database.

These codes are used for XML filings of Ownership Reports (Forms 3, 4, 5), Form D and Form ID in EDGAR.
Table found at https://www.sec.gov/submit-filings/filer-support-resources/edgar-state-country-codes .
Used in PUDL to standardize the state codes in the SEC 10K filings."""

ALPHA_2_COUNTRY_CODES = pd.DataFrame(
    {
        "af": "afghanistan",
        "ax": "åland islands",
        "al": "albania",
        "dz": "algeria",
        "as": "american samoa",
        "ad": "andorra",
        "ao": "angola",
        "ai": "anguilla",
        "aq": "antarctica",
        "ag": "antigua and barbuda",
        "ar": "argentina",
        "am": "armenia",
        "aw": "aruba",
        "au": "australia",
        "at": "austria",
        "az": "azerbaijan",
        "bs": "bahamas",
        "bh": "bahrain",
        "bd": "bangladesh",
        "bb": "barbados",
        "by": "belarus",
        "be": "belgium",
        "bz": "belize",
        "bj": "benin",
        "bm": "bermuda",
        "bt": "bhutan",
        "bo": "bolivia, plurinational state of",
        "bq": "bonaire, sint eustatius and saba",
        "ba": "bosnia and herzegovina",
        "bw": "botswana",
        "bv": "bouvet island",
        "br": "brazil",
        "io": "british indian ocean territory",
        "bn": "brunei darussalam",
        "bg": "bulgaria",
        "bf": "burkina faso",
        "bi": "burundi",
        "cv": "cabo verde",
        "kh": "cambodia",
        "cm": "cameroon",
        "ca": "canada",
        "ky": "cayman islands",
        "cf": "central african republic",
        "td": "chad",
        "cl": "chile",
        "cn": "china",
        "cx": "christmas island",
        "cc": "cocos (keeling) islands",
        "co": "colombia",
        "km": "comoros",
        "cg": "congo",
        "cd": "congo, democratic republic of the",
        "ck": "cook islands",
        "cr": "costa rica",
        "ci": "côte d'ivoire",
        "hr": "croatia",
        "cu": "cuba",
        "cw": "curaçao",
        "cy": "cyprus",
        "cz": "czechia",
        "dk": "denmark",
        "dj": "djibouti",
        "dm": "dominica",
        "do": "dominican republic",
        "ec": "ecuador",
        "eg": "egypt",
        "sv": "el salvador",
        "gq": "equatorial guinea",
        "er": "eritrea",
        "ee": "estonia",
        "sz": "eswatini",
        "et": "ethiopia",
        "fk": "falkland islands (malvinas)",
        "fo": "faroe islands",  # spellchecker:ignore
        "fj": "fiji",
        "fi": "finland",
        "fr": "france",
        "gf": "french guiana",
        "pf": "french polynesia",
        "tf": "french southern territories",
        "ga": "gabon",
        "gm": "gambia",
        "ge": "georgia",
        "de": "germany",
        "gh": "ghana",
        "gi": "gibraltar",
        "gr": "greece",
        "gl": "greenland",
        "gd": "grenada",
        "gp": "guadeloupe",
        "gu": "guam",
        "gt": "guatemala",
        "gg": "guernsey",
        "gn": "guinea",
        "gw": "guinea-bissau",
        "gy": "guyana",
        "ht": "haiti",
        "hm": "heard island and mcdonald islands",
        "va": "holy see",
        "hn": "honduras",
        "hk": "hong kong",
        "hu": "hungary",
        "is": "iceland",
        "in": "india",
        "id": "indonesia",
        "ir": "iran, islamic republic of",
        "iq": "iraq",
        "ie": "ireland",
        "im": "isle of man",
        "il": "israel",
        "it": "italy",
        "jm": "jamaica",
        "jp": "japan",
        "je": "jersey",
        "jo": "jordan",
        "kz": "kazakhstan",
        "ke": "kenya",
        "ki": "kiribati",
        "kp": "korea, democratic people's republic of",
        "kr": "korea, republic of",
        "kw": "kuwait",
        "kg": "kyrgyzstan",
        "la": "lao people's democratic republic",
        "lv": "latvia",
        "lb": "lebanon",
        "ls": "lesotho",
        "lr": "liberia",
        "ly": "libya",
        "li": "liechtenstein",
        "lt": "lithuania",
        "lu": "luxembourg",
        "mo": "macao",
        "mg": "madagascar",
        "mw": "malawi",
        "my": "malaysia",
        "mv": "maldives",
        "ml": "mali",
        "mt": "malta",
        "mh": "marshall islands",
        "mq": "martinique",
        "mr": "mauritania",
        "mu": "mauritius",
        "yt": "mayotte",
        "mx": "mexico",
        "fm": "micronesia, federated states of",
        "md": "moldova, republic of",
        "mc": "monaco",
        "mn": "mongolia",
        "me": "montenegro",
        "ms": "montserrat",
        "ma": "morocco",
        "mz": "mozambique",
        "mm": "myanmar",
        "na": "namibia",
        "nr": "nauru",
        "np": "nepal",
        "nl": "netherlands, kingdom of the",
        "nc": "new caledonia",
        "nz": "new zealand",
        "ni": "nicaragua",
        "ne": "niger",
        "ng": "nigeria",
        "nu": "niue",
        "nf": "norfolk island",
        "mk": "north macedonia",
        "mp": "northern mariana islands",
        "no": "norway",
        "om": "oman",
        "pk": "pakistan",
        "pw": "palau",
        "ps": "palestine, state of",
        "pa": "panama",
        "pg": "papua new guinea",
        "py": "paraguay",
        "pe": "peru",
        "ph": "philippines",
        "pn": "pitcairn",
        "pl": "poland",
        "pt": "portugal",
        "pr": "puerto rico",
        "qa": "qatar",
        "re": "réunion",
        "ro": "romania",
        "ru": "russian federation",
        "rw": "rwanda",
        "bl": "saint barthélemy",
        "sh": "saint helena, ascension and tristan da cunha",
        "kn": "saint kitts and nevis",
        "lc": "saint lucia",
        "mf": "saint martin (french part)",
        "pm": "saint pierre and miquelon",
        "vc": "saint vincent and the grenadines",
        "ws": "samoa",
        "sm": "san marino",
        "st": "sao tome and principe",
        "sa": "saudi arabia",
        "sn": "senegal",
        "rs": "serbia",
        "sc": "seychelles",
        "sl": "sierra leone",
        "sg": "singapore",
        "sx": "sint maarten (dutch part)",
        "sk": "slovakia",
        "si": "slovenia",
        "sb": "solomon islands",
        "so": "somalia",
        "za": "south africa",
        "gs": "south georgia and the south sandwich islands",
        "ss": "south sudan",
        "es": "spain",
        "lk": "sri lanka",
        "sd": "sudan",
        "sr": "suriname",
        "sj": "svalbard and jan mayen",
        "se": "sweden",
        "ch": "switzerland",
        "sy": "syrian arab republic",
        "tw": "taiwan, province of china",
        "tj": "tajikistan",
        "tz": "tanzania, united republic of",
        "th": "thailand",
        "tl": "timor-leste",
        "tg": "togo",
        "tk": "tokelau",
        "to": "tonga",
        "tt": "trinidad and tobago",
        "tn": "tunisia",
        "tr": "türkiye",
        "tm": "turkmenistan",
        "tc": "turks and caicos islands",
        "tv": "tuvalu",
        "ug": "uganda",
        "ua": "ukraine",
        "ae": "united arab emirates",
        "gb": "united kingdom of great britain and northern ireland",
        "us": "united states",
        "um": "united states minor outlying islands",
        "uy": "uruguay",
        "uz": "uzbekistan",
        "vu": "vanuatu",
        "ve": "venezuela, bolivarian republic of",
        "vn": "vietnam",
        "vg": "virgin islands (british)",
        "vi": "virgin islands (u.s.)",
        "wf": "wallis and futuna",
        "eh": "western sahara",
        "ye": "yemen",
        "zm": "zambia",
        "zw": "zimbabwe",
        "uk": "united kingdom",
    }.items(),
    columns=["country_code", "country_name"],
)
"""
Alpha 2 country codes and the country's name.

Most SEC locations from Ex. 21 attachments match the two digit EDGAR codes, however
some use alpha 2 country codes, i.e. us -> united states and ch -> switzerland.
Map these codes as well for location standardization.
"""

STANDARD_INDUSTRIAL_CLASSIFICATION: pd.DataFrame = pd.read_csv(
    StringIO(
        """industry_id_sic,industry_group_sic,industry_name_sic
"0100","Industrial Applications and Services","AGRICULTURAL PRODUCTION-CROPS"
"0200","Industrial Applications and Services","AGRICULTURAL PROD-LIVESTOCK & ANIMAL SPECIALTIES"
"0700","Industrial Applications and Services","AGRICULTURAL SERVICES"
"0800","Industrial Applications and Services","FORESTRY"
"0900","Industrial Applications and Services","FISHING, HUNTING AND TRAPPING"
"1000","Office of Energy & Transportation","METAL MINING"
"1040","Office of Energy & Transportation","GOLD AND SILVER ORES"
"1090","Office of Energy & Transportation","MISCELLANEOUS METAL ORES"
"1220","Office of Energy & Transportation","BITUMINOUS COAL & LIGNITE MINING"
"1221","Office of Energy & Transportation","BITUMINOUS COAL & LIGNITE SURFACE MINING"
"1311","Office of Energy & Transportation","CRUDE PETROLEUM & NATURAL GAS"
"1381","Office of Energy & Transportation","DRILLING OIL & GAS WELLS"
"1382","Office of Energy & Transportation","OIL & GAS FIELD EXPLORATION SERVICES"
"1389","Office of Energy & Transportation","OIL & GAS FIELD SERVICES, NEC"
"1400","Office of Energy & Transportation","MINING & QUARRYING OF NONMETALLIC MINERALS (NO FUELS)"
"1520","Office of Real Estate & Construction","GENERAL BLDG CONTRACTORS - RESIDENTIAL BLDGS"
"1531","Office of Real Estate & Construction","OPERATIVE BUILDERS"
"1540","Office of Real Estate & Construction","GENERAL BLDG CONTRACTORS - NONRESIDENTIAL BLDGS"
"1600","Office of Real Estate & Construction","HEAVY CONSTRUCTION OTHER THAN BLDG CONST - CONTRACTORS"
"1623","Office of Real Estate & Construction","WATER, SEWER, PIPELINE, COMM & POWER LINE CONSTRUCTION"
"1700","Office of Real Estate & Construction","CONSTRUCTION - SPECIAL TRADE CONTRACTORS"
"1731","Office of Real Estate & Construction","ELECTRICAL WORK"
"2000","Office of Manufacturing","FOOD AND KINDRED PRODUCTS"
"2011","Office of Manufacturing","MEAT PACKING PLANTS"
"2013","Office of Manufacturing","SAUSAGES & OTHER PREPARED MEAT PRODUCTS"
"2015","Office of Manufacturing","POULTRY SLAUGHTERING AND PROCESSING"
"2020","Office of Manufacturing","DAIRY PRODUCTS"
"2024","Office of Manufacturing","ICE CREAM & FROZEN DESSERTS"
"2030","Office of Manufacturing","CANNED, FROZEN & PRESERVD FRUIT, VEG & FOOD SPECIALTIES"
"2033","Office of Manufacturing","CANNED, FRUITS, VEG, PRESERVES, JAMS & JELLIES"
"2040","Office of Manufacturing","GRAIN MILL PRODUCTS"
"2050","Office of Manufacturing","BAKERY PRODUCTS"
"2052","Office of Manufacturing","COOKIES & CRACKERS"
"2060","Office of Manufacturing","SUGAR & CONFECTIONERY PRODUCTS"
"2070","Office of Manufacturing","FATS & OILS"
"2080","Office of Manufacturing","BEVERAGES"
"2082","Office of Manufacturing","MALT BEVERAGES"
"2086","Office of Manufacturing","BOTTLED & CANNED SOFT DRINKS & CARBONATED WATERS"
"2090","Office of Manufacturing","MISCELLANEOUS FOOD PREPARATIONS & KINDRED PRODUCTS"
"2092","Office of Manufacturing","PREPARED FRESH OR FROZEN FISH & SEAFOODS"
"2100","Office of Manufacturing","TOBACCO PRODUCTS"
"2111","Office of Manufacturing","CIGARETTES"
"2200","Office of Manufacturing","TEXTILE MILL PRODUCTS"
"2211","Office of Manufacturing","BROADWOVEN FABRIC MILLS, COTTON"
"2221","Office of Manufacturing","BROADWOVEN FABRIC MILLS, MAN MADE FIBER & SILK"
"2250","Office of Manufacturing","KNITTING MILLS"
"2253","Office of Manufacturing","KNIT OUTERWEAR MILLS"
"2273","Office of Manufacturing","CARPETS & RUGS"
"2300","Office of Manufacturing","APPAREL & OTHER FINISHED PRODS OF FABRICS & SIMILAR MATERIAL"
"2320","Office of Manufacturing","MEN'S & BOYS' FURNISHINGS, WORK CLOTHING, & ALLIED GARMENTS"
"2330","Office of Manufacturing","WOMEN'S, MISSES', AND JUNIORS OUTERWEAR"
"2340","Office of Manufacturing","WOMEN'S, MISSES', CHILDREN'S & INFANTS' UNDERGARMENTS"
"2390","Office of Manufacturing","MISCELLANEOUS FABRICATED TEXTILE PRODUCTS"
"2400","Office of Manufacturing","LUMBER & WOOD PRODUCTS (NO FURNITURE)"
"2421","Office of Manufacturing","SAWMILLS & PLANTING MILLS, GENERAL"
"2430","Office of Manufacturing","MILLWOOD, VENEER, PLYWOOD, & STRUCTURAL WOOD MEMBERS"
"2451","Office of Manufacturing","MOBILE HOMES"
"2452","Office of Manufacturing","PREFABRICATED WOOD BLDGS & COMPONENTS"
"2510","Office of Manufacturing","HOUSEHOLD FURNITURE"
"2511","Office of Manufacturing","WOOD HOUSEHOLD FURNITURE, (NO UPHOLSTERED)"
"2520","Office of Manufacturing","OFFICE FURNITURE"
"2522","Office of Manufacturing","OFFICE FURNITURE (NO WOOD)"
"2531","Office of Manufacturing","PUBLIC BLDG & RELATED FURNITURE"
"2540","Office of Manufacturing","PARTITIONS, SHELVG, LOCKERS, & OFFICE & STORE FIXTURES"
"2590","Office of Manufacturing","MISCELLANEOUS FURNITURE & FIXTURES"
"2600","Office of Manufacturing","PAPERS & ALLIED PRODUCTS"
"2611","Office of Manufacturing","PULP MILLS"
"2621","Office of Manufacturing","PAPER MILLS"
"2631","Office of Manufacturing","PAPERBOARD MILLS"
"2650","Office of Manufacturing","PAPERBOARD CONTAINERS & BOXES"
"2670","Office of Manufacturing","CONVERTED PAPER & PAPERBOARD PRODS (NO CONTANERS/BOXES)"
"2673","Office of Manufacturing","PLASTICS, FOIL & COATED PAPER BAGS"
"2711","Office of Manufacturing","NEWSPAPERS: PUBLISHING OR PUBLISHING & PRINTING"
"2721","Office of Manufacturing","PERIODICALS: PUBLISHING OR PUBLISHING & PRINTING"
"2731","Office of Manufacturing","BOOKS: PUBLISHING OR PUBLISHING & PRINTING"
"2732","Office of Manufacturing","BOOK PRINTING"
"2741","Office of Manufacturing","MISCELLANEOUS PUBLISHING"
"2750","Office of Manufacturing","COMMERCIAL PRINTING"
"2761","Office of Manufacturing","MANIFOLD BUSINESS FORMS"
"2771","Office of Manufacturing","GREETING CARDS"
"2780","Office of Manufacturing","BLANKBOOKS, LOOSELEAF BINDERS & BOOKBINDING & RELATED WORK"
"2790","Office of Manufacturing","SERVICE INDUSTRIES FOR THE PRINTING TRADE"
"2800","Industrial Applications and Services","CHEMICALS & ALLIED PRODUCTS"
"2810","Industrial Applications and Services","INDUSTRIAL INORGANIC CHEMICALS"
"2820","Industrial Applications and Services","PLASTIC MATERIAL, SYNTH RESIN/RUBBER, CELLULOSE (NO GLASS)"
"2821","Industrial Applications and Services","PLASTIC MATERIALS, SYNTH RESINS & NONVULCAN ELASTOMERS"
"2833","Office of Life Sciences","MEDICINAL CHEMICALS & BOTANICAL PRODUCTS"
"2834","Office of Life Sciences","PHARMACEUTICAL PREPARATIONS"
"2835","Office of Life Sciences","IN VITRO & IN VIVO DIAGNOSTIC SUBSTANCES"
"2836","Office of Life Sciences","BIOLOGICAL PRODUCTS, (NO DIAGNOSTIC SUBSTANCES)"
"2840","Industrial Applications and Services","SOAP, DETERGENTS, CLEANG PREPARATIONS, PERFUMES, COSMETICS"
"2842","Industrial Applications and Services","SPECIALTY CLEANING, POLISHING AND SANITATION PREPARATIONS"
"2844","Industrial Applications and Services","PERFUMES, COSMETICS & OTHER TOILET PREPARATIONS"
"2851","Industrial Applications and Services","PAINTS, VARNISHES, LACQUERS, ENAMELS & ALLIED PRODS"
"2860","Industrial Applications and Services","INDUSTRIAL ORGANIC CHEMICALS"
"2870","Industrial Applications and Services","AGRICULTURAL CHEMICALS"
"2890","Industrial Applications and Services","MISCELLANEOUS CHEMICAL PRODUCTS"
"2891","Industrial Applications and Services","ADHESIVES & SEALANTS"
"2911","Office of Energy & Transportation","PETROLEUM REFINING"
"2950","Office of Energy & Transportation","ASPHALT PAVING & ROOFING MATERIALS"
"2990","Office of Energy & Transportation","MISCELLANEOUS PRODUCTS OF PETROLEUM & COAL"
"3011","Office of Manufacturing","TIRES & INNER TUBES"
"3021","Office of Manufacturing","RUBBER & PLASTICS FOOTWEAR"
"3050","Office of Manufacturing","GASKETS, PACKG & SEALG DEVICES & RUBBER & PLASTICS HOSE"
"3060","Office of Manufacturing","FABRICATED RUBBER PRODUCTS, NEC"
"3080","Industrial Applications and Services","MISCELLANEOUS PLASTICS PRODUCTS"
"3081","Industrial Applications and Services","UNSUPPORTED PLASTICS FILM & SHEET"
"3086","Industrial Applications and Services","PLASTICS FOAM PRODUCTS"
"3089","Industrial Applications and Services","PLASTICS PRODUCTS, NEC"
"3100","Office of Manufacturing","LEATHER & LEATHER PRODUCTS"
"3140","Office of Manufacturing","FOOTWEAR, (NO RUBBER)"
"3211","Office of Manufacturing","FLAT GLASS"
"3220","Office of Manufacturing","GLASS & GLASSWARE, PRESSED OR BLOWN"
"3221","Office of Manufacturing","GLASS CONTAINERS"
"3231","Office of Manufacturing","GLASS PRODUCTS, MADE OF PURCHASED GLASS"
"3241","Office of Manufacturing","CEMENT, HYDRAULIC"
"3250","Office of Manufacturing","STRUCTURAL CLAY PRODUCTS"
"3260","Office of Manufacturing","POTTERY & RELATED PRODUCTS"
"3270","Office of Manufacturing","CONCRETE, GYPSUM & PLASTER PRODUCTS"
"3272","Office of Manufacturing","CONCRETE PRODUCTS, EXCEPT BLOCK & BRICK"
"3281","Office of Manufacturing","CUT STONE & STONE PRODUCTS"
"3290","Office of Manufacturing","ABRASIVE, ASBESTOS & MISC NONMETALLIC MINERAL PRODS"
"3310","Office of Manufacturing","STEEL WORKS, BLAST FURNACES & ROLLING & FINISHING MILLS"
"3312","Office of Manufacturing","STEEL WORKS, BLAST FURNACES & ROLLING MILLS (COKE OVENS)"
"3317","Office of Manufacturing","STEEL PIPE & TUBES"
"3320","Office of Manufacturing","IRON & STEEL FOUNDRIES"
"3330","Office of Manufacturing","PRIMARY SMELTING & REFINING OF NONFERROUS METALS"
"3334","Office of Manufacturing","PRIMARY PRODUCTION OF ALUMINUM"
"3341","Office of Manufacturing","SECONDARY SMELTING & REFINING OF NONFERROUS METALS"
"3350","Office of Manufacturing","ROLLING DRAWING & EXTRUDING OF NONFERROUS METALS"
"3357","Office of Manufacturing","DRAWING & INSULATING OF NONFERROUS WIRE"
"3360","Office of Manufacturing","NONFERROUS FOUNDRIES (CASTINGS)"
"3390","Office of Manufacturing","MISCELLANEOUS PRIMARY METAL PRODUCTS"
"3411","Office of Manufacturing","METAL CANS"
"3412","Office of Manufacturing","METAL SHIPPING BARRELS, DRUMS, KEGS & PAILS"
"3420","Office of Manufacturing","CUTLERY, HANDTOOLS & GENERAL HARDWARE"
"3430","Office of Manufacturing","HEATING EQUIP, EXCEPT ELEC & WARM AIR; & PLUMBING FIXTURES"
"3433","Office of Manufacturing","HEATING EQUIPMENT, EXCEPT ELECTRIC & WARM AIR FURNACES"
"3440","Office of Manufacturing","FABRICATED STRUCTURAL METAL PRODUCTS"
"3442","Office of Manufacturing","METAL DOORS, SASH, FRAMES, MOLDINGS & TRIM"
"3443","Office of Manufacturing","FABRICATED PLATE WORK (BOILER SHOPS)"
"3444","Office of Manufacturing","SHEET METAL WORK"
"3448","Office of Manufacturing","PREFABRICATED METAL BUILDINGS & COMPONENTS"
"3451","Office of Manufacturing","SCREW MACHINE PRODUCTS"
"3452","Office of Manufacturing","BOLTS, NUTS, SCREWS, RIVETS & WASHERS"
"3460","Office of Manufacturing","METAL FORGINGS & STAMPINGS"
"3470","Office of Manufacturing","COATING, ENGRAVING & ALLIED SERVICES"
"3480","Office of Manufacturing","ORDNANCE & ACCESSORIES, (NO VEHICLES/GUIDED MISSILES)"
"3490","Office of Manufacturing","MISCELLANEOUS FABRICATED METAL PRODUCTS"
"3510","Office of Technology","ENGINES & TURBINES"
"3523","Office of Technology","FARM MACHINERY & EQUIPMENT"
"3524","Office of Technology","LAWN & GARDEN TRACTORS & HOME LAWN & GARDENS EQUIP"
"3530","Office of Technology","CONSTRUCTION, MINING & MATERIALS HANDLING MACHINERY & EQUIP"
"3531","Office of Technology","CONSTRUCTION MACHINERY & EQUIP"
"3532","Office of Technology","MINING MACHINERY & EQUIP (NO OIL & GAS FIELD MACH & EQUIP)"
"3533","Office of Energy & Transportation","OIL & GAS FIELD MACHINERY & EQUIPMENT"
"3537","Office of Technology","INDUSTRIAL TRUCKS, TRACTORS, TRAILORS & STACKERS"
"3540","Office of Technology","METALWORKG MACHINERY & EQUIPMENT"
"3541","Office of Technology","MACHINE TOOLS, METAL CUTTING TYPES"
"3550","Office of Technology","SPECIAL INDUSTRY MACHINERY (NO METALWORKING MACHINERY)"
"3555","Office of Technology","PRINTING TRADES MACHINERY & EQUIPMENT"
"3559","Office of Technology","SPECIAL INDUSTRY MACHINERY, NEC"
"3560","Office of Technology","GENERAL INDUSTRIAL MACHINERY & EQUIPMENT"
"3561","Office of Technology","PUMPS & PUMPING EQUIPMENT"
"3562","Office of Technology","BALL & ROLLER BEARINGS"
"3564","Office of Technology","INDUSTRIAL & COMMERCIAL FANS & BLOWERS & AIR PURIFING EQUIP"
"3567","Office of Technology","INDUSTRIAL PROCESS FURNACES & OVENS"
"3569","Office of Technology","GENERAL INDUSTRIAL MACHINERY & EQUIPMENT, NEC"
"3570","Office of Technology","COMPUTER & OFFICE EQUIPMENT"
"3571","Office of Technology","ELECTRONIC COMPUTERS"
"3572","Office of Technology","COMPUTER STORAGE DEVICES"
"3575","Office of Technology","COMPUTER TERMINALS"
"3576","Office of Technology","COMPUTER COMMUNICATIONS EQUIPMENT"
"3577","Office of Technology","COMPUTER PERIPHERAL EQUIPMENT, NEC"
"3578","Office of Technology","CALCULATING & ACCOUNTING MACHINES (NO ELECTRONIC COMPUTERS)"
"3579","Office of Technology","OFFICE MACHINES, NEC"
"3580","Office of Technology","REFRIGERATION & SERVICE INDUSTRY MACHINERY"
"3585","Office of Technology","AIR-COND & WARM AIR HEATG EQUIP & COMM & INDL REFRIG EQUIP"
"3590","Office of Technology","MISC INDUSTRIAL & COMMERCIAL MACHINERY & EQUIPMENT"
"3600","Office of Manufacturing","ELECTRONIC & OTHER ELECTRICAL EQUIPMENT (NO COMPUTER EQUIP)"
"3612","Office of Manufacturing","POWER, DISTRIBUTION & SPECIALTY TRANSFORMERS"
"3613","Office of Manufacturing","SWITCHGEAR & SWITCHBOARD APPARATUS"
"3620","Office of Manufacturing","ELECTRICAL INDUSTRIAL APPARATUS"
"3621","Office of Manufacturing","MOTORS & GENERATORS"
"3630","Office of Manufacturing","HOUSEHOLD APPLIANCES"
"3634","Office of Manufacturing","ELECTRIC HOUSEWARES & FANS"
"3640","Office of Manufacturing","ELECTRIC LIGHTING & WIRING EQUIPMENT"
"3651","Office of Manufacturing","HOUSEHOLD AUDIO & VIDEO EQUIPMENT"
"3652","Office of Manufacturing","PHONOGRAPH RECORDS & PRERECORDED AUDIO TAPES & DISKS"
"3661","Office of Manufacturing","TELEPHONE & TELEGRAPH APPARATUS"
"3663","Office of Manufacturing","RADIO & TV BROADCASTING & COMMUNICATIONS EQUIPMENT"
"3669","Office of Manufacturing","COMMUNICATIONS EQUIPMENT, NEC"
"3670","Office of Manufacturing","ELECTRONIC COMPONENTS & ACCESSORIES"
"3672","Office of Manufacturing","PRINTED CIRCUIT BOARDS"
"3674","Office of Manufacturing","SEMICONDUCTORS & RELATED DEVICES"
"3677","Office of Manufacturing","ELECTRONIC COILS, TRANSFORMERS & OTHER INDUCTORS"
"3678","Office of Manufacturing","ELECTRONIC CONNECTORS"
"3679","Office of Manufacturing","ELECTRONIC COMPONENTS, NEC"
"3690","Office of Manufacturing","MISCELLANEOUS ELECTRICAL MACHINERY, EQUIPMENT & SUPPLIES"
"3695","Office of Manufacturing","MAGNETIC & OPTICAL RECORDING MEDIA"
"3711","Office of Manufacturing","MOTOR VEHICLES & PASSENGER CAR BODIES"
"3713","Office of Manufacturing","TRUCK & BUS BODIES"
"3714","Office of Manufacturing","MOTOR VEHICLE PARTS & ACCESSORIES"
"3715","Office of Manufacturing","TRUCK TRAILERS"
"3716","Office of Manufacturing","MOTOR HOMES"
"3720","Office of Manufacturing","AIRCRAFT & PARTS"
"3721","Office of Manufacturing","AIRCRAFT"
"3724","Office of Manufacturing","AIRCRAFT ENGINES & ENGINE PARTS"
"3728","Office of Manufacturing","AIRCRAFT PARTS & AUXILIARY EQUIPMENT, NEC"
"3730","Office of Manufacturing","SHIP & BOAT BUILDING & REPAIRING"
"3743","Office of Manufacturing","RAILROAD EQUIPMENT"
"3751","Office of Manufacturing","MOTORCYCLES, BICYCLES & PARTS"
"3760","Office of Manufacturing","GUIDED MISSILES & SPACE VEHICLES & PARTS"
"3790","Office of Manufacturing","MISCELLANEOUS TRANSPORTATION EQUIPMENT"
"3812","Office of Manufacturing","SEARCH, DETECTION, NAVIGATION, GUIDANCE, AERONAUTICAL SYS"
"3821","Industrial Applications and Services","LABORATORY APPARATUS & FURNITURE"
"3822","Industrial Applications and Services","AUTO CONTROLS FOR REGULATING RESIDENTIAL & COMML ENVIRONMENTS"
"3823","Industrial Applications and Services","INDUSTRIAL INSTRUMENTS FOR MEASUREMENT, DISPLAY, AND CONTROL"
"3824","Industrial Applications and Services","TOTALIZING FLUID METERS & COUNTING DEVICES"
"3825","Industrial Applications and Services","INSTRUMENTS FOR MEAS & TESTING OF ELECTRICITY & ELEC SIGNALS"
"3826","Industrial Applications and Services","LABORATORY ANALYTICAL INSTRUMENTS"
"3827","Industrial Applications and Services","OPTICAL INSTRUMENTS & LENSES"
"3829","Industrial Applications and Services","MEASURING & CONTROLLING DEVICES, NEC"
"3841","Industrial Applications and Services","SURGICAL & MEDICAL INSTRUMENTS & APPARATUS"
"3842","Industrial Applications and Services","ORTHOPEDIC, PROSTHETIC & SURGICAL APPLIANCES & SUPPLIES"
"3843","Industrial Applications and Services","DENTAL EQUIPMENT & SUPPLIES"
"3844","Industrial Applications and Services","X-RAY APPARATUS & TUBES & RELATED IRRADIATION APPARATUS"
"3845","Industrial Applications and Services","ELECTROMEDICAL & ELECTROTHERAPEUTIC APPARATUS"
"3851","Industrial Applications and Services","OPHTHALMIC GOODS"
"3861","Industrial Applications and Services","PHOTOGRAPHIC EQUIPMENT & SUPPLIES"
"3873","Industrial Applications and Services","WATCHES, CLOCKS, CLOCKWORK OPERATED DEVICES/PARTS"
"3910","Office of Manufacturing","JEWELRY, SILVERWARE & PLATED WARE"
"3911","Office of Manufacturing","JEWELRY, PRECIOUS METAL"
"3931","Office of Manufacturing","MUSICAL INSTRUMENTS"
"3942","Office of Manufacturing","DOLLS & STUFFED TOYS"
"3944","Office of Manufacturing","GAMES, TOYS & CHILDREN'S VEHICLES (NO DOLLS & BICYCLES)"
"3949","Office of Manufacturing","SPORTING & ATHLETIC GOODS, NEC"
"3950","Office of Manufacturing","PENS, PENCILS & OTHER ARTISTS' MATERIALS"
"3960","Office of Manufacturing","COSTUME JEWELRY & NOVELTIES"
"3990","Office of Manufacturing","MISCELLANEOUS MANUFACTURING INDUSTRIES"
"4011","Office of Energy & Transportation","RAILROADS, LINE-HAUL OPERATING"
"4013","Office of Energy & Transportation","RAILROAD SWITCHING & TERMINAL ESTABLISHMENTS"
"4100","Office of Energy & Transportation","LOCAL & SUBURBAN TRANSIT & INTERURBAN HWY PASSENGER TRANS"
"4210","Office of Energy & Transportation","TRUCKING & COURIER SERVICES (NO AIR)"
"4213","Office of Energy & Transportation","TRUCKING (NO LOCAL)"
"4220","Office of Energy & Transportation","PUBLIC WAREHOUSING & STORAGE"
"4231","Office of Energy & Transportation","TERMINAL MAINTENANCE FACILITIES FOR MOTOR FREIGHT TRANSPORT"
"4400","Office of Energy & Transportation","WATER TRANSPORTATION"
"4412","Office of Energy & Transportation","DEEP SEA FOREIGN TRANSPORTATION OF FREIGHT"
"4512","Office of Energy & Transportation","AIR TRANSPORTATION, SCHEDULED"
"4513","Office of Energy & Transportation","AIR COURIER SERVICES"
"4522","Office of Energy & Transportation","AIR TRANSPORTATION, NONSCHEDULED"
"4581","Office of Energy & Transportation","AIRPORTS, FLYING FIELDS & AIRPORT TERMINAL SERVICES"
"4610","Office of Energy & Transportation","PIPE LINES (NO NATURAL GAS)"
"4700","Office of Energy & Transportation","TRANSPORTATION SERVICES"
"4731","Office of Energy & Transportation","ARRANGEMENT OF TRANSPORTATION OF FREIGHT & CARGO"
"4812","Office of Technology","RADIOTELEPHONE COMMUNICATIONS"
"4813","Office of Technology","TELEPHONE COMMUNICATIONS (NO RADIOTELEPHONE)"
"4822","Office of Technology","TELEGRAPH & OTHER MESSAGE COMMUNICATIONS"
"4832","Office of Technology","RADIO BROADCASTING STATIONS"
"4833","Office of Technology","TELEVISION BROADCASTING STATIONS"
"4841","Office of Technology","CABLE & OTHER PAY TELEVISION SERVICES"
"4899","Office of Technology","COMMUNICATIONS SERVICES, NEC"
"4900","Office of Energy & Transportation","ELECTRIC, GAS & SANITARY SERVICES"
"4911","Office of Energy & Transportation","ELECTRIC SERVICES"
"4922","Office of Energy & Transportation","NATURAL GAS TRANSMISSION"
"4923","Office of Energy & Transportation","NATURAL GAS TRANSMISSION & DISTRIBUTION"
"4924","Office of Energy & Transportation","NATURAL GAS DISTRIBUTION"
"4931","Office of Energy & Transportation","ELECTRIC & OTHER SERVICES COMBINED"
"4932","Office of Energy & Transportation","GAS & OTHER SERVICES COMBINED"
"4941","Office of Energy & Transportation","WATER SUPPLY"
"4950","Office of Energy & Transportation","SANITARY SERVICES"
"4953","Office of Energy & Transportation","REFUSE SYSTEMS"
"4955","Office of Energy & Transportation","HAZARDOUS WASTE MANAGEMENT"
"4961","Office of Energy & Transportation","STEAM & AIR-CONDITIONING SUPPLY"
"4991","Office of Energy & Transportation","COGENERATION SERVICES & SMALL POWER PRODUCERS"
"5000","Office of Trade & Services","WHOLESALE-DURABLE GOODS"
"5010","Office of Trade & Services","WHOLESALE-MOTOR VEHICLES & MOTOR VEHICLE PARTS & SUPPLIES"
"5013","Office of Trade & Services","WHOLESALE-MOTOR VEHICLE SUPPLIES & NEW PARTS"
"5020","Office of Trade & Services","WHOLESALE-FURNITURE & HOME FURNISHINGS"
"5030","Office of Trade & Services","WHOLESALE-LUMBER & OTHER CONSTRUCTION MATERIALS"
"5031","Office of Trade & Services","WHOLESALE-LUMBER, PLYWOOD, MILLWORK & WOOD PANELS"
"5040","Office of Trade & Services","WHOLESALE-PROFESSIONAL & COMMERCIAL EQUIPMENT & SUPPLIES"
"5045","Office of Trade & Services","WHOLESALE-COMPUTERS & PERIPHERAL EQUIPMENT & SOFTWARE"
"5047","Office of Trade & Services","WHOLESALE-MEDICAL, DENTAL & HOSPITAL EQUIPMENT & SUPPLIES"
"5050","Office of Trade & Services","WHOLESALE-METALS & MINERALS (NO PETROLEUM)"
"5051","Office of Trade & Services","WHOLESALE-METALS SERVICE CENTERS & OFFICES"
"5063","Office of Trade & Services","WHOLESALE-ELECTRICAL APPARATUS & EQUIPMENT, WIRING SUPPLIES"
"5064","Office of Trade & Services","WHOLESALE-ELECTRICAL APPLIANCES, TV & RADIO SETS"
"5065","Office of Trade & Services","WHOLESALE-ELECTRONIC PARTS & EQUIPMENT, NEC"
"5070","Office of Trade & Services","WHOLESALE-HARDWARE & PLUMBING & HEATING EQUIPMENT & SUPPLIES"
"5072","Office of Trade & Services","WHOLESALE-HARDWARE"
"5080","Office of Trade & Services","WHOLESALE-MACHINERY, EQUIPMENT & SUPPLIES"
"5082","Office of Trade & Services","WHOLESALE-CONSTRUCTION & MINING (NO PETRO) MACHINERY & EQUIP"
"5084","Office of Trade & Services","WHOLESALE-INDUSTRIAL MACHINERY & EQUIPMENT"
"5090","Office of Trade & Services","WHOLESALE-MISC DURABLE GOODS"
"5094","Office of Trade & Services","WHOLESALE-JEWELRY, WATCHES, PRECIOUS STONES & METALS"
"5099","Office of Trade & Services","WHOLESALE-DURABLE GOODS, NEC"
"5110","Office of Trade & Services","WHOLESALE-PAPER & PAPER PRODUCTS"
"5122","Office of Trade & Services","WHOLESALE-DRUGS, PROPRIETARIES & DRUGGISTS' SUNDRIES"
"5130","Office of Trade & Services","WHOLESALE-APPAREL, PIECE GOODS & NOTIONS"
"5140","Office of Trade & Services","WHOLESALE-GROCERIES & RELATED PRODUCTS"
"5141","Office of Trade & Services","WHOLESALE-GROCERIES, GENERAL LINE"
"5150","Office of Trade & Services","WHOLESALE-FARM PRODUCT RAW MATERIALS"
"5160","Office of Trade & Services","WHOLESALE-CHEMICALS & ALLIED PRODUCTS"
"5171","Office of Trade & Services","WHOLESALE-PETROLEUM BULK STATIONS & TERMINALS"
"5172","Office of Trade & Services","WHOLESALE-PETROLEUM & PETROLEUM PRODUCTS (NO BULK STATIONS)"
"5180","Office of Trade & Services","WHOLESALE-BEER, WINE & DISTILLED ALCOHOLIC BEVERAGES"
"5190","Office of Trade & Services","WHOLESALE-MISCELLANEOUS NONDURABLE GOODS"
"5200","Office of Trade & Services","RETAIL-BUILDING MATERIALS, HARDWARE, GARDEN SUPPLY"
"5211","Office of Trade & Services","RETAIL-LUMBER & OTHER BUILDING MATERIALS DEALERS"
"5271","Office of Trade & Services","RETAIL-MOBILE HOME DEALERS"
"5311","Office of Trade & Services","RETAIL-DEPARTMENT STORES"
"5331","Office of Trade & Services","RETAIL-VARIETY STORES"
"5399","Office of Trade & Services","RETAIL-MISC GENERAL MERCHANDISE STORES"
"5400","Office of Trade & Services","RETAIL-FOOD STORES"
"5411","Office of Trade & Services","RETAIL-GROCERY STORES"
"5412","Office of Trade & Services","RETAIL-CONVENIENCE STORES"
"5500","Office of Trade & Services","RETAIL-AUTO DEALERS & GASOLINE STATIONS"
"5531","Office of Trade & Services","RETAIL-AUTO & HOME SUPPLY STORES"
"5600","Office of Trade & Services","RETAIL-APPAREL & ACCESSORY STORES"
"5621","Office of Trade & Services","RETAIL-WOMEN'S CLOTHING STORES"
"5651","Office of Trade & Services","RETAIL-FAMILY CLOTHING STORES"
"5661","Office of Trade & Services","RETAIL-SHOE STORES"
"5700","Office of Trade & Services","RETAIL-HOME FURNITURE, FURNISHINGS & EQUIPMENT STORES"
"5712","Office of Trade & Services","RETAIL-FURNITURE STORES"
"5731","Office of Trade & Services","RETAIL-RADIO, TV & CONSUMER ELECTRONICS STORES"
"5734","Office of Trade & Services","RETAIL-COMPUTER & COMPUTER SOFTWARE STORES"
"5735","Office of Trade & Services","RETAIL-RECORD & PRERECORDED TAPE STORES"
"5810","Office of Trade & Services","RETAIL-EATING & DRINKING PLACES"
"5812","Office of Trade & Services","RETAIL-EATING PLACES"
"5900","Office of Trade & Services","RETAIL-MISCELLANEOUS RETAIL"
"5912","Office of Trade & Services","RETAIL-DRUG STORES AND PROPRIETARY STORES"
"5940","Office of Trade & Services","RETAIL-MISCELLANEOUS SHOPPING GOODS STORES"
"5944","Office of Trade & Services","RETAIL-JEWELRY STORES"
"5945","Office of Trade & Services","RETAIL-HOBBY, TOY & GAME SHOPS"
"5960","Office of Trade & Services","RETAIL-NONSTORE RETAILERS"
"5961","Office of Trade & Services","RETAIL-CATALOG & MAIL-ORDER HOUSES"
"5990","Office of Trade & Services","RETAIL-RETAIL STORES, NEC"
"6021","Office of Finance","NATIONAL COMMERCIAL BANKS"
"6022","Office of Finance","STATE COMMERCIAL BANKS"
"6029","Office of Finance","COMMERCIAL BANKS, NEC"
"6035","Office of Finance","SAVINGS INSTITUTION, FEDERALLY CHARTERED"
"6036","Office of Finance","SAVINGS INSTITUTIONS, NOT FEDERALLY CHARTERED"
"6099","Office of Finance","FUNCTIONS RELATED TO DEPOSITORY BANKING, NEC"
"6111","Office of Finance","FEDERAL & FEDERALLY-SPONSORED CREDIT AGENCIES"
"6141","Office of Finance","PERSONAL CREDIT INSTITUTIONS"
"6153","Office of Finance","SHORT-TERM BUSINESS CREDIT INSTITUTIONS"
"6159","Office of Finance","MISCELLANEOUS BUSINESS CREDIT INSTITUTION"
"6162","Office of Finance","MORTGAGE BANKERS & LOAN CORRESPONDENTS"
"6163","Office of Finance","LOAN BROKERS"
"6172","Office of Finance","FINANCE LESSORS"
"6189","Office of Structured Finance","ASSET-BACKED SECURITIES"
"6199","Office of Finance or Office of Crypto Assets","FINANCE SERVICES"
"6200","Office of Crypto Assets","SECURITY & COMMODITY BROKERS, DEALERS, EXCHANGES & SERVICES"
"6211","Office of Finance or Office of Crypto Assets","SECURITY BROKERS, DEALERS & FLOTATION COMPANIES"
"6221","Office of Crypto Assets","COMMODITY CONTRACTS BROKERS & DEALERS"
"6282","Office of Finance","INVESTMENT ADVICE"
"6311","Office of Finance","LIFE INSURANCE"
"6321","Office of Finance","ACCIDENT & HEALTH INSURANCE"
"6324","Office of Finance","HOSPITAL & MEDICAL SERVICE PLANS"
"6331","Office of Finance","FIRE, MARINE & CASUALTY INSURANCE"
"6351","Office of Finance","SURETY INSURANCE"
"6361","Office of Finance","TITLE INSURANCE"
"6399","Office of Finance","INSURANCE CARRIERS, NEC"
"6411","Office of Finance","INSURANCE AGENTS, BROKERS & SERVICE"
"6500","Office of Real Estate & Construction","REAL ESTATE"
"6510","Office of Real Estate & Construction","REAL ESTATE OPERATORS (NO DEVELOPERS) & LESSORS"
"6512","Office of Real Estate & Construction","OPERATORS OF NONRESIDENTIAL BUILDINGS"
"6513","Office of Real Estate & Construction","OPERATORS OF APARTMENT BUILDINGS"
"6519","Office of Real Estate & Construction","LESSORS OF REAL PROPERTY, NEC"
"6531","Office of Real Estate & Construction","REAL ESTATE AGENTS & MANAGERS (FOR OTHERS)"
"6532","Office of Real Estate & Construction","REAL ESTATE DEALERS (FOR THEIR OWN ACCOUNT)"
"6552","Office of Real Estate & Construction","LAND SUBDIVIDERS & DEVELOPERS (NO CEMETERIES)"
"6770","Office of Real Estate & Construction","BLANK CHECKS"
"6792","Office of Energy & Transportation","OIL ROYALTY TRADERS"
"6794","Office of Real Estate & Construction","PATENT OWNERS & LESSORS"
"6795","Office of Energy & Transportation","MINERAL ROYALTY TRADERS"
"6798","Office of Real Estate & Construction","REAL ESTATE INVESTMENT TRUSTS"
"6799","Office of Real Estate & Construction","INVESTORS, NEC"
"7000","Office of Real Estate & Construction","HOTELS, ROOMING HOUSES, CAMPS & OTHER LODGING PLACES"
"7011","Office of Real Estate & Construction","HOTELS & MOTELS"
"7200","Office of Trade & Services","SERVICES-PERSONAL SERVICES"
"7310","Office of Trade & Services","SERVICES-ADVERTISING"
"7311","Office of Trade & Services","SERVICES-ADVERTISING AGENCIES"
"7320","Office of Trade & Services","SERVICES-CONSUMER CREDIT REPORTING, COLLECTION AGENCIES"
"7330","Office of Trade & Services","SERVICES-MAILING, REPRODUCTION, COMMERCIAL ART & PHOTOGRAPHY"
"7331","Office of Trade & Services","SERVICES-DIRECT MAIL ADVERTISING SERVICES"
"7340","Office of Trade & Services","SERVICES-TO DWELLINGS & OTHER BUILDINGS"
"7350","Office of Trade & Services","SERVICES-MISCELLANEOUS EQUIPMENT RENTAL & LEASING"
"7359","Office of Trade & Services","SERVICES-EQUIPMENT RENTAL & LEASING, NEC"
"7361","Office of Trade & Services","SERVICES-EMPLOYMENT AGENCIES"
"7363","Office of Trade & Services","SERVICES-HELP SUPPLY SERVICES"
"7370","Office of Technology","SERVICES-COMPUTER PROGRAMMING, DATA PROCESSING, ETC."
"7371","Office of Technology","SERVICES-COMPUTER PROGRAMMING SERVICES"
"7372","Office of Technology","SERVICES-PREPACKAGED SOFTWARE"
"7373","Office of Technology","SERVICES-COMPUTER INTEGRATED SYSTEMS DESIGN"
"7374","Office of Technology","SERVICES-COMPUTER PROCESSING & DATA PREPARATION"
"7377","Office of Trade & Services","SERVICES-COMPUTER RENTAL & LEASING"
"7380","Office of Trade & Services","SERVICES-MISCELLANEOUS BUSINESS SERVICES"
"7381","Office of Trade & Services","SERVICES-DETECTIVE, GUARD & ARMORED CAR SERVICES"
"7384","Office of Trade & Services","SERVICES-PHOTOFINISHING LABORATORIES"
"7385","Office of Trade & Services","SERVICES-TELEPHONE INTERCONNECT SYSTEMS"
"7389","Office of Trade & Services or Office of Energy & Transportation","SERVICES-BUSINESS SERVICES, NEC"
"7500","Office of Trade & Services","SERVICES-AUTOMOTIVE REPAIR, SERVICES & PARKING"
"7510","Office of Trade & Services","SERVICES-AUTO RENTAL & LEASING (NO DRIVERS)"
"7600","Office of Trade & Services","SERVICES-MISCELLANEOUS REPAIR SERVICES"
"7812","Office of Trade & Services","SERVICES-MOTION PICTURE & VIDEO TAPE PRODUCTION"
"7819","Office of Trade & Services","SERVICES-ALLIED TO MOTION PICTURE PRODUCTION"
"7822","Office of Trade & Services","SERVICES-MOTION PICTURE & VIDEO TAPE DISTRIBUTION"
"7829","Office of Trade & Services","SERVICES-ALLIED TO MOTION PICTURE DISTRIBUTION"
"7830","Office of Trade & Services","SERVICES-MOTION PICTURE THEATERS"
"7841","Office of Trade & Services","SERVICES-VIDEO TAPE RENTAL"
"7900","Office of Trade & Services","SERVICES-AMUSEMENT & RECREATION SERVICES"
"7948","Office of Trade & Services","SERVICES-RACING, INCLUDING TRACK OPERATION"
"7990","Office of Trade & Services","SERVICES-MISCELLANEOUS AMUSEMENT & RECREATION"
"7997","Office of Trade & Services","SERVICES-MEMBERSHIP SPORTS & RECREATION CLUBS"
"8000","Industrial Applications and Services","SERVICES-HEALTH SERVICES"
"8011","Industrial Applications and Services","SERVICES-OFFICES & CLINICS OF DOCTORS OF MEDICINE"
"8050","Industrial Applications and Services","SERVICES-NURSING & PERSONAL CARE FACILITIES"
"8051","Industrial Applications and Services","SERVICES-SKILLED NURSING CARE FACILITIES"
"8060","Industrial Applications and Services","SERVICES-HOSPITALS"
"8062","Industrial Applications and Services","SERVICES-GENERAL MEDICAL & SURGICAL HOSPITALS, NEC"
"8071","Industrial Applications and Services","SERVICES-MEDICAL LABORATORIES"
"8082","Industrial Applications and Services","SERVICES-HOME HEALTH CARE SERVICES"
"8090","Industrial Applications and Services","SERVICES-MISC HEALTH & ALLIED SERVICES, NEC"
"8093","Industrial Applications and Services","SERVICES-SPECIALTY OUTPATIENT FACILITIES, NEC"
"8111","Office of Trade & Services","SERVICES-LEGAL SERVICES"
"8200","Office of Trade & Services","SERVICES-EDUCATIONAL SERVICES"
"8300","Industrial Applications and Services","SERVICES-SOCIAL SERVICES"
"8351","Office of Trade & Services","SERVICES-CHILD DAY CARE SERVICES"
"8600","Office of Trade & Services","SERVICES-MEMBERSHIP ORGANIZATIONS"
"8700","Office of Trade & Services","SERVICES-ENGINEERING, ACCOUNTING, RESEARCH, MANAGEMENT"
"8711","Office of Trade & Services","SERVICES-ENGINEERING SERVICES"
"8731","Industrial Applications and Services","SERVICES-COMMERCIAL PHYSICAL & BIOLOGICAL RESEARCH"
"8734","Industrial Applications and Services","SERVICES-TESTING LABORATORIES"
"8741","Office of Trade & Services","SERVICES-MANAGEMENT SERVICES"
"8742","Office of Trade & Services","SERVICES-MANAGEMENT CONSULTING SERVICES"
"8744","Office of Trade & Services","SERVICES-FACILITIES SUPPORT MANAGEMENT SERVICES"
"8880","Office of International Corp Fin","AMERICAN DEPOSITARY RECEIPTS"
"8888","Office of International Corp Fin","FOREIGN GOVERNMENTS"
"8900","Office of Trade & Services","SERVICES-SERVICES, NEC"
"9721","Office of International Corp Fin","INTERNATIONAL AFFAIRS"
"9995","Office of Real Estate & Construction","NON-OPERATING ESTABLISHMENTS"
"""
    ),
    dtype="string",
)
"""
A table of Standard Industrial Classification codes and descriptions used by SEC.

These codes have mostly been supplanted by the NAICS codes, which provide more detail
and modernized industrial categories, but are still in use by SEC. Crosswalks exist that
can convert between SIC and NAICS codes, but the correspondence isn't perfect.

See also:

  * https://www.sec.gov/search-filings/standard-industrial-classification-sic-code-list
  * https://www.osha.gov/data/sic-manual
  * https://www.zacharyschaller.com/data-and-crosswalks
"""
