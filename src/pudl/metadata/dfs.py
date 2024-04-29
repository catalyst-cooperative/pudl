"""Static database tables."""

from io import StringIO

import pandas as pd

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
