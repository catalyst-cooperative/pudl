"""Routines for transforming FERC Form 1 data before loading into the PUDL DB.

This module provides a variety of functions that are used in cleaning up the FERC Form 1
data prior to loading into our database. This includes adopting standardized units and
column names, standardizing the formatting of some string values, and correcting data
entry errors which we can infer based on the existing data. It may also include removing
bad data, or replacing it with the appropriate NA values.

"""
import importlib.resources
import re
from re import Pattern
from typing import Literal

import numpy as np
import pandas as pd
from pydantic import BaseModel

import pudl
from pudl.analysis.classify_plants_ferc1 import (
    plants_steam_assign_plant_ids,
    plants_steam_validate_ids,
)
from pudl.helpers import convert_cols_dtypes, get_logger
from pudl.metadata.classes import DataSource
from pudl.metadata.dfs import FERC_DEPRECIATION_LINES
from pudl.settings import Ferc1Settings

logger = get_logger(__name__)


##############################################################################
# Unit converstion parameters
##############################################################################

PERKW_TO_PERMW = dict(
    multiplier=1e3,
    pattern=r"(.*)_per_kw$",
    repl=r"\1_per_mw",
)
"""Parameters for converting column units from per kW to per MW."""

PERKWH_TO_PERMWH = dict(
    multiplier=1e3,
    pattern=r"(.*)_per_kwh$",
    repl=r"\1_per_mwh",
)
"""Parameters for converting column units from per kWh to per MWh."""

KWH_TO_MWH = dict(
    multiplier=1e-3,
    pattern=r"(.*)_kwh$",
    repl=r"\1_mwh",
)
"""Parameters for converting column units from kWh to MWh."""

BTU_TO_MMBTU = dict(
    multiplier=1e-6,
    pattern=r"(.*)_btu(.*)$",
    repl=r"\1_mmbtu\2",
)
"""Parameters for converting column units from BTU to MMBTU."""


##############################################################################
# Valid ranges to impose on various columns
##############################################################################

VALID_PLANT_YEARS = {
    "lower_bound": 1850,
    "upper_bound": max(DataSource.from_id("ferc1").working_partitions["years"]) + 1,
}
"""Valid range of years for power plant construction."""

##############################################################################
# String categorizations
##############################################################################

FUEL_TYPES: dict[str, set[str]] = {
    "coal": {
        "coal",
        "coal-subbit",
        "lignite",
        "coal(sb)",
        "coal (sb)",
        "coal-lignite",
        "coke",
        "coa",
        "lignite/coal",
        "coal - subbit",
        "coal-subb",
        "coal-sub",
        "coal-lig",
        "coal-sub bit",
        "coals",
        "ciak",
        "petcoke",
        "coal.oil",
        "coal/gas",
        "coal. gas",
        "coal & oil",
        "coal bit",
        "bit coal",
        "coal-unit #3",
        "coal-subbitum",
        "coal tons",
        "coal mcf",
        "coal unit #3",
        "pet. coke",
        "coal-u3",
        "coal&coke",
        "tons",
    },
    "oil": {
        "oil",
        "#6 oil",
        "#2 oil",
        "fuel oil",
        "jet",
        "no. 2 oil",
        "no.2 oil",
        "no.6& used",
        "used oil",
        "oil-2",
        "oil (#2)",
        "diesel oil",
        "residual oil",
        "# 2 oil",
        "resid. oil",
        "tall oil",
        "oil/gas",
        "no.6 oil",
        "oil-fuel",
        "oil-diesel",
        "oil / gas",
        "oil bbls",
        "oil bls",
        "no. 6 oil",
        "#1 kerosene",
        "diesel",
        "no. 2 oils",
        "blend oil",
        "#2oil diesel",
        "#2 oil-diesel",
        "# 2  oil",
        "light oil",
        "heavy oil",
        "gas.oil",
        "#2",
        "2",
        "6",
        "bbl",
        "no 2 oil",
        "no 6 oil",
        "#1 oil",
        "#6",
        "oil-kero",
        "oil bbl",
        "biofuel",
        "no 2",
        "kero",
        "#1 fuel oil",
        "no. 2  oil",
        "blended oil",
        "no 2. oil",
        "# 6 oil",
        "nno. 2 oil",
        "#2 fuel",
        "oill",
        "oils",
        "gas/oil",
        "no.2 oil gas",
        "#2 fuel oil",
        "oli",
        "oil (#6)",
        "oil/diesel",
        "2 oil",
        "#6 hvy oil",
        "jet fuel",
        "diesel/compos",
        "oil-8",
        "oil {6}",  # noqa: FS003
        "oil-unit #1",
        "bbl.",
        "oil.",
        "oil #6",
        "oil (6)",
        "oil(#2)",
        "oil-unit1&2",
        "oil-6",
        "#2 fue oil",
        "dielel oil",
        "dielsel oil",
        "#6 & used",
        "barrels",
        "oil un 1 & 2",
        "jet oil",
        "oil-u1&2",
        "oiul",
        "pil",
        "oil - 2",
        "#6 & used",
        "oial",
        "diesel fuel",
        "diesel/compo",
        "oil (used)",
    },
    "gas": {
        "gas",
        "gass",
        "methane",
        "natural gas",
        "blast gas",
        "gas mcf",
        "propane",
        "prop",
        "natural  gas",
        "nat.gas",
        "nat gas",
        "nat. gas",
        "natl gas",
        "ga",
        "gas`",
        "syngas",
        "ng",
        "mcf",
        "blast gaa",
        "nat  gas",
        "gac",
        "syngass",
        "prop.",
        "natural",
        "coal.gas",
        "n. gas",
        "lp gas",
        "natuaral gas",
        "coke gas",
        "gas #2016",
        "propane**",
        "* propane",
        "propane **",
        "gas expander",
        "gas ct",
        "# 6 gas",
        "#6 gas",
        "coke oven gas",
        "gas & oil",
        "gas/fuel oil",
    },
    "solar": set(),
    "wind": set(),
    "hydro": set(),
    "nuclear": {
        "nuclear",
        "grams of uran",
        "grams of",
        "grams of  ura",
        "grams",
        "nucleur",
        "nulear",
        "nucl",
        "nucleart",
        "nucelar",
        "gr.uranium",
        "grams of urm",
        "nuclear (9)",
        "nulcear",
        "nuc",
        "gr. uranium",
        "uranium",
        "nuclear mw da",
        "grams of ura",
        "nucvlear",
        "nuclear (1)",
    },
    "waste": {
        "tires",
        "tire",
        "refuse",
        "switchgrass",
        "wood waste",
        "woodchips",
        "biomass",
        "wood",
        "wood chips",
        "rdf",
        "tires/refuse",
        "tire refuse",
        "waste oil",
        "waste",
        "woodships",
        "tire chips",
        "tdf",
    },
    "other": {
        "steam",
        "purch steam",
        "all",
        "n/a",
        "purch. steam",
        "other",
        "composite",
        "composit",
        "mbtus",
        "total",
        "avg",
        "avg.",
        "blo",
        "all fuel",
        "comb.",
        "alt. fuels",
        "na",
        "comb",
        "/#=2\x80â\x91?",
        "kã\xadgv¸\x9d?",
        "mbtu's",
        "gas, oil",
        "rrm",
        "3\x9c",
        "average",
        "furfural",
        "0",
        "watson bng",
        "toal",
        "bng",
        "# 6 & used",
        "combined",
        "blo bls",
        "compsite",
        "*",
        "compos.",
        "gas / oil",
        "mw days",
        "g",
        "c",
        "lime",
        "all fuels",
        "at right",
        "20",
        "1",
        "comp oil/gas",
        "all fuels to",
        "the right are",
        "c omposite",
        "all fuels are",
        "total pr crk",
        "all fuels =",
        "total pc",
        "comp",
        "alternative",
        "alt. fuel",
        "bio fuel",
        "total prairie",
        "",
        "kã\xadgv¸?",
        "m",
        "waste heat",
        "/#=2â?",
        "3",
        "—",
    },
}
"""
A mapping a canonical fuel name to a set of strings which are used to represent that
fuel in the FERC Form 1 Reporting. Case is ignored, as all fuel strings are converted to
lower case in the data set.
"""

FUEL_UNITS: dict[str, set[str]] = {
    "ton": {
        "toms",
        "taons",
        "tones",
        "col-tons",
        "toncoaleq",
        "coal",
        "tons coal eq",
        "coal-tons",
        "ton",
        "tons",
        "tons coal",
        "coal-ton",
        "tires-tons",
        "coal tons -2 ",
        "oil-tons",
        "coal tons 200",
        "ton-2000",
        "coal tons",
        "coal tons -2",
        "coal-tone",
        "tire-ton",
        "tire-tons",
        "ton coal eqv",
        "tos",
        "coal tons - 2",
        "c. t.",
        "c.t.",
        "t",
        "toncoalequiv",
    },
    "mcf": {
        "mcf",
        "mcf's",
        "mcfs",
        "mcf.",
        "mcfe",
        "gas mcf",
        '"gas" mcf',
        "gas-mcf",
        "mfc",
        "mct",
        " mcf",
        "msfs",
        "mlf",
        "mscf",
        "mci",
        "mcl",
        "mcg",
        "m.cu.ft.",
        "kcf",
        "(mcf)",
        "mcf *(4)",
        "mcf00",
        "m.cu.ft..",
        "1000 c.f",
    },
    "bbl": {
        "barrel",
        "bbls",
        "bbl",
        "barrels",
        "bbrl",
        "bbl.",
        "bbls.",
        "oil 42 gal",
        "oil-barrels",
        "barrrels",
        "bbl-42 gal",
        "oil-barrel",
        "bb.",
        "barrells",
        "bar",
        "bbld",
        "oil- barrel",
        "barrels    .",
        "bbl .",
        "barels",
        "barrell",
        "berrels",
        "bb",
        "bbl.s",
        "oil-bbl",
        "bls",
        "bbl:",
        "barrles",
        "blb",
        "propane-bbl",
        "barriel",
        "berriel",
        "barrile",
        "(bbl.)",
        "barrel *(4)",
        "(4) barrel",
        "bbf",
        "blb.",
        "(bbl)",
        "bb1",
        "bbsl",
        "barrrel",
        "barrels 100%",
        "bsrrels",
        "bbl's",
        "*barrels",
        "oil - barrels",
        "oil 42 gal ba",
        "bll",
        "boiler barrel",
        "gas barrel",
        '"boiler" barr',
        '"gas" barrel',
        '"boiler"barre',
        '"boiler barre',
        "barrels .",
        "bariel",
        "brrels",
        "oil barrel",
        "barreks",
        "oil-bbls",
        "oil-bbs",
        "boe",
    },
    "mmbbl": {"mmbbls"},
    "gal": {"gallons", "gal.", "gals", "gals.", "gallon", "gal", "galllons"},
    "kgal": {
        "oil(1000 gal)",
        "oil(1000)",
        "oil (1000)",
        "oil(1000",
        "oil(1000ga)",
        "1000 gals",
        "1000 gal",
    },
    "gramsU": {
        "gram",
        "grams",
        "gm u",
        "grams u235",
        "grams u-235",
        "grams of uran",
        "grams: u-235",
        "grams:u-235",
        "grams:u235",
        "grams u308",
        "grams: u235",
        "grams of",
        "grams - n/a",
        "gms uran",
        "s e uo2 grams",
        "gms uranium",
        "grams of urm",
        "gms. of uran",
        "grams (100%)",
        "grams v-235",
        "se uo2 grams",
        "grams u",
        "g",
        "grams of uranium",
    },
    "kgU": {
        "kg of uranium",
        "kg uranium",
        "kilg. u-235",
        "kg u-235",
        "kilograms-u23",
        "kg",
        "kilograms u-2",
        "kilograms",
        "kg of",
        "kg-u-235",
        "kilgrams",
        "kilogr. u235",
        "uranium kg",
        "kg uranium25",
        "kilogr. u-235",
        "kg uranium 25",
        "kilgr. u-235",
        "kguranium 25",
        "kg-u235",
        "kgm",
    },
    "klbs": {
        "k lbs.",
        "k lbs",
        "1000 / lbs",
        "1000 lbs",
    },
    "mmbtu": {
        "mmbtu",
        "mmbtus",
        "mbtus",
        "(mmbtu)",
        "mmbtu's",
        "nuclear-mmbtu",
        "nuclear-mmbt",
        "mmbtul",
    },
    "btu": {
        "btus",
        "btu",
    },
    "mwdth": {
        "mwd therman",
        "mw days-therm",
        "mwd thrml",
        "mwd thermal",
        "mwd/mtu",
        "mw days",
        "mwdth",
        "mwd",
        "mw day",
        "dth",
        "mwdaysthermal",
        "mw day therml",
        "mw days thrml",
        "nuclear mwd",
        "mmwd",
        "mw day/therml" "mw days/therm",
        "mw days (th",
        "ermal)",
    },
    "mwhth": {
        "mwh them",
        "mwh threm",
        "nwh therm",
        "mwhth",
        "mwh therm",
        "mwh",
        "mwh therms.",
        "mwh term.uts",
        "mwh thermal",
        "mwh thermals",
        "mw hr therm",
        "mwh therma",
        "mwh therm.uts",
    },
    "unknown": {
        "",
        "1265",
        "mwh units",
        "composite",
        "therms",
        "n/a",
        "mbtu/kg",
        "uranium 235",
        "oil",
        "ccf",
        "2261",
        "uo2",
        "(7)",
        "oil #2",
        "oil #6",
        '\x99å\x83\x90?"',
        "dekatherm",
        "0",
        "mw day/therml",
        "nuclear",
        "gas",
        "62,679",
        "mw days/therm",
        "na",
        "uranium",
        "oil/gas",
        "thermal",
        "(thermal)",
        "se uo2",
        "181679",
        "83",
        "3070",
        "248",
        "273976",
        "747",
        "-",
        "are total",
        "pr. creek",
        "decatherms",
        "uramium",
        ".",
        "total pr crk",
        ">>>>>>>>",
        "all",
        "total",
        "alternative-t",
        "oil-mcf",
        "3303671",
        "929",
        "7182175",
        "319",
        "1490442",
        "10881",
        "1363663",
        "7171",
        "1726497",
        "4783",
        "7800",
        "12559",
        "2398",
        "creek fuels",
        "propane-barre",
        "509",
        "barrels/mcf",
        "propane-bar",
        "4853325",
        "4069628",
        "1431536",
        "708903",
        "mcf/oil (1000",
        "344",
        'å?"',
        "mcf / gallen",
        "none",
        "—",
    },
}
"""
A mapping of canonical fuel units (keys) to sets of strings representing those
fuel units (values)
"""

PLANT_TYPES: dict[str, set[str]] = {
    "steam": {
        "coal",
        "steam",
        "steam units 1 2 3",
        "steam units 4 5",
        "steam fossil",
        "steam turbine",
        "steam a",
        "steam 100",
        "steam units 1 2 3",
        "steams",
        "steam 1",
        "steam retired 2013",
        "stream",
        "steam units 1,2,3",
        "steam units 4&5",
        "steam units 4&6",
        "steam conventional",
        "unit total-steam",
        "unit total steam",
        "*resp. share steam",
        "resp. share steam",
        "steam (see note 1,",
        "steam (see note 3)",
        "mpc 50%share steam",
        "40% share steam" "steam (2)",
        "steam (3)",
        "steam (4)",
        "steam (5)",
        "steam (6)",
        "steam (7)",
        "steam (8)",
        "steam units 1 and 2",
        "steam units 3 and 4",
        "steam (note 1)",
        "steam (retired)",
        "steam (leased)",
        "coal-fired steam",
        "oil-fired steam",
        "steam/fossil",
        "steam (a,b)",
        "steam (a)",
        "stean",
        "steam-internal comb",
        "steam (see notes)",
        "steam units 4 & 6",
        "resp share stm note3",
        "mpc50% share steam",
        "mpc40%share steam",
        "steam - 64%",
        "steam - 100%",
        "steam (1) & (2)",
        "resp share st note3",
        "mpc 50% shares steam",
        "steam-64%",
        "steam-100%",
        "steam (see note 1)",
        "mpc 50% share steam",
        "steam units 1, 2, 3",
        "steam units 4, 5",
        "steam (2)",
        "steam (1)",
        "steam 4, 5",
        "steam - 72%",
        "steam (incl i.c.)",
        "steam- 72%",
        "steam;retired - 2013",
        "respondent's sh.-st.",
        "respondent's sh-st",
        "40% share steam",
        "resp share stm note3",
        "mpc50% share steam",
        "resp share st note 3",
        "\x02steam (1)",
        "coal fired steam tur",
        "coal fired steam turbine",
        "steam- 64%",
    },
    "combustion_turbine": {
        "combustion turbine",
        "gt",
        "gas turbine",
        "gas turbine # 1",
        "gas turbine",
        "gas turbine (note 1)",
        "gas turbines",
        "simple cycle",
        "combustion turbine",
        "comb.turb.peak.units",
        "gas turbine",
        "combustion turbine",
        "com turbine peaking",
        "gas turbine peaking",
        "comb turb peaking",
        "combustine turbine",
        "comb. turine",
        "conbustion turbine",
        "combustine turbine",
        "gas turbine (leased)",
        "combustion tubine",
        "gas turb",
        "gas turbine peaker",
        "gtg/gas",
        "simple cycle turbine",
        "gas-turbine",
        "gas turbine-simple",
        "gas turbine - note 1",
        "gas turbine #1",
        "simple cycle",
        "gasturbine",
        "combustionturbine",
        "gas turbine (2)",
        "comb turb peak units",
        "jet engine",
        "jet powered turbine",
        "*gas turbine",
        "gas turb.(see note5)",
        "gas turb. (see note",
        "combutsion turbine",
        "combustion turbin",
        "gas turbine-unit 2",
        "gas - turbine",
        "comb turbine peaking",
        "gas expander turbine",
        "jet turbine",
        "gas turbin (lease",
        "gas turbine (leased",
        "gas turbine/int. cm",
        "comb.turb-gas oper.",
        "comb.turb.gas/oil op",
        "comb.turb.oil oper.",
        "jet",
        "comb. turbine (a)",
        "gas turb.(see notes)",
        "gas turb(see notes)",
        "comb. turb-gas oper",
        "comb.turb.oil oper",
        "gas turbin (leasd)",
        "gas turbne/int comb",
        "gas turbine (note1)",
        "combution turbin",
        "* gas turbine",
        "add to gas turbine",
        "gas turbine (a)",
        "gas turbinint comb",
        "gas turbine (note 3)",
        "resp share gas note3",
        "gas trubine",
        "*gas turbine(note3)",
        "gas turbine note 3,6",
        "gas turbine note 4,6",
        "gas turbine peakload",
        "combusition turbine",
        "gas turbine (lease)",
        "comb. turb-gas oper.",
        "combution turbine",
        "combusion turbine",
        "comb. turb. oil oper",
        "combustion burbine",
        "combustion and gas",
        "comb. turb.",
        "gas turbine (lease",
        "gas turbine (leasd)",
        "gas turbine/int comb",
        "*gas turbine(note 3)",
        "gas turbine (see nos",
        "i.c.e./gas turbine",
        "gas turbine/intcomb",
        "cumbustion turbine",
        "gas turb, int. comb.",
        "gas turb, diesel",
        "gas turb, int. comb",
        "i.c.e/gas turbine",
        "diesel turbine",
        "comubstion turbine",
        "i.c.e. /gas turbine",
        "i.c.e/ gas turbine",
        "i.c.e./gas tubine",
        "gas turbine; retired",
    },
    "combined_cycle": {
        "Combined cycle",
        "combined cycle",
        "combined",
        "gas & steam turbine",
        "gas turb. & heat rec",
        "combined cycle",
        "com. cyc",
        "com. cycle",
        "gas turb-combined cy",
        "combined cycle ctg",
        "combined cycle - 40%",
        "com cycle gas turb",
        "combined cycle oper",
        "gas turb/comb. cyc",
        "combine cycle",
        "cc",
        "comb. cycle",
        "gas turb-combined cy",
        "steam and cc",
        "steam cc",
        "gas steam",
        "ctg steam gas",
        "steam comb cycle",
        "gas/steam comb. cycl",
        "gas/steam",
        "gas turbine-combined cycle",
        "steam (comb. cycle)" "gas turbine/steam",
        "steam & gas turbine",
        "gas trb & heat rec",
        "steam & combined ce",
        "st/gas turb comb cyc",
        "gas tur & comb cycl",
        "combined cycle (a,b)",
        "gas turbine/ steam",
        "steam/gas turb.",
        "steam & comb cycle",
        "gas/steam comb cycle",
        "comb cycle (a,b)",
        "igcc",
        "steam/gas turbine",
        "gas turbine / steam",
        "gas tur & comb cyc",
        "comb cyc (a) (b)",
        "comb cycle",
        "comb cyc",
        "combined turbine",
        "combine cycle oper",
        "comb cycle/steam tur",
        "cc / gas turb",
        "steam (comb. cycle)",
        "steam & cc",
        "gas turbine/steam",
        "gas turb/cumbus cycl",
        "gas turb/comb cycle",
        "gasturb/comb cycle",
        "gas turb/cumb. cyc",
        "igcc/gas turbine",
        "gas / steam",
        "ctg/steam-gas",
        "ctg/steam -gas",
        "gas fired cc turbine",
        "combinedcycle",
        "comb cycle gas turb",
        "combined cycle opern",
        "comb. cycle gas turb",
        "ngcc",
    },
    "nuclear": {
        "nuclear",
        "nuclear (3)",
        "steam(nuclear)",
        "nuclear(see note4)" "nuclear steam",
        "nuclear turbine",
        "nuclear - steam",
        "nuclear (a)(b)(c)",
        "nuclear (b)(c)",
        "* nuclear",
        "nuclear (b) (c)",
        "nuclear (see notes)",
        "steam (nuclear)",
        "* nuclear (note 2)",
        "nuclear (note 2)",
        "nuclear (see note 2)",
        "nuclear(see note4)",
        "nuclear steam",
        "nuclear(see notes)",
        "nuclear-steam",
        "nuclear (see note 3)",
    },
    "geothermal": {"steam - geothermal", "steam_geothermal", "geothermal"},
    "internal_combustion": {
        "ic",
        "internal combustion",
        "internal comb.",
        "internl combustion",
        "diesel turbine",
        "int combust (note 1)",
        "int. combust (note1)",
        "int.combustine",
        "comb. cyc",
        "internal comb",
        "diesel",
        "diesel engine",
        "internal combustion",
        "int combust - note 1",
        "int. combust - note1",
        "internal comb recip",
        "internal combustion reciprocating",
        "reciprocating engine",
        "comb. turbine",
        "internal combust.",
        "int. combustion (1)",
        "*int combustion (1)",
        "*internal combust'n",
        "internal",
        "internal comb.",
        "steam internal comb",
        "combustion",
        "int. combustion",
        "int combust (note1)",
        "int. combustine",
        "internl combustion",
        "*int. combustion (1)",
        "internal conbustion",
    },
    "wind": {
        "wind",
        "wind energy",
        "wind turbine",
        "wind - turbine",
        "wind generation",
        "wind turbin",
    },
    "photovoltaic": {"solar photovoltaic", "photovoltaic", "solar", "solar project"},
    "solar_thermal": {"solar thermal"},
    "unknown": {
        "",
        "n/a",
        "see pgs 402.1-402.3",
        "see pgs 403.1-403.9",
        "respondent's share",
        "--",
        "—",
        "footnote",
        "(see note 7)",
        "other",
        "not applicable",
        "peach bottom",
        "none.",
        "fuel facilities",
        "0",
        "not in service",
        "none",
        "common expenses",
        "expenses common to",
        "retired in 1981",
        "retired in 1978",
        "na",
        "unit total (note3)",
        "unit total (note2)",
        "resp. share (note2)",
        "resp. share (note8)",
        "resp. share (note 9)",
        "resp. share (note11)",
        "resp. share (note4)",
        "resp. share (note6)",
        "conventional",
        "expenses commom to",
        "not in service in",
        "unit total (note 3)",
        "unit total (note 2)",
        "resp. share (note 8)",
        "resp. share (note 3)",
        "resp. share note 11",
        "resp. share (note 4)",
        "resp. share (note 6)",
        "(see note 5)",
        "resp. share (note 2)",
        "package",
        "(left blank)",
        "common",
        "0.0000",
        "other generation",
        "resp share (note 11)",
        "retired",
        "storage/pipelines",
        "sold april 16, 1999",
        "sold may 07, 1999",
        "plants sold in 1999",
        "gas",
        "not applicable.",
        "resp. share - note 2",
        "resp. share - note 8",
        "resp. share - note 9",
        "resp share - note 11",
        "resp. share - note 4",
        "resp. share - note 6",
        "plant retired- 2013",
        "retired - 2013",
        "resp share - note 5",
        "resp. share - note 7",
        "non-applicable",
        "other generation plt",
        "combined heat/power",
        "oil",
        "fuel oil",
    },
}
"""
A mapping from canonical plant kinds (keys) to the associated freeform strings (values)
identified as being associated with that kind of plant in the FERC Form 1 raw data.
There are many strings that weren't categorized, Solar and Solar Project were not
classified as these do not indicate if they are solar thermal or photovoltaic. Variants
on Steam (e.g. "steam 72" and "steam and gas") were classified based on additional
research of the plants on the Internet.
"""

CONSTRUCTION_TYPES: dict[str, set[str]] = {
    "outdoor": {
        "outdoor",
        "outdoor boiler",
        "full outdoor",
        "outdoor boiler",
        "outdoor boilers",
        "outboilers",
        "fuel outdoor",
        "full outdoor",
        "outdoors",
        "outdoor",
        "boiler outdoor& full",
        "boiler outdoor&full",
        "outdoor boiler& full",
        "full -outdoor",
        "outdoor steam",
        "outdoor boiler",
        "ob",
        "outdoor automatic",
        "outdoor repower",
        "full outdoor boiler",
        "fo",
        "outdoor boiler & ful",
        "full-outdoor",
        "fuel outdoor",
        "outoor",
        "outdoor",
        "outdoor  boiler&full",
        "boiler outdoor &full",
        "outdoor boiler &full",
        "boiler outdoor & ful",
        "outdoor-boiler",
        "outdoor - boiler",
        "outdoor const.",
        "4 outdoor boilers",
        "3 outdoor boilers",
        "full outdoor",
        "full outdoors",
        "full oudoors",
        "outdoor (auto oper)",
        "outside boiler",
        "outdoor boiler&full",
        "outdoor hrsg",
        "outdoor hrsg",
        "outdoor-steel encl.",
        "boiler-outdr & full",
        "con.& full outdoor",
        "partial outdoor",
        "outdoor (auto. oper)",
        "outdoor (auto.oper)",
        "outdoor construction",
        "1 outdoor boiler",
        "2 outdoor boilers",
        "outdoor enclosure",
        "2 outoor boilers",
        "boiler outdr.& full",
        "boiler outdr. & full",
        "ful outdoor",
        "outdoor-steel enclos",
        "outdoor (auto oper.)",
        "con. & full outdoor",
        "outdore",
        "boiler & full outdor",
        "full & outdr boilers",
        "outodoor (auto oper)",
        "outdoor steel encl.",
        "full outoor",
        "boiler & outdoor ful",
        "otdr. blr. & f. otdr",
        "f.otdr & otdr.blr.",
        "oudoor (auto oper)",
        "outdoor constructin",
        "f. otdr. & otdr. blr",
        "outdoor boiler & fue",
        "outdoor boiler &fuel",
    },
    "semioutdoor": {
        "more than 50% outdoors",
        "more than 50% outdoo",
        "more than 50% outdos",
        "over 50% outdoor",
        "over 50% outdoors",
        "semi-outdoor",
        "semi - outdoor",
        "semi outdoor",
        "semi-enclosed",
        "semi-outdoor boiler",
        "semi outdoor boiler",
        "semi- outdoor",
        "semi - outdoors",
        "semi -outdoor" "conven & semi-outdr",
        "conv & semi-outdoor",
        "conv & semi- outdoor",
        "convent. semi-outdr",
        "conv. semi outdoor",
        "conv(u1)/semiod(u2)",
        "conv u1/semi-od u2",
        "conv-one blr-semi-od",
        "convent semioutdoor",
        "conv. u1/semi-od u2",
        "conv - 1 blr semi od",
        "conv. ui/semi-od u2",
        "conv-1 blr semi-od",
        "conven. semi-outdoor",
        "conv semi-outdoor",
        "u1-conv./u2-semi-od",
        "u1-conv./u2-semi -od",
        "convent. semi-outdoo",
        "u1-conv. / u2-semi",
        "conven & semi-outdr",
        "semi -outdoor",
        "outdr & conventnl",
        "conven. full outdoor",
        "conv. & outdoor blr",
        "conv. & outdoor blr.",
        "conv. & outdoor boil",
        "conv. & outdr boiler",
        "conv. & out. boiler",
        "convntl,outdoor blr",
        "outdoor & conv.",
        "2 conv., 1 out. boil",
        "outdoor/conventional",
        "conv. boiler outdoor",
        "conv-one boiler-outd",
        "conventional outdoor",
        "conventional outdor",
        "conv. outdoor boiler",
        "conv.outdoor boiler",
        "conventional outdr.",
        "conven,outdoorboiler",
        "conven full outdoor",
        "conven,full outdoor",
        "1 out boil, 2 conv",
        "conv. & full outdoor",
        "conv. & outdr. boilr",
        "conv outdoor boiler",
        "convention. outdoor",
        "conv. sem. outdoor",
        "convntl, outdoor blr",
        "conv & outdoor boil",
        "conv & outdoor boil.",
        "outdoor & conv",
        "conv. broiler outdor",
        "1 out boilr, 2 conv",
        "conv.& outdoor boil.",
        "conven,outdr.boiler",
        "conven,outdr boiler",
        "outdoor & conventil",
        "1 out boilr 2 conv",
        "conv & outdr. boilr",
        "conven, full outdoor",
        "conven full outdr.",
        "conven, full outdr.",
        "conv/outdoor boiler",
        "convnt'l outdr boilr",
        "1 out boil 2 conv",
        "conv full outdoor",
        "conven, outdr boiler",
        "conventional/outdoor",
        "conv&outdoor boiler",
        "outdoor & convention",
        "conv & outdoor boilr",
        "conv & full outdoor",
        "convntl. outdoor blr",
        "conv - ob",
        "1conv'l/2odboilers",
        "2conv'l/1odboiler",
        "conv-ob",
        "conv.-ob",
        "1 conv/ 2odboilers",
        "2 conv /1 odboilers",
        "conv- ob",
        "conv -ob",
        "con sem outdoor",
        "cnvntl, outdr, boilr",
        "less than 50% outdoo",
        "less than 50% outdoors",
        "under 50% outdoor",
        "under 50% outdoors",
        "1cnvntnl/2odboilers",
        "2cnvntnl1/1odboiler",
        "con & ob",
        "combination (b)",
        "indoor & outdoor",
        "conven. blr. & full",
        "conv. & otdr. blr.",
        "combination",
        "indoor and outdoor",
        "conven boiler & full",
        "2conv'l/10dboiler",
        "4 indor/outdr boiler",
        "4 indr/outdr boilerr",
        "4 indr/outdr boiler",
        "indoor & outdoof",
    },
    "conventional": {
        "conventional",
        "conventional",
        "conventional boiler",
        "conventional - boiler",
        "conv-b",
        "conventionall",
        "convention",
        "conventional",
        "coventional",
        "conven full boiler",
        "c0nventional",
        "conventtional",
        "convential" "underground",
        "conventional bulb",
        "conventrional",
        "*conventional",
        "convential",
        "convetional",
        "conventioanl",
        "conventioinal",
        "conventaional",
        "indoor construction",
        "convenional",
        "conventional steam",
        "conventinal",
        "convntional",
        "conventionl",
        "conventionsl",
        "conventiional",
        "convntl steam plants",
        "indoor const.",
        "full indoor",
        "indoor",
        "indoor automatic",
        "indoor boiler",
        "indoor boiler and steam turbine",
        "(peak load) indoor",
        "conventionl,indoor",
        "conventionl, indoor",
        "conventional, indoor",
        "conventional;outdoor",
        "conven./outdoor",
        "conventional;semi-ou",
        "comb. cycle indoor",
        "comb cycle indoor",
        "3 indoor boiler",
        "2 indoor boilers",
        "1 indoor boiler",
        "2 indoor boiler",
        "3 indoor boilers",
        "fully contained",
        "conv - b",
        "conventional/boiler",
        "cnventional",
        "comb. cycle indooor",
        "sonventional",
        "ind enclosures",
        "conentional",
        "conventional - boilr",
        "indoor boiler and st",
    },
    "unknown": {
        "",
        "automatic operation",
        "comb. turb. installn",
        "comb. turb. instaln",
        "com. turb. installn",
        "n/a",
        "for detailed info.",
        "for detailed info",
        "combined cycle",
        "na",
        "not applicable",
        "gas",
        "heated individually",
        "metal enclosure",
        "pressurized water",
        "nuclear",
        "jet engine",
        "gas turbine",
        "storage/pipelines",
        "0",
        "during 1994",
        "peaking - automatic",
        "gas turbine/int. cm",
        "2 oil/gas turbines",
        "wind",
        "package",
        "mobile",
        "auto-operated",
        "steam plants",
        "other production",
        "all nuclear plants",
        "other power gen.",
        "automatically operad",
        "automatically operd",
        "circ fluidized bed",
        "jet turbine",
        "gas turbne/int comb",
        "automatically oper.",
        "retired 1/1/95",
        "during 1995",
        "1996. plant sold",
        "reactivated 7/1/96",
        "gas turbine/int comb",
        "portable",
        "head individually",
        "automatic opertion",
        "peaking-automatic",
        "cycle",
        "full order",
        "circ. fluidized bed",
        "gas turbine/intcomb",
        "0.0000",
        "none",
        "2 oil / gas",
        "block & steel",
        "and 2000",
        "comb.turb. instaln",
        "automatic oper.",
        "pakage",
        "---",
        "—",
        "n/a (ct)",
        "comb turb instain",
        "ind encloures",
        "2 oil /gas turbines",
        "combustion turbine",
        "1970",
        "gas/oil turbines",
        "combined cycle steam",
        "pwr",
        "2 oil/ gas",
        "2 oil / gas turbines",
        "gas / oil turbines",
        "no boiler",
        "internal combustion",
        "gasturbine no boiler",
        "boiler",
        "tower -10 unit facy",
        "gas trubine",
        "4 gas/oil trubines",
        "2 oil/ 4 gas/oil tur",
        "5 gas/oil turbines",
        "tower 16",
        "2 on 1 gas turbine",
        "tower 23",
        "tower -10 unit",
        "tower - 101 unit",
        "3 on 1 gas turbine",
        "tower - 10 units",
        "tower - 165 units",
        "wind turbine",
        "fixed tilt pv",
        "tracking pv",
        "o",
        "wind trubine",
        "wind generator",
        "subcritical",
        "sucritical",
        "simple cycle",
        "simple & reciprocat",
        "solar",
        "pre-fab power plant",
        "prefab power plant",
        "prefab. power plant",
        "pump storage",
        "underground",
        "see page 402",
        "conv. underground",
        "conven. underground",
        "conventional (a)",
        "non-applicable",
        "duct burner",
        "see footnote",
        "simple and reciprocat",
    },
}
"""
A dictionary of construction types (keys) and lists of construction type strings
associated with each type (values) from FERC Form 1.

There are many strings that weren't categorized, including crosses between conventional
and outdoor, PV, wind, combined cycle, and internal combustion. The lists are broken out
into the two types specified in Form 1: conventional and outdoor. These lists are
inclusive so that variants of conventional (e.g. "conventional full") and outdoor (e.g.
"outdoor full" and "outdoor hrsg") are included.
"""

##############################################################################
# Fully assembled set of FERC 1 transformation parameters
##############################################################################

TABLE_TRANSFORM_PARAMS = {
    "plants_steam_ferc1": {
        "column_transform_params": {
            "plant_name_ferc1": {
                "simplify_strings": True,
            },
            "construction_year": {
                "valid_range": VALID_PLANT_YEARS,
            },
            "construction_type": {
                "string_categories": CONSTRUCTION_TYPES,
            },
            "plant_type": {
                "string_categories": PLANT_TYPES,
            },
            "installation_year": {
                "valid_range": VALID_PLANT_YEARS,
            },
            "capex_per_kw": {
                "unit_conversion": PERKW_TO_PERMW,
            },
            "opex_per_kwh": {
                "unit_conversion": PERKWH_TO_PERMWH,
            },
            "net_generation_kwh": {
                "unit_converstion": KWH_TO_MWH,
            },
        },
        "rename_cols": {
            "dbf": {
                "cost_structure": "capex_structures",
                "expns_misc_power": "opex_misc_power",
                "plant_name": "plant_name_ferc1",
                "plnt_capability": "plant_capability_mw",
                "expns_plants": "opex_plants",
                "expns_misc_steam": "opex_misc_steam",
                "cost_per_kw": "capex_per_kw",
                "when_not_limited": "not_water_limited_capacity_mw",
                "asset_retire_cost": "asset_retirement_cost",
                "expns_steam_othr": "opex_steam_other",
                "expns_transfer": "opex_transfer",
                "expns_engnr": "opex_engineering",
                "avg_num_of_emp": "avg_num_employees",
                "cost_of_plant_to": "capex_total",
                "expns_rents": "opex_rents",
                "tot_prdctn_expns": "opex_production_total",
                "plant_kind": "plant_type",
                "respondent_id": "utility_id_ferc1",
                "expns_operations": "opex_operations",
                "cost_equipment": "capex_equipment",
                "type_const": "construction_type",
                "plant_hours": "plant_hours_connected_while_generating",
                "expns_coolants": "opex_coolants",
                "expns_fuel": "opex_fuel",
                "when_limited": "water_limited_capacity_mw",
                "expns_kwh": "opex_per_kwh",
                "expns_allowances": "opex_allowances",
                "expns_steam": "opex_steam",
                "yr_const": "construction_year",
                "yr_installed": "installation_year",
                "expns_boiler": "opex_boiler",
                "peak_demand": "peak_demand_mw",
                "cost_land": "capex_land",
                "tot_capacity": "capacity_mw",
                "net_generation": "net_generation_kwh",
                "expns_electric": "opex_electric",
                "expns_structures": "opex_structures",
            },
            "xbrl": {
                "CostOfStructuresAndImprovementsSteamProduction": "capex_structures",
                "MiscellaneousSteamPowerExpenses": "opex_misc_power",
                "PlantNameAxis": "plant_name_ferc1",
                "NetContinuousPlantCapability": "plant_capability_mw",
                "MaintenanceOfElectricPlantSteamPowerGeneration": "opex_plants",
                "MaintenanceOfMiscellaneousSteamPlant": "opex_misc_steam",
                "CostPerKilowattOfInstalledCapacity": "capex_per_kw",
                "NetContinuousPlantCapabilityNotLimitedByCondenserWater": "not_water_limited_capacity_mw",
                "AssetRetirementCostsSteamProduction": "asset_retirement_cost",
                "SteamFromOtherSources": "opex_steam_other",
                "SteamTransferredCredit": "opex_transfer",
                "MaintenanceSupervisionAndEngineeringSteamPowerGeneration": "opex_engineering",
                "PlantAverageNumberOfEmployees": "avg_num_employees",
                "CostOfPlant": "capex_total",
                "RentsSteamPowerGeneration": "opex_rents",
                "PowerProductionExpensesSteamPower": "opex_production_total",
                "PlantKind": "plant_type",
                "OperationSupervisionAndEngineeringExpense": "opex_operations",
                "CostOfEquipmentSteamProduction": "capex_equipment",
                "PlantConstructionType": "construction_type",
                "PlantHoursConnectedToLoad": "plant_hours_connected_while_generating",
                "CoolantsAndWater": "opex_coolants",
                "FuelSteamPowerGeneration": "opex_fuel",
                "NetContinuousPlantCapabilityLimitedByCondenserWater": "water_limited_capacity_mw",
                "ExpensesPerNetKilowattHour": "opex_per_kwh",
                "Allowances": "opex_allowances",
                "SteamExpensesSteamPowerGeneration": "opex_steam",
                "YearPlantOriginallyConstructed": "construction_year",
                "YearLastUnitOfPlantInstalled": "installation_year",
                "MaintenanceOfBoilerPlantSteamPowerGeneration": "opex_boiler",
                "NetPeakDemandOnPlant": "peak_demand_mw",
                "CostOfLandAndLandRightsSteamProduction": "capex_land",
                "InstalledCapacityOfPlant": "capacity_mw",
                "NetGenerationExcludingPlantUse": "net_generation_kwh",
                "ElectricExpensesSteamPowerGeneration": "opex_electric",
                "MaintenanceOfStructuresSteamPowerGeneration": "opex_structures",
            },
        },
    },
    "fuel_ferc1": {
        "column_transforms": {
            "plant_name_ferc1": {
                "simplify_strings": True,
            },
            "fuel_btu_per_unit": {
                "unit_conversion": BTU_TO_MMBTU,
            },
            "fuel_type_code_pudl": {
                "string_categories": FUEL_TYPES,
            },
            "fuel_units": {
                "string_categories": FUEL_UNITS,
            },
        },
        "rename_cols": {
            "dbf": {
                "respondent_id": "utility_id_ferc1",
                "plant_name": "plant_name_ferc1",
                "fuel": "fuel_type_code_pudl",
                "fuel_unit": "fuel_units",
                "fuel_avg_heat": "fuel_btu_per_unit",
                "fuel_quantity": "fuel_consumed_units",
                "fuel_cost_burned": "fuel_cost_per_unit_burned",
                "fuel_cost_delvd": "fuel_cost_per_unit_delivered",
                "fuel_cost_btu": "fuel_cost_per_mmbtu",
                "fuel_generaton": "fuel_mmbtu_per_kwh",
            },
            "xbrl": {
                "PlantNameAxis": "plant_name_ferc1",
                "FuelKindAxis": "fuel_type_code_pudl",
                "FuelUnit": "fuel_units",
                "FuelBurnedAverageHeatContent": "fuel_mmbtu_per_unit",
                "QuantityOfFuelBurned": "fuel_consumed_units",
                "AverageCostOfFuelPerUnitBurned": "fuel_cost_per_unit_burned",
                "AverageCostOfFuelPerUnitAsDelivered": "fuel_cost_per_unit_delivered",
                "AverageCostOfFuelBurnedPerMillionBritishThermalUnit": "fuel_cost_per_mmbtu",
                "AverageBritishThermalUnitPerKilowattHourNetGeneration": "fuel_mmbtu_per_kwh",
                "AverageCostOfFuelBurnedPerKilowattHourNetGeneration": "fuel_cost_per_kwh",
            },
        },
    },
}

################################################################################
# Pydantic classes which contain table and column level transformation parameters
################################################################################


class RenameColsFerc1(BaseModel):
    """Dictionaries for renaming FERC 1 columns."""

    dbf: dict[str, str] = {}
    xbrl: dict[str, str] = {}
    # Validate keys appear in dbf/xbrl sources
    # Validate values appear in PUDL tables
    # Validate that all PUDL table columns are mapped


class StringCategories(BaseModel):
    """Defines mappings to clean up manually categorized freeform strings.

    Each key in a stringmap is a cleaned output category, and each value is the set of
    all strings which should be replaced with associated clean output category.

    """

    categories: dict[str, set[str]]

    def mapping(self) -> dict[str, str]:
        """A 1-to-1 mapping appropriate for use with :meth:`pd.Series.map`."""
        return {
            string: cat for cat in self.categories for string in self.categories[cat]
        }


class UnitConversion(BaseModel):
    """A column-wise unit conversion."""

    multiplier: float = 1.0
    adder: float = 0.0
    pattern: Pattern
    repl: str

    def convert(self, col: pd.Series) -> pd.Series:
        """Convert column units and rename appropriately."""
        new_name = re.sub(pattern=self.pattern, repl=self.repl, string=col.name)
        # only apply the unit conversion if the column name matches pattern
        if new_name == col.name:
            logger.warning(
                f"{col.name} did not match the unit rename pattern. Check for typos "
                "and make sure you're applying the conversion to an appropriate column."
            )
        col = self.multiplier * col + self.adder
        col.name = new_name
        return col


class ValidRange(BaseModel):
    """Column level specification of min and/or max values."""

    lower_bound: float = -np.inf
    upper_bound: float = np.inf

    def nullify_outliers(self, col: pd.Series) -> pd.Series:
        """Set any values outside the valid range to NA."""
        col = pd.to_numeric(col, errors="coerce")
        col[col > self.upper_bound] = np.nan
        col[col < self.lower_bound] = np.nan
        return col


"""There are several instances in which we have two records and almost all of
the content in the records is in one while another record has one datapoint."""

##############################################################################
# FERC TRANSFORM HELPER FUNCTIONS ############################################
##############################################################################


def unpack_table(ferc1_df, table_name, data_cols, data_rows):
    """Normalize a row-and-column based FERC Form 1 table.

    Pulls the named database table from the FERC Form 1 DB and uses the corresponding
    ferc1_row_map to unpack the row_number coded data.

    Args:
        ferc1_df (pandas.DataFrame): Raw FERC Form 1 DataFrame from the DB.
        table_name (str): Original name of the FERC Form 1 DB table.
        data_cols (list): List of strings corresponding to the original FERC Form 1
            database table column labels -- these are the columns of data that we are
            extracting (it can be a subset of the columns which are present in the
            original database).
        data_rows (list): List of row_names to extract, as defined in the FERC 1 row
            maps. Set to slice(None) if you want all rows.

    Returns:
        pandas.DataFrame

    """
    # Read in the corresponding row map:
    row_map = (
        pd.read_csv(
            importlib.resources.open_text(
                "pudl.package_data.ferc1.row_maps", f"{table_name}.csv"
            ),
            index_col=0,
            comment="#",
        )
        .copy()
        .transpose()
        .rename_axis(index="year_index", columns=None)
    )
    row_map.index = row_map.index.astype(int)

    # For each year, rename row numbers to variable names based on row_map.
    rename_dict = {}
    out_df = pd.DataFrame()
    for year in row_map.index:
        rename_dict = {v: k for k, v in dict(row_map.loc[year, :]).items()}
        _ = rename_dict.pop(-1, None)
        df = ferc1_df.loc[ferc1_df.report_year == year].copy()
        df.loc[:, "row_name"] = df.loc[:, "row_number"].replace(rename_dict)
        # The concatenate according to row_name
        out_df = pd.concat([out_df, df], axis="index")

    # Is this list of index columns universal? Or should they be an argument?
    idx_cols = ["respondent_id", "report_year", "report_prd", "spplmnt_num", "row_name"]
    logger.info(
        f"{len(out_df[out_df.duplicated(idx_cols)])/len(out_df):.4%} "
        f"of unpacked records were duplicates, and discarded."
    )
    # Index the dataframe based on the list of index_cols
    # Unstack the dataframe based on variable names
    out_df = (
        out_df.loc[:, idx_cols + data_cols]
        # These lost records should be minimal. If not, something's wrong.
        .drop_duplicates(subset=idx_cols)
        .set_index(idx_cols)
        .unstack("row_name")
        .loc[:, (slice(None), data_rows)]
    )
    return out_df


def cols_to_cats(df, cat_name, col_cats):
    """Turn top-level MultiIndex columns into a categorial column.

    In some cases FERC Form 1 data comes with many different types of related values
    interleaved in the same table -- e.g. current year and previous year income -- this
    can result in DataFrames that are hundreds of columns wide, which is unwieldy. This
    function takes those top level MultiIndex labels and turns them into categories in a
    single column, which can be used to select a particular type of report.

    Args:
        df (pandas.DataFrame): the dataframe to be simplified.
        cat_name (str): the label of the column to be created indicating what
            MultiIndex label the values came from.
        col_cats (dict): a dictionary with top level MultiIndex labels as keys,
            and the category to which they should be mapped as values.

    Returns:
        pandas.DataFrame: A re-shaped/re-labeled dataframe with one fewer levels of
        MultiIndex in the columns, and an additional column containing the assigned
        labels.

    """
    out_df = pd.DataFrame()
    for col, cat in col_cats.items():
        logger.info(f"Col: {col}, Cat: {cat}")
        tmp_df = df.loc[:, col].copy().dropna(how="all")
        tmp_df.loc[:, cat_name] = cat
        out_df = pd.concat([out_df, tmp_df])
    return out_df.reset_index()


def _clean_cols(df, table_name):
    """Adds a FERC record ID and drop FERC columns not to be loaded into PUDL.

    It is often useful to be able to tell exactly which record in the FERC Form 1
    database a given record within the PUDL database came from. Within each FERC Form 1
    table, each record is supposed to be uniquely identified by the combination of:
    report_year, report_prd, respondent_id, spplmnt_num, row_number.

    So this function takes a dataframe, checks to make sure it contains each of those
    columns and that none of them are NULL, and adds a new column to the dataframe
    containing a string of the format:

    {table_name}_{report_year}_{report_prd}_{respondent_id}_{spplmnt_num}_{row_number}

    In some PUDL FERC Form 1 tables (e.g. plant_in_service_ferc1) a single row is
    re-organized into several new records in order to normalize the data and ensure it
    is stored in a "tidy" format. In such cases each of the resulting PUDL records will
    have the same ``record_id``.  Otherwise, the ``record_id`` is expected to be unique
    within each FERC Form 1 table. However there are a handful of cases in which this
    uniqueness constraint is violated due to data reporting issues in FERC Form 1.

    In addition to those primary key columns, there are some columns which are not
    meaningful or useful in the context of PUDL, but which show up in virtually every
    FERC table, and this function drops them if they are present. These columns include:
    row_prvlg, row_seq, item, record_number (a temporary column used in plants_small)
    and all the footnote columns, which end in "_f".

    TODO: remove in xbrl transition. migrated this functionality into
    ``assign_record_id()``. The last chunk of this function that removes the "_f"
    columns should be abandoned in favor of using the metadata to ensure the
    tables have all/only the correct columns.

    Args:
        df (pandas.DataFrame): The DataFrame in which the function looks for columns
            for the unique identification of FERC records, and ensures that those
            columns are not NULL.
        table_name (str): The name of the table that we are cleaning.

    Returns:
        pandas.DataFrame: The same DataFrame with a column appended containing a string
        of the format
        {table_name}_{report_year}_{report_prd}_{respondent_id}_{spplmnt_num}_{row_number}

    Raises:
        AssertionError: If the table input contains NULL columns

    """
    # Make sure that *all* of these columns exist in the proffered table:
    for field in [
        "report_year",
        "report_prd",
        "respondent_id",
        "spplmnt_num",
        "row_number",
    ]:
        if field in df.columns:
            if df[field].isnull().any():
                raise AssertionError(
                    f"Null field {field} found in ferc1 table {table_name}."
                )

    # Create a unique inter-year FERC table record ID:
    df["record_id"] = (
        table_name
        + "_"
        + df.report_year.astype(str)
        + "_"
        + df.report_prd.astype(str)
        + "_"
        + df.respondent_id.astype(str)
        + "_"
        + df.spplmnt_num.astype(str)
    )
    # Because of the way we are re-organizing columns and rows to create well
    # normalized tables, there may or may not be a row number available.
    if "row_number" in df.columns:
        df["record_id"] = df["record_id"] + "_" + df.row_number.astype(str)

        # Check to make sure that the generated record_id is unique... since
        # that's kind of the whole point. There are couple of genuine bad
        # records here that are taken care of in the transform step, so just
        # print a warning.
        n_dupes = df.record_id.duplicated().values.sum()
        if n_dupes:
            dupe_ids = df.record_id[df.record_id.duplicated()].values
            logger.warning(
                f"{n_dupes} duplicate record_id values found "
                f"in pre-transform table {table_name}: {dupe_ids}."
            )
    # May want to replace this with always constraining the cols to the metadata cols
    # at the end of the transform step (or in rename_columns if we don't need any
    # temp columns)
    # Drop any _f columns... since we're not using the FERC Footnotes...
    # Drop columns and don't complain about it if they don't exist:
    no_f = [c for c in df.columns if not re.match(".*_f$", c)]
    df = (
        df.loc[:, no_f]
        .drop(
            [
                "spplmnt_num",
                "row_number",
                "row_prvlg",
                "row_seq",
                "report_prd",
                "item",
                "record_number",
            ],
            errors="ignore",
            axis="columns",
        )
        .rename(columns={"respondent_id": "utility_id_ferc1"})
    )
    return df


########################################################################################
# Christina's Transformer Classes
########################################################################################


class TransformerMeta(BaseModel):
    """FERC1 table transform metadata."""

    table_name: TABLES_LITERAL

    @property
    def unit_conversions(
        self,
    ) -> dict[str, dict[Literal["column_name_old", "conversions"], str | float]]:
        """Dictionary of unit conversions."""
        return TABLE_UNIT_CONVERSIONS.get(self.table_name, {})

    @property
    def columns_to_rename(self) -> dict[str, dict[Literal["xbrl", "dbf"], str]]:
        """Columns."""
        return COLUMN_RENAME[self.table_name]

    def columns_to_rename_by_ferc1_source(self, source_ferc1) -> dict[str, str]:
        """Dictionary of Ferc1 original column name to pudl columns name by source."""
        return {
            sources_dict.get(source_ferc1): pudl_col
            for (pudl_col, sources_dict) in self.columns_to_rename.items()
            if sources_dict.get(source_ferc1)
        }

    @property
    def axis_cols_xbrl(self) -> list:
        """Axis column names, which are analogous to primary keys per filings."""
        return [  # pudl column names if "Axis" is at the end of the xbrl name
            xbrl_col
            for xbrl_col in self.columns_to_rename_by_ferc1_source("xbrl").keys()
            if "Axis" in xbrl_col
        ]

    @property
    def primary_keys_xbrl(self) -> list:
        """List of pudl column names of primary keys for the XBRL tables.

        This is derived from the :py:const:`COLUMN_RENAME` dictionary by grabbing the list of pudl
        column names when the xbrl column name has 'Axis' at the end. XBRL uses 'Axis'
        as an indicator for primary keys. We also add 'entity_id' and 'report_date' to
        the primary keys.
        """
        return ["report_year", "entity_id"] + [  # other primary keys
            self.columns_to_rename_by_ferc1_source("xbrl")[axis_col]
            for axis_col in self.axis_cols_xbrl
        ]

    @property
    def table_column_condensing_dict(
        self,
    ) -> dict[str, dict[Literal["keep_records", "remove_records"], dict[str, tuple]]]:
        """Dictionary of records to keep and records to remove.

        This cleaning happens via :meth:`condense_sets_of_records_with_one_datapoint_in_second_record`
        """
        return COLUMN_CONDENSING_PRE_STRING_CLEAN[self.table_name]

    @property
    def simplify_strings_cols(self) -> list[str | None]:
        """List of columns to treat with :func:`pudl.helpers.simplify_strings`."""
        return TABLE_SIMPLIFY_STRINGS_COLS.get(self.table_name, [])

    @property
    def clean_strings_dict(self) -> dict[Literal["columns", "stringmaps"], list] | None:
        """Dictionary of treatments for :func:`pudl.helpers.cleanstrings`."""
        return TABLE_CLEAN_STRINGS_COLS.get(self.table_name, None)

    @property
    def oob_to_nan_inputs(self) -> dict | None:
        """Dictionary of inputs for :func:`pudl.helpers.oob_to_nan`."""
        return TABLE_OOB_TO_NAN.get(self.table_name, None)


class GenericTransformer(BaseModel):
    """Generic FERC1 table transformer."""

    table_name: TABLES_LITERAL

    @property
    def meta(self) -> TransformerMeta:
        """Metadata for table transformations."""
        return TransformerMeta(table_name=self.table_name)

    @staticmethod
    def add_report_year_column(df):
        """Add a report year column."""
        df = df.assign(report_year=lambda x: pd.to_datetime(x.start_date).dt.year)
        return df

    def assign_record_id(
        self,
        df: pd.DataFrame,
        source_ferc1: Literal["dbf", "xbrl"],
    ) -> pd.DataFrame:
        """Assign a record ID column from either XBRL or DBF ferc1 source table.

        It is often useful to be able to tell exactly which record in the FERC Form 1
        database a given record within the PUDL database came from.

        Within each FERC Form 1 DBF table, each record is supposed to be uniquely
        identified by the combination of:
        report_year, report_prd, utility_id_ferc1, spplmnt_num, row_number.

        The FERC Form 1 XBRL tables do not have these supplement and row number
        columns, so we construct an id based on:
        report_year, entity_id, and the primary key columns of the XBRL table

        Args:
            df: table to assign `record_id` to
            table_name: name of table
            source_ferc1: data source of raw ferc1 database.

        Raises:
            AssertionError: If the resulting `record_id` column is non-unique.
        """
        if source_ferc1 == "xbrl":
            pk_cols = self.meta.primary_keys_xbrl
        elif source_ferc1 == "dbf":
            pk_cols = [
                "report_year",
                "report_prd",
                "utility_id_ferc1",
                "spplmnt_num",
                "row_number",
            ]
            # for the dbf years, we used the original "f1_" table names for the id
            # table_name = pudl_tables_to_dbf_tables.get(table_name)
        else:
            raise ValueError(
                f"source_ferc1 must be either `xbrl` or `dbf`. Got {source_ferc1}"
            )
        if df[pk_cols].isnull().any(axis=None):
            raise AssertionError(
                f"Null field found in ferc1 table {self.table_name}.\n"
                f"{df[pk_cols].isnull().any()}"
            )

        df = df.assign(
            temp=self.table_name,
            record_id=lambda x: x.temp.str.cat(x[pk_cols].astype(str), sep="_"),
        ).drop(columns=["temp"])

        df = pudl.helpers.simplify_strings(df, ["record_id"])
        if df.record_id.duplicated().any():
            raise AssertionError(
                "Record id column cannot result in duplicates. Columns are not "
                f"unique primary keys: {pk_cols}. \nNon-unqiue records:\n"
                f"{df[df.record_id.duplicated()][['record_id'] + pk_cols]}"
            )
        return df

    @staticmethod
    def assign_utility_id_ferc1_xbrl(df: pd.DataFrame) -> pd.DataFrame:
        """Assign utility_id_ferc1.

        This is a shell rn. Issue #1705
        """
        return df.assign(
            utility_id_ferc1=lambda x: x.entity_id.str.replace("C", "")
            .str.lstrip("0")
            .astype("Int64")
        )

    @staticmethod
    def _multiplicative_error_correction(tofix, mask, minval, maxval, mults):
        """Corrects data entry errors where data being multiplied by a factor.

        In many cases we know that a particular column in the database should have a value
        in a particular rage (e.g. the heat content of a ton of coal is a well defined
        physical quantity -- it can be 15 mmBTU/ton or 22 mmBTU/ton, but it can't be 1
        mmBTU/ton or 100 mmBTU/ton). Sometimes these fields are reported in the wrong units
        (e.g. kWh of electricity generated rather than MWh) resulting in several
        distributions that have a similar shape showing up at different ranges of value
        within the data.  This function takes a one dimensional data series, a description
        of a valid range for the values, and a list of factors by which we expect to see
        some of the data multiplied due to unit errors.  Data found in these "ghost"
        distributions are multiplied by the appropriate factor to bring them into the
        expected range.

        Data values which are not found in one of the acceptable multiplicative ranges are
        set to NA.

        Args:
            tofix (pandas.Series): A 1-dimensional data series containing the values to be
                fixed.
            mask (pandas.Series): A 1-dimensional masking array of True/False values, which
                will be used to select a subset of the tofix series onto which we will apply
                the multiplicative fixes.
            min (float): the minimum realistic value for the data series.
            max (float): the maximum realistic value for the data series.
            mults (list of floats): values by which "real" data may have been multiplied
                due to common data entry errors. These values both show us where to look in
                the full data series to find recoverable data, and also tell us by what
                factor those values need to be multiplied to bring them back into the
                reasonable range.

        Returns:
            fixed (pandas.Series): a data series of the same length as the input, but with
            the transformed values.
        """
        # Grab the subset of the input series we are going to work on:
        records_to_fix = tofix[mask]
        # Drop those records from our output series
        fixed = tofix.drop(records_to_fix.index)
        # Iterate over the multipliers, applying fixes to outlying populations
        for mult in mults:
            records_to_fix = records_to_fix.apply(
                lambda x: x * mult if x > minval / mult and x < maxval / mult else x
            )
        # Set any record that wasn't inside one of our identified populations to
        # NA -- we are saying that these are true outliers, which can't be part
        # of the population of values we are examining.
        records_to_fix = records_to_fix.apply(
            lambda x: np.nan if x < minval or x > maxval else x
        )
        # Add our fixed records back to the complete data series and return it
        fixed = pd.concat([fixed, records_to_fix])
        return fixed

    @staticmethod
    def convert_units(df, column_name_new, column_name_old, conversion):
        """Convert the units of a column."""
        df.loc[:, column_name_new] = df.loc[:, column_name_old] * conversion
        return df.drop(columns=[column_name_old])

    def convert_units_in_table(self, df: pd.DataFrame):
        """Convert the units of a table based on.

        If a table does not have any units to convert, the original table will be
        returned.
        """
        for column, col_info in self.meta.unit_conversions.items():
            df = self.convert_units(
                df=df,
                column_name_new=column,
                column_name_old=col_info["column_name_old"],
                conversion=col_info["conversion"],
            )
        return df

    def rename_columns(
        self, raw_table: pd.DataFrame, source: Literal["xbrl", "dbf"]
    ) -> pd.DataFrame:
        """Rename the columns in a FERC1 table based on table name and source.

        This function is a light wrapper around ``pandas.rename``, mostly used to
        grab the appropriate dictionary of old columns (keys) to new columns (values)
        from :py:const:`COLUMN_RENAME` based on the table and the FERC1 source.
        """
        return raw_table.rename(
            columns=self.meta.columns_to_rename_by_ferc1_source(source)
        )

    def merge_instant_and_duration_tables(
        self, duration: pd.DataFrame, instant: pd.DataFrame
    ) -> pd.DataFrame:
        """Merge the XBRL instand and duration tables.

        FERC1 XBRL instant period signifies that it is true as of the reported date,
        while a duration fact is true for the specified time period. The ``date``
        column for an instant fact is effectively equivalent to the ``end_date``
        column of a duration fact.

        Args:
            duration: XBRL duration table.
            instant: XBRL instant table.
            table_name: table name

        Returns:
            one unified table that is a combination of the duration and instant
            input tables. Any overlapping columns (besides the ``merge_on`` columns)
            will be consolidated by filling the duration columns with the instant
            columns.
        """
        # plants_steam_ferc1_duration
        table = pd.merge(
            duration.drop(columns=["filing_name"]),
            instant.drop(columns=["filing_name"]),
            how="outer",
            left_on=["end_date", "entity_id"] + self.meta.axis_cols_xbrl,
            right_on=["date", "entity_id"] + self.meta.axis_cols_xbrl,
            validate="1:1",
        )
        return table

    def pre_concat_clean_and_concat_dbf_xbrl(
        self,
        raw_dbf: pd.DataFrame,
        raw_xbrl_instant: pd.DataFrame | None,
        raw_xbrl_duration: pd.DataFrame | None,
    ) -> pd.DataFrame:
        """Perform pre-concat cleanig of XBRL and DBF tables and concatenate.

        This function takes the raw intput tables from the FERC1 DBF and XBRL original
        sources, performs the necessary pre-concatenation cleanup and then
        concatenates the two FERC1 sources.

        It assumes the format for the name of the table-specific pre-concat function
        is: "pre_concat_{source of FERC1 ("xbrl" or "dbf")}_{``table_name``}"
        """
        dbf_df = self.pre_concat_dbf(raw_dbf)
        xbrl_df = self.pre_concat_xbrl(raw_xbrl_instant, raw_xbrl_duration)
        df_concat = pd.concat([dbf_df, xbrl_df]).reset_index(drop=True)
        return df_concat

    def condense_sets_of_records_with_one_datapoint_in_second_record(
        self, df: pd.DataFrame
    ) -> pd.DataFrame:
        """Condense two records with one datapoint in one column.

        There are instances in which FERC records are semi-duplicated with almost
        all the information in one record (the "keep_records") and another record
        with one column containing data (the "remove_records") that we want to move
        into the more fleshed out record. This function uses the information in
        `table_column_condensing_dict` to move contents from our "keep_records"
        into our "remove_records" and then we drop the "remove_records".

        Args:
            df: input dataframe.
        """
        df = df.set_index(self.meta.primary_keys_xbrl).sort_index()
        for column, fixes in self.meta.table_column_condensing_dict.items():
            df.loc[fixes["keep_records"], column] = df.loc[
                fixes["remove_records"], column
            ]
            df = df.drop(index=df.loc[fixes["remove_records"]].index)
        df = df.reset_index()
        return df

    def simplify_strings_for_table(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply ``pudl.helpers.simplify_strings`` to a ferc1 table."""
        df = pudl.helpers.simplify_strings(df, columns=self.meta.simplify_strings_cols)
        return df

    def clean_strings_for_table(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply ``pudl.helpers.cleanstrings`` to a ferc1 table."""
        df = pudl.helpers.cleanstrings(
            df=df,
            columns=self.meta.clean_strings_dict.get("columns"),
            stringmaps=self.meta.clean_strings_dict.get("stringmaps"),
            unmapped=self.meta.clean_strings_dict.get("unmapped", pd.NA),
        )
        return df

    def replace_unknown_w_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        """Replace the ``unknown``'s from ``cleanstrings`` with nulls."""
        df = df.replace(
            {
                column: "unknown"
                for column in self.meta.clean_strings_dict.get("columns")
            },
            pd.NA,
        )
        return df

    def oob_to_nan_for_table(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply ``pudl.helpers.oob_to_nan`` to a ferc1 table."""
        df = pudl.helpers.oob_to_nan(
            df=df,
            cols=self.meta.oob_to_nan_inputs.get("columns"),
            lb=self.meta.oob_to_nan_inputs.get("lower_bound"),
            ub=self.meta.oob_to_nan_inputs.get("upper_bound"),
        )
        return df

    def convert_table_dtypes(self, df):
        """Convert a table's datatypes via :func:`pudl.helpers.convert_cols_dtypes`."""
        return convert_cols_dtypes(df, name=self.table_name, data_source="ferc1")

    def restrict_to_pudl_cols(self, df):
        """Ensure all the pudl columns are present and drop all others."""
        return df


##############################################################################
# DATABASE TABLE SPECIFIC PROCEDURES #########################################
##############################################################################


class PlantsSteamFerc1(GenericTransformer):
    """Transformer class for the plants_steam_ferc1 table."""

    def execute(
        self,
        raw_dbf: pd.DataFrame,
        raw_xbrl_instant: pd.DataFrame,
        raw_xbrl_duration: pd.DataFrame,
        transformed_fuel: pd.DataFrame,
    ):
        """Perform table transformations for the plants_steam_ferc1 table."""
        plants_steam_combo = (
            self.pre_concat_clean_and_concat_dbf_xbrl(
                raw_dbf, raw_xbrl_instant, raw_xbrl_duration
            )
            .pipe(self.simplify_strings_for_table)
            .pipe(self.clean_strings_for_table)
            .pipe(self.oob_to_nan_for_table)
            .pipe(self.convert_units_in_table)
            .pipe(self.convert_table_dtypes)
            .pipe(plants_steam_assign_plant_ids, ferc1_fuel_df=transformed_fuel)
        )
        plants_steam_validate_ids(plants_steam_combo)
        return plants_steam_combo

    def pre_concat_dbf(self, raw_dbf):
        """Modifications of the dbf plants_steam_ferc1 table before concat w/ xbrl."""
        plants_steam_dbf = self.rename_columns(
            raw_table=raw_dbf,
            source="dbf",
        ).pipe(self.assign_record_id, source_ferc1="dbf")
        return plants_steam_dbf

    def pre_concat_xbrl(self, raw_xbrl_instant, raw_xbrl_duration):
        """Modifications of the xbrl plants_steam_ferc1 table before concat w/ xbrl."""
        plants_steam_xbrl = (
            self.merge_instant_and_duration_tables(
                duration=raw_xbrl_duration,
                instant=raw_xbrl_instant,
            )
            .pipe(
                self.rename_columns,
                source="xbrl",
            )
            .pipe(self.add_report_year_column)
            .pipe(
                self.assign_record_id,
                source_ferc1="xbrl",
            )
            .pipe(self.assign_utility_id_ferc1_xbrl)
        )
        return plants_steam_xbrl


class FuelFerc1(GenericTransformer):
    """Transformer class for the fuel_ferc1 table."""

    def execute(
        self,
        raw_dbf: pd.DataFrame,
        raw_xbrl_instant: None,
        raw_xbrl_duration: pd.DataFrame,
    ):
        """Transforms FERC Form 1 fuel data for loading into PUDL Database."""
        fuel_df = (
            self.pre_concat_clean_and_concat_dbf_xbrl(
                raw_dbf, raw_xbrl_instant, raw_xbrl_duration
            )
            .pipe(self.simplify_strings_for_table)
            .pipe(self.convert_float_nulls)
            .pipe(self.convert_table_dtypes)
            .pipe(self.fuel_correct_data_errors)
            .pipe(self.fuel_drop_bad)
        )
        return fuel_df

    def pre_concat_dbf(self, raw_dbf):
        """Modifications of the dbf fuel_ferc1 table before concat w/ xbrl."""
        fuel_dbf = (
            self.rename_columns(
                raw_table=raw_dbf,
                source="dbf",
            )
            .pipe(self.assign_record_id, source_ferc1="dbf")
            .pipe(self.clean_strings_for_table)
            .pipe(self.convert_units_in_table)
        )
        return fuel_dbf

    def pre_concat_xbrl(self, raw_xbrl_instant: None, raw_xbrl_duration: pd.DataFrame):
        """Modifications of the xbrl fuel_ferc1 table before concat w/ dbf."""
        fuel_xbrl = (
            self.rename_columns(
                raw_table=raw_xbrl_duration,
                source="xbrl",
            )
            .pipe(self.add_report_year_column)
            .pipe(self.condense_sets_of_records_with_one_datapoint_in_second_record)
            # clean the strings b4 the record_id assignment bc cols are pk/pk adjacent
            .pipe(self.clean_strings_for_table)
            .pipe(self.aggregate_fuel_dupes)
            .pipe(self.assign_record_id, source_ferc1="xbrl")
            .pipe(self.assign_utility_id_ferc1_xbrl)
        )
        return fuel_xbrl

    def aggregate_fuel_dupes(self, fuel_xbrl: pd.DataFrame) -> pd.DataFrame:
        """Aggregate the duplicate fuel records with a duplicate primary key."""
        pk_cols = self.meta.primary_keys_xbrl
        fuel_xbrl.loc[:, "fuel_units_count"] = fuel_xbrl.groupby(pk_cols, dropna=False)[
            "fuel_units"
        ].transform("nunique")

        # split
        dupe_mask = fuel_xbrl.duplicated(subset=pk_cols, keep=False)
        multi_unit_mask = fuel_xbrl.fuel_units_count != 1

        fuel_pk_dupes = fuel_xbrl[dupe_mask & ~multi_unit_mask].copy()
        fuel_multi_unit = fuel_xbrl[dupe_mask & multi_unit_mask].copy()
        fuel_non_dupes = fuel_xbrl[~dupe_mask & ~multi_unit_mask]
        logger.info(
            f"Aggregating {len(fuel_pk_dupes)} PK duplicates and nulling/dropping "
            f"{len(fuel_multi_unit)} fuel unit records from the fuel_ferc1 table "
            f"out of {len(fuel_xbrl)}"
        )
        if (
            fuel_to_agg := (
                (len(fuel_pk_dupes) + len(fuel_multi_unit)) / len(fuel_xbrl)
            )
            > 0.15
        ):
            raise AssertionError(
                f"Too many fuel table primary key duplicates ({fuel_to_agg:.0%})"
            )
        data_cols = [
            "fuel_consumed_units",
            "fuel_mmbtu_per_unit",
            "fuel_cost_per_unit_delivered",
            "fuel_cost_per_unit_burned",
            "fuel_cost_per_mmbtu",
            "fuel_cost_per_kwh",
            "fuel_mmbtu_per_kwh",
        ]
        # apply
        fuel_pk_dupes = pudl.helpers.sum_and_weighted_average_agg(
            df_in=fuel_pk_dupes,
            by=pk_cols + ["filing_name", "start_date", "end_date", "fuel_units"],
            sum_cols=["fuel_consumed_units"],
            wtavg_dict={
                k: "fuel_consumed_units"
                for k in data_cols
                if k != "fuel_consumed_units"
            },
        )
        fuel_multi_unit.loc[:, data_cols] = pd.NA
        fuel_multi_unit = fuel_multi_unit.drop_duplicates(subset=pk_cols, keep="first")
        # combine
        fuel_deduped = pd.concat([fuel_non_dupes, fuel_pk_dupes, fuel_multi_unit]).drop(
            columns=["fuel_units_count"]
        )
        return fuel_deduped

    def fuel_correct_data_errors(self, fuel_ferc1_df):
        """Correct data errors for the fuel table.

        Note: this is a direct copy/paste from fuel_old. Still need to do extensive
        data testing on the xbrl fuel table to see if these corrections are similarly
        applicable.
        """
        coal_mask = fuel_ferc1_df["fuel_type_code_pudl"] == "coal"
        gas_mask = fuel_ferc1_df["fuel_type_code_pudl"] == "gas"
        oil_mask = fuel_ferc1_df["fuel_type_code_pudl"] == "oil"

        corrections = [
            # mult = 2000: reported in units of lbs instead of short tons
            # mult = 1e6:  reported BTUs instead of mmBTUs
            # minval and maxval of 10 and 29 mmBTUs are the range of values
            # specified by EIA 923 instructions at:
            # https://www.eia.gov/survey/form/eia_923/instructions.pdf
            ["fuel_mmbtu_per_unit", coal_mask, 10.0, 29.0, (2e3, 1e6)],
            # mult = 1e-2: reported cents/mmBTU instead of USD/mmBTU
            # minval and maxval of .5 and 7.5 dollars per mmBTUs are the
            # end points of the primary distribution of EIA 923 fuel receipts
            # and cost per mmBTU data weighted by quantity delivered
            ["fuel_cost_per_mmbtu", coal_mask, 0.5, 7.5, (1e-2,)],
            # mult = 1e3: reported fuel quantity in cubic feet, not mcf
            # mult = 1e6: reported fuel quantity in BTU, not mmBTU
            # minval and maxval of .8 and 1.2 mmBTUs are the range of values
            # specified by EIA 923 instructions
            ["fuel_mmbtu_per_unit", gas_mask, 0.8, 1.2, (1e3, 1e6)],
            # mult = 1e-2: reported in cents/mmBTU instead of USD/mmBTU
            # minval and maxval of 1 and 35 dollars per mmBTUs are the
            # end points of the primary distribution of EIA 923 fuel receipts
            # and cost per mmBTU data weighted by quantity delivered
            ["fuel_cost_per_mmbtu", gas_mask, 1, 35, (1e-2,)],
            # mult = 42: reported fuel quantity in gallons, not barrels
            # mult = 1e6: reported fuel quantity in BTU, not mmBTU
            # minval and maxval of 3 and 6.9 mmBTUs are the range of values
            # specified by EIA 923 instructions
            ["fuel_mmbtu_per_unit", oil_mask, 3, 6.9, (42,)],
            # mult = 1e-2: reported in cents/mmBTU instead of USD/mmBTU
            # minval and maxval of 5 and 33 dollars per mmBTUs are the
            # end points of the primary distribution of EIA 923 fuel receipts
            # and cost per mmBTU data weighted by quantity delivered
            ["fuel_cost_per_mmbtu", oil_mask, 5, 33, (1e-2,)],
        ]

        for (coltofix, mask, minval, maxval, mults) in corrections:
            fuel_ferc1_df[coltofix] = self._multiplicative_error_correction(
                fuel_ferc1_df[coltofix], mask, minval, maxval, mults
            )
        return fuel_ferc1_df

    @staticmethod
    def fuel_drop_bad(fuel_df: pd.DataFrame) -> pd.DataFrame:
        """Drop known to be bad fuel records.

        If all of the data columns are null excpet one (`fuel_mmbtu_per_kwh`) which
        is known to be a column that shows up for FERC's weird total records.
        """
        data_cols = [
            "fuel_consumed_units",
            "fuel_mmbtu_per_unit",
            "fuel_cost_per_unit_delivered",
            "fuel_cost_per_unit_burned",
            "fuel_cost_per_mmbtu",
            "fuel_cost_per_kwh",
            "fuel_mmbtu_per_kwh",
        ]
        probably_totals_index = fuel_df[
            fuel_df[[col for col in data_cols if col != "fuel_mmbtu_per_kwh"]]
            .isnull()
            .all(axis="columns")
            & fuel_df.fuel_mmbtu_per_kwh.notnull()
            & (fuel_df.fuel_type_code_pudl == "other")
            & (fuel_df.fuel_units == "unknown")
        ].index

        fuel_df = fuel_df.drop(index=probably_totals_index)
        return fuel_df

    @staticmethod
    def convert_float_nulls(df):
        """Convert the nulls of float columns in a table.

        Note: WHY THOUGH! I kept getting this error via convert_cols_dtypes:
        TypeError: float() argument must be a string or a real number, not 'NAType'
        """
        float_cols = [
            col
            for col, dtype in pudl.helpers.get_pudl_dtypes(group="ferc1").items()
            if col in df.columns and "float" in dtype
        ]
        df.loc[df.fuel_consumed_units.isnull(), float_cols] = np.nan
        return df


########################################################################################
# Old per-table transform functions
########################################################################################


def plants_small(ferc1_dbf_raw_dfs, ferc1_xbrl_raw_dfs, ferc1_transformed_dfs):
    """Transforms FERC Form 1 plant_small data for loading into PUDL Database.

    This FERC Form 1 table contains information about a large number of small plants,
    including many small hydroelectric and other renewable generation facilities.
    Unfortunately the data is not well standardized, and so the plants have been
    categorized manually, with the results of that categorization stored in an Excel
    spreadsheet. This function reads in the plant type data from the spreadsheet and
    merges it with the rest of the information from the FERC DB based on record number,
    FERC respondent ID, and report year. When possible the FERC license number for small
    hydro plants is also manually extracted from the data.

    This categorization will need to be renewed with each additional year of FERC data
    we pull in. As of v0.1 the small plants have been categorized for 2004-2015.

    Args:
        ferc1_raw_dfs (dict): Each entry in this dictionary of DataFrame objects
            corresponds to a table from the  FERC Form 1 DBC database.
        ferc1_transformed_dfs (dict): A dictionary of DataFrames to be transformed.

    Returns:
        dict: The dictionary of transformed dataframes.
    """
    # grab table from dictionary of dfs
    ferc1_small_df = ferc1_dbf_raw_dfs["plants_small_ferc1"]
    # Standardize plant_name_raw capitalization and remove leading/trailing
    # white space -- necesary b/c plant_name_raw is part of many foreign keys.
    ferc1_small_df = pudl.helpers.simplify_strings(
        ferc1_small_df, ["plant_name", "kind_of_fuel"]
    )

    # Force the construction and installation years to be numeric values, and
    # set them to NA if they can't be converted. (table has some junk values)
    ferc1_small_df = pudl.helpers.oob_to_nan(
        ferc1_small_df,
        cols=["yr_constructed"],
        lb=1850,
        ub=max(DataSource.from_id("ferc1").working_partitions["years"]) + 1,
    )

    # Convert from cents per mmbtu to dollars per mmbtu to be consistent
    # with the f1_fuel table data. Also, let's use a clearer name.
    ferc1_small_df["fuel_cost_per_mmbtu"] = ferc1_small_df["fuel_cost"] / 100.0
    ferc1_small_df.drop("fuel_cost", axis=1, inplace=True)

    # Create a single "record number" for the individual lines in the FERC
    # Form 1 that report different small plants, so that we can more easily
    # tell whether they are adjacent to each other in the reporting.
    ferc1_small_df["record_number"] = (
        46 * ferc1_small_df["spplmnt_num"] + ferc1_small_df["row_number"]
    )

    # Unforunately the plant types were not able to be parsed automatically
    # in this table. It's been done manually for 2004-2015, and the results
    # get merged in in the following section.
    small_types_file = importlib.resources.open_binary(
        "pudl.package_data.ferc1", "small_plants_2004-2016.xlsx"
    )
    small_types_df = pd.read_excel(small_types_file)

    # Only rows with plant_type set will give us novel information.
    small_types_df.dropna(
        subset=[
            "plant_type",
        ],
        inplace=True,
    )
    # We only need this small subset of the columns to extract the plant type.
    small_types_df = small_types_df[
        [
            "report_year",
            "respondent_id",
            "record_number",
            "plant_name_clean",
            "plant_type",
            "ferc_license",
        ]
    ]

    # Munge the two dataframes together, keeping everything from the
    # frame we pulled out of the FERC1 DB, and supplementing it with the
    # plant_name, plant_type, and ferc_license fields from our hand
    # made file.
    ferc1_small_df = pd.merge(
        ferc1_small_df,
        small_types_df,
        how="left",
        on=["report_year", "respondent_id", "record_number"],
    )

    # Remove extraneous columns and add a record ID
    ferc1_small_df = _clean_cols(ferc1_small_df, "f1_gnrt_plant")

    # Standardize plant_name capitalization and remove leading/trailing white
    # space, so that plant_name matches formatting of plant_name_raw
    ferc1_small_df = pudl.helpers.simplify_strings(ferc1_small_df, ["plant_name_clean"])

    # in order to create one complete column of plant names, we have to use the
    # cleaned plant names when available and the orignial plant names when the
    # cleaned version is not available, but the strings first need cleaning
    ferc1_small_df["plant_name_clean"] = ferc1_small_df["plant_name_clean"].fillna(
        value=""
    )
    ferc1_small_df["plant_name_clean"] = ferc1_small_df.apply(
        lambda row: row["plant_name"]
        if (row["plant_name_clean"] == "")
        else row["plant_name_clean"],
        axis=1,
    )

    # now we don't need the uncleaned version anymore
    # ferc1_small_df.drop(['plant_name'], axis=1, inplace=True)

    ferc1_small_df.rename(
        columns={
            # FERC 1 DB Name      PUDL DB Name
            "plant_name": "plant_name_ferc1",
            "ferc_license": "ferc_license_id",
            "yr_constructed": "construction_year",
            "capacity_rating": "capacity_mw",
            "net_demand": "peak_demand_mw",
            "net_generation": "net_generation_mwh",
            "plant_cost": "total_cost_of_plant",
            "plant_cost_mw": "capex_per_mw",
            "operation": "opex_operations",
            "expns_fuel": "opex_fuel",
            "expns_maint": "opex_maintenance",
            "kind_of_fuel": "fuel_type",
            "fuel_cost": "fuel_cost_per_mmbtu",
        },
        inplace=True,
    )

    ferc1_transformed_dfs["plants_small_ferc1"] = ferc1_small_df
    return ferc1_transformed_dfs


def plants_hydro(ferc1_dbf_raw_dfs, ferc1_xbrl_raw_dfs, ferc1_transformed_dfs):
    """Transforms FERC Form 1 plant_hydro data for loading into PUDL Database.

    Standardizes plant names (stripping whitespace and Using Title Case). Also converts
    into our preferred units of MW and MWh.

    Args:
        ferc1_raw_dfs (dict): Each entry in this dictionary of DataFrame objects
            corresponds to a table from the  FERC Form 1 DBC database.
        ferc1_transformed_dfs (dict): A dictionary of DataFrames to be transformed.

    Returns:
        dict: The dictionary of transformed dataframes.
    """
    # grab table from dictionary of dfs
    ferc1_hydro_df = (
        _clean_cols(ferc1_dbf_raw_dfs["plants_hydro_ferc1"], "f1_hydro")
        # Standardize plant_name capitalization and remove leading/trailing
        # white space -- necesary b/c plant_name is part of many foreign keys.
        .pipe(pudl.helpers.simplify_strings, ["plant_name"])
        .pipe(
            pudl.helpers.cleanstrings,
            ["plant_const"],
            [CONSTRUCTION_TYPES],
            unmapped=pd.NA,
        )
        .assign(
            # Converting kWh to MWh
            net_generation_mwh=lambda x: x.net_generation / 1000.0,
            # Converting cost per kW installed to cost per MW installed:
            cost_per_mw=lambda x: x.cost_per_kw * 1000.0,
            # Converting kWh to MWh
            expns_per_mwh=lambda x: x.expns_kwh * 1000.0,
        )
        .pipe(
            pudl.helpers.oob_to_nan,
            cols=["yr_const", "yr_installed"],
            lb=1850,
            ub=max(DataSource.from_id("ferc1").working_partitions["years"]) + 1,
        )
        .drop(columns=["net_generation", "cost_per_kw", "expns_kwh"])
        .rename(
            columns={
                # FERC1 DB          PUDL DB
                "plant_name": "plant_name_ferc1",
                "project_no": "project_num",
                "yr_const": "construction_year",
                "plant_kind": "plant_type",
                "plant_const": "construction_type",
                "yr_installed": "installation_year",
                "tot_capacity": "capacity_mw",
                "peak_demand": "peak_demand_mw",
                "plant_hours": "plant_hours_connected_while_generating",
                "favorable_cond": "net_capacity_favorable_conditions_mw",
                "adverse_cond": "net_capacity_adverse_conditions_mw",
                "avg_num_of_emp": "avg_num_employees",
                "cost_of_land": "capex_land",
                "cost_structure": "capex_structures",
                "cost_facilities": "capex_facilities",
                "cost_equipment": "capex_equipment",
                "cost_roads": "capex_roads",
                "cost_plant_total": "capex_total",
                "cost_per_mw": "capex_per_mw",
                "expns_operations": "opex_operations",
                "expns_water_pwr": "opex_water_for_power",
                "expns_hydraulic": "opex_hydraulic",
                "expns_electric": "opex_electric",
                "expns_generation": "opex_generation_misc",
                "expns_rents": "opex_rents",
                "expns_engineering": "opex_engineering",
                "expns_structures": "opex_structures",
                "expns_dams": "opex_dams",
                "expns_plant": "opex_plant",
                "expns_misc_plant": "opex_misc_plant",
                "expns_per_mwh": "opex_per_mwh",
                "expns_engnr": "opex_engineering",
                "expns_total": "opex_total",
                "asset_retire_cost": "asset_retirement_cost",
                "": "",
            }
        )
        .drop_duplicates(
            subset=[
                "report_year",
                "utility_id_ferc1",
                "plant_name_ferc1",
                "capacity_mw",
            ],
            keep=False,
        )
    )
    if ferc1_hydro_df["construction_type"].isnull().any():
        raise AssertionError(
            "NA values found in construction_type column during FERC1 hydro clean, add string to CONSTRUCTION_TYPES"
        )
    ferc1_hydro_df = ferc1_hydro_df.replace({"construction_type": "unknown"}, pd.NA)
    ferc1_transformed_dfs["plants_hydro_ferc1"] = ferc1_hydro_df
    return ferc1_transformed_dfs


def plants_pumped_storage(ferc1_dbf_raw_dfs, ferc1_xbrl_raw_dfs, ferc1_transformed_dfs):
    """Transforms FERC Form 1 pumped storage data for loading into PUDL.

    Standardizes plant names (stripping whitespace and Using Title Case). Also converts
    into our preferred units of MW and MWh.

    Args:
        ferc1_raw_dfs (dict): Each entry in this dictionary of DataFrame objects
            corresponds to a table from the  FERC Form 1 DBC database.
        ferc1_transformed_dfs (dict): A dictionary of DataFrames to be transformed.

    Returns:
        dict: The dictionary of transformed dataframes.
    """
    # grab table from dictionary of dfs
    ferc1_pump_df = (
        _clean_cols(
            ferc1_dbf_raw_dfs["plants_pumped_storage_ferc1"], "f1_pumped_storage"
        )
        # Standardize plant_name capitalization and remove leading/trailing
        # white space -- necesary b/c plant_name is part of many foreign keys.
        .pipe(pudl.helpers.simplify_strings, ["plant_name"])
        # Clean up the messy plant construction type column:
        .pipe(
            pudl.helpers.cleanstrings,
            ["plant_kind"],
            [CONSTRUCTION_TYPES],
            unmapped=pd.NA,
        )
        .assign(
            # Converting from kW/kWh to MW/MWh
            net_generation_mwh=lambda x: x.net_generation / 1000.0,
            energy_used_for_pumping_mwh=lambda x: x.energy_used / 1000.0,
            net_load_mwh=lambda x: x.net_load / 1000.0,
            cost_per_mw=lambda x: x.cost_per_kw * 1000.0,
            expns_per_mwh=lambda x: x.expns_kwh * 1000.0,
        )
        .pipe(
            pudl.helpers.oob_to_nan,
            cols=["yr_const", "yr_installed"],
            lb=1850,
            ub=max(DataSource.from_id("ferc1").working_partitions["years"]) + 1,
        )
        .drop(
            columns=[
                "net_generation",
                "energy_used",
                "net_load",
                "cost_per_kw",
                "expns_kwh",
            ]
        )
        .rename(
            columns={
                # FERC1 DB          PUDL DB
                "plant_name": "plant_name_ferc1",
                "project_number": "project_num",
                "tot_capacity": "capacity_mw",
                "project_no": "project_num",
                "plant_kind": "construction_type",
                "peak_demand": "peak_demand_mw",
                "yr_const": "construction_year",
                "yr_installed": "installation_year",
                "plant_hours": "plant_hours_connected_while_generating",
                "plant_capability": "plant_capability_mw",
                "avg_num_of_emp": "avg_num_employees",
                "cost_wheels": "capex_wheels_turbines_generators",
                "cost_land": "capex_land",
                "cost_structures": "capex_structures",
                "cost_facilties": "capex_facilities",
                "cost_wheels_turbines_generators": "capex_wheels_turbines_generators",
                "cost_electric": "capex_equipment_electric",
                "cost_misc_eqpmnt": "capex_equipment_misc",
                "cost_roads": "capex_roads",
                "asset_retire_cost": "asset_retirement_cost",
                "cost_of_plant": "capex_total",
                "cost_per_mw": "capex_per_mw",
                "expns_operations": "opex_operations",
                "expns_water_pwr": "opex_water_for_power",
                "expns_pump_strg": "opex_pumped_storage",
                "expns_electric": "opex_electric",
                "expns_misc_power": "opex_generation_misc",
                "expns_rents": "opex_rents",
                "expns_engneering": "opex_engineering",
                "expns_structures": "opex_structures",
                "expns_dams": "opex_dams",
                "expns_plant": "opex_plant",
                "expns_misc_plnt": "opex_misc_plant",
                "expns_producton": "opex_production_before_pumping",
                "pumping_expenses": "opex_pumping",
                "tot_prdctn_exns": "opex_total",
                "expns_per_mwh": "opex_per_mwh",
            }
        )
        .drop_duplicates(
            subset=[
                "report_year",
                "utility_id_ferc1",
                "plant_name_ferc1",
                "capacity_mw",
            ],
            keep=False,
        )
    )
    if ferc1_pump_df["construction_type"].isnull().any():
        raise AssertionError(
            "NA values found in construction_type column during FERC 1 pumped storage "
            "clean, add string to CONSTRUCTION_TYPES."
        )
    ferc1_pump_df = ferc1_pump_df.replace({"construction_type": "unknown"}, pd.NA)
    ferc1_transformed_dfs["plants_pumped_storage_ferc1"] = ferc1_pump_df
    return ferc1_transformed_dfs


def plant_in_service(ferc1_raw_dfs, ferc1_transformed_dfs):
    """Transforms FERC Form 1 Plant in Service data for loading into PUDL.

    Re-organizes the original FERC Form 1 Plant in Service data by unpacking the rows as
    needed on a year by year basis, to organize them into columns. The "columns" in the
    original FERC Form 1 denote starting balancing, ending balance, additions,
    retirements, adjustments, and transfers -- these categories are turned into labels
    in a column called "amount_type". Because each row in the transformed table is
    composed of many individual records (rows) from the original table, row_number can't
    be part of the record_id, which means they are no longer unique. To infer exactly
    what record a given piece of data came from, the record_id and the row_map (found in
    the PUDL package_data directory) can be used.

    Args:
        ferc1_raw_dfs (dict): Each entry in this dictionary of DataFrame objects
            corresponds to a table from the FERC Form 1 DBC database.
        ferc1_transformed_dfs (dict): A dictionary of DataFrames to be transformed.

    Returns:
        dict: The dictionary of the transformed DataFrames.

    """
    pis_df = (
        unpack_table(
            ferc1_df=ferc1_raw_dfs["plant_in_service_ferc1"],
            table_name="f1_plant_in_srvce",
            data_rows=slice(None),  # Gotta catch 'em all!
            data_cols=[
                "begin_yr_bal",
                "addition",
                "retirements",
                "adjustments",
                "transfers",
                "yr_end_bal",
            ],
        )
        .pipe(  # Convert top level of column index into a categorical column:
            cols_to_cats,
            cat_name="amount_type",
            col_cats={
                "begin_yr_bal": "starting_balance",
                "addition": "additions",
                "retirements": "retirements",
                "adjustments": "adjustments",
                "transfers": "transfers",
                "yr_end_bal": "ending_balance",
            },
        )
        .rename_axis(columns=None)
        .pipe(_clean_cols, "f1_plant_in_srvce")
        .set_index(["utility_id_ferc1", "report_year", "amount_type", "record_id"])
        .reset_index()
    )

    # Get rid of the columns corresponding to "header" rows in the FERC
    # form, which should *never* contain data... but in about 2 dozen cases,
    # they do. See this issue on Github for more information:
    # https://github.com/catalyst-cooperative/pudl/issues/471
    pis_df = pis_df.drop(columns=pis_df.filter(regex=".*_head$").columns)

    ferc1_transformed_dfs["plant_in_service_ferc1"] = pis_df
    return ferc1_transformed_dfs


def purchased_power(ferc1_dbf_raw_dfs, ferc1_xbrl_raw_dfs, ferc1_transformed_dfs):
    """Transforms FERC Form 1 pumped storage data for loading into PUDL.

    This table has data about inter-utility power purchases into the PUDL DB. This
    includes how much electricty was purchased, how much it cost, and who it was
    purchased from. Unfortunately the field describing which other utility the power was
    being bought from is poorly standardized, making it difficult to correlate with
    other data. It will need to be categorized by hand or with some fuzzy matching
    eventually.

    Args:
        ferc1_raw_dfs (dict): Each entry in this dictionary of DataFrame objects
            corresponds to a table from the  FERC Form 1 DBC database.
        ferc1_transformed_dfs (dict): A dictionary of DataFrames to be transformed.

    Returns:
        dict: The dictionary of the transformed DataFrames.
    """
    # grab table from dictionary of dfs
    df = (
        _clean_cols(ferc1_dbf_raw_dfs["purchased_power_ferc1"], "f1_purchased_pwr")
        .rename(
            columns={
                "athrty_co_name": "seller_name",
                "sttstcl_clssfctn": "purchase_type_code",
                "rtsched_trffnbr": "tariff",
                "avgmth_bill_dmnd": "billing_demand_mw",
                "avgmth_ncp_dmnd": "non_coincident_peak_demand_mw",
                "avgmth_cp_dmnd": "coincident_peak_demand_mw",
                "mwh_purchased": "purchased_mwh",
                "mwh_recv": "received_mwh",
                "mwh_delvd": "delivered_mwh",
                "dmnd_charges": "demand_charges",
                "erg_charges": "energy_charges",
                "othr_charges": "other_charges",
                "settlement_tot": "total_settlement",
            }
        )
        .assign(  # Require these columns to numeric, or NaN
            billing_demand_mw=lambda x: pd.to_numeric(
                x.billing_demand_mw, errors="coerce"
            ),
            non_coincident_peak_demand_mw=lambda x: pd.to_numeric(
                x.non_coincident_peak_demand_mw, errors="coerce"
            ),
            coincident_peak_demand_mw=lambda x: pd.to_numeric(
                x.coincident_peak_demand_mw, errors="coerce"
            ),
        )
        .fillna(
            {  # Replace blanks w/ 0.0 in data columns.
                "purchased_mwh": 0.0,
                "received_mwh": 0.0,
                "delivered_mwh": 0.0,
                "demand_charges": 0.0,
                "energy_charges": 0.0,
                "other_charges": 0.0,
                "total_settlement": 0.0,
            }
        )
    )

    # Reencode the power purchase types:
    df = (
        pudl.metadata.classes.Package.from_resource_ids()
        .get_resource("purchased_power_ferc1")
        .encode(df)
    )

    # Drop records containing no useful data and also any completely duplicate
    # records -- there are 6 in 1998 for utility 238 for some reason...
    df = df.drop_duplicates().drop(
        df.loc[
            (
                (df.purchased_mwh == 0)
                & (df.received_mwh == 0)
                & (df.delivered_mwh == 0)
                & (df.demand_charges == 0)
                & (df.energy_charges == 0)
                & (df.other_charges == 0)
                & (df.total_settlement == 0)
            ),
            :,
        ].index
    )

    ferc1_transformed_dfs["purchased_power_ferc1"] = df

    return ferc1_transformed_dfs


def accumulated_depreciation(
    ferc1_dbf_raw_dfs, ferc1_xbrl_raw_dfs, ferc1_transformed_dfs
):
    """Transforms FERC Form 1 depreciation data for loading into PUDL.

    This information is organized by FERC account, with each line of the FERC Form 1
    having a different descriptive identifier like 'balance_end_of_year' or
    'transmission'.

    Args:
        ferc1_raw_dfs (dict): Each entry in this dictionary of DataFrame objects
            corresponds to a table from the FERC Form 1 DBC database.
        ferc1_transformed_dfs (dict): A dictionary of DataFrames to be transformed.

    Returns:
        dict: The dictionary of the transformed DataFrames.
    """
    # grab table from dictionary of dfs
    ferc1_apd_df = ferc1_dbf_raw_dfs["accumulated_depreciation_ferc1"]

    ferc1_acct_apd = FERC_DEPRECIATION_LINES.drop(["ferc_account_description"], axis=1)
    ferc1_acct_apd.dropna(inplace=True)
    ferc1_acct_apd["row_number"] = ferc1_acct_apd["row_number"].astype(int)

    ferc1_accumdepr_prvsn_df = pd.merge(
        ferc1_apd_df, ferc1_acct_apd, how="left", on="row_number"
    )
    ferc1_accumdepr_prvsn_df = _clean_cols(
        ferc1_accumdepr_prvsn_df, "f1_accumdepr_prvsn"
    )

    ferc1_accumdepr_prvsn_df.rename(
        columns={
            # FERC1 DB   PUDL DB
            "total_cde": "total"
        },
        inplace=True,
    )

    ferc1_transformed_dfs["accumulated_depreciation_ferc1"] = ferc1_accumdepr_prvsn_df

    return ferc1_transformed_dfs


def transform(
    ferc1_dbf_raw_dfs,
    ferc1_xbrl_raw_dfs,
    ferc1_settings: Ferc1Settings = Ferc1Settings(),
):
    """Transforms FERC 1.

    Args:
        ferc1_dbf_raw_dfs (dict): Dictionary pudl table names (keys) and raw DBF
            dataframes (values).
        ferc1_xbrl_raw_dfs (dict): Dictionary pudl table names with `_instant`
            or `_duration` (keys) and raw XRBL dataframes (values).
        ferc1_settings: Validated ETL parameters required by
            this data source.

    Returns:
        dict: A dictionary of the transformed DataFrames.

    """
    ferc1_tfr_classes = {
        # fuel must come before steam b/c fuel proportions are used to aid in
        # plant # ID assignment.
        "fuel_ferc1": FuelFerc1,
        # "plants_small_ferc1": plants_small,
        # "plants_hydro_ferc1": plants_hydro,
        # "plants_pumped_storage_ferc1": plants_pumped_storage,
        # "plant_in_service_ferc1": plant_in_service,
        # "purchased_power_ferc1": purchased_power,
        # "accumulated_depreciation_ferc1": accumulated_depreciation,
    }
    # create an empty ditctionary to fill up through the transform fuctions
    ferc1_transformed_dfs = {}

    # for each ferc table,
    for table in ferc1_tfr_classes:
        if table in ferc1_settings.tables:
            logger.info(
                f"Transforming raw FERC Form 1 dataframe for loading into {table}"
            )

            ferc1_transformed_dfs[table] = ferc1_tfr_classes[table](
                table_name=table
            ).execute(
                raw_dbf=ferc1_dbf_raw_dfs.get(table),
                raw_xbrl_instant=ferc1_xbrl_raw_dfs.get(table).get("instant", None),
                raw_xbrl_duration=ferc1_xbrl_raw_dfs.get(table).get("duration", None),
            )

    if "plants_steam_ferc1" in ferc1_settings.tables:
        ferc1_transformed_dfs["plants_steam_ferc1"] = PlantsSteamFerc1(
            table_name="plants_steam_ferc"
        ).execute(
            raw_dbf=ferc1_dbf_raw_dfs.get(table),
            raw_xbrl_instant=ferc1_xbrl_raw_dfs.get(table).get("instant", None),
            raw_xbrl_duration=ferc1_xbrl_raw_dfs.get(table).get("duration", None),
            transformed_fuel=ferc1_transformed_dfs["fuel_ferc1"],
        )

    # convert types and return:
    return {
        name: convert_cols_dtypes(df, data_source="ferc1")
        for name, df in ferc1_transformed_dfs.items()
    }


def transform_xbrl(ferc1_raw_dfs, ferc1_settings: Ferc1Settings = Ferc1Settings()):
    """Transforms FERC 1 XBRL data.

    Args:
        ferc1_raw_dfs (dict): Each entry in this dictionary of DataFrame objects
            corresponds to a table from the FERC Form 1 DBC database
        ferc1_settings: Validated ETL parameters required by
            this data source.

    Returns:
        dict: A dictionary of the transformed DataFrames.

    """
    pass


def fuel_by_plant_ferc1(fuel_df, thresh=0.5):
    """Calculates useful FERC Form 1 fuel metrics on a per plant-year basis.

    Each record in the FERC Form 1 corresponds to a particular type of fuel. Many plants
    -- especially coal plants -- use more than one fuel, with gas and/or diesel serving
    as startup fuels. In order to be able to classify the type of plant based on
    relative proportions of fuel consumed or fuel costs it is useful to aggregate these
    per-fuel records into a single record for each plant.

    Fuel cost (in nominal dollars) and fuel heat content (in mmBTU) are calculated for
    each fuel based on the cost and heat content per unit, and the number of units
    consumed, and then summed by fuel type (there can be more than one record for a
    given type of fuel in each plant because we are simplifying the fuel categories).
    The per-fuel records are then pivoted to create one column per fuel type. The total
    is summed and stored separately, and the individual fuel costs & heat contents are
    divided by that total, to yield fuel proportions.  Based on those proportions and a
    minimum threshold that's passed in, a "primary" fuel type is then assigned to the
    plant-year record and given a string label.

    Args:
        fuel_df (pandas.DataFrame): Pandas DataFrame resembling the post-transform
            result for the fuel_ferc1 table.
        thresh (float): A value between 0.5 and 1.0 indicating the minimum fraction of
            overall heat content that must have been provided by a fuel in a plant-year
            for it to be considered the "primary" fuel for the plant in that year.
            Default value: 0.5.

    Returns:
        pandas.DataFrame: A DataFrame with a single record for each plant-year,
        including the columns required to merge it with the plants_steam_ferc1
        table/DataFrame (report_year, utility_id_ferc1, and plant_name) as well as
        totals for fuel mmbtu consumed in that plant-year, and the cost of fuel in that
        year, the proportions of heat content and fuel costs for each fuel in that year,
        and a column that labels the plant's primary fuel for that year.

    Raises:
        AssertionError: If the DataFrame input does not have the columns required to
            run the function.

    """
    keep_cols = [
        "report_year",  # key
        "utility_id_ferc1",  # key
        "plant_name_ferc1",  # key
        "fuel_type_code_pudl",  # pivot
        "fuel_consumed_units",  # value
        "fuel_mmbtu_per_unit",  # value
        "fuel_cost_per_unit_burned",  # value
    ]

    # Ensure that the dataframe we've gotten has all the information we need:
    for col in keep_cols:
        if col not in fuel_df.columns:
            raise AssertionError(f"Required column {col} not found in input fuel_df.")

    # Calculate per-fuel derived values and add them to the DataFrame
    df = (
        # Really there should *not* be any duplicates here but... there's a
        # bug somewhere that introduces them into the fuel_ferc1 table.
        fuel_df[keep_cols]
        .drop_duplicates()
        # Calculate totals for each record based on per-unit values:
        .assign(fuel_mmbtu=lambda x: x.fuel_consumed_units * x.fuel_mmbtu_per_unit)
        .assign(fuel_cost=lambda x: x.fuel_consumed_units * x.fuel_cost_per_unit_burned)
        # Drop the ratios and heterogeneous fuel "units"
        .drop(
            ["fuel_mmbtu_per_unit", "fuel_cost_per_unit_burned", "fuel_consumed_units"],
            axis=1,
        )
        # Group by the keys and fuel type, and sum:
        .groupby(
            [
                "utility_id_ferc1",
                "plant_name_ferc1",
                "report_year",
                "fuel_type_code_pudl",
            ]
        )
        .agg(sum)
        .reset_index()
        # Set the index to the keys, and pivot to get per-fuel columns:
        .set_index(["utility_id_ferc1", "plant_name_ferc1", "report_year"])
        .pivot(columns="fuel_type_code_pudl")
        .fillna(0.0)
    )

    # Undo pivot. Could refactor this old function
    plant_year_totals = df.stack("fuel_type_code_pudl").groupby(level=[0, 1, 2]).sum()

    # Calculate total heat content burned for each plant, and divide it out
    mmbtu_group = (
        pd.merge(
            # Sum up all the fuel heat content, and divide the individual fuel
            # heat contents by it (they are all contained in single higher
            # level group of columns labeled fuel_mmbtu)
            df.loc[:, "fuel_mmbtu"].div(
                df.loc[:, "fuel_mmbtu"].sum(axis=1), axis="rows"
            ),
            # Merge that same total into the dataframe separately as well.
            plant_year_totals.loc[:, "fuel_mmbtu"],
            right_index=True,
            left_index=True,
        )
        .rename(columns=lambda x: re.sub(r"$", "_fraction_mmbtu", x))
        .rename(columns=lambda x: re.sub(r"_mmbtu_fraction_mmbtu$", "_mmbtu", x))
    )

    # Calculate total fuel cost for each plant, and divide it out
    cost_group = (
        pd.merge(
            # Sum up all the fuel costs, and divide the individual fuel
            # costs by it (they are all contained in single higher
            # level group of columns labeled fuel_cost)
            df.loc[:, "fuel_cost"].div(df.loc[:, "fuel_cost"].sum(axis=1), axis="rows"),
            # Merge that same total into the dataframe separately as well.
            plant_year_totals.loc[:, "fuel_cost"],
            right_index=True,
            left_index=True,
        )
        .rename(columns=lambda x: re.sub(r"$", "_fraction_cost", x))
        .rename(columns=lambda x: re.sub(r"_cost_fraction_cost$", "_cost", x))
    )

    # Re-unify the cost and heat content information:
    df = pd.merge(
        mmbtu_group, cost_group, left_index=True, right_index=True
    ).reset_index()

    # Label each plant-year record by primary fuel:
    for fuel_str in FUEL_TYPES:
        try:
            mmbtu_mask = df[f"{fuel_str}_fraction_mmbtu"] > thresh
            df.loc[mmbtu_mask, "primary_fuel_by_mmbtu"] = fuel_str
        except KeyError:
            pass

        try:
            cost_mask = df[f"{fuel_str}_fraction_cost"] > thresh
            df.loc[cost_mask, "primary_fuel_by_cost"] = fuel_str
        except KeyError:
            pass

    df[["primary_fuel_by_cost", "primary_fuel_by_mmbtu"]] = df[
        ["primary_fuel_by_cost", "primary_fuel_by_mmbtu"]
    ].fillna("")

    return df
