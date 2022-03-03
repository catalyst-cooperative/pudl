"""
Routines for transforming FERC Form 1 data before loading into the PUDL DB.

This module provides a variety of functions that are used in cleaning up the FERC Form 1
data prior to loading into our database. This includes adopting standardized units and
column names, standardizing the formatting of some string values, and correcting data
entry errors which we can infer based on the existing data. It may also include removing
bad data, or replacing it with the appropriate NA values.

"""
import importlib.resources
import logging
import re
from difflib import SequenceMatcher
from typing import Dict, List

# NetworkX is used to knit incomplete ferc plant time series together.
import networkx as nx
import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, ClassifierMixin
from sklearn.compose import ColumnTransformer
from sklearn.feature_extraction.text import TfidfVectorizer
# These modules are required for the FERC Form 1 Plant ID & Time Series
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import MinMaxScaler, Normalizer, OneHotEncoder

import pudl
from pudl.helpers import convert_cols_dtypes
from pudl.metadata.classes import DataSource
from pudl.metadata.dfs import FERC_DEPRECIATION_LINES
from pudl.settings import Ferc1Settings

logger = logging.getLogger(__name__)

##############################################################################
# Dicts for categorizing freeform strings ####################################
##############################################################################
FUEL_STRINGS: Dict[str, List[str]] = {
    "coal": [
        'coal', 'coal-subbit', 'lignite', 'coal(sb)', 'coal (sb)', 'coal-lignite',
        'coke', 'coa', 'lignite/coal', 'coal - subbit', 'coal-subb', 'coal-sub',
        'coal-lig', 'coal-sub bit', 'coals', 'ciak', 'petcoke', 'coal.oil', 'coal/gas',
        'bit coal', 'coal-unit #3', 'coal-subbitum', 'coal tons', 'coal mcf',
        'coal unit #3', 'pet. coke', 'coal-u3', 'coal&coke', 'tons',
    ],
    "oil": [
        'oil', '#6 oil', '#2 oil', 'fuel oil', 'jet', 'no. 2 oil', 'no.2 oil',
        'no.6& used', 'used oil', 'oil-2', 'oil (#2)', 'diesel oil',
        'residual oil', '# 2 oil', 'resid. oil', 'tall oil', 'oil/gas',
        'no.6 oil', 'oil-fuel', 'oil-diesel', 'oil / gas', 'oil bbls', 'oil bls',
        'no. 6 oil', '#1 kerosene', 'diesel', 'no. 2 oils', 'blend oil',
        '#2oil diesel', '#2 oil-diesel', '# 2  oil', 'light oil', 'heavy oil',
        'gas.oil', '#2', '2', '6', 'bbl', 'no 2 oil', 'no 6 oil', '#1 oil', '#6',
        'oil-kero', 'oil bbl', 'biofuel', 'no 2', 'kero', '#1 fuel oil',
        'no. 2  oil', 'blended oil', 'no 2. oil', '# 6 oil', 'nno. 2 oil',
        '#2 fuel', 'oill', 'oils', 'gas/oil', 'no.2 oil gas', '#2 fuel oil',
        'oli', 'oil (#6)', 'oil/diesel', '2 oil', '#6 hvy oil', 'jet fuel',
        'diesel/compos', 'oil-8', 'oil {6}', 'oil-unit #1', 'bbl.', 'oil.',  # noqa: FS003
        'oil #6', 'oil (6)', 'oil(#2)', 'oil-unit1&2', 'oil-6', '#2 fue oil',
        'dielel oil', 'dielsel oil', '#6 & used', 'barrels', 'oil un 1 & 2',
        'jet oil', 'oil-u1&2', 'oiul', 'pil', 'oil - 2', '#6 & used', 'oial',
        'diesel fuel', 'diesel/compo', 'oil (used)',

    ],
    "gas": [
        'gas', 'gass', 'methane', 'natural gas', 'blast gas', 'gas mcf',
        'propane', 'prop', 'natural  gas', 'nat.gas', 'nat gas',
        'nat. gas', 'natl gas', 'ga', 'gas`', 'syngas', 'ng', 'mcf',
        'blast gaa', 'nat  gas', 'gac', 'syngass', 'prop.', 'natural', 'coal.gas',
        'n. gas', 'lp gas', 'natuaral gas', 'coke gas', 'gas #2016', 'propane**',
        '* propane', 'propane **', 'gas expander', 'gas ct', '# 6 gas', '#6 gas',
        'coke oven gas'
    ],
    "solar": [],
    "wind": [],
    "hydro": [],
    "nuclear": [
        'nuclear', 'grams of uran', 'grams of', 'grams of  ura',
        'grams', 'nucleur', 'nulear', 'nucl', 'nucleart', 'nucelar',
        'gr.uranium', 'grams of urm', 'nuclear (9)', 'nulcear', 'nuc',
        'gr. uranium', 'nuclear mw da', 'grams of ura', 'nucvlear',
    ],
    "waste": [
        'tires', 'tire', 'refuse', 'switchgrass', 'wood waste', 'woodchips',
        'biomass', 'wood', 'wood chips', 'rdf', 'tires/refuse', 'tire refuse',
        'waste oil', 'waste', 'woodships', 'tire chips', 'tdf',
    ],
    "other": [
        'steam', 'purch steam', 'all', 'n/a', 'purch. steam', 'other',
        'composite', 'composit', 'mbtus', 'total', 'avg', 'avg.', 'blo',
        'all fuel', 'comb.', 'alt. fuels', 'na', 'comb', '/#=2\x80â\x91?',
        'kã\xadgv¸\x9d?', "mbtu's", 'gas, oil', 'rrm', '3\x9c', 'average',
        'furfural', '0', 'watson bng', 'toal', 'bng', '# 6 & used', 'combined',
        'blo bls', 'compsite', '*', 'compos.', 'gas / oil', 'mw days', 'g', 'c',
        'lime', 'all fuels', 'at right', '20', '1', 'comp oil/gas', 'all fuels to',
        'the right are', 'c omposite', 'all fuels are', 'total pr crk',
        'all fuels =', 'total pc', 'comp', 'alternative', 'alt. fuel', 'bio fuel',
        'total prairie', '', 'kã\xadgv¸?', 'm', 'waste heat', '/#=2â?', '3',
    ],
}
"""
A mapping a canonical fuel name to a list of strings which are used to represent that
fuel in the FERC Form 1 Reporting. Case is ignored, as all fuel strings are converted to
a lower case in the data set.
"""

FUEL_UNIT_STRINGS: Dict[str, List[str]] = {
    "ton": [
        'toms', 'taons', 'tones', 'col-tons', 'toncoaleq', 'coal', 'tons coal eq',
        'coal-tons', 'ton', 'tons', 'tons coal', 'coal-ton', 'tires-tons',
        'coal tons -2 ', 'oil-tons', 'coal tons 200', 'ton-2000', 'coal tons',
        'coal tons -2', 'coal-tone', 'tire-ton', 'tire-tons', 'ton coal eqv', 'tos',
        'coal tons - 2', 'c. t.', 'c.t.', 'toncoalequiv',
    ],
    "mcf": [
        'mcf', "mcf's", 'mcfs', 'mcf.', 'gas mcf', '"gas" mcf', 'gas-mcf',
        'mfc', 'mct', ' mcf', 'msfs', 'mlf', 'mscf', 'mci', 'mcl', 'mcg',
        'm.cu.ft.', 'kcf', '(mcf)', 'mcf *(4)', 'mcf00', 'm.cu.ft..', '1000 c.f',
    ],
    "bbl": [
        'barrel', 'bbls', 'bbl', 'barrels', 'bbrl', 'bbl.', 'bbls.', 'oil 42 gal',
        'oil-barrels', 'barrrels', 'bbl-42 gal', 'oil-barrel', 'bb.', 'barrells',
        'bar', 'bbld', 'oil- barrel', 'barrels    .', 'bbl .', 'barels', 'barrell',
        'berrels', 'bb', 'bbl.s', 'oil-bbl', 'bls', 'bbl:', 'barrles', 'blb',
        'propane-bbl', 'barriel', 'berriel', 'barrile', '(bbl.)', 'barrel *(4)',
        '(4) barrel', 'bbf', 'blb.', '(bbl)', 'bb1', 'bbsl', 'barrrel', 'barrels 100%',
        'bsrrels', "bbl's", '*barrels', 'oil - barrels', 'oil 42 gal ba', 'bll',
        'boiler barrel', 'gas barrel', '"boiler" barr', '"gas" barrel',
        '"boiler"barre', '"boiler barre', 'barrels .', 'bariel', 'brrels', 'oil barrel',
        'barreks', 'oil-bbls', 'oil-bbs',
    ],
    "gal": ['gallons', 'gal.', 'gals', 'gals.', 'gallon', 'gal', 'galllons'],
    "kgal": [
        'oil(1000 gal)', 'oil(1000)', 'oil (1000)', 'oil(1000', 'oil(1000ga)',
        '1000 gals', '1000 gal',
    ],
    "gramsU": [
        'gram', 'grams', 'gm u', 'grams u235', 'grams u-235', 'grams of uran',
        'grams: u-235', 'grams:u-235', 'grams:u235', 'grams u308', 'grams: u235',
        'grams of', 'grams - n/a', 'gms uran', 's e uo2 grams', 'gms uranium',
        'grams of urm', 'gms. of uran', 'grams (100%)', 'grams v-235', 'se uo2 grams',
        'grams u',
    ],
    "kgU": [
        'kg of uranium', 'kg uranium', 'kilg. u-235', 'kg u-235', 'kilograms-u23',
        'kg', 'kilograms u-2', 'kilograms', 'kg of', 'kg-u-235', 'kilgrams',
        'kilogr. u235', 'uranium kg', 'kg uranium25', 'kilogr. u-235',
        'kg uranium 25', 'kilgr. u-235', 'kguranium 25', 'kg-u235', 'kgm',
    ],
    "klbs": ['k lbs.', 'k lbs', '1000 / lbs', '1000 lbs', ],
    "mmbtu": [
        'mmbtu', 'mmbtus', 'mbtus', '(mmbtu)', "mmbtu's", 'nuclear-mmbtu',
        'nuclear-mmbt', 'mmbtul',
    ],
    "btu": ['btus', 'btu', ],
    "mwdth": [
        'mwd therman', 'mw days-therm', 'mwd thrml', 'mwd thermal',
        'mwd/mtu', 'mw days', 'mwdth', 'mwd', 'mw day', 'dth', 'mwdaysthermal',
        'mw day therml', 'mw days thrml', 'nuclear mwd', 'mmwd', 'mw day/therml'
        'mw days/therm', 'mw days (th', 'ermal)',
    ],
    "mwhth": [
        'mwh them', 'mwh threm', 'nwh therm', 'mwhth',
        'mwh therm', 'mwh', 'mwh therms.', 'mwh term.uts',
        'mwh thermal', 'mwh thermals', 'mw hr therm',
        'mwh therma', 'mwh therm.uts',
    ],
    "unknown": [
        '', '1265', 'mwh units', 'composite', 'therms', 'n/a', 'mbtu/kg', 'uranium 235',
        'oil', 'ccf', '2261', 'uo2', '(7)', 'oil #2', 'oil #6', '\x99å\x83\x90?"',
        'dekatherm', '0', 'mw day/therml', 'nuclear', 'gas', '62,679', 'mw days/therm',
        'na', 'uranium', 'oil/gas', 'thermal', '(thermal)', 'se uo2', '181679', '83',
        '3070', '248', '273976', '747', '-', 'are total', 'pr. creek', 'decatherms',
        'uramium', '.', 'total pr crk', '>>>>>>>>', 'all', 'total', 'alternative-t',
        'oil-mcf', '3303671', '929', '7182175', '319', '1490442', '10881', '1363663',
        '7171', '1726497', '4783', '7800', '12559', '2398', 'creek fuels',
        'propane-barre', '509', 'barrels/mcf', 'propane-bar', '4853325', '4069628',
        '1431536', '708903', 'mcf/oil (1000', '344', 'å?"', 'mcf / gallen',
    ],
}
"""
A dictionary linking fuel units (keys) to lists of various strings representing those
fuel units (values)
"""

PLANT_KIND_STRINGS: Dict[str, List[str]] = {
    "steam": [
        'coal', 'steam', 'steam units 1 2 3', 'steam units 4 5',
        'steam fossil', 'steam turbine', 'steam a', 'steam 100',
        'steam units 1 2 3', 'steams', 'steam 1', 'steam retired 2013', 'stream',
        'steam units 1,2,3', 'steam units 4&5', 'steam units 4&6',
        'steam conventional', 'unit total-steam', 'unit total steam',
        '*resp. share steam', 'resp. share steam', 'steam (see note 1,',
        'steam (see note 3)', 'mpc 50%share steam', '40% share steam'
        'steam (2)', 'steam (3)', 'steam (4)', 'steam (5)', 'steam (6)',
        'steam (7)', 'steam (8)', 'steam units 1 and 2', 'steam units 3 and 4',
        'steam (note 1)', 'steam (retired)', 'steam (leased)', 'coal-fired steam',
        'oil-fired steam', 'steam/fossil', 'steam (a,b)', 'steam (a)', 'stean',
        'steam-internal comb', 'steam (see notes)', 'steam units 4 & 6',
        'resp share stm note3', 'mpc50% share steam', 'mpc40%share steam',
        'steam - 64%', 'steam - 100%', 'steam (1) & (2)', 'resp share st note3',
        'mpc 50% shares steam', 'steam-64%', 'steam-100%', 'steam (see note 1)',
        'mpc 50% share steam', 'steam units 1, 2, 3', 'steam units 4, 5',
        'steam (2)', 'steam (1)', 'steam 4, 5', 'steam - 72%', 'steam (incl i.c.)',
        'steam- 72%', 'steam;retired - 2013', "respondent's sh.-st.",
        "respondent's sh-st", '40% share steam', 'resp share stm note3',
        'mpc50% share steam', 'resp share st note 3', '\x02steam (1)',
        'coal fired steam tur', 'steam- 64%',
    ],
    "combustion_turbine": [
        'combustion turbine', 'gt', 'gas turbine',
        'gas turbine # 1', 'gas turbine', 'gas turbine (note 1)',
        'gas turbines', 'simple cycle', 'combustion turbine',
        'comb.turb.peak.units', 'gas turbine', 'combustion turbine',
        'com turbine peaking', 'gas turbine peaking', 'comb turb peaking',
        'combustine turbine', 'comb. turine', 'conbustion turbine',
        'combustine turbine', 'gas turbine (leased)', 'combustion tubine',
        'gas turb', 'gas turbine peaker', 'gtg/gas', 'simple cycle turbine',
        'gas-turbine', 'gas turbine-simple', 'gas turbine - note 1',
        'gas turbine #1', 'simple cycle', 'gasturbine', 'combustionturbine',
        'gas turbine (2)', 'comb turb peak units', 'jet engine',
        'jet powered turbine', '*gas turbine', 'gas turb.(see note5)',
        'gas turb. (see note', 'combutsion turbine', 'combustion turbin',
        'gas turbine-unit 2', 'gas - turbine', 'comb turbine peaking',
        'gas expander turbine', 'jet turbine', 'gas turbin (lease',
        'gas turbine (leased', 'gas turbine/int. cm', 'comb.turb-gas oper.',
        'comb.turb.gas/oil op', 'comb.turb.oil oper.', 'jet', 'comb. turbine (a)',
        'gas turb.(see notes)', 'gas turb(see notes)', 'comb. turb-gas oper',
        'comb.turb.oil oper', 'gas turbin (leasd)', 'gas turbne/int comb',
        'gas turbine (note1)', 'combution turbin', '* gas turbine',
        'add to gas turbine', 'gas turbine (a)', 'gas turbinint comb',
        'gas turbine (note 3)', 'resp share gas note3', 'gas trubine',
        '*gas turbine(note3)', 'gas turbine note 3,6', 'gas turbine note 4,6',
        'gas turbine peakload', 'combusition turbine', 'gas turbine (lease)',
        'comb. turb-gas oper.', 'combution turbine', 'combusion turbine',
        'comb. turb. oil oper', 'combustion burbine', 'combustion and gas',
        'comb. turb.', 'gas turbine (lease', 'gas turbine (leasd)',
        'gas turbine/int comb', '*gas turbine(note 3)', 'gas turbine (see nos',
        'i.c.e./gas turbine', 'gas turbine/intcomb', 'cumbustion turbine',
        'gas turb, int. comb.', 'gas turb, diesel', 'gas turb, int. comb',
        'i.c.e/gas turbine', 'diesel turbine', 'comubstion turbine',
        'i.c.e. /gas turbine', 'i.c.e/ gas turbine', 'i.c.e./gas tubine',
        'gas turbine; retired',
    ],
    "combined_cycle": [
        'Combined cycle', 'combined cycle', 'combined', 'gas & steam turbine',
        'gas turb. & heat rec', 'combined cycle', 'com. cyc', 'com. cycle',
        'gas turb-combined cy', 'combined cycle ctg', 'combined cycle - 40%',
        'com cycle gas turb', 'combined cycle oper', 'gas turb/comb. cyc',
        'combine cycle', 'cc', 'comb. cycle', 'gas turb-combined cy',
        'steam and cc', 'steam cc', 'gas steam', 'ctg steam gas',
        'steam comb cycle', 'gas/steam comb. cycl', 'steam (comb. cycle)'
        'gas turbine/steam', 'steam & gas turbine', 'gas trb & heat rec',
        'steam & combined ce', 'st/gas turb comb cyc', 'gas tur & comb cycl',
        'combined cycle (a,b)', 'gas turbine/ steam', 'steam/gas turb.',
        'steam & comb cycle', 'gas/steam comb cycle', 'comb cycle (a,b)', 'igcc',
        'steam/gas turbine', 'gas turbine / steam', 'gas tur & comb cyc',
        'comb cyc (a) (b)', 'comb cycle', 'comb cyc', 'combined turbine',
        'combine cycle oper', 'comb cycle/steam tur', 'cc / gas turb',
        'steam (comb. cycle)', 'steam & cc', 'gas turbine/steam',
        'gas turb/cumbus cycl', 'gas turb/comb cycle', 'gasturb/comb cycle',
        'gas turb/cumb. cyc', 'igcc/gas turbine', 'gas / steam', 'ctg/steam-gas',
        'ctg/steam -gas', 'gas fired cc turbine', 'combinedcycle', 'comb cycle gas turb',
        'combined cycle opern', 'comb. cycle gas turb', 'ngcc',
    ],
    "nuclear": [
        'nuclear', 'nuclear (3)', 'steam(nuclear)', 'nuclear(see note4)'
        'nuclear steam', 'nuclear turbine', 'nuclear - steam',
        'nuclear (a)(b)(c)', 'nuclear (b)(c)', '* nuclear', 'nuclear (b) (c)',
        'nuclear (see notes)', 'steam (nuclear)', '* nuclear (note 2)',
        'nuclear (note 2)', 'nuclear (see note 2)', 'nuclear(see note4)',
        'nuclear steam', 'nuclear(see notes)', 'nuclear-steam',
        'nuclear (see note 3)'
    ],
    "geothermal": ['steam - geothermal', 'steam_geothermal', 'geothermal'],
    "internal_combustion": [
        'ic', 'internal combustion', 'internal comb.', 'internl combustion'
        'diesel turbine', 'int combust (note 1)', 'int. combust (note1)',
        'int.combustine', 'comb. cyc', 'internal comb', 'diesel', 'diesel engine',
        'internal combustion', 'int combust - note 1', 'int. combust - note1',
        'internal comb recip', 'reciprocating engine', 'comb. turbine',
        'internal combust.', 'int. combustion (1)', '*int combustion (1)',
        "*internal combust'n", 'internal', 'internal comb.', 'steam internal comb',
        'combustion', 'int. combustion', 'int combust (note1)', 'int. combustine',
        'internl combustion', '*int. combustion (1)', 'internal conbustion',
    ],
    "wind": [
        'wind', 'wind energy', 'wind turbine', 'wind - turbine', 'wind generation',
        'wind turbin',
    ],
    "photovoltaic": ['solar photovoltaic', 'photovoltaic', 'solar', 'solar project'],
    "solar_thermal": ['solar thermal'],
    "unknown": [
        '', 'n/a', 'see pgs 402.1-402.3', 'see pgs 403.1-403.9', "respondent's share",
        '--', '(see note 7)', 'other', 'not applicable', 'peach bottom', 'none.',
        'fuel facilities', '0', 'not in service', 'none', 'common expenses',
        'expenses common to', 'retired in 1981', 'retired in 1978', 'na',
        'unit total (note3)', 'unit total (note2)', 'resp. share (note2)',
        'resp. share (note8)', 'resp. share (note 9)', 'resp. share (note11)',
        'resp. share (note4)', 'resp. share (note6)', 'conventional',
        'expenses commom to', 'not in service in', 'unit total (note 3)',
        'unit total (note 2)', 'resp. share (note 8)', 'resp. share (note 3)',
        'resp. share note 11', 'resp. share (note 4)', 'resp. share (note 6)',
        '(see note 5)', 'resp. share (note 2)', 'package', '(left blank)', 'common',
        '0.0000', 'other generation', 'resp share (note 11)', 'retired',
        'storage/pipelines', 'sold april 16, 1999', 'sold may 07, 1999',
        'plants sold in 1999', 'gas', 'not applicable.', 'resp. share - note 2',
        'resp. share - note 8', 'resp. share - note 9', 'resp share - note 11',
        'resp. share - note 4', 'resp. share - note 6', 'plant retired- 2013',
        'retired - 2013', 'resp share - note 5', 'resp. share - note 7',
        'non-applicable', 'other generation plt', 'combined heat/power', 'oil'
    ],
}
"""
A mapping from canonical plant kinds (keys) to the associated freeform strings (values)
identified as being associated with that kind of plant in the FERC Form 1 raw data.
There are many strings that weren't categorized, Solar and Solar Project were not
classified as these do not indicate if they are solar thermal or photovoltaic. Variants
on Steam (e.g. "steam 72" and "steam and gas") were classified based on additional
research of the plants on the Internet.
"""

CONSTRUCTION_TYPE_STRINGS: Dict[str, List[str]] = {
    "outdoor": [
        'outdoor', 'outdoor boiler', 'full outdoor', 'outdoor boiler',
        'outdoor boilers', 'outboilers', 'fuel outdoor', 'full outdoor',
        'outdoors', 'outdoor', 'boiler outdoor& full', 'boiler outdoor&full',
        'outdoor boiler& full', 'full -outdoor', 'outdoor steam',
        'outdoor boiler', 'ob', 'outdoor automatic', 'outdoor repower',
        'full outdoor boiler', 'fo', 'outdoor boiler & ful', 'full-outdoor',
        'fuel outdoor', 'outoor', 'outdoor', 'outdoor  boiler&full',
        'boiler outdoor &full', 'outdoor boiler &full', 'boiler outdoor & ful',
        'outdoor-boiler', 'outdoor - boiler', 'outdoor const.',
        '4 outdoor boilers', '3 outdoor boilers', 'full outdoor', 'full outdoors',
        'full oudoors', 'outdoor (auto oper)', 'outside boiler',
        'outdoor boiler&full', 'outdoor hrsg', 'outdoor hrsg',
        'outdoor-steel encl.', 'boiler-outdr & full',
        'con.& full outdoor', 'partial outdoor', 'outdoor (auto. oper)',
        'outdoor (auto.oper)', 'outdoor construction', '1 outdoor boiler',
        '2 outdoor boilers', 'outdoor enclosure', '2 outoor boilers',
        'boiler outdr.& full', 'boiler outdr. & full', 'ful outdoor',
        'outdoor-steel enclos', 'outdoor (auto oper.)', 'con. & full outdoor',
        'outdore', 'boiler & full outdor', 'full & outdr boilers',
        'outodoor (auto oper)', 'outdoor steel encl.', 'full outoor',
        'boiler & outdoor ful', 'otdr. blr. & f. otdr', 'f.otdr & otdr.blr.',
        'oudoor (auto oper)', 'outdoor constructin', 'f. otdr. & otdr. blr',
        'outdoor boiler & fue', 'outdoor boiler &fuel',
    ],
    "semioutdoor": [
        'more than 50% outdoo', 'more than 50% outdos', 'over 50% outdoor',
        'over 50% outdoors', 'semi-outdoor', 'semi - outdoor', 'semi outdoor',
        'semi-enclosed', 'semi-outdoor boiler', 'semi outdoor boiler',
        'semi- outdoor', 'semi - outdoors', 'semi -outdoor'
        'conven & semi-outdr', 'conv & semi-outdoor', 'conv & semi- outdoor',
        'convent. semi-outdr', 'conv. semi outdoor', 'conv(u1)/semiod(u2)',
        'conv u1/semi-od u2', 'conv-one blr-semi-od', 'convent semioutdoor',
        'conv. u1/semi-od u2', 'conv - 1 blr semi od', 'conv. ui/semi-od u2',
        'conv-1 blr semi-od', 'conven. semi-outdoor', 'conv semi-outdoor',
        'u1-conv./u2-semi-od', 'u1-conv./u2-semi -od', 'convent. semi-outdoo',
        'u1-conv. / u2-semi', 'conven & semi-outdr', 'semi -outdoor',
        'outdr & conventnl', 'conven. full outdoor', 'conv. & outdoor blr',
        'conv. & outdoor blr.', 'conv. & outdoor boil', 'conv. & outdr boiler',
        'conv. & out. boiler', 'convntl,outdoor blr', 'outdoor & conv.',
        '2 conv., 1 out. boil', 'outdoor/conventional', 'conv. boiler outdoor',
        'conv-one boiler-outd', 'conventional outdoor', 'conventional outdor',
        'conv. outdoor boiler', 'conv.outdoor boiler', 'conventional outdr.',
        'conven,outdoorboiler', 'conven full outdoor', 'conven,full outdoor',
        '1 out boil, 2 conv', 'conv. & full outdoor', 'conv. & outdr. boilr',
        'conv outdoor boiler', 'convention. outdoor', 'conv. sem. outdoor',
        'convntl, outdoor blr', 'conv & outdoor boil', 'conv & outdoor boil.',
        'outdoor & conv', 'conv. broiler outdor', '1 out boilr, 2 conv',
        'conv.& outdoor boil.', 'conven,outdr.boiler', 'conven,outdr boiler',
        'outdoor & conventil', '1 out boilr 2 conv', 'conv & outdr. boilr',
        'conven, full outdoor', 'conven full outdr.', 'conven, full outdr.',
        'conv/outdoor boiler', "convnt'l outdr boilr", '1 out boil 2 conv',
        'conv full outdoor', 'conven, outdr boiler', 'conventional/outdoor',
        'conv&outdoor boiler', 'outdoor & convention', 'conv & outdoor boilr',
        'conv & full outdoor', 'convntl. outdoor blr', 'conv - ob',
        "1conv'l/2odboilers", "2conv'l/1odboiler", 'conv-ob', 'conv.-ob',
        '1 conv/ 2odboilers', '2 conv /1 odboilers', 'conv- ob', 'conv -ob',
        'con sem outdoor', 'cnvntl, outdr, boilr', 'less than 50% outdoo',
        'under 50% outdoor', 'under 50% outdoors', '1cnvntnl/2odboilers',
        '2cnvntnl1/1odboiler', 'con & ob', 'combination (b)', 'indoor & outdoor',
        'conven. blr. & full', 'conv. & otdr. blr.', 'combination',
        'indoor and outdoor', 'conven boiler & full', "2conv'l/10dboiler",
        '4 indor/outdr boiler', '4 indr/outdr boilerr', '4 indr/outdr boiler',
        'indoor & outdoof',
    ],
    "conventional": [
        'conventional', 'conventional', 'conventional boiler', 'conv-b',
        'conventionall', 'convention', 'conventional', 'coventional',
        'conven full boiler', 'c0nventional', 'conventtional', 'convential'
        'underground', 'conventional bulb', 'conventrional',
        '*conventional', 'convential', 'convetional', 'conventioanl',
        'conventioinal', 'conventaional', 'indoor construction', 'convenional',
        'conventional steam', 'conventinal', 'convntional', 'conventionl',
        'conventionsl', 'conventiional', 'convntl steam plants', 'indoor const.',
        'full indoor', 'indoor', 'indoor automatic', 'indoor boiler',
        '(peak load) indoor', 'conventionl,indoor', 'conventionl, indoor',
        'conventional, indoor', 'conventional;outdoor', 'conven./outdoor',
        'conventional;semi-ou', 'comb. cycle indoor', '3 indoor boiler',
        '2 indoor boilers', '1 indoor boiler', '2 indoor boiler',
        '3 indoor boilers', 'fully contained', 'conv - b', 'conventional/boiler',
        'cnventional', 'comb. cycle indooor', 'sonventional', 'ind enclosures',
        'conentional', 'conventional - boilr', 'indoor boiler and st',
    ],
    "unknown": [
        '', 'automatic operation', 'comb. turb. installn', 'comb. turb. instaln',
        'com. turb. installn', 'n/a', 'for detailed info.', 'for detailed info',
        'combined cycle', 'na', 'not applicable', 'gas', 'heated individually',
        'metal enclosure', 'pressurized water', 'nuclear', 'jet engine', 'gas turbine',
        'storage/pipelines', '0', 'during 1994', 'peaking - automatic',
        'gas turbine/int. cm', '2 oil/gas turbines', 'wind', 'package', 'mobile',
        'auto-operated', 'steam plants', 'other production', 'all nuclear plants',
        'other power gen.', 'automatically operad', 'automatically operd',
        'circ fluidized bed', 'jet turbine', 'gas turbne/int comb',
        'automatically oper.', 'retired 1/1/95', 'during 1995', '1996. plant sold',
        'reactivated 7/1/96', 'gas turbine/int comb', 'portable', 'head individually',
        'automatic opertion', 'peaking-automatic', 'cycle', 'full order',
        'circ. fluidized bed', 'gas turbine/intcomb', '0.0000', 'none', '2 oil / gas',
        'block & steel', 'and 2000', 'comb.turb. instaln', 'automatic oper.', 'pakage',
        '---', 'n/a (ct)', 'comb turb instain', 'ind encloures', '2 oil /gas turbines',
        'combustion turbine', '1970', 'gas/oil turbines', 'combined cycle steam', 'pwr',
        '2 oil/ gas', '2 oil / gas turbines', 'gas / oil turbines', 'no boiler',
        'internal combustion', 'gasturbine no boiler', 'boiler', 'tower -10 unit facy',
        'gas trubine', '4 gas/oil trubines', '2 oil/ 4 gas/oil tur',
        '5 gas/oil turbines', 'tower 16', '2 on 1 gas turbine', 'tower 23',
        'tower -10 unit', 'tower - 101 unit', '3 on 1 gas turbine', 'tower - 10 units',
        'tower - 165 units', 'wind turbine', 'fixed tilt pv', 'tracking pv', 'o',
        'wind trubine', 'subcritical', 'sucritical', 'simple cycle',
        'simple & reciprocat', 'solar', 'pre-fab power plant', 'prefab power plant',
        'prefab. power plant', 'pump storage', 'underground', 'see page 402',
        'conv. underground', 'conven. underground', 'conventional (a)', 'non-applicable',
    ],
}
"""
A dictionary of construction types (keys) and lists of construction type strings
associated with each type (values) from FERC Form 1.

There are many strings that weren't categorized, including crosses between conventional
and outdoor, PV, wind, combined cycle, and internal combustion. The lists are broken out
into the two types specified in Form 1: conventional and outdoor. These lists are
inclusive so that variants of conventional (e.g.  "conventional full") and outdoor (e.g.
"outdoor full" and "outdoor hrsg") are included.
"""

##############################################################################
# FERC TRANSFORM HELPER FUNCTIONS ############################################
##############################################################################


def unpack_table(ferc1_df, table_name, data_cols, data_rows):
    """
    Normalize a row-and-column based FERC Form 1 table.

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
                "pudl.package_data.ferc1.row_maps", f"{table_name}.csv"),
            index_col=0, comment="#")
        .copy().transpose()
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
    idx_cols = [
        "respondent_id",
        "report_year",
        "report_prd",
        "spplmnt_num",
        "row_name"
    ]
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
    """
    Turn top-level MultiIndex columns into a categorial column.

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
        tmp_df = df.loc[:, col].copy().dropna(how='all')
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
    for field in ['report_year', 'report_prd', 'respondent_id',
                  'spplmnt_num', 'row_number']:
        if field in df.columns:
            if df[field].isnull().any():
                raise AssertionError(
                    f"Null field {field} found in ferc1 table {table_name}."
                )

    # Create a unique inter-year FERC table record ID:
    df['record_id'] = (
        table_name + '_' +
        df.report_year.astype(str) + '_' +
        df.report_prd.astype(str) + '_' +
        df.respondent_id.astype(str) + '_' +
        df.spplmnt_num.astype(str)
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

    # Drop any _f columns... since we're not using the FERC Footnotes...
    # Drop columns and don't complain about it if they don't exist:
    no_f = [c for c in df.columns if not re.match(".*_f$", c)]
    df = (
        df.loc[:, no_f]
        .drop(['spplmnt_num', 'row_number', 'row_prvlg', 'row_seq',
               'report_prd', 'item', 'record_number'],
              errors='ignore', axis="columns")
        .rename(columns={"respondent_id": "utility_id_ferc1"})
    )
    return df


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
        records_to_fix = records_to_fix.apply(lambda x: x * mult
                                              if x > minval / mult
                                              and x < maxval / mult
                                              else x)
    # Set any record that wasn't inside one of our identified populations to
    # NA -- we are saying that these are true outliers, which can't be part
    # of the population of values we are examining.
    records_to_fix = records_to_fix.apply(lambda x: np.nan
                                          if x < minval
                                          or x > maxval
                                          else x)
    # Add our fixed records back to the complete data series and return it
    fixed = pd.concat([fixed, records_to_fix])
    return fixed


##############################################################################
# DATABASE TABLE SPECIFIC PROCEDURES ##########################################
##############################################################################
def plants_steam(ferc1_raw_dfs, ferc1_transformed_dfs):
    """Transforms FERC Form 1 plant_steam data for loading into PUDL Database.

    This includes converting to our preferred units of MWh and MW, as well as
    standardizing the strings describing the kind of plant and construction.

    Args:
        ferc1_raw_dfs (dict): Each entry in this dictionary of DataFrame objects
            corresponds to a table from the  FERC Form 1 DBC database.
        ferc1_transformed_dfs (dict): A dictionary of DataFrames to be transformed.

    Returns:
        dict: of transformed dataframes, including the newly transformed
        plants_steam_ferc1 dataframe.

    """
    ferc1_steam_df = (
        ferc1_raw_dfs['plants_steam_ferc1'].
        pipe(_plants_steam_clean).
        pipe(_plants_steam_assign_plant_ids,
             ferc1_transformed_dfs['fuel_ferc1'])
    )
    plants_steam_validate_ids(ferc1_steam_df)
    ferc1_steam_df = ferc1_steam_df.replace(
        {'construction_type': 'unknown', 'plant_type': 'unknown'}, pd.NA)
    ferc1_transformed_dfs['plants_steam_ferc1'] = ferc1_steam_df
    return ferc1_transformed_dfs


def _plants_steam_clean(ferc1_steam_df):
    ferc1_steam_df = (
        ferc1_steam_df.rename(columns={
            "plant_name": "plant_name_ferc1",
            "yr_const": 'construction_year',
            "plant_kind": 'plant_type',
            "type_const": 'construction_type',
            "asset_retire_cost": 'asset_retirement_cost',
            "yr_installed": 'installation_year',
            "tot_capacity": 'capacity_mw',
            "peak_demand": 'peak_demand_mw',
            "plant_hours": 'plant_hours_connected_while_generating',
            "plnt_capability": 'plant_capability_mw',
            "when_limited": 'water_limited_capacity_mw',
            "when_not_limited": 'not_water_limited_capacity_mw',
            "avg_num_of_emp": 'avg_num_employees',
            "net_generation": 'net_generation_kwh',
            "cost_land": 'capex_land',
            "cost_structure": 'capex_structures',
            "cost_equipment": 'capex_equipment',
            "cost_of_plant_to": 'capex_total',
            "cost_per_kw": 'capex_per_kw',
            "expns_operations": 'opex_operations',
            "expns_fuel": 'opex_fuel',
            "expns_coolants": 'opex_coolants',
            "expns_steam": 'opex_steam',
            "expns_steam_othr": 'opex_steam_other',
            "expns_transfer": 'opex_transfer',
            "expns_electric": 'opex_electric',
            "expns_misc_power": 'opex_misc_power',
            "expns_rents": 'opex_rents',
            "expns_allowances": 'opex_allowances',
            "expns_engnr": 'opex_engineering',
            "expns_structures": 'opex_structures',
            "expns_boiler": 'opex_boiler',
            "expns_plants": 'opex_plants',
            "expns_misc_steam": 'opex_misc_steam',
            "tot_prdctn_expns": 'opex_production_total',
            "expns_kwh": 'opex_per_kwh'})
        .pipe(_clean_cols, "f1_steam")
        .pipe(pudl.helpers.simplify_strings, ['plant_name_ferc1'])
        .pipe(pudl.helpers.cleanstrings, ['construction_type', 'plant_type'], [CONSTRUCTION_TYPE_STRINGS, PLANT_KIND_STRINGS], unmapped=pd.NA)
        .pipe(pudl.helpers.oob_to_nan,
              cols=["construction_year", "installation_year"],
              lb=1850, ub=max(DataSource.from_id("ferc1").working_partitions["years"]) + 1)
        .assign(
            capex_per_mw=lambda x: 1000.0 * x.capex_per_kw,
            opex_per_mwh=lambda x: 1000.0 * x.opex_per_kwh,
            net_generation_mwh=lambda x: x.net_generation_kwh / 1000.0,
        )
        .drop(columns=["capex_per_kw", "opex_per_kwh", "net_generation_kwh"])
    )

    for col in ['construction_type', 'plant_type']:
        if ferc1_steam_df[col].isnull().any():
            raise AssertionError(
                f"NA values found in {col} column during FERC 1 steam clean, add string to dictionary for this column"
            )
    return ferc1_steam_df


def _plants_steam_assign_plant_ids(ferc1_steam_df, ferc1_fuel_df):
    """Assign IDs to the large steam plants."""
    ###########################################################################
    # FERC PLANT ID ASSIGNMENT
    ###########################################################################
    # Now we need to assign IDs to the large steam plants, since FERC doesn't
    # do this for us.
    logger.info("Identifying distinct large FERC plants for ID assignment.")

    # scikit-learn still doesn't deal well with NA values (this will be fixed
    # eventually) We need to massage the type and missing data for the
    # Classifier to work.
    ferc1_steam_df = pudl.helpers.fix_int_na(
        ferc1_steam_df, columns=['construction_year'])

    # Grab fuel consumption proportions for use in assigning plant IDs:
    fuel_fractions = fuel_by_plant_ferc1(ferc1_fuel_df)
    ffc = list(fuel_fractions.filter(regex='.*_fraction_mmbtu$').columns)

    ferc1_steam_df = (
        ferc1_steam_df.merge(
            fuel_fractions[
                ['utility_id_ferc1', 'plant_name_ferc1', 'report_year'] + ffc],
            on=['utility_id_ferc1', 'plant_name_ferc1', 'report_year'],
            how='left'
        )
    )
    # We need to fill the null values for these numerical feature vectors with
    # zeros. not ideal, but the model requires dealing with nulls
    null_to_zero = ffc + ['capacity_mw']
    ferc1_steam_df[null_to_zero] = (
        ferc1_steam_df[null_to_zero].fillna(value=0.0))

    # Train the classifier using DEFAULT weights, parameters not listed here.
    ferc1_clf = pudl.transform.ferc1.make_ferc1_clf(ferc1_steam_df)
    ferc1_clf = ferc1_clf.fit_transform(ferc1_steam_df)

    # Use the classifier to generate groupings of similar records:
    record_groups = ferc1_clf.predict(ferc1_steam_df.record_id)
    n_tot = len(ferc1_steam_df)
    n_grp = len(record_groups)
    pct_grp = n_grp / n_tot
    logger.info(
        f"Successfully associated {n_grp} of {n_tot} ({pct_grp:.2%}) "
        f"FERC Form 1 plant records with multi-year plant entities.")

    record_groups.columns = record_groups.columns.astype(str)
    cols = record_groups.columns
    record_groups = record_groups.reset_index()

    # Now we are going to create a graph (network) that describes all of the
    # binary relationships between a seed_id and the record_ids that it has
    # been associated with in any other year. Each connected component of that
    # graph is a ferc plant time series / plant_id
    logger.info("Assigning IDs to multi-year FERC plant entities.")
    edges_df = pd.DataFrame(columns=['source', 'target'])
    for col in cols:
        new_edges = record_groups[['seed_id', col]]
        new_edges = new_edges.rename(
            {'seed_id': 'source', col: 'target'}, axis=1)
        edges_df = pd.concat([edges_df, new_edges], sort=True)

    # Drop any records where there's no target ID (no match in a year)
    edges_df = edges_df[edges_df.target != '']

    # We still have to deal with the orphaned records -- any record which
    # wasn't place in a time series but is still valid should be included as
    # its own independent "plant" for completeness, and use in aggregate
    # analysis.
    orphan_record_ids = np.setdiff1d(ferc1_steam_df.record_id.unique(),
                                     record_groups.values.flatten())
    logger.info(
        f"Identified {len(orphan_record_ids)} orphaned FERC plant records. "
        f"Adding orphans to list of plant entities.")
    orphan_df = pd.DataFrame({'source': orphan_record_ids,
                              'target': orphan_record_ids})
    edges_df = pd.concat([edges_df, orphan_df], sort=True)

    # Use the data frame we've compiled to create a graph
    G = nx.from_pandas_edgelist(edges_df,  # noqa: N806
                                source='source',
                                target='target')
    # Find the connected components of the graph
    ferc1_plants = (G.subgraph(c) for c in nx.connected_components(G))

    # Now we'll iterate through the connected components and assign each of
    # them a FERC Plant ID, and pull the results back out into a dataframe:
    plants_w_ids = []
    for plant_id_ferc1, plant in enumerate(ferc1_plants):
        nx.set_edge_attributes(plant,
                               plant_id_ferc1 + 1,
                               name='plant_id_ferc1')
        new_plant_df = nx.to_pandas_edgelist(plant)
        plants_w_ids.append(new_plant_df)
    plants_w_ids = pd.concat(plants_w_ids)
    logger.info(
        f"Successfully Identified {plant_id_ferc1+1-len(orphan_record_ids)} "
        f"multi-year plant entities.")

    # Set the construction year back to numeric because it is.
    ferc1_steam_df['construction_year'] = pd.to_numeric(
        ferc1_steam_df['construction_year'], errors='coerce')
    # We don't actually want to save the fuel fractions in this table... they
    # were only here to help us match up the plants.
    ferc1_steam_df = ferc1_steam_df.drop(ffc, axis=1)

    # Now we need a list of all the record IDs, with their associated
    # FERC 1 plant IDs. However, the source-target listing isn't
    # guaranteed to list every one of the nodes in either list, so we
    # need to compile them together to ensure that we get every single
    sources = (
        plants_w_ids.
        drop('target', axis=1).
        drop_duplicates().
        rename({'source': 'record_id'}, axis=1)
    )
    targets = (
        plants_w_ids.
        drop('source', axis=1).
        drop_duplicates().
        rename({'target': 'record_id'}, axis=1)
    )
    plants_w_ids = (
        pd.concat([sources, targets]).
        drop_duplicates().
        sort_values(['plant_id_ferc1', 'record_id'])
    )
    steam_rids = ferc1_steam_df.record_id.values
    pwids_rids = plants_w_ids.record_id.values
    missing_ids = [rid for rid in steam_rids if rid not in pwids_rids]
    if missing_ids:
        raise AssertionError(
            f"Uh oh, we lost {abs(len(steam_rids)-len(pwids_rids))} FERC "
            f"steam plant record IDs: {missing_ids}"
        )
    ferc1_steam_df = pd.merge(ferc1_steam_df, plants_w_ids, on='record_id')
    return ferc1_steam_df


def plants_steam_validate_ids(ferc1_steam_df):
    """Tests that plant_id_ferc1 times series includes one record per year.

    Args:
        ferc1_steam_df (pandas.DataFrame): A DataFrame of the data from the FERC 1
            Steam table.

    Returns:
        None
    """
    ##########################################################################
    # FERC PLANT ID ERROR CHECKING STUFF
    ##########################################################################

    # Test to make sure that we don't have any plant_id_ferc1 time series
    # which include more than one record from a given year. Warn the user
    # if we find such cases (which... we do, as of writing)
    year_dupes = (
        ferc1_steam_df.
        groupby(['plant_id_ferc1', 'report_year'])['utility_id_ferc1'].
        count().
        reset_index().
        rename(columns={'utility_id_ferc1': 'year_dupes'}).
        query('year_dupes>1')
    )
    if len(year_dupes) > 0:
        for dupe in year_dupes.itertuples():
            logger.error(
                f"Found report_year={dupe.report_year} "
                f"{dupe.year_dupes} times in "
                f"plant_id_ferc1={dupe.plant_id_ferc1}"
            )
    else:
        logger.info(
            "No duplicate years found in any plant_id_ferc1. Hooray!"
        )


def fuel(ferc1_raw_dfs, ferc1_transformed_dfs):
    """Transforms FERC Form 1 fuel data for loading into PUDL Database.

    This process includes converting some columns to be in terms of our preferred units,
    like MWh and mmbtu instead of kWh and btu. Plant names are also standardized
    (stripped & lower). Fuel and fuel unit strings are also standardized using our
    cleanstrings() function and string cleaning dictionaries found above (FUEL_STRINGS,
    etc.)

    Args:
        ferc1_raw_dfs (dict): Each entry in this dictionary of DataFrame objects
            corresponds to a table from the  FERC Form 1 DBC database.
        ferc1_transformed_dfs (dict): A dictionary of DataFrames to be transformed.

    Returns:
        dict: The dictionary of transformed dataframes.

    """
    # grab table from dictionary of dfs, clean it up a bit
    fuel_ferc1_df = (
        _clean_cols(ferc1_raw_dfs['fuel_ferc1'], 'f1_fuel').
        # Standardize plant_name capitalization and remove leading/trailing
        # white space -- necesary b/c plant_name is part of many foreign keys.
        pipe(pudl.helpers.simplify_strings, ['plant_name']).
        # Take the messy free-form fuel & fuel_unit fields, and do our best to
        # map them to some canonical categories... this is necessarily
        # imperfect:
        pipe(pudl.helpers.cleanstrings, ['fuel', 'fuel_unit'],
             [FUEL_STRINGS, FUEL_UNIT_STRINGS],
             unmapped=pd.NA).
        # Fuel cost per kWh is a per-unit value that doesn't make sense to
        # report for a single fuel that may be only a small part of the fuel
        # consumed. "fuel generaton" is heat rate, but as it's based only on
        # the heat content of a given fuel which may only be a small portion of
        # the overall fuel # consumption, it doesn't make any sense here. Drop
        # it.
        drop(['fuel_cost_kwh', 'fuel_generaton'], axis=1).
        # Convert from BTU/unit of fuel to 1e6 BTU/unit.
        assign(fuel_avg_mmbtu_per_unit=lambda x: x.fuel_avg_heat / 1e6).
        drop('fuel_avg_heat', axis=1).
        # Rename the columns to match our DB definitions
        rename(columns={
            # FERC 1 DB Name      PUDL DB Name
            "plant_name": "plant_name_ferc1",
            'fuel': 'fuel_type_code_pudl',
            'fuel_unit': 'fuel_units',
            'fuel_avg_mmbtu_per_unit': 'fuel_mmbtu_per_unit',
            'fuel_quantity': 'fuel_consumed_units',
            'fuel_cost_burned': 'fuel_cost_per_unit_burned',
            'fuel_cost_delvd': 'fuel_cost_per_unit_delivered',
            'fuel_cost_btu': 'fuel_cost_per_mmbtu'})
    )

    if fuel_ferc1_df['fuel_units'].isnull().any():
        raise AssertionError(
            "NA values found in fuel_units column during FERC 1 fuel clean, add string to dictionary"
        )

    #########################################################################
    # CORRECT DATA ENTRY ERRORS #############################################
    #########################################################################
    coal_mask = fuel_ferc1_df['fuel_type_code_pudl'] == 'coal'
    gas_mask = fuel_ferc1_df['fuel_type_code_pudl'] == 'gas'
    oil_mask = fuel_ferc1_df['fuel_type_code_pudl'] == 'oil'

    corrections = [
        # mult = 2000: reported in units of lbs instead of short tons
        # mult = 1e6:  reported BTUs instead of mmBTUs
        # minval and maxval of 10 and 29 mmBTUs are the range of values
        # specified by EIA 923 instructions at:
        # https://www.eia.gov/survey/form/eia_923/instructions.pdf
        ['fuel_mmbtu_per_unit', coal_mask, 10.0, 29.0, (2e3, 1e6)],

        # mult = 1e-2: reported cents/mmBTU instead of USD/mmBTU
        # minval and maxval of .5 and 7.5 dollars per mmBTUs are the
        # end points of the primary distribution of EIA 923 fuel receipts
        # and cost per mmBTU data weighted by quantity delivered
        ['fuel_cost_per_mmbtu', coal_mask, 0.5, 7.5, (1e-2, )],

        # mult = 1e3: reported fuel quantity in cubic feet, not mcf
        # mult = 1e6: reported fuel quantity in BTU, not mmBTU
        # minval and maxval of .8 and 1.2 mmBTUs are the range of values
        # specified by EIA 923 instructions
        ['fuel_mmbtu_per_unit', gas_mask, 0.8, 1.2, (1e3, 1e6)],

        # mult = 1e-2: reported in cents/mmBTU instead of USD/mmBTU
        # minval and maxval of 1 and 35 dollars per mmBTUs are the
        # end points of the primary distribution of EIA 923 fuel receipts
        # and cost per mmBTU data weighted by quantity delivered
        ['fuel_cost_per_mmbtu', gas_mask, 1, 35, (1e-2, )],

        # mult = 42: reported fuel quantity in gallons, not barrels
        # mult = 1e6: reported fuel quantity in BTU, not mmBTU
        # minval and maxval of 3 and 6.9 mmBTUs are the range of values
        # specified by EIA 923 instructions
        ['fuel_mmbtu_per_unit', oil_mask, 3, 6.9, (42, )],

        # mult = 1e-2: reported in cents/mmBTU instead of USD/mmBTU
        # minval and maxval of 5 and 33 dollars per mmBTUs are the
        # end points of the primary distribution of EIA 923 fuel receipts
        # and cost per mmBTU data weighted by quantity delivered
        ['fuel_cost_per_mmbtu', oil_mask, 5, 33, (1e-2, )]
    ]

    for (coltofix, mask, minval, maxval, mults) in corrections:
        fuel_ferc1_df[coltofix] = \
            _multiplicative_error_correction(fuel_ferc1_df[coltofix],
                                             mask, minval, maxval, mults)

    #########################################################################
    # REMOVE BAD DATA #######################################################
    #########################################################################
    # Drop any records that are missing data. This is a blunt instrument, to
    # be sure. In some cases we lose data here, because some utilities have
    # (for example) a "Total" line w/ only fuel_mmbtu_per_kwh on it. Grr.
    fuel_ferc1_df.dropna(inplace=True)

    # Replace "unkown" fuel unit with NAs - this comes after we drop missing data with NAs
    fuel_ferc1_df = fuel_ferc1_df.replace({'fuel_units': 'unknown'}, pd.NA)
    ferc1_transformed_dfs['fuel_ferc1'] = fuel_ferc1_df

    return ferc1_transformed_dfs


def plants_small(ferc1_raw_dfs, ferc1_transformed_dfs):
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
    ferc1_small_df = ferc1_raw_dfs['plants_small_ferc1']
    # Standardize plant_name_raw capitalization and remove leading/trailing
    # white space -- necesary b/c plant_name_raw is part of many foreign keys.
    ferc1_small_df = pudl.helpers.simplify_strings(
        ferc1_small_df, ['plant_name', 'kind_of_fuel']
    )

    # Force the construction and installation years to be numeric values, and
    # set them to NA if they can't be converted. (table has some junk values)
    ferc1_small_df = pudl.helpers.oob_to_nan(
        ferc1_small_df, cols=["yr_constructed"],
        lb=1850, ub=max(DataSource.from_id("ferc1").working_partitions["years"]) + 1)

    # Convert from cents per mmbtu to dollars per mmbtu to be consistent
    # with the f1_fuel table data. Also, let's use a clearer name.
    ferc1_small_df['fuel_cost_per_mmbtu'] = ferc1_small_df['fuel_cost'] / 100.0
    ferc1_small_df.drop('fuel_cost', axis=1, inplace=True)

    # Create a single "record number" for the individual lines in the FERC
    # Form 1 that report different small plants, so that we can more easily
    # tell whether they are adjacent to each other in the reporting.
    ferc1_small_df['record_number'] = 46 * ferc1_small_df['spplmnt_num'] + \
        ferc1_small_df['row_number']

    # Unforunately the plant types were not able to be parsed automatically
    # in this table. It's been done manually for 2004-2015, and the results
    # get merged in in the following section.
    small_types_file = importlib.resources.open_binary(
        'pudl.package_data.ferc1', 'small_plants_2004-2016.xlsx')
    small_types_df = pd.read_excel(small_types_file)

    # Only rows with plant_type set will give us novel information.
    small_types_df.dropna(subset=['plant_type', ], inplace=True)
    # We only need this small subset of the columns to extract the plant type.
    small_types_df = small_types_df[['report_year', 'respondent_id',
                                     'record_number', 'plant_name_clean',
                                     'plant_type', 'ferc_license']]

    # Munge the two dataframes together, keeping everything from the
    # frame we pulled out of the FERC1 DB, and supplementing it with the
    # plant_name, plant_type, and ferc_license fields from our hand
    # made file.
    ferc1_small_df = pd.merge(ferc1_small_df,
                              small_types_df,
                              how='left',
                              on=['report_year',
                                  'respondent_id',
                                  'record_number'])

    # Remove extraneous columns and add a record ID
    ferc1_small_df = _clean_cols(ferc1_small_df, 'f1_gnrt_plant')

    # Standardize plant_name capitalization and remove leading/trailing white
    # space, so that plant_name matches formatting of plant_name_raw
    ferc1_small_df = pudl.helpers.simplify_strings(
        ferc1_small_df, ['plant_name_clean'])

    # in order to create one complete column of plant names, we have to use the
    # cleaned plant names when available and the orignial plant names when the
    # cleaned version is not available, but the strings first need cleaning
    ferc1_small_df['plant_name_clean'] = ferc1_small_df['plant_name_clean'].fillna(
        value="")
    ferc1_small_df['plant_name_clean'] = ferc1_small_df.apply(
        lambda row: row['plant_name']
        if (row['plant_name_clean'] == "")
        else row['plant_name_clean'],
        axis=1)

    # now we don't need the uncleaned version anymore
    # ferc1_small_df.drop(['plant_name'], axis=1, inplace=True)

    ferc1_small_df.rename(columns={
        # FERC 1 DB Name      PUDL DB Name
        'plant_name': 'plant_name_ferc1',
        'ferc_license': 'ferc_license_id',
        'yr_constructed': 'construction_year',
        'capacity_rating': 'capacity_mw',
        'net_demand': 'peak_demand_mw',
        'net_generation': 'net_generation_mwh',
        'plant_cost': 'total_cost_of_plant',
        'plant_cost_mw': 'capex_per_mw',
        'operation': 'opex_total',
        'expns_fuel': 'opex_fuel',
        'expns_maint': 'opex_maintenance',
        'kind_of_fuel': 'fuel_type',
        'fuel_cost': 'fuel_cost_per_mmbtu'},
        inplace=True)

    ferc1_transformed_dfs['plants_small_ferc1'] = ferc1_small_df
    return ferc1_transformed_dfs


def plants_hydro(ferc1_raw_dfs, ferc1_transformed_dfs):
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
        _clean_cols(ferc1_raw_dfs['plants_hydro_ferc1'], 'f1_hydro')
        # Standardize plant_name capitalization and remove leading/trailing
        # white space -- necesary b/c plant_name is part of many foreign keys.
        .pipe(pudl.helpers.simplify_strings, ['plant_name'])
        .pipe(pudl.helpers.cleanstrings, ['plant_const'],
              [CONSTRUCTION_TYPE_STRINGS], unmapped=pd.NA)
        .assign(
            # Converting kWh to MWh
            net_generation_mwh=lambda x: x.net_generation / 1000.0,
            # Converting cost per kW installed to cost per MW installed:
            cost_per_mw=lambda x: x.cost_per_kw * 1000.0,
            # Converting kWh to MWh
            expns_per_mwh=lambda x: x.expns_kwh * 1000.0)
        .pipe(pudl.helpers.oob_to_nan, cols=["yr_const", "yr_installed"],
              lb=1850, ub=max(DataSource.from_id("ferc1").working_partitions["years"]) + 1)
        .drop(columns=['net_generation', 'cost_per_kw', 'expns_kwh'])
        .rename(columns={
            # FERC1 DB          PUDL DB
            "plant_name": "plant_name_ferc1",
            'project_no': 'project_num',
            'yr_const': 'construction_year',
            'plant_kind': 'plant_type',
            'plant_const': 'construction_type',
            'yr_installed': 'installation_year',
            'tot_capacity': 'capacity_mw',
            'peak_demand': 'peak_demand_mw',
            'plant_hours': 'plant_hours_connected_while_generating',
            'favorable_cond': 'net_capacity_favorable_conditions_mw',
            'adverse_cond': 'net_capacity_adverse_conditions_mw',
            'avg_num_of_emp': 'avg_num_employees',
            'cost_of_land': 'capex_land',
            'cost_structure': 'capex_structures',
            'cost_facilities': 'capex_facilities',
            'cost_equipment': 'capex_equipment',
            'cost_roads': 'capex_roads',
            'cost_plant_total': 'capex_total',
            'cost_per_mw': 'capex_per_mw',
            'expns_operations': 'opex_operations',
            'expns_water_pwr': 'opex_water_for_power',
            'expns_hydraulic': 'opex_hydraulic',
            'expns_electric': 'opex_electric',
            'expns_generation': 'opex_generation_misc',
            'expns_rents': 'opex_rents',
            'expns_engineering': 'opex_engineering',
            'expns_structures': 'opex_structures',
            'expns_dams': 'opex_dams',
            'expns_plant': 'opex_plant',
            'expns_misc_plant': 'opex_misc_plant',
            'expns_per_mwh': 'opex_per_mwh',
            'expns_engnr': 'opex_engineering',
            'expns_total': 'opex_total',
            'asset_retire_cost': 'asset_retirement_cost',
            '': '',
        })
        .drop_duplicates(
            subset=["report_year",
                    "utility_id_ferc1",
                    "plant_name_ferc1",
                    "capacity_mw"],
            keep=False)
    )
    if ferc1_hydro_df['construction_type'].isnull().any():
        raise AssertionError(
            "NA values found in construction_type column during FERC1 hydro clean, add string to CONSTRUCTION_TYPE_STRINGS"
        )
    ferc1_hydro_df = ferc1_hydro_df.replace({'construction_type': 'unknown'}, pd.NA)
    ferc1_transformed_dfs['plants_hydro_ferc1'] = ferc1_hydro_df
    return ferc1_transformed_dfs


def plants_pumped_storage(ferc1_raw_dfs, ferc1_transformed_dfs):
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
            ferc1_raw_dfs['plants_pumped_storage_ferc1'], 'f1_pumped_storage')
        # Standardize plant_name capitalization and remove leading/trailing
        # white space -- necesary b/c plant_name is part of many foreign keys.
        .pipe(pudl.helpers.simplify_strings, ['plant_name'])
        # Clean up the messy plant construction type column:
        .pipe(pudl.helpers.cleanstrings, ['plant_kind'],
              [CONSTRUCTION_TYPE_STRINGS], unmapped=pd.NA)
        .assign(
            # Converting from kW/kWh to MW/MWh
            net_generation_mwh=lambda x: x.net_generation / 1000.0,
            energy_used_for_pumping_mwh=lambda x: x.energy_used / 1000.0,
            net_load_mwh=lambda x: x.net_load / 1000.0,
            cost_per_mw=lambda x: x.cost_per_kw * 1000.0,
            expns_per_mwh=lambda x: x.expns_kwh * 1000.0)
        .pipe(pudl.helpers.oob_to_nan, cols=["yr_const", "yr_installed"],
              lb=1850, ub=max(DataSource.from_id("ferc1").working_partitions["years"]) + 1)
        .drop(columns=['net_generation', 'energy_used', 'net_load',
                       'cost_per_kw', 'expns_kwh'])
        .rename(columns={
            # FERC1 DB          PUDL DB
            "plant_name": "plant_name_ferc1",
            'project_number': 'project_num',
            'tot_capacity': 'capacity_mw',
            'project_no': 'project_num',
            'plant_kind': 'construction_type',
            'peak_demand': 'peak_demand_mw',
            'yr_const': 'construction_year',
            'yr_installed': 'installation_year',
            'plant_hours': 'plant_hours_connected_while_generating',
            'plant_capability': 'plant_capability_mw',
            'avg_num_of_emp': 'avg_num_employees',
            'cost_wheels': 'capex_wheels_turbines_generators',
            'cost_land': 'capex_land',
            'cost_structures': 'capex_structures',
            'cost_facilties': 'capex_facilities',
            'cost_wheels_turbines_generators': 'capex_wheels_turbines_generators',
            'cost_electric': 'capex_equipment_electric',
            'cost_misc_eqpmnt': 'capex_equipment_misc',
            'cost_roads': 'capex_roads',
            'asset_retire_cost': 'asset_retirement_cost',
            'cost_of_plant': 'capex_total',
            'cost_per_mw': 'capex_per_mw',
            'expns_operations': 'opex_operations',
            'expns_water_pwr': 'opex_water_for_power',
            'expns_pump_strg': 'opex_pumped_storage',
            'expns_electric': 'opex_electric',
            'expns_misc_power': 'opex_generation_misc',
            'expns_rents': 'opex_rents',
            'expns_engneering': 'opex_engineering',
            'expns_structures': 'opex_structures',
            'expns_dams': 'opex_dams',
            'expns_plant': 'opex_plant',
            'expns_misc_plnt': 'opex_misc_plant',
            'expns_producton': 'opex_production_before_pumping',
            'pumping_expenses': 'opex_pumping',
            'tot_prdctn_exns': 'opex_total',
            'expns_per_mwh': 'opex_per_mwh',
        })
        .drop_duplicates(
            subset=["report_year",
                    "utility_id_ferc1",
                    "plant_name_ferc1",
                    "capacity_mw"],
            keep=False)
    )
    if ferc1_pump_df['construction_type'].isnull().any():
        raise AssertionError(
            "NA values found in construction_type column during FERC 1 pumped storage clean, add string to CONSTRUCTION_TYPE_STRINGS"
        )
    ferc1_pump_df = ferc1_pump_df.replace({'construction_type': 'unknown'}, pd.NA)
    ferc1_transformed_dfs['plants_pumped_storage_ferc1'] = ferc1_pump_df
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
                "yr_end_bal"
            ])
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
            })
        .rename_axis(columns=None)
        .pipe(_clean_cols, "f1_plant_in_srvce")
        .set_index([
            "utility_id_ferc1",
            "report_year",
            "amount_type",
            "record_id"])
        .reset_index()
    )

    # Get rid of the columns corresponding to "header" rows in the FERC
    # form, which should *never* contain data... but in about 2 dozen cases,
    # they do. See this issue on Github for more information:
    # https://github.com/catalyst-cooperative/pudl/issues/471
    pis_df = pis_df.drop(columns=pis_df.filter(regex=".*_head$").columns)

    ferc1_transformed_dfs["plant_in_service_ferc1"] = pis_df
    return ferc1_transformed_dfs


def purchased_power(ferc1_raw_dfs, ferc1_transformed_dfs):
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
        _clean_cols(ferc1_raw_dfs['purchased_power_ferc1'], 'f1_purchased_pwr')
        .rename(columns={
            'athrty_co_name': 'seller_name',
            'sttstcl_clssfctn': 'purchase_type_code',
            'rtsched_trffnbr': 'tariff',
            'avgmth_bill_dmnd': 'billing_demand_mw',
            'avgmth_ncp_dmnd': 'non_coincident_peak_demand_mw',
            'avgmth_cp_dmnd': 'coincident_peak_demand_mw',
            'mwh_purchased': 'purchased_mwh',
            'mwh_recv': 'received_mwh',
            'mwh_delvd': 'delivered_mwh',
            'dmnd_charges': 'demand_charges',
            'erg_charges': 'energy_charges',
            'othr_charges': 'other_charges',
            'settlement_tot': 'total_settlement'})
        .assign(  # Require these columns to numeric, or NaN
            billing_demand_mw=lambda x: pd.to_numeric(
                x.billing_demand_mw, errors="coerce"),
            non_coincident_peak_demand_mw=lambda x: pd.to_numeric(
                x.non_coincident_peak_demand_mw, errors="coerce"),
            coincident_peak_demand_mw=lambda x: pd.to_numeric(
                x.coincident_peak_demand_mw, errors="coerce"))
        .fillna({  # Replace blanks w/ 0.0 in data columns.
            "purchased_mwh": 0.0,
            "received_mwh": 0.0,
            "delivered_mwh": 0.0,
            "demand_charges": 0.0,
            "energy_charges": 0.0,
            "other_charges": 0.0,
            "total_settlement": 0.0,
        })
    )

    # Reencode the power purchase types:
    df = (
        pudl.metadata.classes.Package.from_resource_ids()
        .get_resource("purchased_power_ferc1")
        .encode(df)
    )

    # Drop records containing no useful data and also any completely duplicate
    # records -- there are 6 in 1998 for utility 238 for some reason...
    df = (
        df.drop_duplicates()
        .drop(df.loc[((df.purchased_mwh == 0)
                      & (df.received_mwh == 0)
                      & (df.delivered_mwh == 0)
                      & (df.demand_charges == 0)
                      & (df.energy_charges == 0)
                      & (df.other_charges == 0)
                      & (df.total_settlement == 0)), :].index)
    )

    ferc1_transformed_dfs['purchased_power_ferc1'] = df

    return ferc1_transformed_dfs


def accumulated_depreciation(ferc1_raw_dfs, ferc1_transformed_dfs):
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
    ferc1_apd_df = ferc1_raw_dfs['accumulated_depreciation_ferc1']

    ferc1_acct_apd = FERC_DEPRECIATION_LINES.drop(
        ['ferc_account_description'], axis=1)
    ferc1_acct_apd.dropna(inplace=True)
    ferc1_acct_apd['row_number'] = ferc1_acct_apd['row_number'].astype(int)

    ferc1_accumdepr_prvsn_df = pd.merge(ferc1_apd_df, ferc1_acct_apd,
                                        how='left', on='row_number')
    ferc1_accumdepr_prvsn_df = _clean_cols(
        ferc1_accumdepr_prvsn_df, 'f1_accumdepr_prvsn')

    ferc1_accumdepr_prvsn_df.rename(columns={
        # FERC1 DB   PUDL DB
        'total_cde': 'total'},
        inplace=True)

    ferc1_transformed_dfs['accumulated_depreciation_ferc1'] = ferc1_accumdepr_prvsn_df

    return ferc1_transformed_dfs


def transform(ferc1_raw_dfs, ferc1_settings: Ferc1Settings = Ferc1Settings()):
    """Transforms FERC 1.

    Args:
        ferc1_raw_dfs (dict): Each entry in this dictionary of DataFrame objects
            corresponds to a table from the FERC Form 1 DBC database
        ferc1_settings: Validated ETL parameters required by
            this data source.

    Returns:
        dict: A dictionary of the transformed DataFrames.

    """
    ferc1_tfr_funcs = {
        # fuel must come before steam b/c fuel proportions are used to aid in
        # plant # ID assignment.
        'fuel_ferc1': fuel,
        'plants_steam_ferc1': plants_steam,
        'plants_small_ferc1': plants_small,
        'plants_hydro_ferc1': plants_hydro,
        'plants_pumped_storage_ferc1': plants_pumped_storage,
        'plant_in_service_ferc1': plant_in_service,
        'purchased_power_ferc1': purchased_power,
        'accumulated_depreciation_ferc1': accumulated_depreciation
    }
    # create an empty ditctionary to fill up through the transform fuctions
    ferc1_tfr_dfs = {}

    # for each ferc table,
    for table in ferc1_tfr_funcs:
        if table in ferc1_settings.tables:
            logger.info(
                f"Transforming raw FERC Form 1 dataframe for "
                f"loading into {table}")
            ferc1_tfr_funcs[table](ferc1_raw_dfs, ferc1_tfr_dfs)

    # convert types and return:
    return {
        name: convert_cols_dtypes(df, data_source="ferc1")
        for name, df in ferc1_tfr_dfs.items()
    }

###############################################################################
# Identifying FERC Plants
###############################################################################
# Sadly FERC doesn't provide any kind of real IDs for the plants that report to
# them -- all we have is their names (a freeform string) and the data that is
# reported alongside them. This is often enough information to be able to
# recognize which records ought to be associated with each other year to year
# to create a continuous time series. However, we want to do that
# programmatically, which means using some clustering / categorization tools
# from scikit-learn


class FERCPlantClassifier(BaseEstimator, ClassifierMixin):
    """A classifier for identifying FERC plant time series in FERC Form 1 data.

    We want to be able to give the classifier a FERC plant record, and get back the
    group of records(or the ID of the group of records) that it ought to be part of.

    There are hundreds of different groups of records, and we can only know what they
    are by looking at the whole dataset ahead of time. This is the "fitting" step, in
    which the groups of records resulting from a particular set of model parameters(e.g.
    the weights that are attributes of the class) are generated.

    Once we have that set of record categories, we can test how well the classifier
    performs, by checking it against test / training data which we have already
    classified by hand. The test / training set is a list of lists of unique FERC plant
    record IDs(each record ID is the concatenation of: report year, respondent id,
    supplement number, and row number). It could also be stored as a dataframe where
    each column is associated with a year of data(some of which could be empty). Not
    sure what the best structure would be.

    If it's useful, we can assign each group a unique ID that is the time ordered
    concatenation of each of the constituent record IDs. Need to understand what the
    process for checking the classification of an input record looks like.

    To score a given classifier, we can look at what proportion of the records in the
    test dataset are assigned to the same group as in our manual classification of those
    records. There are much more complicated ways to do the scoring too... but for now
    let's just keep it as simple as possible.

    """

    def __init__(self, min_sim=0.75, plants_df=None):
        """
        Initialize the classifier.

        Args:
            min_sim : Number between 0.0 and 1.0, indicating the minimum value of
                cosine similarity that we are willing to accept as indicating two
                records are part of the same plant record time series. All entries in
                the pairwise similarity matrix below this value will be zeroed out.
            plants_df : The entire FERC Form 1 plant table as a dataframe. Needed in
                order to calculate the distance metrics between all of the records so we
                can group the plants in the fit() step, so we can check how well they
                are categorized later...

        Todo:
            Zane revisit plants_df

        """
        self.min_sim = min_sim
        self.plants_df = plants_df
        self._years = self.plants_df.report_year.unique()

    def fit(self, X, y=None):  # noqa: N803 Canonical capital letter...
        """
        Use weighted FERC plant features to group records into time series.

        The fit method takes the vectorized, normalized, weighted FERC plant
        features (X) as input, calculates the pairwise cosine similarity matrix
        between all records, and groups the records in their best time series.
        The similarity matrix and best time series are stored as data members
        in the object for later use in scoring & predicting.

        This isn't quite the way a fit method would normally work.

        Args:
            X (): a sparse matrix of size n_samples x n_features.
            y ():

        Returns:
            pandas.DataFrame:

        TODO:
            Zane revisit args and returns
        """
        self._cossim_df = pd.DataFrame(cosine_similarity(X))
        self._best_of = self._best_by_year()
        # Make the best match indices integers rather than floats w/ NA values.
        self._best_of[self._years] = self._best_of[self._years].fillna(
            -1).astype(int)

        return self

    def transform(self, X, y=None):  # noqa: N803
        """Passthrough transform method -- just returns self."""
        return self

    def predict(self, X, y=None):  # noqa: N803
        """
        Identify time series of similar records to input record_ids.

        Given a one-dimensional dataframe X, containing FERC record IDs, return
        a dataframe in which each row corresponds to one of the input record_id
        values (ordered as the input was ordered), with each column
        corresponding to one of the years worth of data. Values in the returned
        dataframe are the FERC record_ids of the record most similar to the
        input record within that year. Some of them may be null, if there was
        no sufficiently good match.

        Row index is the seed record IDs. Column index is years.

        TODO:
        * This method is hideously inefficient. It should be vectorized.
        * There's a line that throws a FutureWarning that needs to be fixed.

        """
        try:
            getattr(self, "_cossim_df")
        except AttributeError:
            raise RuntimeError(
                "You must train classifer before predicting data!")

        tmp_best = pd.concat([
            self._best_of.loc[:, ["record_id"] + list(self._years)],
            pd.DataFrame(data=[""], index=[-1], columns=["record_id"])
        ])
        out_dfs = []
        # For each record_id we've been given:
        for x in X:
            # Find the index associated with the record ID we are predicting
            # a grouping for:
            idx = tmp_best[tmp_best.record_id == x].index.values[0]

            # Mask the best_of dataframe, keeping only those entries where
            # the index of the chosen record_id appears -- this results in a
            # huge dataframe almost full of NaN values.
            w_m = (
                tmp_best[self._years][tmp_best[self._years] == idx]
                # Grab the index values of the rows in the masked dataframe which
                # are NOT all NaN -- these are the indices of the *other* records
                # which found the record x to be one of their best matches.
                .dropna(how="all").index.values
            )

            # Now look up the indices of the records which were found to be
            # best matches to the record x.
            b_m = tmp_best.loc[idx, self._years].astype(int)

            # Here we require that there is no conflict between the two sets
            # of indices -- that every time a record shows up in a grouping,
            # that grouping is either the same, or a subset of the other
            # groupings that it appears in. When no sufficiently good match
            # is found the "index" in the _best_of array is set to -1, so
            # requiring that the b_m value be >=0 screens out those no-match
            # cases. This is okay -- we're just trying to require that the
            # groupings be internally self-consistent, not that they are
            # completely identical. Being flexible on this dramatically
            # increases the number of records that get assigned a plant ID.
            if np.array_equiv(w_m, b_m[b_m >= 0].values):
                # This line is causing a warning. In cases where there are
                # some years no sufficiently good match exists, and so b_m
                # doesn't contain an index. Instead, it has a -1 sentinel
                # value, which isn't a label for which a record exists, which
                # upsets .loc. Need to find some way around this... but for
                # now it does what we want. We could use .iloc instead, but
                # then the -1 sentinel value maps to the last entry in the
                # dataframe, which also isn't what we want.  Blargh.
                new_grp = tmp_best.loc[b_m, "record_id"]

                # Stack the new list of record_ids on our output DataFrame:
                out_dfs.append(
                    pd.DataFrame(
                        data=new_grp.values.reshape(1, len(self._years)),
                        index=pd.Index(
                            [tmp_best.loc[idx, "record_id"]],
                            name="seed_id"),
                        columns=self._years
                    )
                )
        return pd.concat(out_dfs)

    def score(self, X, y=None):  # noqa: N803
        """Scores a collection of FERC plant categorizations.

        For every record ID in X, predict its record group and calculate
        a metric of similarity between the prediction and the "ground
        truth" group that was passed in for that value of X.

        Args:
            X (pandas.DataFrame): an n_samples x 1 pandas dataframe of FERC
                Form 1 record IDs.
            y (pandas.DataFrame): a dataframe of "ground truth" FERC Form 1
                record groups, corresponding to the list record IDs in X

        Returns:
            numpy.ndarray: The average of all the similarity metrics as the
            score.
        """
        scores = []
        for true_group in y:
            true_group = str.split(true_group, sep=',')
            true_group = [s for s in true_group if s != '']
            predicted_groups = self.predict(pd.DataFrame(true_group))
            for rec_id in true_group:
                sm = SequenceMatcher(None, true_group,
                                     predicted_groups.loc[rec_id])
                scores = scores + [sm.ratio()]

        return np.mean(scores)

    def _best_by_year(self):
        """Finds the best match for each plant record in each other year."""
        # only keep similarity matrix entries above our minimum threshold:
        out_df = self.plants_df.copy()
        sim_df = self._cossim_df[self._cossim_df >= self.min_sim]

        # Add a column for each of the years, in which we will store indices
        # of the records which best match the record in question:
        for yr in self._years:
            newcol = yr
            out_df[newcol] = -1

        # seed_yr is the year we are matching *from* -- we do the entire
        # matching process from each year, since it may not be symmetric:
        for seed_yr in self._years:
            seed_idx = self.plants_df.index[
                self.plants_df.report_year == seed_yr]
            # match_yr is all the other years, in which we are finding the best
            # match
            for match_yr in self._years:
                best_of_yr = match_yr
                match_idx = self.plants_df.index[
                    self.plants_df.report_year == match_yr]
                # For each record specified by seed_idx, obtain the index of
                # the record within match_idx that that is the most similar.
                best_idx = sim_df.iloc[seed_idx, match_idx].idxmax(axis=1)
                out_df.iloc[seed_idx,
                            out_df.columns.get_loc(best_of_yr)] = best_idx

        return out_df


def make_ferc1_clf(plants_df,
                   ngram_min=2,
                   ngram_max=10,
                   min_sim=0.75,
                   plant_name_ferc1_wt=2.0,
                   plant_type_wt=2.0,
                   construction_type_wt=1.0,
                   capacity_mw_wt=1.0,
                   construction_year_wt=1.0,
                   utility_id_ferc1_wt=1.0,
                   fuel_fraction_wt=1.0):
    """
    Create a FERC Plant Classifier using several weighted features.

    Given a FERC steam plants dataframe plants_df, which also includes fuel consumption
    information, transform a selection of useful columns into features suitable for use
    in calculating inter-record cosine similarities. Individual features are weighted
    according to the keyword arguments.

    Features include:

      * plant_name (via TF-IDF, with ngram_min and ngram_max as parameters)
      * plant_type (OneHot encoded categorical feature)
      * construction_type (OneHot encoded categorical feature)
      * capacity_mw (MinMax scaled numerical feature)
      * construction year (OneHot encoded categorical feature)
      * utility_id_ferc1 (OneHot encoded categorical feature)
      * fuel_fraction_mmbtu (several MinMax scaled numerical columns, which are
        normalized and treated as a single feature.)

    This feature matrix is then used to instantiate a FERCPlantClassifier.

    The combination of the ColumnTransformer and FERCPlantClassifier are combined in a
    sklearn Pipeline, which is returned by the function.

    Arguments:
        ngram_min (int): the minimum lengths to consider in the vectorization of the
            plant_name feature.
        ngram_max (int): the maximum n-gram lengths to consider in the vectorization of
            the plant_name feature.
        min_sim (float): the minimum cosine similarity between two records that can be
            considered a "match" (a number between 0.0 and 1.0).
        plant_name_ferc1_wt (float): weight used to determine the relative importance
            of each of the features in the feature matrix used to calculate the cosine
            similarity between records. Used to scale each individual feature before the
            vectors are normalized.
        plant_type_wt (float): weight used to determine the relative importance of each
            of the features in the feature matrix used to calculate the cosine
            similarity between records. Used to scale each individual feature before the
            vectors are normalized.
        construction_type_wt (float): weight used to determine the relative importance
            of each of the features in the feature matrix used to calculate the cosine
            similarity between records. Used to scale each individual feature before the
            vectors are normalized.
        capacity_mw_wt (float):weight used to determine the relative importance of each
            of the features in the feature matrix used to calculate the cosine
            similarity between records. Used to scale each individual feature before the
            vectors are normalized.
        construction_year_wt (float): weight used to determine the relative importance
            of each of the features in the feature matrix used to calculate the cosine
            similarity between records. Used to scale each individual feature before the
            vectors are normalized.
        utility_id_ferc1_wt (float): weight used to determine the relative importance
            of each of the features in the feature matrix used to calculate the cosine
            similarity between records. Used to scale each individual feature before the
            vectors are normalized.
        fuel_fraction_wt (float): weight used to determine the relative importance of
            each of the features in the feature matrix used to calculate the cosine
            similarity between records. Used to scale each individual feature before the
            vectors are normalized.

    Returns:
        sklearn.pipeline.Pipeline: an sklearn Pipeline that performs reprocessing and
        classification with a FERCPlantClassifier object.

    """
    # Make a list of all the fuel fraction columns for use as one feature.
    fuel_cols = list(plants_df.filter(regex='.*_fraction_mmbtu$').columns)

    ferc1_pipe = Pipeline([
        ('preprocessor', ColumnTransformer(
            transformers=[
                ('plant_name_ferc1', TfidfVectorizer(
                    analyzer='char',
                    ngram_range=(ngram_min, ngram_max)),
                 'plant_name_ferc1'),
                ('plant_type', OneHotEncoder(
                    categories='auto'), ['plant_type']),
                ('construction_type', OneHotEncoder(
                    categories='auto'), ['construction_type']),
                ('capacity_mw', MinMaxScaler(), ['capacity_mw']),
                ('construction_year', OneHotEncoder(
                    categories='auto'), ['construction_year']),
                ('utility_id_ferc1', OneHotEncoder(
                    categories='auto'), ['utility_id_ferc1']),
                ('fuel_fraction_mmbtu', Pipeline([
                    ('scaler', MinMaxScaler()),
                    ('norm', Normalizer())
                ]), fuel_cols),
            ],

            transformer_weights={
                'plant_name_ferc1': plant_name_ferc1_wt,
                'plant_type': plant_type_wt,
                'construction_type': construction_type_wt,
                'capacity_mw': capacity_mw_wt,
                'construction_year': construction_year_wt,
                'utility_id_ferc1': utility_id_ferc1_wt,
                'fuel_fraction_mmbtu': fuel_fraction_wt,
            })
         ),
        ('classifier', pudl.transform.ferc1.FERCPlantClassifier(
            min_sim=min_sim, plants_df=plants_df))
    ])
    return ferc1_pipe


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
        'report_year',  # key
        'utility_id_ferc1',  # key
        'plant_name_ferc1',  # key
        'fuel_type_code_pudl',  # pivot
        'fuel_consumed_units',  # value
        'fuel_mmbtu_per_unit',  # value
        'fuel_cost_per_unit_burned',  # value
    ]

    # Ensure that the dataframe we've gotten has all the information we need:
    for col in keep_cols:
        if col not in fuel_df.columns:
            raise AssertionError(
                f"Required column {col} not found in input fuel_df."
            )

    # Calculate per-fuel derived values and add them to the DataFrame
    df = (
        # Really there should *not* be any duplicates here but... there's a
        # bug somewhere that introduces them into the fuel_ferc1 table.
        fuel_df[keep_cols].drop_duplicates().
        # Calculate totals for each record based on per-unit values:
        assign(fuel_mmbtu=lambda x: x.fuel_consumed_units * x.fuel_mmbtu_per_unit).
        assign(fuel_cost=lambda x: x.fuel_consumed_units * x.fuel_cost_per_unit_burned).
        # Drop the ratios and heterogeneous fuel "units"
        drop(['fuel_mmbtu_per_unit', 'fuel_cost_per_unit_burned', 'fuel_consumed_units'], axis=1).
        # Group by the keys and fuel type, and sum:
        groupby(['utility_id_ferc1', 'plant_name_ferc1', 'report_year', 'fuel_type_code_pudl']).
        agg(sum).reset_index().
        # Set the index to the keys, and pivot to get per-fuel columns:
        set_index(['utility_id_ferc1', 'plant_name_ferc1', 'report_year']).
        pivot(columns='fuel_type_code_pudl').fillna(0.0)
    )

    # undo pivot. Could refactor this old function but out of scope for now (fixing a pandas API deprecation)
    plant_year_totals = df.stack('fuel_type_code_pudl').groupby(level=[0, 1, 2]).sum()

    # Calculate total heat content burned for each plant, and divide it out
    mmbtu_group = (
        pd.merge(
            # Sum up all the fuel heat content, and divide the individual fuel
            # heat contents by it (they are all contained in single higher
            # level group of columns labeled fuel_mmbtu)
            df.loc[:, 'fuel_mmbtu'].div(
                df.loc[:, 'fuel_mmbtu'].sum(axis=1), axis='rows'),
            # Merge that same total into the dataframe separately as well.
            plant_year_totals.loc[:, 'fuel_mmbtu'],
            right_index=True, left_index=True).
        rename(columns=lambda x: re.sub(r'$', '_fraction_mmbtu', x)).
        rename(columns=lambda x: re.sub(r'_mmbtu_fraction_mmbtu$', '_mmbtu', x))
    )

    # Calculate total fuel cost for each plant, and divide it out
    cost_group = (
        pd.merge(
            # Sum up all the fuel costs, and divide the individual fuel
            # costs by it (they are all contained in single higher
            # level group of columns labeled fuel_cost)
            df.loc[:, 'fuel_cost'].div(
                df.loc[:, 'fuel_cost'].sum(axis=1), axis='rows'),
            # Merge that same total into the dataframe separately as well.
            plant_year_totals.loc[:, 'fuel_cost'],
            right_index=True, left_index=True).
        rename(columns=lambda x: re.sub(r'$', '_fraction_cost', x)).
        rename(columns=lambda x: re.sub(r'_cost_fraction_cost$', '_cost', x))
    )

    # Re-unify the cost and heat content information:
    df = pd.merge(mmbtu_group, cost_group,
                  left_index=True, right_index=True).reset_index()

    # Label each plant-year record by primary fuel:
    for fuel_str in FUEL_STRINGS:
        try:
            mmbtu_mask = df[f'{fuel_str}_fraction_mmbtu'] > thresh
            df.loc[mmbtu_mask, 'primary_fuel_by_mmbtu'] = fuel_str
        except KeyError:
            pass

        try:
            cost_mask = df[f'{fuel_str}_fraction_cost'] > thresh
            df.loc[cost_mask, 'primary_fuel_by_cost'] = fuel_str
        except KeyError:
            pass

    df[['primary_fuel_by_cost', 'primary_fuel_by_mmbtu']] = df[[
        'primary_fuel_by_cost', 'primary_fuel_by_mmbtu']].fillna('')

    return df
