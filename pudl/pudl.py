import numpy as np
import pandas as pd
from pudl import eiaf923
from pudl import fercf1

# This is the top level PUDL (Public Utility Data Liberation) module. It
# uses helper functions & object definitions to create high level objects
# describing individual plants, utilities, states, mines, etc. based on
# a variety of public datasets.
#
# Each public data provider and form/source has its own collection of helper
# functions & objects, which are used by this higher level module.
# e.g. fercf1.py, eiaf923.py...

class ElecUtil(object):
    """An object describing an electric utility operating in the US.
    
    The ElecUtil object is populated with publicly reported data collected from
    FERC, EIA, EPA and state Public Utility Commission proceedings.

    Most internal data is stored using pandas Data Frames, allowing it to
    reflect various different data types (numerical, categorical, geographic,
    strings, dates, etc).

    Data members within the class are named with a prefix that indicates where
    the data originally came from. Each of these is a pandas DataFrame object.

    f1 => FERC Form 1
    -----------------
     - f1_ancil_svcs:
       - Ancillary services purchased and sold.
       - p398, F1_398_ANCL_PS.DBF

     - f1_cwip:
       - Construction work in progress.
       - p216, F1_70.DBF

     - f1_fuel:
       - Large thermal plant fuel consumption.
       - p402-403b, lines 36-44, F1_31.DBF

     - f1_hydro:
       - Hydroelectric plant information.
       - p406-407, F1_86.DBF

     - f1_plant_in_svc:
       - FERC account values & changes for various asset categories.
       - p202-207, F1_52.DBF

     - f1_pump_stor:
       - Data about pumped hydro storage facilities.
       - p408-409, F1_53.DBF

     - f1_purch_pwr:
       - Purchased power.
       - p326-327, F1_54.DBF

     - f1_small_plant:
       - Details of smaller generation facilities.
       - p410-411, F1_33.DBF

     - f1_steam:
       - Large thermal plant details by plant.
       - p402-403a, lines 1-35, F1_89.DBF

     - f1_trans_add:
       - Annual transmission line additions.
       - p424-425, F1_71.DBF

    f714 => FERC Form 714
    eia923 => EIA Form 923
    eia860 => EIA Form 860

    """

    def __init__(self, f1_id=145, eia923_id=None, years=[2014,]):
        """Construct an ElecUtil object based on publicly available data.
        
        Inputs come from our archive of original data, for the years in the
        list/array named years.  For now the data being ingested is somewhat
        limited, but it should grow over time, as we learn how to make more of
        it useful.
        """
        # Create several internal dataframes, which will aggregate data from
        # different original sources, grouped by year.
    
        # For testing, we will use: * f1_id=145 (Public Service Company of
        # Colorado) * years = [2014,] (so we can compare against Xcel's FERC
        # Form 1 data)

        # FERC Form 1 fuel information by plant, from the yearly F1_31.DBF files.
        # This data was originally reported on FERC Form 1, page 402, lines 36-44
        self.f1_fuel = fercf1.get_f1_fuel(years=years, util_ids=[f1_id,])

        # Steam (generation) information by plant, from the yearly
        # F1_89.DBF files. This data was originally reported on FERC Form 1, page
        # 402, lines 1-35.
        self.f1_steam        = fercf1.get_f1_steam(years=years, util_ids=[f1_id,])

        # There are some special summary rows within the plant in service data:
        self.f1_plant_in_svc = fercf1.get_f1_plant_in_svc(years=years, util_ids=[f1_id,])

        self.f1_cwip         = fercf1.get_f1_cwip(years=years, util_ids=[f1_id,])
        self.f1_purch_pwr    = fercf1.get_f1_purch_pwr(years=years, util_ids=[f1_id,])
        self.f1_small_plant  = fercf1.get_f1_small_plant(years=years, util_ids=[f1_id,])
        self.f1_ancil_svcs   = fercf1.get_f1_ancil_svcs(years=years, util_ids=[f1_id,])
        self.f1_trans_add    = fercf1.get_f1_trans_add(years=years, util_ids=[f1_id,])
        self.f1_pump_stor    = fercf1.get_f1_pump_stor(years=years, util_ids=[f1_id,])
        self.f1_hydro        = fercf1.get_f1_hydro(years=years, util_ids=[f1_id,])

        # EIA Form 923 fuel consumption data by unit... haven't worked out how
        # to ingest this data just yet...
        # self.eia923_fuel = eiaf923.get_eia293_fuel(years=years, util_ids=(eia923_id,))
