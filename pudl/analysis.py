"""A module with functions to aid in data analysis using the PUDL database."""

# Useful high-level external modules.
import numpy as np
import pandas as pd
import sqlalchemy as sa
import matplotlib.pyplot as plt

# Our own code...
from pudl import pudl, ferc1, eia923, settings, constants
from pudl import models, models_ferc1, models_eia923
from pudl import clean_eia923, clean_ferc1, clean_pudl


def eia_operator_plants(operator_id, pudl_engine):
    """Return all the EIA plant IDs associated with a given EIA operator ID."""
    Session = sa.orm.sessionmaker()
    Session.configure(bind=pudl_engine)
    session = Session()
    pudl_plant_ids = [p.plant_id for p in session.query(models.UtilityEIA923).
                      filter_by(operator_id=operator_id).
                      first().util_pudl.plants]
    eia923_plant_ids = [p.plant_id for p in
                        session.query(models.PlantEIA923).
                        filter(models.
                               PlantEIA923.
                               plant_id_pudl.
                               in_(pudl_plant_ids))]
    session.close_all()
    return(eia923_plant_ids)


def ferc1_expns_corr(pudl_engine, capacity_factor=0.6):
    """
    Calculate generation vs. expense correlation for FERC Form 1 plants.

    This function helped us identify which of the expns_* fields in the FERC
    Form 1 dataset represent production costs, and which are non-production
    costs, for the purposes of modeling marginal cost of electricity from
    various plants.  We expect the difference in expenses vs. generation to
    be more indicative of production vs. non-production costs for plants with
    higher capacity factors, and since what we're trying to do here is
    identify which *fields* in the FERC Form 1 data are production costs, we
    allow a capacity_factor threshold to be set -- analysis is only done for
    those plants with capacity factors larger than the threshold.

    Additionaly, some types of plants simply do not have some types of
    expenses, so to keep those plants from dragging down otherwise meaningful
    correlations, any zero expense values are dropped before calculating the
    correlations.

    Returns a dictionary with expns_ field names as the keys, and correlations
    as the values.
    """
    steam_df = pd.read_sql('SELECT * FROM plants_steam_ferc1', pudl_engine)
    steam_df['capacity_factor'] = \
        (steam_df['net_generation_mwh'] / 8760 * steam_df['total_capacity_mw'])

    # Limit plants by capacity factor
    steam_df = steam_df[steam_df['capacity_factor'] > capacity_factor]
    expns_corr = {}
    for expns in steam_df.filter(regex='expns').columns.tolist():
        mwh_plants = steam_df.net_generation_mwh[steam_df[expns] != 0]
        expns_plants = steam_df[expns][steam_df[expns] != 0]
        expns_corr[expns] = np.corrcoef(mwh_plants, expns_plants)[0, 1]

    return(expns_corr)


def mcoe_by_plant(plant_id, pudl_engine, years=range(2007, 2016)):
    """
    Extract data relevant to the calculation of a power plant's MCOE.

    Given a plant ID (PUDL or EIA), return several data series relevant to the
    calculation of the plant's marginal cost of electricity (MCOE) including:

    Index & plant specific constants...
    - Nameplate Capacity
    - Summer Capacity
    - PUDL Plant ID
    - PUDL Utility ID
    - Plant Name
    - Year

    Yearly data by plant:
    - Total electricity Generated (MWh)
    - Capacity factor
    - Gross energy consumed per unit of net generation (mmBTU/MWh)
    - Fuel cost per unit net generation ($/MWh)
    - Non-fuel production costs (aka Variable O&M? $/MWh)
    - Non-production costs (aka Fixed O&M? $/MWh)

    Final Output we're building toward:
    - Marginal cost of electricity (MCOE) the final output ($/MWh)

    Once we have it working by plant (esp. for FERC data) then we need to
    take the data that we have from EIA, and calculate or estimate the same
    values on a per-generator basis.

    Given a plant PUDL id
     - get a list of all corresponding FERC plants
       -

    """
    # For testing purposes right now...
    plant_id_pudl = 122  # Comanche's PUDL plant_id
    pudl_tables = models.PUDLBase.metadata.tables
    respondents_ferc1_tbl = pudl_tables['utilities_ferc1']
    plants_ferc1_tbl = pudl_tables['plants_ferc1']
    fuel_ferc1_tbl = pudl_tables['fuel_ferc1']
    steam_ferc1_tbl = pudl_tables['plants_steam_ferc1']

    # We need to pull the fuel information separately, because it has several
    # entries for each plant for each year -- we'll groupby() plant before
    # merging it with the steam plant info
    fuel_ferc1_select = sa.sql.select([
        fuel_ferc1_tbl.c.report_year,
        respondents_ferc1_tbl.c.respondent_id,
        respondents_ferc1_tbl.c.util_id_pudl,
        respondents_ferc1_tbl.c.respondent_name,
        plants_ferc1_tbl.c.plant_id_pudl,
        fuel_ferc1_tbl.c.plant_name,
        fuel_ferc1_tbl.c.fuel,
        fuel_ferc1_tbl.c.fuel_qty_burned,
        fuel_ferc1_tbl.c.fuel_avg_mmbtu_per_unit,
        fuel_ferc1_tbl.c.fuel_cost_per_unit_burned,
        fuel_ferc1_tbl.c.fuel_cost_per_unit_delivered,
        fuel_ferc1_tbl.c.fuel_cost_per_mmbtu,
        fuel_ferc1_tbl.c.fuel_cost_per_mwh,
        fuel_ferc1_tbl.c.fuel_mmbtu_per_mwh]).\
        where(sa.sql.and_(
            respondents_ferc1_tbl.c.respondent_id == respondent_id_ferc1,
            fuel_ferc1_tbl.c.respondent_id == respondent_id_ferc1,
            plants_ferc1_tbl.c.respondent_id == respondent_id_ferc1,
            plants_ferc1_tbl.c.plant_name == fuel_ferc1_tbl.c.plant_name))

    fuel_df = pd.read_sql(fuel_ferc1_select, pudl_engine)

    # Add some columns with totals so we can sum things up...
    fuel_df['fuel_burned_mmbtu_total'] = \
        fuel_df['fuel_qty_burned'] * fuel_df['fuel_avg_mmbtu_per_unit']
    fuel_df['fuel_burned_cost_total'] = \
        fuel_df['fuel_qty_burned'] * fuel_df['fuel_cost_per_unit_burned']

    fuel_merge = fuel_df[['report_year', 'plant_id_pudl', 'plant_name']]
    fuel_merge = fuel_merge.drop_duplicates(
        subset=['report_year', 'plant_id_pudl'])

    gb_plant_yr = fuel_df.groupby(['plant_id_pudl', 'report_year'])

    # Create single column data frames with year and plant as the index,
    # and the field summed up by plant that we're trying to bring into our
    # output data frame...
    mmbtu_sum = pd.DataFrame(gb_plant_yr['fuel_burned_mmbtu_total'].sum())
    cost_sum = pd.DataFrame(gb_plant_yr['fuel_burned_cost_total'].sum())

    # Merge the total heat and total cost into our output dataframe
    fuel_merge = fuel_merge.merge(mmbtu_sum,
                                  left_on=['plant_id_pudl', 'report_year'],
                                  right_index=True)
    fuel_merge = fuel_merge.merge(cost_sum,
                                  left_on=['plant_id_pudl', 'report_year'],
                                  right_index=True)

    # Pull relevant cost/expense data from the FERC large plant table:
    steam_ferc1_select = sa.sql.select([
        steam_ferc1_tbl.c.report_year,
        respondents_ferc1_tbl.c.respondent_id,
        respondents_ferc1_tbl.c.util_id_pudl,
        respondents_ferc1_tbl.c.respondent_name,
        plants_ferc1_tbl.c.plant_id_pudl,
        steam_ferc1_tbl.c.plant_name,
        steam_ferc1_tbl.c.total_capacity_mw,
        steam_ferc1_tbl.c.net_generation_mwh,
        steam_ferc1_tbl.c.expns_operations,
        steam_ferc1_tbl.c.expns_fuel,
        steam_ferc1_tbl.c.expns_coolants,
        steam_ferc1_tbl.c.expns_steam,
        steam_ferc1_tbl.c.expns_steam_other,
        steam_ferc1_tbl.c.expns_transfer,
        steam_ferc1_tbl.c.expns_electric,
        steam_ferc1_tbl.c.expns_misc_power,
        steam_ferc1_tbl.c.expns_rents,
        steam_ferc1_tbl.c.expns_allowances,
        steam_ferc1_tbl.c.expns_engineering,
        steam_ferc1_tbl.c.expns_structures,
        steam_ferc1_tbl.c.expns_boiler,
        steam_ferc1_tbl.c.expns_plants,
        steam_ferc1_tbl.c.expns_misc_steam,
        steam_ferc1_tbl.c.expns_production_total,
        steam_ferc1_tbl.c.expns_per_mwh]).\
        where(sa.sql.and_(
            respondents_ferc1_tbl.c.respondent_id == respondent_id_ferc1,
            steam_ferc1_tbl.c.respondent_id == respondent_id_ferc1,
            plants_ferc1_tbl.c.respondent_id == respondent_id_ferc1,
            plants_ferc1_tbl.c.plant_name == steam_ferc1_tbl.c.plant_name))

    steam_df = pd.read_sql(steam_ferc1_select, pudl_engine)

    # Calculate correlation of expenses to net power generation. Require a
    # minimum plant capacity factor of 0.6 so we the signal will be high,
    # but we'll still have lots of plants to look at:
    expns_corr = ferc1_expns_corr(pudl_engine, capacity_factor=0.6)

    # These are columns that pertain to the plant, and are not expenses.
    steam_common_cols = ['report_year',
                         'plant_id_pudl',
                         'plant_name',
                         'total_capacity_mw']

    # These aren't individual total expense fields, and should be left out
    steam_cols_to_remove = ['expns_per_mwh',
                            'expns_production_total']

    # Remove the expns_* columns that we don't want
    for key in steam_cols_to_remove:
        x = expns_corr.pop(key, None)

    # For now using correlation with net_generation > 0.5 as indication of
    # "production expenses" (px) vs. "non-production expenses" (npx)
    nonfuel_px = [k for k in expns_corr.keys() if expns_corr[k] >= 0.5]
    npx = [k for k in expns_corr.keys() if expns_corr[k] < 0.5]

    # Grab the common columns for our output:
    steam_out = steam_df[steam_common_cols].copy()

    # 3 categories of expense that we are pulling together:
    # - fuel production expenses
    # - non-fuel production expenses
    # - non-production expenses
    steam_out['total_fuel_px'] = steam_df['expns_fuel']
    steam_out['net_generation_mwh'] = steam_df['net_generation_mwh']
    steam_out['total_nonfuel_px'] = steam_df[nonfuel_px].copy().sum(axis=1)
    steam_out['total_npx'] = steam_df[npx].copy().sum(axis=1)

    steam_out['fuel_expns_per_mwh'] = \
        steam_out['total_fuel_px'] / steam_out['net_generation_mwh']

    steam_out['total_nonfuel_px'] = \
        steam_out['total_nonfuel_px'] / steam_out['net_generation_mwh']

    steam_out['npx_per_mwh'] = \
        steam_out['total_npx'] / steam_out['net_generation_mwh']

    steam_prod_gb = steam_out.groupby(['plant_id_pudl', 'report_year'])

    return(output)
