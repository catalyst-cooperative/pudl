"""Functions for pulling EIA 923 data out of the PUDl DB."""
import logging
import os

import numpy as np
import pandas as pd
import requests
import sqlalchemy as sa

import pudl

logger = logging.getLogger(__name__)

API_KEY_EIA = os.environ.get('API_KEY_EIA')

BASE_URL_EIA = "http://api.eia.gov/"

FUEL_TYPE_EIAAPI_MAP = {
    "COW": "coal",
    "NG": "gas",
    "PEL": "oil",
}

FUEL_COST_CATEGORIES_EIAAPI = [41696, 41762, 41740]
"""
The category ids for fuel costs by fuel for electricity for coal, gas and oil.

Each category id is a peice of a query to EIA's API. Each query here contains
a set of state-level child series which contain fuel cost data.

See EIA's query browse here:
 - Coal: https://www.eia.gov/opendata/qb.php?category=41696
 - Gas: https://www.eia.gov/opendata/qb.php?category=41762
 - Oil: https://www.eia.gov/opendata/qb.php?category=41740

"""


def generation_fuel_eia923(pudl_engine, freq=None,
                           start_date=None, end_date=None):
    """
    Pull records from the generation_fuel_eia923 table in given date range.

    Optionally, aggregate the records over some timescale -- monthly, yearly,
    quarterly, etc. as well as by fuel type within a plant.

    If the records are not being aggregated, all of the database fields are
    available. If they're being aggregated, then we preserve the following
    fields. Per-unit values are re-calculated based on the aggregated totals.
    Totals are summed across whatever time range is being used, within a
    given plant and fuel type.

    - ``plant_id_eia``
    - ``report_date``
    - ``fuel_type_code_pudl``
    - ``fuel_consumed_units``
    - ``fuel_consumed_for_electricity_units``
    - ``fuel_mmbtu_per_unit``
    - ``fuel_consumed_mmbtu``
    - ``fuel_consumed_for_electricity_mmbtu``
    - ``net_generation_mwh``

    In addition, plant and utility names and IDs are pulled in from the EIA
    860 tables.

    Args:
        pudl_engine (sqlalchemy.engine.Engine): SQLAlchemy connection engine
            for the PUDL DB.
        freq (str): a pandas timeseries offset alias. The original data is
            reported monthly, so the best time frequencies to use here are
            probably month start (freq='MS') and year start (freq='YS').
        start_date (date-like): date-like object, including a string of the
            form 'YYYY-MM-DD' which will be used to specify the date range of
            records to be pulled.  Dates are inclusive.
        end_date (date-like): date-like object, including a string of the
            form 'YYYY-MM-DD' which will be used to specify the date range of
            records to be pulled.  Dates are inclusive.

    Returns:
        pandas.DataFrame: A DataFrame containing all records from the EIA 923
        Generation Fuel table.

    """
    pt = pudl.output.pudltabl.get_table_meta(pudl_engine)
    gf_tbl = pt['generation_fuel_eia923']
    gf_select = sa.sql.select([gf_tbl, ])
    if start_date is not None:
        gf_select = gf_select.where(
            gf_tbl.c.report_date >= start_date)
    if end_date is not None:
        gf_select = gf_select.where(
            gf_tbl.c.report_date <= end_date)

    gf_df = pd.read_sql(gf_select, pudl_engine)

    cols_to_drop = ['id']
    gf_df = gf_df.drop(cols_to_drop, axis=1)

    # fuel_type_code_pudl was formerly aer_fuel_category
    by = ['plant_id_eia', 'fuel_type_code_pudl']
    if freq is not None:
        # Create a date index for temporal resampling:
        gf_df = gf_df.set_index(pd.DatetimeIndex(gf_df.report_date))
        by = by + [pd.Grouper(freq=freq)]

        # Sum up these values so we can calculate quantity weighted averages
        gf_gb = gf_df.groupby(by=by)
        gf_df = gf_gb.agg({
            'fuel_consumed_units': pudl.helpers.sum_na,
            'fuel_consumed_for_electricity_units': pudl.helpers.sum_na,
            'fuel_consumed_mmbtu': pudl.helpers.sum_na,
            'fuel_consumed_for_electricity_mmbtu': pudl.helpers.sum_na,
            'net_generation_mwh': pudl.helpers.sum_na,
        })
        gf_df['fuel_mmbtu_per_unit'] = \
            gf_df['fuel_consumed_mmbtu'] / gf_df['fuel_consumed_units']

        gf_df = gf_df.reset_index()

    # Bring in some generic plant & utility information:
    pu_eia = pudl.output.eia860.plants_utils_eia860(pudl_engine,
                                                    start_date=start_date,
                                                    end_date=end_date)

    first_cols = ['report_date',
                  'plant_id_eia',
                  'plant_id_pudl',
                  'plant_name_eia',
                  'utility_id_eia',
                  'utility_id_pudl',
                  'utility_name_eia', ]

    out_df = (
        pudl.helpers.merge_on_date_year(gf_df, pu_eia, on=['plant_id_eia'])
        # Drop any records where we've failed to get the 860 data merged in...
        .dropna(subset=[
            'plant_id_eia',
            'utility_id_eia',
        ])
        .pipe(pudl.helpers.organize_cols, first_cols)
        .astype({
            "plant_id_eia": "Int64",
            "plant_id_pudl": "Int64",
            "utility_id_eia": "Int64",
            "utility_id_pudl": "Int64",
        })
    )

    return out_df


def fuel_receipts_costs_eia923(pudl_engine, freq=None,
                               start_date=None, end_date=None,
                               fill=False, roll=False):
    """
    Pull records from ``fuel_receipts_costs_eia923`` table in given date range.

    Optionally, aggregate the records at a monthly or longer timescale, as well
    as by fuel type within a plant, by setting freq to something other than
    the default None value.

    If the records are not being aggregated, then all of the fields found in
    the PUDL database are available.  If they are being aggregated, then the
    following fields are preserved, and appropriately summed or re-calculated
    based on the specified aggregation. In both cases, new total values are
    calculated, for total fuel heat content and total fuel cost.

    - ``plant_id_eia``
    - ``report_date``
    - ``fuel_type_code_pudl`` (formerly energy_source_simple)
    - ``fuel_qty_units`` (sum)
    - ``fuel_cost_per_mmbtu`` (weighted average)
    - ``total_fuel_cost`` (sum)
    - ``total_heat_content_mmbtu`` (sum)
    - ``heat_content_mmbtu_per_unit`` (weighted average)
    - ``sulfur_content_pct`` (weighted average)
    - ``ash_content_pct`` (weighted average)
    - ``moisture_content_pct`` (weighted average)
    - ``mercury_content_ppm`` (weighted average)
    - ``chlorine_content_ppm`` (weighted average)

    In addition, plant and utility names and IDs are pulled in from the EIA
    860 tables.

    Args:
        pudl_engine (sqlalchemy.engine.Engine): SQLAlchemy connection engine
            for the PUDL DB.
        freq (str): a pandas timeseries offset alias. The original data is
            reported monthly, so the best time frequencies to use here are
            probably month start (freq='MS') and year start (freq='YS').
        start_date (date-like): date-like object, including a string of the
            form 'YYYY-MM-DD' which will be used to specify the date range of
            records to be pulled.  Dates are inclusive.
        end_date (date-like): date-like object, including a string of the
            form 'YYYY-MM-DD' which will be used to specify the date range of
            records to be pulled.  Dates are inclusive.
        fill (boolean): if set to True, fill in missing coal, gas and oil fuel
            cost per mmbtu from EIA's API. This fills with montly state-level
            averages.
        roll (boolean): if set to True, apply a rolling average to a
            subset of output table's columns (currently only
            'fuel_cost_per_mmbtu' for the frc table).

    Returns:
        pandas.DataFrame: A DataFrame containing all records from the EIA 923
        Fuel Receipts and Costs table.

    """
    pt = pudl.output.pudltabl.get_table_meta(pudl_engine)
    # Most of the fields we want come direclty from Fuel Receipts & Costs
    frc_tbl = pt['fuel_receipts_costs_eia923']
    frc_select = sa.sql.select([frc_tbl, ])

    # Need to re-integrate the MSHA coalmine info:
    cmi_tbl = pt['coalmine_eia923']
    cmi_select = sa.sql.select([cmi_tbl, ])
    cmi_df = pd.read_sql(cmi_select, pudl_engine)

    if start_date is not None:
        frc_select = frc_select.where(
            frc_tbl.c.report_date >= start_date)
    if end_date is not None:
        frc_select = frc_select.where(
            frc_tbl.c.report_date <= end_date)

    frc_df = pd.read_sql(frc_select, pudl_engine)

    frc_df = pd.merge(frc_df, cmi_df,
                      how='left',
                      on='mine_id_pudl')

    cols_to_drop = ['id', 'mine_id_pudl']
    frc_df = frc_df.drop(cols_to_drop, axis=1)
    frc_df = pudl.helpers.convert_cols_dtypes(frc_df, data_source='eia')

    if fill:
        logger.info('filling in fuel cost NaNs EIA APIs monthly state averages')
        fuel_costs_avg_eiaapi = get_fuel_cost_avg_eiaapi(
            FUEL_COST_CATEGORIES_EIAAPI)
        # add the state from the plants table
        frc_df = (
            pudl.helpers.merge_on_date_year(
                frc_df,
                pudl.output.eia860.plants_eia860(
                    pudl_engine, start_date=start_date, end_date=end_date)[
                        ['report_date', 'plant_id_eia', 'state']],
                on=['plant_id_eia', ], how='left')
            .merge(fuel_costs_avg_eiaapi,
                   on=['report_date', 'state', 'fuel_type_code_pudl'],
                   how='left')
            .assign(
                # add a flag column to note if we are using the api data
                fuel_cost_from_eiaapi=lambda x:
                np.where(x.fuel_cost_per_mmbtu.isnull()
                         & x.fuel_cost_per_unit.notnull(),
                         True, False),
                fuel_cost_per_mmbtu=lambda x:
                np.where(x.fuel_cost_per_mmbtu.isnull(),
                         (x.fuel_cost_per_unit
                          / x.heat_content_mmbtu_per_unit),
                         x.fuel_cost_per_mmbtu)
            )
        )
    # add the flag column to note that we didn't fill in with API data
    else:
        frc_df = frc_df.assign(fuel_cost_from_eiaapi=False)
    # this next step smoothes fuel_cost_per_mmbtu as a rolling monthly average.
    # for each month where there is any data make weighted averages of each
    # plant/fuel/month.
    if roll:
        logger.info('filling in fuel cost NaNs with rolling averages')
        frc_df = pudl.helpers.fillna_w_rolling_avg(
            frc_df,
            group_cols=['plant_id_eia', 'energy_source_code'],
            data_col='fuel_cost_per_mmbtu',
            window=12,
            min_periods=6,
            win_type='triang'
        )

    # Calculate a few totals that are commonly needed:
    frc_df['total_heat_content_mmbtu'] = \
        frc_df['heat_content_mmbtu_per_unit'] * frc_df['fuel_qty_units']
    frc_df['total_fuel_cost'] = \
        frc_df['total_heat_content_mmbtu'] * frc_df['fuel_cost_per_mmbtu']

    if freq is not None:
        by = ['plant_id_eia', 'fuel_type_code_pudl', pd.Grouper(freq=freq)]
        # Create a date index for temporal resampling:
        frc_df = frc_df.set_index(pd.DatetimeIndex(frc_df.report_date))
        # Sum up these values so we can calculate quantity weighted averages
        frc_df['total_ash_content'] = \
            frc_df['ash_content_pct'] * frc_df['fuel_qty_units']
        frc_df['total_sulfur_content'] = \
            frc_df['sulfur_content_pct'] * frc_df['fuel_qty_units']
        frc_df['total_mercury_content'] = \
            frc_df['mercury_content_ppm'] * frc_df['fuel_qty_units']
        frc_df['total_moisture_content'] = \
            frc_df['moisture_content_pct'] * frc_df['fuel_qty_units']
        frc_df['total_chlorine_content'] = \
            frc_df['chlorine_content_ppm'] * frc_df['fuel_qty_units']

        frc_gb = frc_df.groupby(by=by)
        frc_df = frc_gb.agg({
            'fuel_qty_units': pudl.helpers.sum_na,
            'total_heat_content_mmbtu': pudl.helpers.sum_na,
            'total_fuel_cost': pudl.helpers.sum_na,
            'total_sulfur_content': pudl.helpers.sum_na,
            'total_ash_content': pudl.helpers.sum_na,
            'total_mercury_content': pudl.helpers.sum_na,
            'total_moisture_content': pudl.helpers.sum_na,
            'total_chlorine_content': pudl.helpers.sum_na,
            'fuel_cost_from_eiaapi': 'any',
        })
        frc_df['fuel_cost_per_mmbtu'] = \
            frc_df['total_fuel_cost'] / frc_df['total_heat_content_mmbtu']
        frc_df['heat_content_mmbtu_per_unit'] = \
            frc_df['total_heat_content_mmbtu'] / frc_df['fuel_qty_units']
        frc_df['sulfur_content_pct'] = \
            frc_df['total_sulfur_content'] / frc_df['fuel_qty_units']
        frc_df['ash_content_pct'] = \
            frc_df['total_ash_content'] / frc_df['fuel_qty_units']
        frc_df['mercury_content_ppm'] = \
            frc_df['total_mercury_content'] / frc_df['fuel_qty_units']
        frc_df['chlorine_content_ppm'] = \
            frc_df['total_chlorine_content'] / frc_df['fuel_qty_units']
        frc_df['moisture_content_pct'] = \
            frc_df['total_moisture_content'] / frc_df['fuel_qty_units']
        frc_df = frc_df.reset_index()
        frc_df = frc_df.drop(['total_ash_content',
                              'total_sulfur_content',
                              'total_moisture_content',
                              'total_chlorine_content',
                              'total_mercury_content'], axis=1)

    # Bring in some generic plant & utility information:
    pu_eia = pudl.output.eia860.plants_utils_eia860(pudl_engine,
                                                    start_date=start_date,
                                                    end_date=end_date)

    out_df = (
        pudl.helpers.merge_on_date_year(frc_df, pu_eia, on=['plant_id_eia'])
        .dropna(subset=['utility_id_eia'])
        .pipe(
            pudl.helpers.organize_cols,
            cols=[
                'report_date',
                'plant_id_eia',
                'plant_id_pudl',
                'plant_name_eia',
                'utility_id_eia',
                'utility_id_pudl',
                'utility_name_eia',
            ]
        )
        .astype({
            "plant_id_eia": "Int64",
            "plant_id_pudl": "Int64",
            "utility_id_eia": "Int64",
            "utility_id_pudl": "Int64",
        })
    )

    if freq is None:
        # There are a couple of invalid records with no specified fuel.
        out_df = out_df.dropna(subset=['fuel_group_code'])

    return out_df


def boiler_fuel_eia923(pudl_engine, freq=None,
                       start_date=None, end_date=None):
    """
    Pull records from the boiler_fuel_eia923 table in a given data range.

    Optionally, aggregate the records over some timescale -- monthly, yearly,
    quarterly, etc. as well as by fuel type within a plant.

    If the records are not being aggregated, all of the database fields are
    available. If they're being aggregated, then we preserve the following
    fields. Per-unit values are re-calculated based on the aggregated totals.
    Totals are summed across whatever time range is being used, within a
    given plant and fuel type.

    * ``fuel_consumed_units`` (sum)
    * ``fuel_mmbtu_per_unit`` (weighted average)
    * ``total_heat_content_mmbtu`` (sum)
    * ``sulfur_content_pct`` (weighted average)
    * ``ash_content_pct`` (weighted average)

    In addition, plant and utility names and IDs are pulled in from the EIA
    860 tables.

    Args:
        pudl_engine (sqlalchemy.engine.Engine): SQLAlchemy connection engine
            for the PUDL DB.
        freq (str): a pandas timeseries offset alias. The original data is
            reported monthly, so the best time frequencies to use here are
            probably month start (freq='MS') and year start (freq='YS').
        start_date (date-like): date-like object, including a string of the
            form 'YYYY-MM-DD' which will be used to specify the date range of
            records to be pulled.  Dates are inclusive.
        end_date (date-like): date-like object, including a string of the
            form 'YYYY-MM-DD' which will be used to specify the date range of
            records to be pulled.  Dates are inclusive.

    Returns:
        pandas.DataFrame: A DataFrame containing all records from the EIA 923
        Boiler Fuel table.

    """
    pt = pudl.output.pudltabl.get_table_meta(pudl_engine)
    bf_eia923_tbl = pt['boiler_fuel_eia923']
    bf_eia923_select = sa.sql.select([bf_eia923_tbl, ])
    if start_date is not None:
        bf_eia923_select = bf_eia923_select.where(
            bf_eia923_tbl.c.report_date >= start_date
        )
    if end_date is not None:
        bf_eia923_select = bf_eia923_select.where(
            bf_eia923_tbl.c.report_date <= end_date
        )
    bf_df = pd.read_sql(bf_eia923_select, pudl_engine)

    # The total heat content is also useful in its own right, and we'll keep it
    # around.  Also needed to calculate average heat content per unit of fuel.
    bf_df['total_heat_content_mmbtu'] = bf_df['fuel_consumed_units'] * \
        bf_df['fuel_mmbtu_per_unit']

    # Create a date index for grouping based on freq
    by = ['plant_id_eia', 'boiler_id', 'fuel_type_code_pudl']
    if freq is not None:
        # In order to calculate the weighted average sulfur
        # content and ash content we need to calculate these totals.
        bf_df['total_sulfur_content'] = bf_df['fuel_consumed_units'] * \
            bf_df['sulfur_content_pct']
        bf_df['total_ash_content'] = bf_df['fuel_consumed_units'] * \
            bf_df['ash_content_pct']
        bf_df = bf_df.set_index(pd.DatetimeIndex(bf_df.report_date))
        by = by + [pd.Grouper(freq=freq)]
        bf_gb = bf_df.groupby(by=by)

        # Sum up these totals within each group, and recalculate the per-unit
        # values (weighted in this case by fuel_consumed_units)
        bf_df = bf_gb.agg({
            'total_heat_content_mmbtu': pudl.helpers.sum_na,
            'fuel_consumed_units': pudl.helpers.sum_na,
            'total_sulfur_content': pudl.helpers.sum_na,
            'total_ash_content': pudl.helpers.sum_na,
        })

        bf_df['fuel_mmbtu_per_unit'] = bf_df['total_heat_content_mmbtu'] / \
            bf_df['fuel_consumed_units']
        bf_df['sulfur_content_pct'] = bf_df['total_sulfur_content'] / \
            bf_df['fuel_consumed_units']
        bf_df['ash_content_pct'] = bf_df['total_ash_content'] / \
            bf_df['fuel_consumed_units']
        bf_df = bf_df.reset_index()
        bf_df = bf_df.drop(['total_ash_content', 'total_sulfur_content'],
                           axis=1)

    # Grab some basic plant & utility information to add.
    pu_eia = pudl.output.eia860.plants_utils_eia860(pudl_engine,
                                                    start_date=start_date,
                                                    end_date=end_date)
    out_df = (
        pudl.helpers.merge_on_date_year(bf_df, pu_eia, on=['plant_id_eia'])
        .dropna(subset=['plant_id_eia', 'utility_id_eia', 'boiler_id'])
        .pipe(pudl.helpers.organize_cols,
              cols=['report_date',
                    'plant_id_eia',
                    'plant_id_pudl',
                    'plant_name_eia',
                    'utility_id_eia',
                    'utility_id_pudl',
                    'utility_name_eia',
                    'boiler_id'])
        .astype({
            'plant_id_eia': "Int64",
            'plant_id_pudl': "Int64",
            'utility_id_eia': "Int64",
            'utility_id_pudl': "Int64",
        })
    )

    if freq is None:
        out_df = out_df.drop(['id'], axis=1)

    return out_df


def generation_eia923(pudl_engine, freq=None,
                      start_date=None, end_date=None):
    """
    Pull records from the boiler_fuel_eia923 table in a given data range.

    Args:
        pudl_engine (sqlalchemy.engine.Engine): SQLAlchemy connection engine
            for the PUDL DB.
        freq (str): a pandas timeseries offset alias. The original data is
            reported monthly, so the best time frequencies to use here are
            probably month start (freq='MS') and year start (freq='YS').
        start_date (date-like): date-like object, including a string of the
            form 'YYYY-MM-DD' which will be used to specify the date range of
            records to be pulled.  Dates are inclusive.
        end_date (date-like): date-like object, including a string of the
            form 'YYYY-MM-DD' which will be used to specify the date range of
            records to be pulled.  Dates are inclusive.

    Returns:
        pandas.DataFrame: A DataFrame containing all records from the EIA 923
        Generation table.

    """
    pt = pudl.output.pudltabl.get_table_meta(pudl_engine)
    g_eia923_tbl = pt['generation_eia923']
    g_eia923_select = sa.sql.select([g_eia923_tbl, ])
    if start_date is not None:
        g_eia923_select = g_eia923_select.where(
            g_eia923_tbl.c.report_date >= start_date
        )
    if end_date is not None:
        g_eia923_select = g_eia923_select.where(
            g_eia923_tbl.c.report_date <= end_date
        )
    g_df = pd.read_sql(g_eia923_select, pudl_engine)

    # Index by date and aggregate net generation.
    # Create a date index for grouping based on freq
    by = ['plant_id_eia', 'generator_id']
    if freq is not None:
        g_df = g_df.set_index(pd.DatetimeIndex(g_df.report_date))
        by = by + [pd.Grouper(freq=freq)]
        g_gb = g_df.groupby(by=by)
        g_df = g_gb.agg(
            {'net_generation_mwh': pudl.helpers.sum_na}).reset_index()

    # Grab EIA 860 plant and utility specific information:
    pu_eia = pudl.output.eia860.plants_utils_eia860(pudl_engine,
                                                    start_date=start_date,
                                                    end_date=end_date)

    # Merge annual plant/utility data in with the more granular dataframe
    out_df = (
        pudl.helpers.merge_on_date_year(g_df, pu_eia, on=['plant_id_eia'])
        .dropna(subset=['plant_id_eia', 'utility_id_eia', 'generator_id'])
        .pipe(pudl.helpers.organize_cols, cols=[
            'report_date',
            'plant_id_eia',
            'plant_id_pudl',
            'plant_name_eia',
            'utility_id_eia',
            'utility_id_pudl',
            'utility_name_eia',
            'generator_id',
        ])
        .astype({
            "plant_id_eia": "Int64",
            "plant_id_pudl": "Int64",
            "utility_id_eia": "Int64",
            "utility_id_pudl": "Int64",
        })
    )

    if freq is None:
        out_df = out_df.drop(['id'], axis=1)

    return out_df


def make_url_cat_eiaapi(category_id):
    """Generate a url for a category from EIA's API."""
    return f"{BASE_URL_EIA}category/?api_key={API_KEY_EIA}&category_id={category_id}"


def make_url_series_eiaapi(series_id):
    """Generate a url for a series EIA's API."""
    if series_id.count(';') > 100:
        raise AssertionError(
            f"""
            Too many series ids in this request: {series_id.count(';')}
            EIA allows up to 100 series in a request. Reduce the selection.
            """)
    return f"{BASE_URL_EIA}series/?api_key={API_KEY_EIA}&series_id={series_id}"


def get_response(url):
    """Get a response from the API's url."""
    response = requests.get(url)
    if response.status_code != 200:
        raise ValueError(
            f"API response code may be invalid. Code: {response.status_code}")
    return response


def grab_fuel_state_monthly(cat_id):
    """
    Grab an API response for monthly fuel costs for one fuel category.

    The data we want from EIA is in monthly, state-level series for each fuel
    type. For each fuel category, there are at least 51 embeded child series.
    This function compiles one fuel type's child categories into one request.
    The resulting api response should contain a list of series responses from
    each state which we can convert into a pandas.DataFrame using
    convert_cost_json_to_df.

    Args:
        cat_id (int): category id for one fuel type. Known to be
    """
    # we are going to compile a string of series ids to put into one request
    series_all = ""
    fuel_level_cat = get_response(make_url_cat_eiaapi(cat_id))
    try:
        for child in fuel_level_cat.json()['category']['childseries']:
            # get only the monthly... the f in the childseries seems to refer
            # the recporting to frequency
            if child['f'] == 'M':
                logger.debug(f"    {child['series_id']}")
                series_all = series_all + ";" + str(child['series_id'])

    except KeyError:
        raise AssertionError(
            f"Error in Response: {fuel_level_cat.json()['data']['error']}")
    return get_response(make_url_series_eiaapi(series_all))


def convert_cost_json_to_df(response_fuel_state_annual):
    """
    Convert a fuel-type/state response into a clean dataframe.

    Args:
        response_fuel_state_annual (api response): an EIA API response which
            contains state-level series including monthly fuel cost data.

    Returns:
        pandas.DataFrame: a dataframe containing state-level montly fuel cost.
        The table contains the following columns, some of which are refernce
        columns: 'report_date', 'fuel_cost_per_unit', 'state',
        'fuel_type_code_pudl', 'units' (ref), 'series_id' (ref),
        'name' (ref).
    """
    cost_df = (
        pd.json_normalize(
            data=response_fuel_state_annual.json()['series'],
            record_path='data',
            meta=['geography', 'units', 'series_id', 'name', ])
        .rename(columns={0: 'report_date',
                         1: 'fuel_cost_per_unit',
                         'geography': 'state', })
        .assign(state=lambda x: x.state.str.partition('-', True)[2],
                # break up the long series_id to extract the fuel code
                fuel_type_code_pudl=lambda x:
                (x.series_id.str.partition('.', True)[2]
                 .str.partition('.', True)[2]
                 .str.partition('-', True)[0])
                )
        .replace({'fuel_type_code_pudl': FUEL_TYPE_EIAAPI_MAP})
    )
    cost_df.loc[:, 'report_date'] = pd.to_datetime(
        cost_df['report_date'], format='%Y%m')
    return cost_df


def get_fuel_cost_avg_eiaapi(fuel_cost_cat_ids):
    """
    Get a dataframe of state-level average fuel costs for EIA's API.

    Args:
        fuel_cost_cat_ids (list): list of category ids. Known/testing working
            ids are stored in FUEL_COST_CATEGORIES_EIAAPI.

    Returns:
        pandas.DataFrame: a dataframe containing state-level montly fuel cost.
        The table contains the following columns, some of which are refernce
        columns: 'report_date', 'fuel_cost_per_unit', 'state',
        'fuel_type_code_pudl', 'units' (ref), 'series_id' (ref),
        'name' (ref).
    """
    # grab_fuel_state_monthly compiles childseries for us to make larger
    # requests, but we can request up to 100 series from EIA but each
    # state-level fuel type is over 50 so we need to pull one fuel type at a
    # time and concat the resulting df
    dfs_to_concat = []
    for fuel_cat_id in fuel_cost_cat_ids:
        dfs_to_concat.append(convert_cost_json_to_df(
            grab_fuel_state_monthly(fuel_cat_id)))
    return pd.concat(dfs_to_concat)
