"""Module to flag total rows in FERC1 steam table.

* **plant total:** pudl plant totals where the plant is owned by more than one utility.
* **utility owned total:** all of the assets owned by a utility
* **utility owned plant total:** a utility's owned portion of a pudl plant
* **utility owned plant total steam:** all of the utility's steam assets within a given pudl plant
* **utility owned plant total nuclear:** all of the utility's nuclear assets within a given pudl plant
* **utility owned subtotal:** the sum of several units within a pudl plant owned by one utility
* **utility owned plant extra:** any extra amounts that are associated with a plant
* **utility owned extra:** any extra amounts that are associated with all a utility's assets
* **unit total:** the sum of a co-owned unit (sub-pudl plant id)

"""

import logging

import pudl.analysis.fill_ferc1_fuel_gaps as fill_fuel

logger = logging.getLogger(__name__)

#######################################################################################
# DEFINE USEFUL VARIABLES
#######################################################################################

total_plant_list = ['total plant', 'plant total', 'total plt', 'ttl plt',
                    'tot. plt.', '100%', 'ttl p']
extra_list = ['common', 'maint.', 'exp', 'misc', 'facilities', 'residual', 'various']
gt_list = ['gt', 'g t', 'g.t', 'g. t']
cost_list = ['chgs', 'indirect', 'exp']

#######################################################################################
# DEFINE TOTAL FLAGGING FUNCTIONS
#######################################################################################


def flag_specific_totals(df, col_name):
    """Blah."""
    logger.info("flagging specific totals")

    def _is_total(row):
        if any(x in row for x in total_plant_list):
            # some of these could be utility owned plant totals....
            return 'plant total'
        elif 'general' in row and 'general electric' not in row:
            return 'utility owned total'  # but is it a duplicate or is it extra...
        elif any(x in row for x in extra_list):  # order matters here
            return 'utility owned extra'
        elif any(x in row for x in ['chgs', 'indirect']) and any(x in row for x in gt_list):
            return 'utility owned combustion turbine extra'
        elif any(x in row for x in cost_list) and 'comb' in row:
            return 'utility owned combustion turbine extra'
        elif any(x in row for x in cost_list) and 'steam' in row:
            return 'utility owned steam extra'
        elif any(x in row for x in cost_list) and 'nuclear' in row:
            return 'utility owned nuclear extra'
        else:
            return None

    df[col_name] = df.apply(lambda x: _is_total(x.plant_name_ferc1), axis=1)

    return df


def add_manual_totals(df):
    """Add manual totals to the total_type column."""
    logger.info("adding manual totals")
    out_df = (
        df.pipe(fill_fuel.add_manual_values, 'total_type_manual', '/Users/aesharpe/Desktop/manual_total_types.xlsx'))

    out_df.total_type.update(out_df.total_type_manual)

    out_df = out_df.drop(columns='total_type_manual')

    return out_df


def backfill_by_capacity_all(df):
    """Add the same total flag to rows in the same plant_id with the same capacity."""
    logger.info("backfilling totals by capacity")

    def _backfill_by_capacity(df_group):
        # First make sure there is a total type otherwise just return the df
        if df_group.total_type.isna().all():
            return df_group
        # If there is a total row...
        else:
            # Make a dictionary of total types to capacity -- capacity must be the key
            # or else duplicate total types (in the case of two utilities reporting a
            # total for the same plant) will get deleted even if they have different
            # capacities. B/C key must be unique.
            tot_df = df_group[df_group['total_type'].notna()]
            capacity_dict = dict(zip(tot_df['capacity_mw'], tot_df['total_type']))
            for cap, total_type in capacity_dict.items():
                if cap > 0:
                    df_group.loc[(df_group['capacity_mw'] == cap) & (
                        df_group['total_type'].isna()), 'total_type'] = total_type
                    return df_group
                else:
                    return df_group

    out_df = df.groupby(['plant_id_pudl']).apply(lambda x: _backfill_by_capacity(x))

    return out_df


#######################################################################################
# PULL IT ALL TOGETHER
#######################################################################################

def flag_totals(df):
    """Flag all total rows."""
    out_df = (
        df.pipe(flag_specific_totals, col_name='total_type')
        .pipe(add_manual_totals)
        .pipe(backfill_by_capacity_all)
    )

    return out_df
