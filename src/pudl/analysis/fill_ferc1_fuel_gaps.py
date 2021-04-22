"""A Module to create a more specific fuel field for ferc1."""

# Standard libraries
import pickle

# 3rd party libraries
import numpy as np
import pandas as pd

#######################################################################################
# ESTABLISH A CONNECTION TO THE NECESSARY TABLES
#######################################################################################

with open('/Users/aesharpe/Desktop/ferc1_transformed.pickle', 'rb') as handle:
    ferc1_transformed_dfs = pickle.load(handle)

fuel = ferc1_transformed_dfs['fuel_ferc1']

#######################################################################################
# DEFINE USEFUL VARIABLES
#######################################################################################

# Useful merge columns
ferc_merge_cols = ['report_year', 'utility_id_ferc1', 'plant_name_ferc1']
eia_merge_cols = ['report_date', 'plant_id_pudl', 'generator_id']

# Flags
flag1 = 'fuel in technology name'
flag2 = 'primary fuel by mmbtu'
flag3 = 'eia plant id has one fuel'
flag4 = 'primary fuel by cost'
flag5 = 'ferc pudl map all'  # Make better one
flag6 = 'similar heat rate to other years'
flag7 = 'ferc plant id has one fuel'
flag8 = 'pudl plant id has one fuel'
flag9 = 'manually filled in'
flag10 = 'front and back filled based on ferc id'
flag11 = 'flipped lone fuel outlier in ferc1 id groups'
flag12 = 'flipped pockets of fuel outliers in ferc1 id groups'

bad_plant_ids = [11536, 8469, 381, 8468, 8467]

# RMI vaule cols
value_cols_no_cap = [
    'net_generation_mwh', 'avg_num_employees',
    'capex_land', 'capex_equipment', 'capex_structures', 'capex_total',
    'asset_retirement_cost', 'opex_operations', 'opex_fuel', 'opex_coolants',
    'opex_steam', 'opex_steam_other', 'opex_transfer', 'opex_electric',
    'opex_misc_power', 'opex_rents', 'opex_allowances', 'opex_engineering',
    'opex_structures', 'opex_boiler', 'opex_plants', 'opex_misc_steam',
    'opex_production_total']

value_cols = value_cols_no_cap + ['capacity_mw']

#######################################################################################
# SETUP AND TESTING FUNCTIONS
#######################################################################################


def _test_for_duplicates(df, subset):

    test = df.copy()
    test['dup'] = test.duplicated(subset=subset)
    return f"number of duplicate index values for table: {len(test[test['dup']])}"


def _add_new_fuel_and_flag(df, new_fuel_col, flag):
    """Add new fuels to the primary fuel column and flag where they came from."""
    df.loc[(df['primary_fuel'].isna())
           & (df[f'{new_fuel_col}'].notna()), 'primary_fuel_flag'
           ] = flag
    df['primary_fuel'] = df['primary_fuel'].fillna(df[f'{new_fuel_col}'])
    return df


def _check_flags(df):
    fuel = df[df['primary_fuel'].notna()]
    flag = df[df['primary_fuel_flag'].notna()]
    assert len(fuel) == len(flag), 'imputed fuels must be associated with a flag'
    return df


def add_manual_values(df, col_name, file_path):
    """Add flags for manually categorized rows.

    This function will read the manually categorized csv of "bad" or double counted rows
    and add then to the appropriate column. It begins by converting the start_year and
    end_year columns from the manually compiled data and turns then into full ranges
    with each year comprising a new row. Next, the years are used to reconstruct the
    unique record_id column. Finally, the newly formulated record_ids undergo a QC
    before the flags are merged in with the original steam table.

    Args:
        df (pandas.DataFrame): a table with a column for the field you'd like to replace
            as well as the suffix of the record_id (ex: _12_17_0_5), the start year, and
            the end year (which is really the end year plus one for the range function).
            The name of the column you're trying to replace values in must be the same
            in this csv as in the steam table you're merging with.
        col_name (str): the name of the column with the field you'd ike to replace.
        file_path (str): the file path the the excel file where the manual fixes are
            stored.
    Returns:
        pd.Dataframe: a table with the manually categorized column values added to the
        desired column.

    """
    full_years = (
        pd.read_excel(file_path)
        # Make a range list from start to end date
        .assign(report_year=lambda x: (
                [list(range(start, end)) for start, end
                 in x[['start_year', 'end_year']].values]))
        # Turn range into rows
        .explode('report_year')
        # Recreate the unique record_id column
        .assign(record_id=lambda x: (
            'f1_steam_' + x.report_year.astype('str') + x.id_suffix))
        # Only keep relevant rows
        [['record_id', f'{col_name}']].copy()
    )

    # Check that manual records are in steam table
    problem_list = (
        [x for x in list(full_years['record_id']) if x not in list(df['record_id'])])
    assert len(problem_list) == 0, \
        f"the following record_ids are not in the steam table: {problem_list}"

    # Check that there are no duplicate record_ids
    record_dups = list(full_years[full_years['record_id'].duplicated()]['record_id'])
    assert len(record_dups) == 0, \
        f"the following record_ids are duplicated: {record_dups}"

    # Add new values to steam table
    full_years = full_years.set_index('record_id')
    df = df.set_index('record_id')
    df.update(full_years)
    with_new_values = df.reset_index()

    return with_new_values


def _show_unfilled_rows(df, fill_col):
    print(len(df[df[f'{fill_col}'].isna()]), '/', len(df))


def _drop_bad_plants(df, bad_plants):
    no_bad_plants_df = (
        df.loc[~df['plant_id_pudl'].isin(bad_plants)].copy()
        .dropna(subset=value_cols_no_cap, how='all').copy())
    return no_bad_plants_df

#######################################################################################
# FUNCTIONS TO SUPPLIMENT FUEL TYPE
#######################################################################################


def fill_obvious_names_fuel(df):
    """Blah."""
    print('filling fuels with obvious names')

    df.loc[df['plant_name_ferc1'].str.contains('solar'), 'name_based'] = 'solar'
    df.loc[df['plant_type'].str.contains('solar'), 'name_based'] = 'solar'
    df.loc[df['plant_type'].str.contains('photovoltaic'), 'name_based'] = 'solar'
    df.loc[df['plant_name_ferc1'].str.contains('wind'), 'name_based'] = 'wind'
    df.loc[df['plant_type'].str.contains('wind'), 'name_based'] = 'wind'
    df.loc[df['plant_type'].str.contains('nuclear'), 'name_based'] = 'nuclear'
    df.loc[df['plant_name_ferc1'].str.contains('nuclear'), 'name_based'] = 'nuclear'
    df.loc[df['plant_type'].str.contains('geothermal'), 'name_based'] = 'geothermal'

    out_df = (
        df.pipe(_add_new_fuel_and_flag, 'name_based', flag1)
        .pipe(_check_flags))

    _show_unfilled_rows(out_df, 'primary_fuel')

    return out_df


def primary_fuel_by_mmbtu(df, fbp_small):
    """Blah."""
    print('filling in primary fuel by mmbtu')

    out_df = (
        pd.merge(df, fbp_small, on=ferc_merge_cols, how='left')
        .assign(primary_fuel_by_mmbtu=lambda x: (
            x.primary_fuel_by_mmbtu.replace({'': np.nan, 'unknown': np.nan})))
        .pipe(_add_new_fuel_and_flag, 'primary_fuel_by_mmbtu', flag2)
        .pipe(_check_flags))

    _show_unfilled_rows(out_df, 'primary_fuel')

    return out_df


def eia_one_reported_fuel(df, gens):
    """Blah."""
    print('filling in eia plants with one reported fuel')

    # Only keep eia plants with one fuel
    gens_by_plant = (
        gens.dropna(subset=['plant_id_pudl'])
        .copy()
        .assign(
            one_fuel=lambda x: (
                x.groupby(['report_date', 'plant_id_pudl'])['fuel_type_code_pudl']
                .transform(lambda x: len(x.dropna().unique()) == 1)),
            report_year=lambda x: x.report_date.dt.year))

    gens_one_fuel = (
        gens_by_plant[gens_by_plant['one_fuel']]
        [['report_year', 'plant_id_pudl', 'fuel_type_code_pudl']].copy())

    out_df = (
        pd.merge(df, gens_one_fuel, on=['report_year', 'plant_id_pudl'], how='left')
        .pipe(_add_new_fuel_and_flag, 'fuel_type_code_pudl', flag3)
        .pipe(_check_flags)
        .drop_duplicates())

    _show_unfilled_rows(out_df, 'primary_fuel')

    return out_df


def primary_fuel_by_cost(df):
    """Blah."""
    print('filling in primary fuel by cost')

    out_df = (
        df.assign(primary_fuel_by_cost=lambda x: (
            x.primary_fuel_by_cost.replace({
                '': np.nan, 'unknown': np.nan, 'other': np.nan})))
        .pipe(_add_new_fuel_and_flag, 'primary_fuel_by_cost', flag4)
        .pipe(_check_flags))

    _show_unfilled_rows(out_df, 'primary_fuel')

    return out_df


def raw_ferc1_fuel(df):
    """Blah."""
    print('filling in raw ferc1 fuels')

    # Identify duplicate columns
    fuel_dupes = (
        fuel.loc[fuel['fuel_type_code_pudl'] != 'unknown'].copy()
        .assign(dup=lambda x: x.duplicated(subset=ferc_merge_cols, keep=False)))

    # Only take fuels from plants without duplicate name/utility/year combos
    fuel_ferc_no_dup = (
        fuel_dupes[~fuel_dupes['dup']][[
            'report_year', 'utility_id_ferc1', 'plant_name_ferc1',
            'fuel_type_code_pudl', 'fuel_avg_heat_raw', 'fuel_qty_burned']].copy()  # keep fuel_avg_heat_raw and fuel_qty_burned in there for next round
        .rename(columns={'fuel_type_code_pudl': 'fuel_type_code_pudl_ferc'}))

    out_df = (
        pd.merge(df, fuel_ferc_no_dup, on=ferc_merge_cols, how='left')
        .pipe(_add_new_fuel_and_flag, 'fuel_type_code_pudl_ferc', flag5)
        .pipe(_check_flags))

    _show_unfilled_rows(out_df, 'primary_fuel')

    return out_df


def ferc1_heat_rate(df):
    """Blah."""
    def _create_dict(df):
        """Create a dict of fuel types and fuel_avg_heat ranges within 1% of median."""
        no_unk = df[(df['primary_fuel'].notna()) & (df['fuel_avg_heat_raw'].notna())]
        median_df = (
            no_unk.groupby(['plant_name_ferc1', 'primary_fuel'])['fuel_avg_heat_raw']
            .median().reset_index())
        # Create dictionary of plant name, fuel, and heat rate
        fuel_dict = {}
        for i in median_df['plant_name_ferc1'].unique():
            fuel_dict[i] = dict(zip(
                median_df.loc[median_df['plant_name_ferc1'] == i]['primary_fuel'],
                median_df.loc[median_df['plant_name_ferc1'] == i]['fuel_avg_heat_raw']
            ))
        # Turn the median values into ranges based on 1% buffer
        for k, v in fuel_dict.items():
            for kk, vv in v.items():
                v[kk] = range(int(vv - vv * 0.01), int(vv + vv * 0.01))
        return fuel_dict

    def _test_for_overlap(fuel_dict):
        """See if there are any overlapping heat rate ranges for the same plant."""
        for k, v in fuel_dict.items():
            overlap_list = []
            rr = tuple(v.values())
            overlap = set(rr[0]).intersection(rr[1:])
            if overlap:
                overlap_list = overlap_list.append(k)
        return overlap_list

    df = df.assign(ferc_fuel_by_heat_rate=np.nan)

    fuel_dict = _create_dict(df)
    overlap_list = _test_for_overlap(fuel_dict)
    if overlap_list:
        print('The following plants have fuels with overlapping heat rates')
        print(overlap_list)

    for plant_name, hr_dict in fuel_dict.items():
        # Get all heat rate values for a given plant name
        plant_df = df.loc[df['plant_name_ferc1'] == plant_name].copy()
        # If the heat rate is within the range of one of the given fuels in the
        # dictionary associated with it's name, then give it that fuel label.
        plant_df['fuel_by_heat'] = (
            plant_df['fuel_avg_heat_raw']
            .apply(lambda x: next((fuel for fuel, heat_rate in hr_dict.items()
                                   if x in heat_rate), np.nan)))
        # print(plant_df['fuel_by_heat'])
        # Add these new fuels to the full table
        df['ferc_fuel_by_heat_rate'].update(plant_df['fuel_by_heat'])

    out_df = (
        df.pipe(_add_new_fuel_and_flag, 'ferc_fuel_by_heat_rate', flag6)
        .pipe(_check_flags))

    _show_unfilled_rows(out_df, 'primary_fuel')

    return out_df


def ferc1_id_has_one_fuel(df):
    """Blah."""
    print('filling in ferc plants with one fuel')

    plant_df = (
        df.groupby(['plant_id_ferc1'])['primary_fuel']
        .apply(lambda x: x.dropna().unique()[0]
               if len(x.dropna().unique()) == 1 else np.nan)
        .reset_index()
        .rename(columns={'primary_fuel': 'ferc1_id_has_one_fuel'}))

    out_df = (
        pd.merge(df, plant_df, on=['plant_id_ferc1'], how='left')
        .pipe(_add_new_fuel_and_flag, 'ferc1_id_has_one_fuel', flag7)
        .pipe(_check_flags))

    _show_unfilled_rows(out_df, 'primary_fuel')

    return out_df


def pudl_id_has_one_fuel(df):
    """Blah."""
    print('filling in pudl plants with one fuel')

    plant_df = (
        df.groupby(['plant_id_pudl'])['primary_fuel']
        .apply(lambda x: x.dropna().unique()[0]
               if len(x.dropna().unique()) == 1 else np.nan)
        .reset_index()
        .rename(columns={'primary_fuel': 'pudl_id_has_one_fuel'}))

    out_df = (
        pd.merge(df, plant_df, on=['plant_id_pudl'], how='left')
        .pipe(_add_new_fuel_and_flag, 'pudl_id_has_one_fuel', flag8)
        .pipe(_check_flags))

    _show_unfilled_rows(out_df, 'primary_fuel')

    return out_df


def manually_add_fuel(df):
    """Blah."""
    print('filling in manually mapped fuels')

    out_df = (
        df.assign(fuel_type=lambda x: x.primary_fuel)
        .pipe(add_manual_values, 'fuel_type', '/Users/aesharpe/Desktop/fill_fuel_type.xlsx')
        .pipe(_add_new_fuel_and_flag, 'fuel_type', flag9)
        .pipe(_check_flags))

    _show_unfilled_rows(out_df, 'primary_fuel')

    return out_df


def _fbfill(df, fill_col):

    print('front and backfilling values with the same ferc1 id')

    out_df = (
        df.sort_values(['plant_id_ferc1', 'report_year'])
        .assign(fbfill=lambda x: (
            x.groupby(['plant_id_ferc1'])[f'{fill_col}']
            .apply(lambda x: x.ffill().bfill())))
        .pipe(_add_new_fuel_and_flag, 'fbfill', flag10)
        .pipe(_check_flags)
    )

    _show_unfilled_rows(out_df, fill_col)

    return out_df


def flip_one_outlier_all(df):
    """Blah."""
    print('flipping single fuel outliers')

    def flip_one_outlier(df, flip_col):
        """
        Flips a fuel is there are only two unique fuels besides NA.

        Args:
            df (pandas.DataFrame): a groupby object that you'd like to review for fuel types.
            flip_col (str): the name of the column you'd like to flip (ex: tech_fuel or primary_fuel).

        Returns:
            pandas.DataFrame: a groupby group (technically) has been fully flipped.

        """
        if len(df[f'{flip_col}'].dropna().unique()) == 2:

            f1 = df[f'{flip_col}'].dropna().unique()[0]
            f2 = df[f'{flip_col}'].dropna().unique()[1]

            if len(df[df[f'{flip_col}'] == f1]) == 1 \
                    and len(df[df[f'{flip_col}'] == f2]) > 1:
                df['single_fuel_flip'] = df[f'{flip_col}'].replace({f1: f2})
                return df
            elif len(df[df[f'{flip_col}'] == f2]) == 1 \
                    and len(df[df[f'{flip_col}'] == f1]) > 1:
                df['single_fuel_flip'] = df[f'{flip_col}'].replace({f2: f1})
                return df
            else:
                return df
        else:
            return df

    out_df = (
        df.assign(single_fuel_flip=np.nan)
        .groupby(['plant_id_ferc1'])
        .apply(lambda x: flip_one_outlier(x, 'single_fuel_flip'))
    )
    # We can't use the same flagging mechanisms as the other functions because the
    # output is slightly different. This adds a flag to the flipped rows.
    out_df.loc[(out_df['single_fuel_flip'].notna())
               & (out_df['single_fuel_flip'] != out_df['primary_fuel']),
               'primary_fuel_flag'] = flag11
    # Add the flipped values to the primary_fuel column
    out_df['primary_fuel'].update(out_df['single_fuel_flip'])

    # this number should be the same because no NA values are filled
    _show_unfilled_rows(out_df, 'primary_fuel')

    return out_df


def create_groups(df, flip_col):
    """Squish df down into group components.

    This function takes a plant_id_ferc1 group, creates a mini dataframe (fuel_swap) that contains
    one row for each fuel group and a count for how many times that fuel appeared in that group -- there
    can be two groups of the same fuel, meaning that there were other fuel types in between.

    Args:
        df (pandas.DataFrame): a groupby object that you'd like to condense.

    Returns:
        pandas.DataFrame: a condensed version of that dataframe input grouped by
            breaks in fuel type over the years.

    """
    group = df.groupby((df[f'{flip_col}'].shift() !=
                        df[f'{flip_col}']).cumsum(), as_index=False)
    fuel_swap_df = (
        group.agg(fuel=(f'{flip_col}', 'first'), val_count=(f'{flip_col}', 'count'))
        .assign(val_count2=lambda x: x.val_count))  # Create column based on val_count column

    # Set first and last counts to NA so they are ignored.
    fuel_swap_df.loc[fuel_swap_df.index ==
                     fuel_swap_df.index.max(), 'val_count2'] = np.nan
    fuel_swap_df.loc[fuel_swap_df.index ==
                     fuel_swap_df.index.min(), 'val_count2'] = np.nan

    return group, fuel_swap_df


def flip_fuel_outliers_all(df, max_group_size):
    """Blah."""
    print('flipping multiple fuel outliers')

    def flip_fuel_outliers(df, flip_col, max_group_size):
        """
        Flip groups of fuel flanked by others.

        This function takes a plant_id_ferc1 group, creates a mini dataframe (fuel_swap)
        that contains one row for each fuel group and a count for how many times that fuel
        appeared in that group -- there can be two groups of the same fuel, meaning that
        there were other fuel types in between. Ex: gas, coal, gas, means that there was a
        period of years that the plant used gas, a period of time when they used coal, and
        another period of time that it whent back to coal. The data are organized by year.
        Each of these groups will appear in this mini dataframe.

        Next, the function determines which fuel groups are considered outliers. Outliers
        are defined as any group of rows less than or equal to the max_group_size that are
        flanked on either side by the same fuel (This means that groups at the beginning or
        end are excluded). Outlier groups are then flipped to match the fuel type on either
        side of the group.

        Args:
            df (pandas.DataFrame): a groupby object that you'd like to condense.
            flip_col (str): the name of the column you'd like to flip (ex: tech_fuel or
                primary_fuel).
            max_group_size (int): the number of rows of the same column flanked by fuel of
                the same type that you'd like to flip (ex: 3 = gas, coal, coal, coal, gas
                becomes all gas).

        Returns:
            pandas.DataFrame: a groupby group (technically) has been fully flipped.

        """
        # If the group of rows in the desired column is completely null, skip it.
        if df[f'{flip_col}'].isna().all():
            return df
        else:
            # Create a mini group
            group, fuel_swap_df = create_groups(df, flip_col)
            # Find out fuel that appears the most
            fuel_swap_df['val_count'].max()
            # Create a list of the index values of the mini group that are under the
            # desired max group limit
            size_limit_list = list(
                fuel_swap_df[fuel_swap_df['val_count2'] < max_group_size + 1].index)
            # Create a list of the values under this size limit that are also flanked by
            # the same fuel
            outlier_list = [x for x in size_limit_list if fuel_swap_df.iloc[x + 1]
                            ['fuel'] == fuel_swap_df.iloc[x - 1]['fuel']]

            # As long as the list of outliers is more than 0:
            while len(outlier_list) > 0:
                for outlier in outlier_list:
                    # Identify the flanking fuel you'll replace the outliers with
                    swap_fuel = fuel_swap_df.iloc[outlier + 1]['fuel']
                    # Replace the outliers with the flank fuel
                    df.loc[df.index.isin(group.groups[outlier + 1]),
                           flip_col] = swap_fuel
                    # Get rid of None values that are a relic of the switch() function in
                    # the create groups function
                    df[f'{flip_col}'] = df[f'{flip_col}'].replace({None: np.nan})
                    # Reset the group, fuel_swap, and outlier lists
                    group, fuel_swap_df = create_groups(df, flip_col)
                    size_limit_list = list(
                        fuel_swap_df[fuel_swap_df['val_count2'] < max_group_size + 1].index)
                    outlier_list = [x for x in size_limit_list if fuel_swap_df.iloc[x + 1]
                                    ['fuel'] == fuel_swap_df.iloc[x - 1]['fuel']]
                    break  # Break the loop so that you can see if the most recent flip changed the outliers
                    # CHECK THAT THIS BREAK DOES WHAT IT IS SUPPOSED TO DO IN ALL CASES!

            return df

    out_df = (
        df.assign(multi_fuel_flip=df.primary_fuel)
        .groupby(['plant_id_ferc1'])
        .apply(lambda x: flip_fuel_outliers(x, 'multi_fuel_flip', 6))
    )
    # We can't use the same flagging mechanisms as the other functions because the
    # output is slightly different. This adds a flag to the flipped rows.
    out_df.loc[(out_df['multi_fuel_flip'].notna())
               & (out_df['multi_fuel_flip'] != out_df['primary_fuel']),
               'primary_fuel_flag'] = flag12
    # Add the flipped values to the primary_fuel column
    out_df['primary_fuel'].update(out_df['multi_fuel_flip'])

    # this number should be the same because no NA values are filled
    _show_unfilled_rows(out_df, 'primary_fuel')

    out_df = df.groupby(['plant_id_ferc1']).apply(
        lambda x: flip_fuel_outliers(x, 'multi_fuel_flip', 6))

    return out_df


#######################################################################################
# FUNCTIONS TO SUPPLIMENT PLANT TYPE
#######################################################################################


def fill_obvious_names_plant(df):
    print('filling plants with obvious names')
    df.loc[df['plant_name_ferc1'].str.contains('nuclear'), 'plant_type'] = 'nuclear'
    df.loc[(df['plant_type'].isna()) & (df['plant_name_ferc1'].str.contains(
        '(s)', regex=False)), 'plant_type'] = 'steam'
    df.loc[(df['plant_type'].isna()) & (df['plant_name_ferc1'].str.contains(
        '(ct)', regex=False)), 'plant_type'] = 'combustion_turbine'
    df.loc[(df['plant_type'].isna()) & (df['plant_name_ferc1'].str.contains(
        'combined cycle', regex=False)), 'plant_type'] = 'combustion_turbine'
    df.loc[(df['plant_type'].isna()) & (df['plant_name_ferc1'].str.contains(
        '(gt)', regex=False)), 'plant_type'] = 'combustion_turbine'
    df.loc[(df['plant_type'].isna()) & (df['plant_name_ferc1'].str.contains(
        ' gt', regex=False)), 'plant_type'] = 'combustion_turbine'
    df.loc[(df['plant_type'].isna()) & (df['plant_name_ferc1'].str.contains(
        'unseg nuclear', regex=False)), 'plant_type'] = 'nuclear'

    _show_unfilled_rows(df, 'plant_type')

    return df


#######################################################################################
# FUNCTIONS TO PULL IT ALL TOGETHER
#######################################################################################


def impute_fuel_type(df, pudl_out):
    """Blah."""
    fbp = pudl_out.fbp_ferc1()
    fbp_small = fbp[ferc_merge_cols + ['primary_fuel_by_mmbtu', 'primary_fuel_by_cost']]
    gens = pudl_out.gens_eia860()

    out_df = (
        df.pipe(_drop_bad_plants, bad_plant_ids)
        .pipe(fill_obvious_names_fuel)
        .pipe(primary_fuel_by_mmbtu, fbp_small)
        .pipe(eia_one_reported_fuel, gens)
        .pipe(primary_fuel_by_cost)
        .pipe(raw_ferc1_fuel)
        .pipe(ferc1_id_has_one_fuel)
        .pipe(pudl_id_has_one_fuel)
        .pipe(manually_add_fuel)
        .drop_duplicates()
        .pipe(_fbfill, 'primary_fuel'))

    return out_df


def impute_plant_type(df):
    """Blah."""
    df['plant_type'] = df['plant_type'].replace({'unknown': np.nan})

    out_df = (
        df.pipe(fill_obvious_names_plant)
    )

    return out_df
