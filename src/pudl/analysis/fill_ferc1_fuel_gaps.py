"""A Module to create a more specific technology field for ferc1."""

import logging

# 3rd party libraries
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

#######################################################################################
# DEFINE USEFUL VARIABLES
#######################################################################################

# Useful merge columns
ferc_merge_cols = ['report_year', 'utility_id_ferc1', 'plant_name_ferc1']
eia_merge_cols = ['report_date', 'plant_id_pudl', 'generator_id']

expected_fuels = ['coal', 'gas', 'nuclear', 'oil', 'waste', 'geothermal',
                  'other', 'wind', 'solar', np.nan]

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
# look at this...same as 14?
flag12 = 'flipped lone fuel outlier in pudl id and plant type groups'
flag13 = 'flipped pockets of fuel outliers in ferc1 id groups'
flag14 = 'flipped lone fuel outlier in pudl_id with matching capacity'

td_flag1 = 'direct from eia860'
td_flag2 = 'backfill from other year'
td_flag3 = 'backfill from eia year'
td_flag4 = 'obvious names'
td_flag5 = 'primary fuel by mmbtu no dups'

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

# Map technology_descriptions
rmi_tech_desc = {
    "coal":
        ['coal_combustion_turbine', 'coal_steam_turbine', 'coal_steam',
         'coal_combined_cycle', 'coal_internal_combustion', 'coal_unknown',
         'conventional_steam_coal'],
    "gas_combined_cycle":
        ['natural_gas_fired_combined_cycle'],
    "gas_combustion_turbine":
        ['natural_gas_fired_combusion_turbine'],
    "gas_internal_combustion_engine":
        ['natural_gas_fired_internal_combusion_entine', 'gas_internal_combustion',
         'natural_gas_internal_combustion_engine', 'natural_gas_fired_combustion_turbine'],
    "gas_steam_turbine":
        ['natural_gas_steam_turbine', 'gas_steam'],
    "geothermal":
        ['geothermal_geothermal', 'geothermal'],
    "hydroelectric_conventional":
        ['conventional_hydroelectric'],
    "hydroelectric_pumped_storage":
        [],
    "nuclear":
        ['nuclear_nuclear', 'nuclear_unknown'],
    "petroleum_liquids":
        ['oil_combustion_turbine', 'oil_steam', 'oil_internal_combustion',
         'oil_combined_cycle', 'oil_unknown'],
    "solar":
        ['solar_photovoltaic'],
    "waste":
        ['waste_steam', 'w', 'wood/wood_waste_biomass',
         'municipal_solid_waste', 'landfill_gas'],
    "wind":
        ['wind_wind', 'wind_unknown', 'onshore_wind_turbine'],
    np.nan:
        ['all_other']
}

#######################################################################################
# SETUP AND TESTING FUNCTIONS
#######################################################################################


def _test_for_duplicates(df, subset):

    test = df.copy()
    test['dup'] = test.duplicated(subset=subset)
    return f"number of duplicate index values for table: {len(test[test['dup']])}"


def add_new_fuel_and_flag(df, flag, common_col, new_col, overwrite=False, keep_new_col=False):
    """Add new values to the primary column and flag where they came from."""
    # If you want to be able to override some of the existing fuel types with new values
    # (only current context for this is with the manual overrides):
    if overwrite:
        df.loc[df[f'{new_col}'].notna(), f'{common_col}_flag'] = flag
        df.loc[df[f'{new_col}'].notna(), common_col] = df[f'{new_col}']
    # Else if you want each round of fuel additions to just fill in the gaps (where
    # there are still NA values):
    else:
        df.loc[(df[f'{common_col}'].isna())
               & (df[f'{new_col}'].notna()), f'{common_col}_flag'
               ] = flag
        df[f'{common_col}'] = df[f'{common_col}'].fillna(df[f'{new_col}'])

    # Get rid of the column you just flagged and integrated into the common col
    if not keep_new_col:
        df = df.drop(columns=new_col)

    return df


def _check_flags(df):
    fuel = df[df['primary_fuel'].notna()]
    flag = df[df['primary_fuel_flag'].notna()]
    assert len(fuel) == len(flag), 'imputed fuels must be associated with a flag'
    return df


def add_manual_values(df, col_name, file_path):
    """Add manually categorized rows.

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
    # full_years = full_years.set_index('record_id')
    # df = df.set_index('record_id')
    # df.update(full_years, overwrite=True)
    # with_new_values = df.reset_index()
    with_new_values = pd.merge(df, full_years, on='record_id', how='left')

    return with_new_values


def show_unfilled_rows(df, fill_col):
    """Blah."""
    unfilled = len(df[df[f'{fill_col}'].isna()])
    logger.info(f"{unfilled} / {len(df)} rows left unfilled")


def _drop_bad_plants(df, bad_plants):
    no_bad_plants_df = (
        df.loc[~df['plant_id_pudl'].isin(bad_plants)].copy()
        .dropna(subset=value_cols_no_cap, how='all').copy())
    return no_bad_plants_df


#######################################################################################
# FUNCTIONS TO SUPPLIMENT TECHNOLOGY DESCRIPTION
#######################################################################################

def make_cols_for_new_units(df):
    """Create columns to indicate whether there were units added or retired.

    This function runs a subfunction check_for_new_units() on each plant_id_pudl group
    to see whether there were any new units added or retired in a given year. This is
    determined by intallation_year field (the date the the most recent unit installed)
    and construction_year (the date of the oldest unit). The subfunction takes will
    mark any rows TRUE that do no have an installation_year or construction_year that
    matches the first reported installation_year and construction_year. These columns
    are used for backfilling technology type to ensure that only years with no unit
    changes can have the same fuel type.

    Args:
        df (pandas.DataFrame): The FERC1 steam table merged with EIA860 technology
            description.
    Returns:
        pandas.DataFrame: The same DataFrame but with one bool column to show a change
            in installation year and another to show a change in construction year.

    """
    def check_for_new_units(df, year_col, bool_col):
        """Check the construction_year and installation_year fields."""
        init_years = df[df['report_year'] == df.report_year.min()][year_col].unique()
        df[f'{bool_col}'] = ~df[f'{year_col}'].isin(init_years)

        return df
    # Make columns to see if there have been any unit additions or retirements
    out_df = (
        df.groupby(['plant_id_pudl'])
        .apply(lambda x: check_for_new_units(x, 'construction_year', 'retired_unit'))
        .groupby(['plant_id_pudl'])
        .apply(lambda x: check_for_new_units(x, 'installation_year', 'new_unit')))

    return out_df  # Might need tweaking


def get_tech_descrip_from_eia(eia_gens):
    """Get technology_description from plants with only one tech description.

    This function looks at the EIA860 generator table and collects the
    technology_description field from all pudl plant id groups that only have one
    technology description (besides NA). We only take plant groups with one fuel
    because we can't assign sub-plant technology types between EIA and FERC. The
    function has two outputs: one is a pandas Dataframe that maps EIA
    technology_descriptions to FERC records by plant_id_pudl and year, and the second
    is a dictionary that maps plant_id_pudl to technology description. The first is used
    to show which years are definitely accounted for in EIA
    (merge_with_eia_tech_desc()); the dictionary is used for backfilling in years where
    there has not been any newly installed units (see backfill_tech_desc_by_year()).

    Args:
        eia_gens (pandas.DataFrame): The gens_eia860 table.
    Returns:
        tuple: A tuple containing a pandas.DataFrame: A dataframe linking plant_id_pudl
            and year to EIA860 technology_description and a dictionary mapping
            plant_id_pudl to technology_description.

    """
    # Get eia plants with only one technology description (besides NA)
    eia_one_tech = (
        eia_gens.groupby('plant_id_pudl')
        .filter(lambda x: len(x.technology_description.dropna().unique()) == 1)
        [['report_year', 'plant_id_pudl', 'technology_description']].drop_duplicates()
        .assign(dup=lambda x: x.duplicated(subset=['report_year', 'plant_id_pudl'],
                                           keep=False)))
    # Drop cases when there is a None and a Tech Desc. for the same plant and year
    # (so there is one per plant-year)
    eia_one_tech = (
        eia_one_tech.drop(eia_one_tech[
            (eia_one_tech['dup'])
            & (eia_one_tech['technology_description'].isna())].index))

    # Make technology description lower case
    eia_one_tech['technology_description'] = (
        eia_one_tech.technology_description.str.lower())

    # Get the technology description associated with the plant regardless of year...
    plant_id_tech_type = (
        eia_one_tech.groupby(['plant_id_pudl']).agg(
            {'technology_description': lambda x: x.dropna().unique().item()})
        .rename(columns={'technology_description': 'same_tech'})
        .reset_index())
    plant_tech_dict = dict(
        zip(plant_id_tech_type['plant_id_pudl'], plant_id_tech_type['same_tech']))

    return eia_one_tech, plant_tech_dict


def merge_with_eia_tech_desc(df, eia_one_tech, plant_tech_dict):
    """Blah."""
    logger.info("merging single-tech EIA technology_description with FERC")
    # Add a column for technology_type by year and one that shows the technology type
    # regardless of year (same_tech)
    out_df = (
        pd.merge(df, eia_one_tech, on=['report_year', 'plant_id_pudl'], how='left')
        .assign(same_tech=lambda x: x.plant_id_pudl.map(plant_tech_dict))
        .pipe(add_new_fuel_and_flag, td_flag1, common_col='tech_desc',
              new_col='technology_description', keep_new_col=True))

    show_unfilled_rows(out_df, 'tech_desc')

    return out_df


def backfill_tech_desc_by_year(df, eia_one_tech):
    """Backfill technology_descriptions from EIA.

    This function looks to fill the technology_description field for for plants with EIA
    technology descriptions that don't extend back to the years reported in FERC. There
    are two types of cases: 1) The plant shows us in FERC and EIA and some years map
    directly to a tech type while others are None. 2) The plant shows up in FERC and EIA
    but there is no year overlap between the two. To verify that we can use the same
    technology type moving back in time, we look at the EIA operating_date field and the
    FERC1 installation_year field. If they match with the EIA records that have
    descriptions, then we can reasonably assume it's the same technology moving
    backwards. There are some instances where this is not the case and we can manually
    override them.

    """
    logger.info("backfilling EIA technology_description by year if no new units installed")
    df_check_years = make_cols_for_new_units(df)

    def backfill_if_matching_year(df, eia):
        """Backfill where there's an eia tech desc on matching latest install years."""
        # If there is a technology type taken directly from eia in the ferc data:
        if df.technology_description.notna().any():
            install_years = list(df[df['technology_description'].notna()]
                                 ['installation_year'].unique())
            assert len(df['technology_description'].dropna().unique(
            )) == 1, 'backfilling only works when there is one tech description per plant'
            tech_type = df['technology_description'].dropna().unique().item()
            df.loc[df['installation_year'].isin(
                install_years), 'backfill_by_year'] = tech_type
            return df
        # Else if there is a technology type but it is only in EIA data (other years not present in FERC):
        elif df.same_tech.notna().any():
            # Get the eia rows for this plant that have a tech descrip
            plant_eia = (
                eia[(eia['plant_id_pudl'] == df.plant_id_pudl.unique().item())
                    & eia['technology_description'].notna()])
            # Make sure there is only one technology description in the given plant group
            assert len(plant_eia['technology_description'].unique(
            )) == 1, 'backfilling only works when there is one tech description per plant'
            # Convert the op date to a year and make sure the tech_desc is in lower case!
            plant_eia = (
                plant_eia.assign(
                    operating_date=pd.to_datetime(plant_eia.operating_date).dt.year,
                    technology_description=lambda x: x.technology_description.str.lower()))
            install_years = list(plant_eia['operating_date'].unique())
            tech_type = plant_eia['technology_description'].unique().item()
            df.loc[df['installation_year'].isin(
                install_years), 'backfill_by_eia_year'] = tech_type
            return df
        else:
            return df

    out_df = (
        df_check_years
        .groupby(['plant_id_pudl']).apply(lambda x: backfill_if_matching_year(x, eia_one_tech))
        .pipe(add_new_fuel_and_flag, td_flag2, common_col='tech_desc',
              new_col='backfill_by_year')
        .pipe(add_new_fuel_and_flag, td_flag3, common_col='tech_desc',
              new_col='backfill_by_eia_year')
        .drop(columns=['new_unit', 'retired_unit', 'same_tech']))
    show_unfilled_rows(out_df, 'tech_desc')

    return out_df


def fuel_plant_type_to_tech(df):
    """Combine primary fuel and plant type rows to make mock technology type."""
    logger.info('combining primary_fuel and plant_type columns')
    # Combine the primary_fuel and plant_type columns and add them to the tech_desc
    # column where there are still gaps. Provide a temporary flag.

    # Turn unknown coal plant types into unknown instead of NA so they will be caught
    # and changed by the RMI tech type dictionary
    fuel_is_coal = df['primary_fuel'] == 'coal'
    plant_is_na = df['plant_type'].isna()
    df.loc[fuel_is_coal & plant_is_na, 'plant_type'] = 'unknown'

    out_df = (
        df.assign(
            tech_fpt=lambda x: x.primary_fuel + '_' + x.plant_type)
        .pipe(add_new_fuel_and_flag, flag='temp', common_col='tech_desc',
              new_col='tech_fpt'))

    # Add the primary_fuel_flag to the tech_desc_flag column
    out_df.loc[out_df['tech_desc_flag'] == 'temp',
               'tech_desc_flag'] = 'FPT ' + out_df.primary_fuel_flag

    show_unfilled_rows(out_df, 'tech_desc')

    return out_df


def map_tech_desc(df):
    """Streamline categories in tech_desc column."""
    logger.info("making uniform tech description col")
    df['tech_desc'] = df['tech_desc'].replace(' ', '_', regex=True)
    df['tech_desc_no_map'] = df['tech_desc']
    for tech in rmi_tech_desc:
        df['tech_desc'] = df.tech_desc.replace(rmi_tech_desc[tech], tech)

    return df


#######################################################################################
# FUNCTIONS TO SUPPLIMENT FUEL TYPE
#######################################################################################


def fill_obvious_names_fuel(df, common_col, flag):
    """Blah."""
    logger.info('filling fuels with obvious names')

    df.loc[df['plant_name_ferc1'].str.contains('solar'), 'name_based'] = 'solar'
    df.loc[df['plant_type'].str.contains('solar'), 'name_based'] = 'solar'
    df.loc[df['plant_type'].str.contains('photovoltaic'), 'name_based'] = 'solar'
    df.loc[df['plant_name_ferc1'].str.contains('wind'), 'name_based'] = 'wind'
    df.loc[df['plant_type'].str.contains('wind'), 'name_based'] = 'wind'
    df.loc[df['plant_type'].str.contains('nuclear'), 'name_based'] = 'nuclear'
    df.loc[df['plant_name_ferc1'].str.contains('nuclear'), 'name_based'] = 'nuclear'
    df.loc[df['plant_type'].str.contains('geothermal'), 'name_based'] = 'geothermal'

    out_df = (
        df.pipe(add_new_fuel_and_flag, flag,
                common_col=common_col, new_col='name_based'))
    # .pipe(_check_flags))

    show_unfilled_rows(out_df, common_col)

    return out_df


def primary_fuel_by_mmbtu(df, fbp_small):
    """Blah."""
    logger.info('filling in primary fuel by mmbtu')

    out_df = (
        pd.merge(df, fbp_small, on=ferc_merge_cols, how='left')
        .assign(primary_fuel_by_mmbtu=lambda x: (
            x.primary_fuel_by_mmbtu.replace({'': np.nan, 'unknown': np.nan})))
        .pipe(add_new_fuel_and_flag, flag2, common_col='primary_fuel',
              new_col='primary_fuel_by_mmbtu')
        .pipe(_check_flags))

    show_unfilled_rows(out_df, 'primary_fuel')

    return out_df


def eia_one_reported_fuel(df, gens):
    """Blah."""
    logger.info('filling in eia plants with one reported fuel')

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
        .pipe(add_new_fuel_and_flag, flag3, common_col='primary_fuel',
              new_col='fuel_type_code_pudl')
        .pipe(_check_flags)
        .drop_duplicates())

    show_unfilled_rows(out_df, 'primary_fuel')

    return out_df


def primary_fuel_by_cost(df):
    """Blah."""
    logger.info('filling in primary fuel by cost')

    out_df = (
        df.assign(primary_fuel_by_cost=lambda x: (
            x.primary_fuel_by_cost.replace({
                '': np.nan, 'unknown': np.nan, 'other': np.nan})))
        .pipe(add_new_fuel_and_flag, flag4, common_col='primary_fuel',
              new_col='primary_fuel_by_cost')
        .pipe(_check_flags))

    show_unfilled_rows(out_df, 'primary_fuel')

    return out_df


def raw_ferc1_fuel(df, fuel):
    """Blah."""
    logger.info('filling in raw ferc1 fuels')

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

    out_df = (  # ferc merge cols cause a problem with plant 'h. b. robinson' because it uses the same name for multiple units
        pd.merge(df, fuel_ferc_no_dup, on=ferc_merge_cols, how='left')
        .pipe(add_new_fuel_and_flag, flag5, common_col='primary_fuel',
              new_col='fuel_type_code_pudl_ferc')
        .pipe(_check_flags))

    show_unfilled_rows(out_df, 'primary_fuel')

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
        df.pipe(add_new_fuel_and_flag, flag6, common_col='primary_fuel',
                new_col='ferc_fuel_by_heat_rate')
        .pipe(_check_flags))

    show_unfilled_rows(out_df, 'primary_fuel')

    return out_df


def ferc1_id_has_one_fuel(df):
    """Blah."""
    logger.info('filling in ferc plants with one fuel')

    plant_df = (
        df.groupby(['plant_id_ferc1'])['primary_fuel']
        .apply(lambda x: x.dropna().unique()[0]
               if len(x.dropna().unique()) == 1 else np.nan)
        .reset_index()
        .rename(columns={'primary_fuel': 'ferc1_id_has_one_fuel'}))

    out_df = (
        pd.merge(df, plant_df, on=['plant_id_ferc1'], how='left')
        .pipe(add_new_fuel_and_flag, flag7, common_col='primary_fuel',
              new_col='ferc1_id_has_one_fuel')
        .pipe(_check_flags))

    show_unfilled_rows(out_df, 'primary_fuel')

    return out_df


def pudl_id_has_one_fuel(df):
    """Blah."""
    logger.info('filling in pudl plants with one fuel')

    plant_df = (
        df.groupby(['plant_id_pudl'])['primary_fuel']
        .apply(lambda x: x.dropna().unique()[0]
               if len(x.dropna().unique()) == 1 else np.nan)
        .reset_index()
        .rename(columns={'primary_fuel': 'pudl_id_has_one_fuel'}))

    out_df = (
        pd.merge(df, plant_df, on=['plant_id_pudl'], how='left')
        .pipe(add_new_fuel_and_flag, flag8, common_col='primary_fuel',
              new_col='pudl_id_has_one_fuel')
        .pipe(_check_flags))

    show_unfilled_rows(out_df, 'primary_fuel')

    return out_df


def manually_add_fuel(df):
    """Blah."""
    logger.info('filling in manually mapped fuels')

    out_df = (
        # df.assign(fuel_type=lambda x: x.primary_fuel)
        df.pipe(add_manual_values, 'fuel_type',
                '/Users/aesharpe/Desktop/fill_fuel_type.xlsx')
        .pipe(add_new_fuel_and_flag, flag9, common_col='primary_fuel', new_col='fuel_type', overwrite=True)
        .pipe(_check_flags))

    show_unfilled_rows(out_df, 'primary_fuel')

    return out_df


def _fbfill(df, fill_col):

    logger.info('front and backfilling values with the same ferc1 id')

    out_df = (
        df.sort_values(['plant_id_ferc1', 'report_year'])
        .assign(fbfill=lambda x: (
            x.groupby(['plant_id_ferc1'])[f'{fill_col}']
            .apply(lambda x: x.ffill().bfill())))
        .pipe(add_new_fuel_and_flag, flag10, common_col='primary_fuel',
              new_col='fbfill')
        .pipe(_check_flags)
    )

    show_unfilled_rows(out_df, fill_col)

    return out_df


def flip_one_outlier_all(df, group_level):
    """Flip single outlier fuels in groups of plant_id_ferc1.

    Args:
        df (pandas.DataFrame): a DataFrame with fuel columns you'd like to flip.
        group_level (str): the name of the column by which you'd like to group the rows
            and determine whether to flip any fuel. Must be either 'plant_id_ferc1' or
            'plant_id_pudl'

    Returns:
        pandas.DataFrame: a DataFrame with the fuel columns appropriately flipped and
            flagged.

    """
    logger.info(f'flipping single fuel outliers for {group_level}')

    def flip_one_outlier(df, flip_col):
        """
        Flip a fuel when is there are only two unique fuels and one appears once.

        When there are two unique fuels (besides NA) and one of them only appears once,
        swap that fuel out for the other dominant fuel.

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
                df['single_flip'] = df[f'{flip_col}'].replace({f1: f2})
                # print(list(df['plant_id_pudl'].unique()))
                return df
            elif len(df[df[f'{flip_col}'] == f2]) == 1 \
                    and len(df[df[f'{flip_col}'] == f1]) > 1:
                df['single_flip'] = df[f'{flip_col}'].replace({f2: f1})
                # print(list(df['plant_id_pudl'].unique()))
                return df
            else:
                return df
        else:
            return df

    # When grouping at the pudl id, make sure you're also grouping by plant type.
    # This is to make sure you're not converting fuel from different units.
    # Define unique flags for each circumstance.
    if group_level == 'plant_id_pudl':
        group_level = ['plant_id_pudl', 'plant_type']
        flag = flag12
    elif group_level == 'plant_id_ferc1':
        flag = flag11

    out_df = (
        df.assign(single_flip=np.nan)
        .groupby(group_level)
        .apply(lambda x: flip_one_outlier(x, 'primary_fuel'))
    )
    # We can't use the same flagging mechanisms as the other functions because the
    # output is slightly different. This adds a flag to the flipped rows.
    out_df.loc[(out_df['single_flip'].notna())
               & (out_df['single_flip'] != out_df['primary_fuel']),
               'primary_fuel_flag'] = flag
    # Add the flipped values to the primary_fuel column and drop the old col
    out_df['primary_fuel'].update(out_df['single_flip'])
    out_df.drop(columns='single_flip')

    # this number should be the same because no NA values are filled
    show_unfilled_rows(out_df, 'primary_fuel')

    return out_df


def create_groups(df, flip_col):
    """Count fuel apperances in plant_id_ferc1 groups.

    This function takes a plant_id_ferc1 group and creates a mini dataframe (fuel_swap)
    that contains one row for each fuel group and a count for how many times that fuel
    appeared in that group -- there can be two groups of the same fuel, meaning that
    there were other fuel types in between.

    Ex: If you pass in a df with the flip_col values: coal, coal, coal, coal, oil, coal,
    coal, coal, coal, coal, the mini df output will have three rows: coal - 4, oil - 1,
    and coal - 5.

    There are three columns for the mini output df: one for the fuel type, and one for
    the number of values (val_count). The third, val_count2 sets the first and last
    val_count values to NA. This is because we only want to flip pockets of fuel
    outliers that have the same fuel on either side of them. We keep the val_count
    column because there is an exception for oulier pockets that occur strictly at the
    beginning of the group (index=0) for which we need the first val_count.

    Args:
        df (pandas.DataFrame): a groupby object that you'd like to condense.

    Returns:
        pandas.DataFrame: a condensed version of that dataframe input grouped by
            breaks in fuel type over the years.

    """
    # Make groups based on sections of fuel ex: coal, coal, gas, coal, coal is three
    # groups
    group = df.groupby((df[f'{flip_col}'].shift() !=
                        df[f'{flip_col}']).cumsum(), as_index=False)
    # identify the first (and only) fuel for each group and count how many rows are in
    # each fuel group.
    fuel_swap_df = (
        group.agg(fuel=(f'{flip_col}', 'first'), val_count=(f'{flip_col}', 'count'))
        .assign(val_count2=lambda x: x.val_count))  # Create column based on val_count column

    # Set first and last counts to NA so they are ignored. When identifying fuel groups
    # To flip, we only want groups that are flanked by the same fuel, therefore they
    # can't be on the ends. There is an exception for fuel groups at the front which is
    # why val_count and val_count2 are seperate.
    fuel_swap_df.loc[fuel_swap_df.index ==
                     fuel_swap_df.index.max(), 'val_count2'] = np.nan
    fuel_swap_df.loc[fuel_swap_df.index ==
                     fuel_swap_df.index.min(), 'val_count2'] = np.nan

    # Check for times when there is a swith from gas back to coal (which is probably wrong)
    # if 'coal' and 'gas' in fuel_swap_df.fuel.values:
    #     coal_max = fuel_swap_df[fuel_swap_df['fuel'] == 'coal'].index.max()
    #     gas_max = fuel_swap_df[fuel_swap_df['fuel'] == 'gas'].index.max()
    #     if coal_max > gas_max:
    #         print(list(df['plant_id_ferc1'].unique()))
    #         print('heyyyy so this one has a gas to coal')

    return group, fuel_swap_df


def flip_fuel_outliers_all(df, max_group_size):
    """Flip small pockets of fuel type that don't reflect the technology type.

    This function runs a sub-function, flip_fuel_outliers, on each group of
    plant_id_ferc1 from the steam table. It must be utilized after there is a
    primary_fuel field filled in. The flip_fuel_outliers function makes the fuel column
    more homogenous by flipping groups of fuel that are under the stated max_group_size
    to reflect the fuel types around them. See the docs for flip_fuel_outliers for
    more details. Once the fuels have been flipped, the flags for the flipped rows are
    updated to show that they were flipped and the flipped fuel column is merged with
    the official primary fuel column.

    """
    logger.info(f'flipping multiple fuel outliers for groups under {max_group_size}')

    def flip_fuel_outliers(df, flip_col, max_group_size):
        """
        Flip groups of fuel flanked by others.

        This function takes a plant_id_ferc1 group, creates a mini dataframe (fuel_swap)
        that contains one row for each fuel group and a count for how many times that
        fuel appeared in that group -- there can be two groups of the same fuel, meaning
        that there were other fuel types in between. Ex: gas, coal, gas, means that
        there was a period of years that the plant used gas, a period of time when they
        used coal, and another period of time that it whent back to coal. The data are
        organized by year. Each of these groups will appear in this mini dataframe.

        Next, the function determines which fuel groups are considered outliers.
        Outliers are defined as any group of rows less than or equal to the
        max_group_size that are flanked on either side by the same fuel (This means that
        groups at the beginning or end are excluded). This function currently includes
        an exception for rows that are less than or equal to the max_group_size that are
        at the beginning of the group (i.e. the earliest reporting year) in the list of
        outliers. Outlier groups are then flipped to match the fuel type on either side
        of the group (or in the case of the outliers at the front--flipped to match the
        fuel that succeeds them).

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
            # Find out the fuel that appears the most
            fuel_swap_df['val_count'].max()  # is this doing anything?
            # Create a list of the index values of the mini group that are under the
            # desired max group limit
            size_limit_no_first_last = list(
                fuel_swap_df[fuel_swap_df['val_count2'] < max_group_size + 1].index)
            # Create a list of the values under this size limit that are also flanked by
            # the same fuel
            outlier_list = [x for x in size_limit_no_first_last if fuel_swap_df.iloc[x + 1]
                            ['fuel'] == fuel_swap_df.iloc[x - 1]['fuel']]
            # Create a list of index values in the mini df that are under the desired
            # max group limit and are at the beginning or end of the list (diff between
            # val_count and val_count2).
            size_limit_with_first = list(
                fuel_swap_df[fuel_swap_df['val_count'] < max_group_size + 1].index)
            # If there is a group under the max desired group size that is also at the
            # beginning of the group AND it is followed by a group that exceeds the max
            # group size, then add that group's index (always 0) to the outlier_list
            # & (fuel_swap_df.iloc[1]['val_count'] > (max_group_size + 1)):
            if (0 in size_limit_with_first) & (1 in fuel_swap_df.index):
                if fuel_swap_df.iloc[1].val_count > (max_group_size + 1):
                    outlier_list = outlier_list + [0]
            # As long as the list of outliers is more than 0:
            while len(outlier_list) > 0:
                for outlier in outlier_list:
                    # Identify the flanking fuel you'll replace the outliers with
                    # print(fuel_swap_df.index)
                    swap_fuel = fuel_swap_df.iloc[outlier + 1]['fuel']
                    # Replace the outliers with the flank fuel
                    df.loc[df.index.isin(group.groups[outlier + 1]),
                           flip_col] = swap_fuel
                    # Get rid of None values that are a relic of the switch() function in
                    # the create groups function
                    df[f'{flip_col}'] = df[f'{flip_col}'].replace({None: np.nan})
                    # Reset the group, fuel_swap, and outlier lists
                    group, fuel_swap_df = create_groups(df, flip_col)
                    size_limit_no_first_last = list(
                        fuel_swap_df[fuel_swap_df['val_count2'] < max_group_size + 1].index)
                    outlier_list = [x for x in size_limit_no_first_last if fuel_swap_df.iloc[x + 1]
                                    ['fuel'] == fuel_swap_df.iloc[x - 1]['fuel']]
                    size_limit_with_first = list(
                        fuel_swap_df[fuel_swap_df['val_count'] < max_group_size + 1].index)
                    if (0 in size_limit_with_first) & (1 in fuel_swap_df.index):
                        if fuel_swap_df.iloc[1].val_count > (max_group_size + 1):
                            outlier_list = outlier_list + [0]
                    break  # Break the loop so that you can see if the most recent flip changed the outliers
                    # CHECK THAT THIS BREAK DOES WHAT IT IS SUPPOSED TO DO IN ALL CASES!

            return df  # df

    # Create a flipped fuel column to keep track of changes for flagging purposes and
    # run the fuel_flipper function on the ferc plant groups
    out_df = (
        df.assign(multi_fuel_flip=df.primary_fuel)
        .groupby(['plant_id_ferc1'])
        .apply(lambda x: flip_fuel_outliers(x, 'multi_fuel_flip', 6))
    )
    # We can't use the same flagging mechanisms as the other functions because the
    # output is slightly different. This adds a flag to the flipped rows.
    out_df.loc[(out_df['multi_fuel_flip'].notna())
               & (out_df['multi_fuel_flip'] != out_df['primary_fuel']),
               'primary_fuel_flag'] = flag13
    # Add the flipped values to the primary_fuel column
    out_df['primary_fuel'].update(out_df['multi_fuel_flip'])
    out_df.drop(columns='multi_fuel_flip')

    # this number should be the same because no NA values are filled
    show_unfilled_rows(out_df, 'primary_fuel')

    return out_df


def show_year_outliers(df):
    """Check for fuel outliers that are not consistent over multiple years.

    This function displays the problem plants. In order to work it must
    be fed a dataframe where primary_fuel has no NA values. I recommend
    temporarily changing them to string value 'unknown' as done in
    flip_single_outliers_by_capacity.

    The output dataframe contains a row for each plant that does not comply with the
    rules set below. These are fuel_appearances < 2 and total_appearances > 1.
    fuel_appearances indicates the number of times that a fuel group appears over the
    course of the plant id reporting. Currently, this function selects rows that
    have ONE outlier. For example, most years a plant id reports rows for gas and coal
    however one year it reports gas and oil. The gas and oil year would be considered a
    deviant year and would show up in the output table. total_appearances indicates
    the number of years a plant has reported for. If there is one outlier but only three
    reporting years then it doesn't say much about the outlier. If there are many years
    then it's more likely to be an outlier. unique_fuel_groups indicates the number of
    groups that appear over the course of a plant's reporting life. For example, if a
    plant consistently reports gas and coal plants but one year reports gas and oil
    plants then their unique_fuel_groups would be two.
    """
    # Look at the number of fuels in a given year per plant id pudl
    df = df.assign(primary_fuel=lambda x: x.primary_fuel.fillna('unknown'))

    fuel_count = (
        df.groupby(['report_year', 'plant_id_pudl'])
        .apply(lambda x: ', '.join(x.primary_fuel.sort_values().unique()))
        .reset_index()
        .rename(columns={0: 'unique_fuels'})
        .assign(new_year=lambda x: x.groupby(
            'plant_id_pudl')['report_year'].apply(lambda x: x == x.max())))

    # Check how consistent these fuel type totals are across years
    fuel_appearances = (
        fuel_count.groupby(['plant_id_pudl', 'unique_fuels']).size()
        .reset_index()
        .rename(columns={0: 'fuel_appearances'})
        .assign(
            total_appearances=lambda x: x.groupby(
                'plant_id_pudl')['fuel_appearances'].transform(lambda x: x.sum()),
            unique_fuel_groups=lambda x: x.groupby(
                'plant_id_pudl')['fuel_appearances'].transform('count')))

    # Only show instances where the a fuel or fuel pairing only appears in one year.
    # Also show that year.
    low_appearances = fuel_appearances.query(
        "fuel_appearances < 2 and total_appearances > 1")
    out_df = pd.merge(fuel_count, low_appearances, on=[
        'plant_id_pudl', 'unique_fuels'], how='inner')

    return out_df


def flip_single_outliers_by_capacity(df):
    """Flip single outliers if there is a matching capacity of another fuel type.

    This function starts by checking for outlier fuels. It uses the show_year_outliers()
    function to do this. It then selects for outlier fuels with two unique_fuel_groups,
    meaning that there are two types of fuels reported over the reporting life of a
    plant. For example, a plant reports coal and gas for most years and then gas and oil
    for another year. It also selects for outliers that are not from new_years. This
    is because values from new years are more liekly to relect technology changes than
    reporting errors.

    """
    logger.info("flipping single outliers by capacity")

    def flip_outliers(plant_df):
        """
        Check outlier fuel capacity against other capacities in same pudl id for match.

        This function looks at each pudl id group and checks to see whether that fuel
        was consistently reported as another fuel in other years. We use the capacity_mw
        field to determine if it is a match.

        """
        # Make a sub-dataframe with each of the capacities associated with a fuel and the
        # number of times they appear for a given plant
        val_count_df = (
            plant_df[['primary_fuel', 'capacity_mw']].value_counts().reset_index()
            .rename(columns={0: 'value_count'}))
        # print(val_count_df)

        # Seperate the outlier values from the "conformist" values and make a dictionary
        # of the outliers so it can be iterated over in the event there are more than
        # one.
        outlier_df = val_count_df.query("value_count==1")
        conformist_df = val_count_df.query("value_count>1")
        outlier_dict = dict(zip(outlier_df['primary_fuel'], outlier_df['capacity_mw']))
        # print(outlier_dict)
        for fuel, cap in outlier_dict.items():
            # If a similar capacity exists in conformist_df.capacity_mw.values:
            if len(conformist_df[conformist_df[
                    'capacity_mw'].isin(range(int(cap - 1), int(cap + 2)))]) > 0:
                # Record the fuel type from the conformist df
                flip_fuel = conformist_df[conformist_df[
                    'capacity_mw'].isin(range(int(cap - 1), int(cap + 2)))].primary_fuel.item()
                # Replae the outlier fuel with the conformist fuel
                plant_df.loc[(plant_df['primary_fuel'] == fuel) & (
                    plant_df['capacity_mw'] == cap), 'flip_field'] = flip_fuel

        return plant_df

    # Copy the dataframe so that changes aren't make the the df passed in
    df = df.copy()

    # Create a dataframe with all of the outlier rows from pudl plants
    outlier_preview = show_year_outliers(df)
    outlier_preview = outlier_preview.query(
        "unique_fuel_groups==2 and new_year==False")
    outlier_plants = outlier_preview.plant_id_pudl.unique()

    # Create a sub-df from the original that contains all rows (not just outliers) from
    # the plants with an outlier row indicated. This is so we can look for years with
    # matching capacities that we could use to flip the outlier fuels
    outlier_df = df[df['plant_id_pudl'].isin(outlier_plants)].copy()
    df['flip_field'] = np.nan

    # Flip fuels in the outlier df and then add those flipped values to the full df
    outlier_df = outlier_df.groupby(['plant_id_pudl']).apply(lambda x: flip_outliers(x))
    df.update(outlier_df)

    # Weirdly makes report_year into a float col...so change it back to dt
    # pd.to_datetime(df.report_year, format="%Y").dt.year
    df['report_year'] = df.report_year.astype('int')
    df['plant_id_pudl'] = df.plant_id_pudl.astype('int')

    # Flag these fields and add them to the primary_fuel column
    out_df = (
        df.pipe(add_new_fuel_and_flag, flag14, common_col='primary_fuel',
                new_col='flip_field', overwrite=True))

    return out_df


#######################################################################################
# FUNCTIONS TO SUPPLIMENT PLANT TYPE
#######################################################################################


def fill_obvious_names_plant(df):
    """Blah."""
    logger.info('filling plants with obvious names')
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

    show_unfilled_rows(df, 'plant_type')

    return df


def manually_add_plant(df):
    """Blah."""
    logger.info('filling in manually mapped plant types')

    out_df = (
        # df.assign(fuel_type=lambda x: x.primary_fuel)
        df.pipe(add_manual_values, 'plant_type_manual',
                '/Users/aesharpe/Desktop/fill_plant_type.xlsx'))

    out_df['plant_type'].update(out_df['plant_type_manual'])
    return out_df


#######################################################################################
# FUNCTIONS TO PULL IT ALL TOGETHER
#######################################################################################


def impute_fuel_type(df, pudl_out):
    """Blah."""
    logger.info("**** ADDING FUEL TYPES ****")
    fbp = pudl_out.fbp_ferc1()
    fbp_small = fbp[ferc_merge_cols + ['primary_fuel_by_mmbtu', 'primary_fuel_by_cost']]
    gens = pudl_out.gens_eia860()
    fuel = pudl_out.fuel_ferc1()

    common_col = 'primary_fuel'

    out_df = (
        df.pipe(_drop_bad_plants, bad_plant_ids)
        .pipe(fill_obvious_names_fuel, common_col, flag1)
        .pipe(primary_fuel_by_mmbtu, fbp_small)
        .pipe(eia_one_reported_fuel, gens)
        .pipe(primary_fuel_by_cost)
        .pipe(raw_ferc1_fuel, fuel)
        .pipe(ferc1_id_has_one_fuel)
        .pipe(pudl_id_has_one_fuel)
        .pipe(manually_add_fuel)
        .drop_duplicates()
        .pipe(_fbfill, common_col)
        .pipe(flip_one_outlier_all, 'plant_id_ferc1')
        .pipe(flip_fuel_outliers_all, max_group_size=7)
        .pipe(flip_single_outliers_by_capacity))

    # Check the primary_fuel column and make sure it contains expected fuel values
    if len(bad_fuels := [
            fuel for fuel in out_df.primary_fuel if fuel not in expected_fuels]) == 0:
        print(f'found unexpected fuel types: {list(set(bad_fuels))}')

    return out_df


def impute_plant_type(df):
    """Blah."""
    logger.info('**** ADDING PLANT TYPES ****')
    df['plant_type'] = df['plant_type'].replace({'unknown': np.nan})

    out_df = (
        df.pipe(fill_obvious_names_plant)
        .pipe(manually_add_plant))

    return out_df


def impute_tech_desc(df, eia_df):
    """Blah."""
    logger.info('**** ADDING TECH TYPES ****')
    eia_df = eia_df.assign(report_year=lambda x: x.report_date.dt.year)
    eia_one_tech, plant_id_tech_dict = get_tech_descrip_from_eia(eia_df)

    # common_col = 'tech_desc'

    out_df = (
        df.assign(tech_desc=np.nan)
        .pipe(merge_with_eia_tech_desc, eia_one_tech, plant_id_tech_dict)
        .pipe(backfill_tech_desc_by_year, eia_df)
        # .pipe(fill_obvious_names_fuel, common_col, td_flag4)
        .pipe(fuel_plant_type_to_tech)
        .pipe(map_tech_desc))

    # Check tech_desc column for any outlier technology types
    if len(bad_techs := [
            tech for tech in out_df.tech_desc if tech not in rmi_tech_desc.keys()]) == 0:
        print(f'found unexpected technology types: {list(bad_techs)}')

    return out_df
