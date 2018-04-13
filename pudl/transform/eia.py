"""Routines specific to cleaning up EIA Form 923 data."""

import pandas as pd
import pudl.constants as pc


def plants(eia_transformed_dfs,
           entities_dfs,
           verbose=True):
    """Compiling plant entities."""
    # create empty df with columns we want
    plants_compiled = pd.DataFrame(columns=['plant_id_eia', 'report_date'])

    if verbose:
        print("Compiling plants for entity tables from:")
    # for each df in the dtc of transformed dfs
    for table_name, transformed_df in eia_transformed_dfs.items():
        # create a copy of the df to muck with
        df = transformed_df.copy()
        # if the if contains the desired columns the grab those columns
        if (df.columns.contains('report_date') &
                df.columns.contains('plant_id_eia')):
            if verbose:
                print("    {}...".format(table_name))
            df = df[['plant_id_eia', 'report_date']]
            # add those records to the compliation
            plants_compiled = plants_compiled.append(df)
    # strip the month and day from the date so we can have annual records
    plants_compiled['report_date'] = plants_compiled['report_date'].dt.year
    plants_compiled = plants_compiled.drop_duplicates()
    plants_compiled.sort_values(['report_date', 'plant_id_eia'],
                                inplace=True, ascending=False)
    # convert the year back into a date_time object
    year = plants_compiled['report_date']
    plants_compiled['report_date'] = \
        pd.to_datetime({'year': year,
                        'month': 1,
                        'day': 1})

    # create the annual and entity dfs
    plants_annual = plants_compiled.copy()
    plants = plants_compiled.drop(['report_date'], axis=1)
    plants = plants.astype(int)
    plants = plants.drop_duplicates(subset=['plant_id_eia'])
    # insert the annual and entity dfs to their respective dtc
    eia_transformed_dfs['plants_annual_eia'] = plants_annual
    entities_dfs['plants_entity_eia'] = plants

    return(entities_dfs, eia_transformed_dfs)


def generators(eia_transformed_dfs,
               entities_dfs,
               verbose=True):
    """Compiling generator entities."""
    # create empty df with columns we want
    gens_compiled = pd.DataFrame(columns=['plant_id_eia',
                                          'generator_id',
                                          'report_date'])

    if verbose:
        print("Compiling generators for entity tables from:")
    # for each df in the dtc of transformed dfs
    for key, value in eia_transformed_dfs.items():
        # create a copy of the df to muck with
        df = value
        # if the if contains the desired columns the grab those columns
        if (df.columns.contains('report_date') &
            df.columns.contains('plant_id_eia') &
                df.columns.contains('generator_id')):
            if verbose:
                print("    {}...".format(key))
            df = df[['plant_id_eia', 'generator_id', 'report_date']]
            # add those records to the compliation
            gens_compiled = gens_compiled.append(df)
    # strip the month and day from the date so we can have annual records
    gens_compiled['report_date'] = gens_compiled['report_date'].dt.year
    gens_compiled.sort_values(['report_date', 'plant_id_eia', 'generator_id'],
                              inplace=True, ascending=False)
    # convert the year back into a date_time object
    year = gens_compiled['report_date']
    gens_compiled['report_date'] = \
        pd.to_datetime({'year': year,
                        'month': 1,
                        'day': 1})

    # create the annual and entity dfs
    gens_compiled['plant_id_eia'] = gens_compiled.plant_id_eia.astype(int)
    gens_compiled['generator_id'] = gens_compiled.generator_id.astype(str)
    gens_compiled = gens_compiled.drop_duplicates()
    gens_annual = gens_compiled
    gens = gens_compiled.drop(['report_date'], axis=1)
    gens = gens.drop_duplicates(subset=['plant_id_eia', 'generator_id'])
    # insert the annual and entity dfs to their respective dtc
    eia_transformed_dfs['generators_annual_eia'] = gens_annual
    entities_dfs['generators_entity_eia'] = gens

    return(entities_dfs, eia_transformed_dfs)


def transform(eia_transformed_dfs,
              entity_tables=pc.entity_tables,
              verbose=True):
    """Creates dfs for EIA Entity tables"""
    eia_transform_functions = {
        'plants_entity_eia': plants,
        'generators_entity_eia': generators
    }
    # create the empty entities df to fill up
    entities_dfs = {}

    if verbose:
        print("Transforming entity tables from EIA:")
    # for each table, run through the eia transform functions
    for table in eia_transform_functions.keys():
        if table in entity_tables:
            if verbose:
                print("    {}...".format(table))
            eia_transform_functions[table](eia_transformed_dfs,
                                           entities_dfs,
                                           verbose=verbose)

    return(entities_dfs, eia_transformed_dfs)
