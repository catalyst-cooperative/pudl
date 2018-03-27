"""Routines specific to cleaning up EIA Form 923 data."""

import pandas as pd
import pudl.constants as pc


def plants(eia_transformed_dfs,
           entities_dfs,
           verbose=True):
    """Compiling plant entities."""
    plants_compiled = pd.DataFrame(columns=['plant_id_eia', 'report_date'])

    if verbose:
        print("Compiling plants for entity tables from:")
    for key, value in eia_transformed_dfs.items():

        df = value
        if (df.columns.contains('report_date') &
                df.columns.contains('plant_id_eia')):
            if verbose:
                print("    {}...".format(key))
            df = df[['plant_id_eia', 'report_date']]
            plants_compiled = plants_compiled.append(df)
    plants_compiled['report_date'] = plants_compiled['report_date'].dt.year
    plants_compiled = plants_compiled.drop_duplicates()
    plants_compiled.sort_values(['report_date', 'plant_id_eia'],
                                inplace=True, ascending=False)
    year = plants_compiled['report_date']
    plants_compiled['report_date'] = \
        pd.to_datetime({'year': year,
                        'month': 1,
                        'day': 1})

    plants_annual = plants_compiled
    plants = plants_compiled.drop(['report_date'], axis=1)
    plants = plants.astype(int)
    plants = plants.drop_duplicates(subset=['plant_id_eia'])

    eia_transformed_dfs['plants_annual_eia'] = plants_annual
    entities_dfs['plants_entity_eia'] = plants

    return(entities_dfs, eia_transformed_dfs)


def generators(eia_transformed_dfs,
               entities_dfs,
               verbose=True):
    """Compiling generator entities."""
    gens_compiled = pd.DataFrame(columns=['plant_id_eia',
                                          'generator_id',
                                          'report_date'])

    if verbose:
        print("Compiling generators for entity tables from:")
    for key, value in eia_transformed_dfs.items():

        df = value
        if (df.columns.contains('report_date') &
            df.columns.contains('plant_id_eia') &
                df.columns.contains('generator_id')):
            if verbose:
                print("    {}...".format(key))
            df = df[['plant_id_eia', 'generator_id', 'report_date']]
            gens_compiled = gens_compiled.append(df)
    gens_compiled['report_date'] = gens_compiled['report_date'].dt.year
    gens_compiled.sort_values(['report_date', 'plant_id_eia', 'generator_id'],
                              inplace=True, ascending=False)
    year = gens_compiled['report_date']
    gens_compiled['report_date'] = \
        pd.to_datetime({'year': year,
                        'month': 1,
                        'day': 1})

    gens_compiled['plant_id_eia'] = gens_compiled.plant_id_eia.astype(int)
    gens_compiled['generator_id'] = gens_compiled.generator_id.astype(str)
    gens_compiled = gens_compiled.drop_duplicates()
    gens_annual = gens_compiled
    gens = gens_compiled.drop(['report_date'], axis=1)
    gens = gens.drop_duplicates(subset=['plant_id_eia', 'generator_id'])

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
    entities_dfs = {}

    if verbose:
        print("Transforming entity tables from EIA:")
    for table in eia_transform_functions.keys():
        if table in entity_tables:
            if verbose:
                print("    {}...".format(table))
            eia_transform_functions[table](eia_transformed_dfs,
                                           entities_dfs,
                                           verbose=verbose)

    return(entities_dfs, eia_transformed_dfs)
