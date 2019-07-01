"""
A module for extracting and transforming the glue tables between FERC Form 1
and EIA (860 and 923).
"""

import os.path
import logging
import pandas as pd
import pudl
import pudl.constants as pc
from pudl.settings import SETTINGS

logger = logging.getLogger(__name__)


def glue(eia923_years,
         eia860_years,
         ferc1_years):
    """
    Generates a dictionary of dataframes for glue tables between FERC1 and
    EIA.

    That data is primarily stored in the plant_output and
    utility_output tabs of results/id_mapping/mapping_eia923_ferc1.xlsx in the
    repository. There are a total of seven relations described in this data:

        - utilities: Unique id and name for each utility for use across the
          PUDL DB.
        - plants: Unique id and name for each plant for use across the PUDL DB.
        - utilities_eia: EIA operator ids and names attached to a PUDL
          utility id.
        - plants_eia: EIA plant ids and names attached to a PUDL plant id.
        - utilities_ferc: FERC respondent ids & names attached to a PUDL
          utility id.
        - plants_ferc: A combination of FERC plant names and respondent ids,
          associated with a PUDL plant ID. This is necessary because FERC does
          not provide plant ids, so the unique plant identifier is a
          combination of the respondent id and plant name.
        - utility_plant_assn: An association table which describes which plants
          have relationships with what utilities. If a record exists in this
          table then combination of PUDL utility id & PUDL plant id does have
          an association of some kind. The nature of that association is
          somewhat fluid, and more scrutiny will likely be required for use in
          analysis.

    Presently, the 'glue' tables are a very basic piece of infrastructure for
    the PUDL DB, because they contain the primary key fields for utilities and
    plants in FERC1.
    """
    # ferc glue tables are structurally entity tables w/ forigen key
    # relationships to ferc datatables, so we need some of the eia/ferc 'glue'
    # even when only ferc is ingested into the database.
    if not ferc1_years:
        return

    map_eia_ferc_file = os.path.join(SETTINGS['pudl_dir'],
                                     'results',
                                     'id_mapping',
                                     'mapping_eia923_ferc1.xlsx')

    plant_map = pd.read_excel(map_eia_ferc_file, 'plants_output',
                              na_values='', keep_default_na=False,
                              converters={'plant_id': int,
                                          'plant_name': str,
                                          'respondent_id_ferc': int,
                                          'respondent_name_ferc': str,
                                          'plant_name_ferc': str,
                                          'plant_id_eia': int,
                                          'plant_name_eia': str,
                                          'operator_name_eia': str,
                                          'operator_id_eia': int})

    utility_map = pd.read_excel(map_eia_ferc_file, 'utilities_output',
                                na_values='', keep_default_na=False,
                                converters={'utility_id': int,
                                            'utility_name': str,
                                            'respondent_id_ferc': int,
                                            'respondent_name_ferc': str,
                                            'operator_id_eia': int,
                                            'operator_name_eia': str})

    # We need to standardize plant names -- same capitalization and no leading
    # or trailing white space... since this field is being used as a key in
    # many cases. This also needs to be done any time plant_name is pulled in
    # from other tables.
    plant_map = pudl.helpers.strip_lower(
        plant_map, columns=['plant_name_ferc'])

    plants = plant_map[['plant_id', 'plant_name']].drop_duplicates('plant_id')\
        .rename(columns={'plant_id': 'plant_id_pudl', 'plant_name': 'name'})

    plants_eia = plant_map[['plant_id_eia',
                            'plant_name_eia',
                            'plant_id_pudl']].drop_duplicates('plant_id_eia').\
        rename(columns={'plant_name_eia': 'plant_name'})
    plants_ferc = plant_map[['plant_name_ferc',
                             'respondent_id_ferc',
                             'plant_id_pudl']].drop_duplicates(
        ['plant_name_ferc', 'respondent_id_ferc']).\
        rename(columns={'respondent_id_ferc': 'utility_id_ferc1',
                        'plant_name_ferc': 'plant_name'})

    utilities = utility_map[['utility_id', 'utility_name']].\
        drop_duplicates('utility_id').rename(
            columns={'utility_id': 'utility_id_pudl',
                     'utility_name': 'name'})
    utilities_eia = utility_map[['operator_id_eia',
                                 'operator_name_eia',
                                 'utility_id_pudl']].\
        drop_duplicates('operator_id_eia').dropna(subset=['operator_id_eia'])

    utilities_ferc = utility_map[['respondent_id_ferc',
                                  'respondent_name_ferc',
                                  'utility_id_pudl']].drop_duplicates(
        'respondent_id_ferc').dropna(subset=['respondent_id_ferc'])

    # Now we need to create a table that indicates which plants are associated
    # with every utility.

    # These dataframes map our plant_id to FERC respondents and EIA
    # operators -- the equivalents of our "utilities"
    plants_respondents = plant_map[['plant_id_pudl', 'respondent_id_ferc']].\
        dropna(subset=['respondent_id_ferc'])
    plants_operators = plant_map[['plant_id', 'operator_id_eia']].\
        dropna(subset=['operator_id_eia'])

    # Here we treat the dataframes like database tables, and join on the
    # FERC respondent_id and EIA operator_id, respectively.
    utility_plant_ferc1 = pd.merge(utilities_ferc,
                                   plants_respondents,
                                   on='respondent_id_ferc')
    utility_plant_eia923 = pd.merge(utilities_eia,
                                    plants_operators,
                                    on='operator_id_eia')
    # Now we can concatenate the two dataframes, and get rid of all the columns
    # except for plant_id and utility_id_pudl (which determine the  utility to
    # plant association), and get rid of any duplicates or lingering NaN values
    utility_plant_assn = pd.concat(
        [utility_plant_eia923, utility_plant_ferc1],
        sort=True)[['plant_id', 'utility_id_pudl']].dropna().drop_duplicates()

    utilities_ferc = utilities_ferc.rename(
        columns={'respondent_id_ferc': 'utility_id_ferc1',
                 'respondent_name_ferc': 'utility_name_ferc1'})

    utilities_eia.rename(columns={'operator_id_eia': 'utility_id_eia',
                                  'operator_name_eia': 'utility_name'})

    # At this point there should be at most one row in each of these data
    # frames with NaN values after we drop_duplicates in each. This is because
    # there will be some plants and utilities that only exist in FERC, or only
    # exist in EIA, and while they will have PUDL IDs, they may not have
    # FERC/EIA info (and it'll get pulled in as NaN)

    for df, df_n in zip([plants_eia, plants_ferc,
                         utilities_eia, utilities_ferc],
                        ['plants_eia', 'plants_ferc',
                         'utilities_eia', 'utilities_ferc']):
        if df[pd.isnull(df).any(axis=1)].shape[0] > 1:
            raise AssertionError(f"FERC to EIA glue breaking in {df_n}")
        df.dropna(inplace=True)

    # Before we start inserting records into the database, let's do some basic
    # sanity checks to ensure that it's (at least kind of) clean.
    # INSERT SANITY HERE

    # Any FERC respondent_id that appears in plants_ferc must also exist in
    # utilities_ferc:
    # INSERT MORE SANITY HERE

    glue_dfs = {'plants': plants,
                'utilities': utilities,
                'utilities_ferc': utilities_ferc,
                'plants_ferc': plants_ferc,
                'utility_plant_assn': utility_plant_assn,
                'utilities_eia': utilities_eia,
                'plants_eia': plants_eia}

    # if we're not ingesting eia, exclude
    if not eia860_years or not eia923_years:
        del glue_dfs['utilities_eia']
        del glue_dfs['plants_eia']

    return(glue_dfs)
