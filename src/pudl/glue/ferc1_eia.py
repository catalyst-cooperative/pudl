"""
Extract and transform glue tables between FERC Form 1 and EIA 860/923.

FERC1 and EIA report on the same plants and utilities, but have no embedded
connection. We have combed through the FERC and EIA plants and utilities to
generate id's which can connect these datasets. The resulting fields in the PUDL
tables are `plant_id_pudl` and `utility_id_pudl`, respectively. This was done by
hand in a spreadsheet which is in the `package_data/glue` directory. When
mapping plants, we considered a plant a co-located collection of electricity
generation equipment. If a coal plant was converted to a natural gas unit, our
aim was to consider this the same plant. This module simply reads in the mapping
spreadsheet and converts it to a dictionary of dataframes.

Because these mappings were done by hand and for every one of FERC Form 1's
reported plants, we are fairly certain that there are probably some incorrect
or incomplete mappings of plants. If you see a `plant_id_pudl` or
`utility_id_pudl` mapping that you think is incorrect, poke us on github about
it.

A thing to note about using the PUDL id's is that they can change over time. The
spreadsheet uses MAX({all cells above}) to generate the PUDL id's for the first
instance of every plant or utility, so when an id in the spreadsheet is changed,
every id below it is also changed.

Another note about these id's: these id's map our definition of plants, which is
not the most granular level of plant unit. The generators are typically the
smaller, more interesting unit. FERC does not typically report in units
(although it sometimes does), but it does often break up gas units from coal
units. EIA reports on the generator and boiler level. When trying to use these
PUDL id's, consider the granularity that you desire and the potential
implications of using a co-located set of plant infrastructure as an id.
"""

import importlib
import logging

import pandas as pd

import pudl

logger = logging.getLogger(__name__)


def glue(ferc1=False, eia=False):
    """Generates a dictionary of dataframes for glue tables between FERC1, EIA.

    That data is primarily stored in the plant_output and
    utility_output tabs of package_data/glue/mapping_eia923_ferc1.xlsx in the
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

    Args:
        ferc1 (bool): Are we ingesting FERC Form 1 data?
        eia (bool): Are we ingesting EIA data?

    Returns:
        dict: a dictionary of glue table DataFrames
    """
    # ferc glue tables are structurally entity tables w/ foreign key
    # relationships to ferc datatables, so we need some of the eia/ferc 'glue'
    # even when only ferc is ingested into the database.
    if not ferc1 and not eia:
        return

    map_eia_ferc_file = importlib.resources.open_binary(
        'pudl.package_data.glue', 'mapping_eia923_ferc1.xlsx')

    plant_map = pd.read_excel(map_eia_ferc_file, 'plants_output',
                              na_values='', keep_default_na=False,
                              converters={'plant_id_pudl': int,
                                          'plant_name': str,
                                          'utility_id_ferc1': int,
                                          'utility_name_ferc1': str,
                                          'plant_name_ferc': str,
                                          'plant_id_eia': int,
                                          'plant_name_eia': str,
                                          'utility_name_eia': str,
                                          'utility_id_eia': int})

    utility_map = pd.read_excel(map_eia_ferc_file, 'utilities_output',
                                na_values='', keep_default_na=False,
                                converters={'utility_id_pudl': int,
                                            'utility_name': str,
                                            'utility_id_ferc1': int,
                                            'utility_name_ferc1': str,
                                            'utility_id_eia': int,
                                            'utility_name_eia': str})

    # We need to standardize plant names -- same capitalization and no leading
    # or trailing white space... since this field is being used as a key in
    # many cases. This also needs to be done any time plant_name is pulled in
    # from other tables.
    plant_map = pudl.helpers.strip_lower(plant_map, ['plant_name_ferc'])

    plants = plant_map[['plant_id_pudl', 'plant_name']]
    plants = plants.drop_duplicates('plant_id_pudl')

    plants_eia = plant_map[['plant_id_eia',
                            'plant_name_eia',
                            'plant_id_pudl']]
    plants_eia = plants_eia.drop_duplicates(
        'plant_id_eia').rename(columns={'plant_name_eia': 'plant_name', })
    plants_ferc = plant_map[['plant_name_ferc',
                             'utility_id_ferc1',
                             'plant_id_pudl']]
    plants_ferc = plants_ferc.drop_duplicates(['plant_name_ferc',
                                               'utility_id_ferc1']).\
        rename(columns={'plant_name_ferc': 'plant_name'})

    utilities = utility_map[['utility_id_pudl', 'utility_name']]
    utilities = utilities.drop_duplicates('utility_id_pudl')
    utilities_eia = utility_map[['utility_id_eia',
                                 'utility_name_eia',
                                 'utility_id_pudl']]
    utilities_eia = utilities_eia.drop_duplicates('utility_id_eia').dropna(
        subset=['utility_id_eia']).rename(columns={'utility_name_eia': 'utility_name'})

    utilities_ferc = utility_map[['utility_id_ferc1',
                                  'utility_name_ferc1',
                                  'utility_id_pudl']]
    utilities_ferc = utilities_ferc.drop_duplicates(
        'utility_id_ferc1').dropna(subset=['utility_id_ferc1']).\
        rename(columns={'utility_name_ferc1': 'utility_name'})

    # Now we need to create a table that indicates which plants are associated
    # with every utility.

    # These dataframes map our plant_id to FERC respondents and EIA
    # operators -- the equivalents of our "utilities"
    plants_respondents = plant_map[['plant_id_pudl', 'utility_id_ferc1']]
    plants_respondents = plants_respondents.dropna(
        subset=['utility_id_ferc1'])
    plants_operators = plant_map[['plant_id_pudl', 'utility_id_eia']]
    plants_operators = plants_operators.dropna(subset=['utility_id_eia'])

    # Here we treat the dataframes like database tables, and join on the
    # FERC respondent_id and EIA operator_id, respectively.
    utility_plant_ferc1 = pd.merge(utilities_ferc,
                                   plants_respondents,
                                   on='utility_id_ferc1')
    utility_plant_eia923 = pd.merge(utilities_eia,
                                    plants_operators,
                                    on='utility_id_eia')
    # Now we can concatenate the two dataframes, and get rid of all the columns
    # except for plant_id and utility_id (which determine the  utility to plant
    # association), and get rid of any duplicates or lingering NaN values...
    utility_plant_assn = pd.concat([utility_plant_eia923, utility_plant_ferc1],
                                   sort=True)
    utility_plant_assn = utility_plant_assn[
        ['plant_id_pudl', 'utility_id_pudl']].dropna().drop_duplicates()

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

    # if we're not ingesting eia, exclude eia only tables
    if not eia:
        del glue_dfs['utilities_eia']
        del glue_dfs['plants_eia']
    # if we're not ingesting ferc, exclude ferc only tables
    if not ferc1:
        del glue_dfs['utilities_ferc']
        del glue_dfs['plants_ferc']

    return(glue_dfs)
