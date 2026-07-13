# pudl.scripts.auto_match_utilities

A CLI tool for matching FERC and EIA utilities.

## Attributes

| [`logger`](#pudl.scripts.auto_match_utilities.logger)                             |    |
|-----------------------------------------------------------------------------------|----|
| [`UTILITY_NAME_CLEANER`](#pudl.scripts.auto_match_utilities.UTILITY_NAME_CLEANER) |    |

## Functions

| [`clean_utility_name`](#pudl.scripts.auto_match_utilities.clean_utility_name)(col)                                               | Apply standard cleaning steps to the utility name column.             |
|----------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------|
| [`match_utility_names`](#pudl.scripts.auto_match_utilities.match_utility_names)(eia_df, ferc_df, false_matches)                  | Match FERC and EIA utilities based on their utility names.            |
| [`get_existing_glue_df`](#pudl.scripts.auto_match_utilities.get_existing_glue_df)()                                              | Read in the existing handmade glue spreadsheet.                       |
| [`get_false_matches`](#pudl.scripts.auto_match_utilities.get_false_matches)()                                                    | Read in the existing handmade false matches spreadsheet.              |
| [`drop_records_with_matches`](#pudl.scripts.auto_match_utilities.drop_records_with_matches)(entity, matches_new, ...)            | Drop records in the original dataframe where matches have been found. |
| [`add_new_matches_to_dataframe`](#pudl.scripts.auto_match_utilities.add_new_matches_to_dataframe)(matches_new, existing_glue_df) | Add new matches to existing hand-mapped dataframe.                    |
| [`write_updated_matches`](#pudl.scripts.auto_match_utilities.write_updated_matches)(test_run, dataframe)                         | Write the updated matching spreadsheet to disk.                       |
| [`main`](#pudl.scripts.auto_match_utilities.main)(→ int)                                                                         | Match EIA and FERC utilities based on utility name alone.             |

## Module Contents

### pudl.scripts.auto_match_utilities.logger

### pudl.scripts.auto_match_utilities.UTILITY_NAME_CLEANER

### pudl.scripts.auto_match_utilities.clean_utility_name(col)

Apply standard cleaning steps to the utility name column.

### pudl.scripts.auto_match_utilities.match_utility_names(eia_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), ferc_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), false_matches: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame))

Match FERC and EIA utilities based on their utility names.

We note how many of these records are already matched to one another, and
ignore these matches.

We also ignore matches contained in
src/pudl/package_data/glue/utility_id_pudl_false_matches.csv. These are matches that
we have hand-labelled as incorrect.

### pudl.scripts.auto_match_utilities.get_existing_glue_df()

Read in the existing handmade glue spreadsheet.

### pudl.scripts.auto_match_utilities.get_false_matches()

Read in the existing handmade false matches spreadsheet.

### pudl.scripts.auto_match_utilities.drop_records_with_matches(entity: Literal['ferc1', 'eia'], matches_new: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), existing_glue_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), matches_existing: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), unmatched_existing: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame))

Drop records in the original dataframe where matches have been found.

This takes the original existing_glue_df dataframe, and drops records which were
previously unmatched and have been matched using the automated matching method.
We do this in preparation for adding the new matches to the spreadsheet.

### pudl.scripts.auto_match_utilities.add_new_matches_to_dataframe(matches_new: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), existing_glue_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame))

Add new matches to existing hand-mapped dataframe.

We add new matches and assign them new PUDL utility IDs, following one of the
following scenarios:
1) If the PUDL utility ID already exists in another match, we keep that utility ID
to avoid splitting up existing sets of matches.
2) If the PUDL utility ID doesn’t show up anywhere else (i.e., this record was
previously unmatched to any other utility), we create a new auto-incremented PUDL ID.
3) If the PUDL utility ID was previously matched to both another EIA and another
FERC utility, this poses a challenge that needs to be resolved by hand and we raise
a ValueError.

Once all PUDL utility IDs are assigned, we flag all records where multiple FERC and
multiple EIA utilities are matched together. These are uncommon cases and benefit
from manual review to ensure that no unexpected connections have been created.

### pudl.scripts.auto_match_utilities.write_updated_matches(test_run: [bool](https://docs.python.org/3/library/functions.html#bool), dataframe: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame))

Write the updated matching spreadsheet to disk.

### pudl.scripts.auto_match_utilities.main(test_run: [bool](https://docs.python.org/3/library/functions.html#bool)) → [int](https://docs.python.org/3/library/functions.html#int)

Match EIA and FERC utilities based on utility name alone.
