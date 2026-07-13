# pudl.glue.ferc714

Extract and transform glue tables between FERC Form 714’s CSV and XBRL raw sources.

## Attributes

| [`logger`](#pudl.glue.ferc714.logger)                             |                                                       |
|-------------------------------------------------------------------|-------------------------------------------------------|
| [`RESP_ID_FERC_MAP_CSV`](#pudl.glue.ferc714.RESP_ID_FERC_MAP_CSV) | Path to the PUDL ID mapping sheet with the plant map. |

## Functions

| [`get_respondent_map_ferc714`](#pudl.glue.ferc714.get_respondent_map_ferc714)(→ pandas.DataFrame)   | Read in the manual CSV to XBRL FERC714 respondent mapping data.         |
|-----------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------|
| [`glue`](#pudl.glue.ferc714.glue)(→ dict[str)                                                       | Make the FERC 714 glue tables out of stored CSVs of association tables. |

## Module Contents

### pudl.glue.ferc714.logger

### pudl.glue.ferc714.RESP_ID_FERC_MAP_CSV

Path to the PUDL ID mapping sheet with the plant map.

### pudl.glue.ferc714.get_respondent_map_ferc714() → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Read in the manual CSV to XBRL FERC714 respondent mapping data.

### pudl.glue.ferc714.glue() → dict[str:pd.DataFrame]

Make the FERC 714 glue tables out of stored CSVs of association tables.

This function was mirrored off of ferc1_eia.glue, but is much more
paired down.
