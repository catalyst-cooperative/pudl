# pudl.transform.ferccid

Clean the FERC Company Identifier table.

## Functions

| [`clean_cid_string_cols`](#pudl.transform.ferccid.clean_cid_string_cols)(→ pandas.Series)                | Clean string columns: remove unicode, strip whitespace, enforce single spaces, standardize NAs.   |
|----------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------|
| [`core_ferc__entity_companies`](#pudl.transform.ferccid.core_ferc__entity_companies)(→ pandas.DataFrame) | Clean the FERC Company Identifier table.                                                          |

## Module Contents

### pudl.transform.ferccid.clean_cid_string_cols(col: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)) → [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)

Clean string columns: remove unicode, strip whitespace, enforce single spaces, standardize NAs.

### pudl.transform.ferccid.core_ferc_\_entity_companies(raw_ferc_\_entity_companies: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Clean the FERC Company Identifier table.
