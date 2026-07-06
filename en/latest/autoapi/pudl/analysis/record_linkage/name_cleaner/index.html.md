# pudl.analysis.record_linkage.name_cleaner

This module contains the implementation of CompanyNameCleaner class from OS-Climate’s financial-entity-cleaner package.

## Attributes

| [`logger`](#pudl.analysis.record_linkage.name_cleaner.logger)                                           |    |
|---------------------------------------------------------------------------------------------------------|----|
| [`CLEANING_RULES_DICT`](#pudl.analysis.record_linkage.name_cleaner.CLEANING_RULES_DICT)                 |    |
| [`DEFAULT_CLEANING_RULES_LIST`](#pudl.analysis.record_linkage.name_cleaner.DEFAULT_CLEANING_RULES_LIST) |    |
| [`NAME_LEGAL_TERMS_DICT_FILE`](#pudl.analysis.record_linkage.name_cleaner.NAME_LEGAL_TERMS_DICT_FILE)   |    |
| [`NAME_JSON_ENTRY_LEGAL_TERMS`](#pudl.analysis.record_linkage.name_cleaner.NAME_JSON_ENTRY_LEGAL_TERMS) |    |

## Classes

| [`LegalTermLocation`](#pudl.analysis.record_linkage.name_cleaner.LegalTermLocation)   | The location of the legal terms within the name string.   |
|---------------------------------------------------------------------------------------|-----------------------------------------------------------|
| [`Lettercase`](#pudl.analysis.record_linkage.name_cleaner.Lettercase)                 | Allowed cases for output strings.                         |
| [`HandleLegalTerms`](#pudl.analysis.record_linkage.name_cleaner.HandleLegalTerms)     | Whether to leave, remove, or normalize legal terms.       |
| [`CompanyNameCleaner`](#pudl.analysis.record_linkage.name_cleaner.CompanyNameCleaner) | Class to normalize/clean up text based company names.     |

## Functions

| [`_get_legal_terms_dict`](#pudl.analysis.record_linkage.name_cleaner._get_legal_terms_dict)(→ dict[str, list])   |    |
|------------------------------------------------------------------------------------------------------------------|----|

## Module Contents

### pudl.analysis.record_linkage.name_cleaner.logger

### pudl.analysis.record_linkage.name_cleaner.CLEANING_RULES_DICT

### pudl.analysis.record_linkage.name_cleaner.DEFAULT_CLEANING_RULES_LIST *= ['remove_word_the_from_the_end', 'remove_word_the_from_the_beginning',...*

### pudl.analysis.record_linkage.name_cleaner.NAME_LEGAL_TERMS_DICT_FILE *= 'us_legal_forms.json'*

### pudl.analysis.record_linkage.name_cleaner.NAME_JSON_ENTRY_LEGAL_TERMS *= 'legal_forms'*

### *class* pudl.analysis.record_linkage.name_cleaner.LegalTermLocation(\*args, \*\*kwds)

Bases: [`enum.Enum`](https://docs.python.org/3/library/enum.html#enum.Enum)

The location of the legal terms within the name string.

#### AT_THE_END *= 1*

#### ANYWHERE *= 2*

### *class* pudl.analysis.record_linkage.name_cleaner.Lettercase(\*args, \*\*kwds)

Bases: [`enum.Enum`](https://docs.python.org/3/library/enum.html#enum.Enum)

Allowed cases for output strings.

#### LOWER *= 1*

#### TITLE *= 2*

#### UPPER *= 3*

### *class* pudl.analysis.record_linkage.name_cleaner.HandleLegalTerms(\*args, \*\*kwds)

Bases: [`enum.Enum`](https://docs.python.org/3/library/enum.html#enum.Enum)

Whether to leave, remove, or normalize legal terms.

#### NORMALIZE *= 3*

#### LEAVE_AS_IS *= 1*

#### REMOVE *= 2*

### pudl.analysis.record_linkage.name_cleaner.\_get_legal_terms_dict() → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [list](https://docs.python.org/3/library/stdtypes.html#list)]

### *class* pudl.analysis.record_linkage.name_cleaner.CompanyNameCleaner(/, \*\*data: Any)

Bases: [`pydantic.BaseModel`](https://pydantic.dev/docs/validation/latest/api/pydantic/base_model/#pydantic.BaseModel)

Class to normalize/clean up text based company names.

#### cleaning_rules_list *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= ['remove_word_the_from_the_end', 'remove_word_the_from_the_beginning',...*

A list of cleaning rules that the CompanyNameCleaner should apply.

Will be validated to ensure rules comply to allowed cleaning functions.

#### handle_legal_terms *: [HandleLegalTerms](#pudl.analysis.record_linkage.name_cleaner.HandleLegalTerms)*

A flag to indicate how to habndle legal terms.

Options are to remove, normalize, or keep them as is.

#### place_word_the_at_beginning *: [bool](https://docs.python.org/3/library/functions.html#bool)* *= False*

A flag to indicate whether to move ‘the’ to the start of a string.

If True, then if the word ‘the’ appears at the end of a string,
remove it and place ‘the’ at the beginning of the string.

#### remove_unicode *: [bool](https://docs.python.org/3/library/functions.html#bool)* *= False*

Define if unicode characters should be removed from text’s name.

This cleaning rule is treated separated from the regex rules because it
depends on the language of the text’s name. For instance, Russian or
Japanese text’s may contain unicode characters, while Portuguese and
French companies may not.

#### output_lettercase *: [Lettercase](#pudl.analysis.record_linkage.name_cleaner.Lettercase)*

Define the letter case of the cleaning output.

#### legal_term_location *: [LegalTermLocation](#pudl.analysis.record_linkage.name_cleaner.LegalTermLocation)*

Indicates where in the string legal terms are found.

#### remove_accents *: [bool](https://docs.python.org/3/library/functions.html#bool)* *= False*

Flag to indicate whether to remove accents from strings.

If True, replace letters with accents with non-accented ones.

#### legal_terms_dict *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [list](https://docs.python.org/3/library/stdtypes.html#list)]* *= None*

#### \_validate_cleaning_rules() → Self

#### \_apply_regex_rules(col: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series), dict_regex_rules: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]]) → [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)

Applies several cleaning rules based on a custom dictionary.

The dictionary must contain cleaning rules written in regex format.

* **Parameters:**
  * **col** – The column that needs to be cleaned.
  * **dict_regex_rules** – a dictionary of cleaning rules written in regex with the
    format [rule name] : [‘replacement’, ‘regex rule’]
* **Returns:**
  The modified/cleaned column.

#### \_remove_unicode_chars(col: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)) → [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)

Removes unicode characters that are unreadable in ASCII format.

* **Parameters:**
  **col** – series containing unicode characters.
* **Returns:**
  the corresponding input series without unicode characters.

#### \_move_the_to_beginning(col: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)) → [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)

#### \_apply_cleaning_rules(col: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)) → [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)

Apply the cleaning rules from the dictionary of regex rules.

#### \_apply_normalization_of_legal_terms(col: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)) → [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)

Apply the normalization of legal terms according to dictionary of regex rules.

#### \_apply_removal_of_legal_terms(col: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)) → [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)

Remove legal terms from a string.

#### get_clean_data(col: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)) → [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)

Clean names and normalize legal terms.

* **Parameters:**
  **col** – the column that is to be cleaned
* **Returns:**
  A the clean version of the column.

#### apply_name_cleaning(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), return_as_dframe: [bool](https://docs.python.org/3/library/functions.html#bool) = False) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Clean up text names in a dataframe.

* **Parameters:**
  * **df** – the input dataframe that contains the text’s name to be cleaned.
  * **return_as_dframe** – whether to return the cleaned data as a dataframe or
    series. Useful to return as a dataframe if used in a cleaning pipeline
    with no vectorization step after name cleaning. If multiple columns are
    passed in for cleaning then output will be a dataframe regardless of
    this parameter.
* **Returns:**
  A clean version of the input dataframe.
