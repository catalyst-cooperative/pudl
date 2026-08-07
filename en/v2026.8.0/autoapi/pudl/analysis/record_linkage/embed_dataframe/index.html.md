# pudl.analysis.record_linkage.embed_dataframe

Tools for embedding a DataFrame to create feature matrix for models.

## Attributes

| [`logger`](#pudl.analysis.record_linkage.embed_dataframe.logger)   |    |
|--------------------------------------------------------------------|----|

## Classes

| [`FeatureMatrix`](#pudl.analysis.record_linkage.embed_dataframe.FeatureMatrix)                     | Class to wrap a feature matrix returned from dataframe embedding.                                                                                                                                                                        |
|----------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`TransformStep`](#pudl.analysis.record_linkage.embed_dataframe.TransformStep)                     | TransformStep's can be combined to vectorize one or more columns.                                                                                                                                                                        |
| [`ColumnVectorizer`](#pudl.analysis.record_linkage.embed_dataframe.ColumnVectorizer)               | Define a set of transformations to apply to one or more columns.                                                                                                                                                                         |
| [`TextVectorizer`](#pudl.analysis.record_linkage.embed_dataframe.TextVectorizer)                   | Implement TransformStep for [`sklearn.feature_extraction.text.TfidfVectorizer`](https://scikit-learn.org/stable/modules/generated/sklearn.feature_extraction.text.TfidfVectorizer.html#sklearn.feature_extraction.text.TfidfVectorizer). |
| [`CategoricalVectorizer`](#pudl.analysis.record_linkage.embed_dataframe.CategoricalVectorizer)     | Implement TransformStep for [`sklearn.preprocessing.OneHotEncoder`](https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.OneHotEncoder.html#sklearn.preprocessing.OneHotEncoder).                                     |
| [`NumericalVectorizer`](#pudl.analysis.record_linkage.embed_dataframe.NumericalVectorizer)         | Implement ColumnTransformation for MinMaxScaler.                                                                                                                                                                                         |
| [`NumericalNormalizer`](#pudl.analysis.record_linkage.embed_dataframe.NumericalNormalizer)         | Implement ColumnTransformation for Normalizer.                                                                                                                                                                                           |
| [`ColumnCleaner`](#pudl.analysis.record_linkage.embed_dataframe.ColumnCleaner)                     | Implement ColumnTransformation for cleaning functions.                                                                                                                                                                                   |
| [`NameCleaner`](#pudl.analysis.record_linkage.embed_dataframe.NameCleaner)                         | Implement ColumnTransformation for CompanyNameCleaner.                                                                                                                                                                                   |
| [`FuelTypeFiller`](#pudl.analysis.record_linkage.embed_dataframe.FuelTypeFiller)                   | Fill missing fuel types from another column.                                                                                                                                                                                             |
| [`StringSimilarityScorer`](#pudl.analysis.record_linkage.embed_dataframe.StringSimilarityScorer)   | Vectorize two string columns with Jaro Winkler similarity.                                                                                                                                                                               |
| [`NumericSimilarityScorer`](#pudl.analysis.record_linkage.embed_dataframe.NumericSimilarityScorer) | Vectorize two numeric columns with a similarity score.                                                                                                                                                                                   |

## Functions

| [`log_dataframe_embedder_config`](#pudl.analysis.record_linkage.embed_dataframe.log_dataframe_embedder_config)(embedder_name, ...)            | Log embedder config to mlflow experiment.                                |
|-----------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------|
| [`dataframe_embedder_factory`](#pudl.analysis.record_linkage.embed_dataframe.dataframe_embedder_factory)(name_prefix, vectorizers)            | Return a configured op graph to embed an input dataframe.                |
| [`dataframe_cleaner_factory`](#pudl.analysis.record_linkage.embed_dataframe.dataframe_cleaner_factory)(name_prefix, vectorizers)              | Return a configured op graph to clean an input dataframe.                |
| [`_apply_cleaning_func`](#pudl.analysis.record_linkage.embed_dataframe._apply_cleaning_func)(df[, function_key])                              |                                                                          |
| [`_extract_keyword_from_column`](#pudl.analysis.record_linkage.embed_dataframe._extract_keyword_from_column)(→ pandas.Series)                 | Extract keywords contained in a Pandas series with a regular expression. |
| [`_fill_fuel_type_from_name`](#pudl.analysis.record_linkage.embed_dataframe._fill_fuel_type_from_name)(→ pandas.DataFrame)                    | Impute missing fuel type data from a name column.                        |
| [`_apply_string_similarity_func`](#pudl.analysis.record_linkage.embed_dataframe._apply_string_similarity_func)(df, function_key, col1, col2)  |                                                                          |
| [`_apply_numeric_similarity_func`](#pudl.analysis.record_linkage.embed_dataframe._apply_numeric_similarity_func)(df, function_key, col1, ...) |                                                                          |

## Module Contents

### pudl.analysis.record_linkage.embed_dataframe.logger

### *class* pudl.analysis.record_linkage.embed_dataframe.FeatureMatrix

Class to wrap a feature matrix returned from dataframe embedding.

Depending on the transformations applied, a feature matrix may be sparse or dense
matrix. Using this wrapper enables Dagsters type checking while allowing both dense
and sparse matrices underneath.

#### matrix *: [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray) | [scipy.sparse.csr_matrix](https://docs.scipy.org/doc/scipy/reference/generated/scipy.sparse.csr_matrix.html#scipy.sparse.csr_matrix)*

#### index *: [pandas.Index](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Index.html#pandas.Index)*

### *class* pudl.analysis.record_linkage.embed_dataframe.TransformStep(/, \*\*data: Any)

Bases: [`pydantic.BaseModel`](https://pydantic.dev/docs/validation/latest/api/pydantic/base_model/#pydantic.BaseModel), [`abc.ABC`](https://docs.python.org/3/library/abc.html#abc.ABC)

TransformStep’s can be combined to vectorize one or more columns.

This class defines a very simple interface for TransformStep’s, which essentially
says that a TransformStep should take configuration and implement the method as_transformer.

#### name *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### *abstractmethod* as_transformer() → [sklearn.base.BaseEstimator](https://scikit-learn.org/stable/modules/generated/sklearn.base.BaseEstimator.html#sklearn.base.BaseEstimator)

This method should use configuration to produce a [`sklearn.base.BaseEstimator`](https://scikit-learn.org/stable/modules/generated/sklearn.base.BaseEstimator.html#sklearn.base.BaseEstimator).

### *class* pudl.analysis.record_linkage.embed_dataframe.ColumnVectorizer(/, \*\*data: Any)

Bases: [`pydantic.BaseModel`](https://pydantic.dev/docs/validation/latest/api/pydantic/base_model/#pydantic.BaseModel)

Define a set of transformations to apply to one or more columns.

#### transform_steps *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[TransformStep](#pudl.analysis.record_linkage.embed_dataframe.TransformStep)]*

#### weight *: [float](https://docs.python.org/3/library/functions.html#float)* *= 1.0*

#### columns *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]*

#### as_pipeline()

Return [`sklearn.pipeline.Pipeline`](https://scikit-learn.org/stable/modules/generated/sklearn.pipeline.Pipeline.html#sklearn.pipeline.Pipeline) with configuration.

#### as_config_dict()

Return config dict formatted for logging to mlflow.

### pudl.analysis.record_linkage.embed_dataframe.log_dataframe_embedder_config(embedder_name: [str](https://docs.python.org/3/library/stdtypes.html#str), vectorizers: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [ColumnVectorizer](#pudl.analysis.record_linkage.embed_dataframe.ColumnVectorizer)], experiment_tracker: [pudl.analysis.ml_tools.experiment_tracking.ExperimentTracker](../../ml_tools/experiment_tracking/index.md#pudl.analysis.ml_tools.experiment_tracking.ExperimentTracker))

Log embedder config to mlflow experiment.

### pudl.analysis.record_linkage.embed_dataframe.dataframe_embedder_factory(name_prefix: [str](https://docs.python.org/3/library/stdtypes.html#str), vectorizers: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [ColumnVectorizer](#pudl.analysis.record_linkage.embed_dataframe.ColumnVectorizer)])

Return a configured op graph to embed an input dataframe.

### pudl.analysis.record_linkage.embed_dataframe.dataframe_cleaner_factory(name_prefix: [str](https://docs.python.org/3/library/stdtypes.html#str), vectorizers: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [ColumnVectorizer](#pudl.analysis.record_linkage.embed_dataframe.ColumnVectorizer)])

Return a configured op graph to clean an input dataframe.

### *class* pudl.analysis.record_linkage.embed_dataframe.TextVectorizer(/, \*\*data: Any)

Bases: [`TransformStep`](#pudl.analysis.record_linkage.embed_dataframe.TransformStep)

Implement TransformStep for [`sklearn.feature_extraction.text.TfidfVectorizer`](https://scikit-learn.org/stable/modules/generated/sklearn.feature_extraction.text.TfidfVectorizer.html#sklearn.feature_extraction.text.TfidfVectorizer).

#### name *: [str](https://docs.python.org/3/library/stdtypes.html#str)* *= 'tfidf_vectorizer'*

#### options *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)*

#### as_transformer()

Return configured TfidfVectorizer.

### *class* pudl.analysis.record_linkage.embed_dataframe.CategoricalVectorizer(/, \*\*data: Any)

Bases: [`TransformStep`](#pudl.analysis.record_linkage.embed_dataframe.TransformStep)

Implement TransformStep for [`sklearn.preprocessing.OneHotEncoder`](https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.OneHotEncoder.html#sklearn.preprocessing.OneHotEncoder).

#### name *: [str](https://docs.python.org/3/library/stdtypes.html#str)* *= 'one_hot_encoder_vectorizer'*

#### options *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)*

#### as_transformer()

Return configured OneHotEncoder.

### *class* pudl.analysis.record_linkage.embed_dataframe.NumericalVectorizer(/, \*\*data: Any)

Bases: [`TransformStep`](#pudl.analysis.record_linkage.embed_dataframe.TransformStep)

Implement ColumnTransformation for MinMaxScaler.

#### name *: [str](https://docs.python.org/3/library/stdtypes.html#str)* *= 'numerical_vectorizer'*

#### options *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)*

#### as_transformer()

Return configured MinMaxScalerConfig.

### *class* pudl.analysis.record_linkage.embed_dataframe.NumericalNormalizer(/, \*\*data: Any)

Bases: [`TransformStep`](#pudl.analysis.record_linkage.embed_dataframe.TransformStep)

Implement ColumnTransformation for Normalizer.

#### name *: [str](https://docs.python.org/3/library/stdtypes.html#str)* *= 'numerical_normalizer'*

#### options *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)*

#### as_transformer()

Return configured NormalizerConfig.

### pudl.analysis.record_linkage.embed_dataframe.\_apply_cleaning_func(df, function_key: [str](https://docs.python.org/3/library/stdtypes.html#str) = None)

### *class* pudl.analysis.record_linkage.embed_dataframe.ColumnCleaner(/, \*\*data: Any)

Bases: [`TransformStep`](#pudl.analysis.record_linkage.embed_dataframe.TransformStep)

Implement ColumnTransformation for cleaning functions.

#### name *: [str](https://docs.python.org/3/library/stdtypes.html#str)* *= 'column_cleaner'*

#### cleaning_function *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### as_transformer()

Return configured NormalizerConfig.

### *class* pudl.analysis.record_linkage.embed_dataframe.NameCleaner(/, \*\*data: Any)

Bases: [`TransformStep`](#pudl.analysis.record_linkage.embed_dataframe.TransformStep)

Implement ColumnTransformation for CompanyNameCleaner.

#### name *: [str](https://docs.python.org/3/library/stdtypes.html#str)* *= 'name_cleaner'*

#### company_cleaner *: [pudl.analysis.record_linkage.name_cleaner.CompanyNameCleaner](../name_cleaner/index.md#pudl.analysis.record_linkage.name_cleaner.CompanyNameCleaner)*

#### return_as_dframe *: [bool](https://docs.python.org/3/library/functions.html#bool)* *= False*

#### as_transformer()

Return configured CompanyNameCleaner.

### *class* pudl.analysis.record_linkage.embed_dataframe.FuelTypeFiller(/, \*\*data: Any)

Bases: [`TransformStep`](#pudl.analysis.record_linkage.embed_dataframe.TransformStep)

Fill missing fuel types from another column.

#### name *: [str](https://docs.python.org/3/library/stdtypes.html#str)* *= 'fuel_type_filler'*

#### fuel_type_col *: [str](https://docs.python.org/3/library/stdtypes.html#str)* *= 'fuel_type_code_pudl'*

#### name_col *: [str](https://docs.python.org/3/library/stdtypes.html#str)* *= 'plant_name'*

#### as_transformer()

Return configured FuelTypeFiller.

### pudl.analysis.record_linkage.embed_dataframe.\_extract_keyword_from_column(ser: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series), keyword_list: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]) → [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)

Extract keywords contained in a Pandas series with a regular expression.

### pudl.analysis.record_linkage.embed_dataframe.\_fill_fuel_type_from_name(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), fuel_type_col: [str](https://docs.python.org/3/library/stdtypes.html#str), name_col: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Impute missing fuel type data from a name column.

If a missing fuel type code is contained in the plant name,
fill in the fuel type code PUDL for that record. E.g. “Washington Hydro”

### pudl.analysis.record_linkage.embed_dataframe.\_apply_string_similarity_func(df, function_key: [str](https://docs.python.org/3/library/stdtypes.html#str), col1: [str](https://docs.python.org/3/library/stdtypes.html#str), col2: [str](https://docs.python.org/3/library/stdtypes.html#str))

### *class* pudl.analysis.record_linkage.embed_dataframe.StringSimilarityScorer(/, \*\*data: Any)

Bases: [`TransformStep`](#pudl.analysis.record_linkage.embed_dataframe.TransformStep)

Vectorize two string columns with Jaro Winkler similarity.

#### name *: [str](https://docs.python.org/3/library/stdtypes.html#str)* *= 'string_sim'*

#### metric *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### col1 *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### col2 *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### as_transformer()

Return configured Jaro Winkler similarity function.

### pudl.analysis.record_linkage.embed_dataframe.\_apply_numeric_similarity_func(df, function_key: [str](https://docs.python.org/3/library/stdtypes.html#str), col1: [str](https://docs.python.org/3/library/stdtypes.html#str), col2: [str](https://docs.python.org/3/library/stdtypes.html#str), scale: [float](https://docs.python.org/3/library/functions.html#float), offset: [float](https://docs.python.org/3/library/functions.html#float), origin: [float](https://docs.python.org/3/library/functions.html#float), missing_value: [float](https://docs.python.org/3/library/functions.html#float), label: [str](https://docs.python.org/3/library/stdtypes.html#str))

### *class* pudl.analysis.record_linkage.embed_dataframe.NumericSimilarityScorer(/, \*\*data: Any)

Bases: [`TransformStep`](#pudl.analysis.record_linkage.embed_dataframe.TransformStep)

Vectorize two numeric columns with a similarity score.

If two values are the same the similarity is 1 and in case of complete
disagreement it is 0. The implementation is adapted from the recordlinkage
Python package Numeric comparison library and is similar with numeric
comparing in ElasticSearch, a full-text search tool.

* **Parameters:**
  * **name** – The name of the transformation step. Default is numeric_sim.
  * **col1** – The name of the first column to compare. Must be a numeric column.
  * **col2** – The name of the second column to compare. Must be a numeric column.
  * **output_name** – The name of the output Series of compared values.
  * **method** – The metric used. Options are “exponential”, “linear”, “exact”.
  * **scale** – The rate of decay, how quickly the score should drop the further
    from the origin that a value lies. Default is 1.0.
  * **offset** – Setting a nonzero offset expands the central point to cover a
    range of values instead of just the single point specified by the
    origin. Default is 0.
  * **origin** – The central point, or the best possible value for the difference
    between records. Differences that fall at the origin will get a
    similarity score of 1.0. The default is 0.
  * **missing_value** – The value if one or both records have a missing value on the
    compared field. Default 0.

#### name *: [str](https://docs.python.org/3/library/stdtypes.html#str)* *= 'numeric_sim'*

#### col1 *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### col2 *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### output_name *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### method *: [str](https://docs.python.org/3/library/stdtypes.html#str)* *= 'linear'*

#### scale *: [float](https://docs.python.org/3/library/functions.html#float)* *= 1.0*

#### offset *: [float](https://docs.python.org/3/library/functions.html#float)* *= 0.0*

#### origin *: [float](https://docs.python.org/3/library/functions.html#float)* *= 0.0*

#### missing_value *: [float](https://docs.python.org/3/library/functions.html#float)* *= 0.0*

#### as_transformer()

Return configured exponential similarity function.
