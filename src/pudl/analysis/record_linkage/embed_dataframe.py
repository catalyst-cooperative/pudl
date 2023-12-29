"""Tools for embedding a DataFrame to create feature matrix for models."""
from abc import ABC, abstractmethod
from dataclasses import dataclass

import numpy as np
import pandas as pd
import scipy
from dagster import graph, op
from jellyfish import jaro_winkler_similarity
from pydantic import BaseModel
from sklearn.base import BaseEstimator
from sklearn.compose import ColumnTransformer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import (
    FunctionTransformer,
    MinMaxScaler,
    Normalizer,
    OneHotEncoder,
)

import pudl
from pudl.analysis.record_linkage.name_cleaner import CompanyNameCleaner


@dataclass
class FeatureMatrix:
    """Class to wrap a feature matrix returned from dataframe embedding.

    Depending on the transformations applied, a feature matrix may be sparse or dense
    matrix. Using this wrapper enables Dagsters type checking while allowing both dense
    and sparse matrices underneath.
    """

    matrix: np.ndarray | scipy.sparse.csr_matrix


class TransformStep(BaseModel, ABC):
    """TransformStep's can be combined to vectorize one or more columns.

    This class defines a very simple interface for TransformStep's, which essentially
    says that a TransformStep should take configuration and implement the method as_transformer.
    """

    name: str

    @abstractmethod
    def as_transformer(self) -> BaseEstimator:
        """This method should use configuration to produce a :class:`sklearn.base.BaseEstimator`."""
        ...


class ColumnVectorizer(BaseModel):
    """Define a set of transformations to apply to one or more columns."""

    transform_steps: list[TransformStep]
    weight: float = 1.0
    columns: list[str]

    def as_pipeline(self):
        """Return :class:`sklearn.pipeline.Pipeline` with configuration."""
        return Pipeline(
            [
                (
                    step.name,
                    step.as_transformer(),
                )
                for step in self.transform_steps
            ]
        )


@op
def train_dataframe_embedder_new(
    df: pd.DataFrame, vectorizers: dict[str, ColumnVectorizer]
):
    """Train :class:`sklearn.compose.ColumnTransformer` on input."""
    column_transformer = ColumnTransformer(
        transformers=[
            (name, column_transform.as_pipeline(), column_transform.columns)
            for name, column_transform in vectorizers.items()
        ],
        transformer_weights={
            name: column_transform.weight
            for name, column_transform in vectorizers.items()
        },
    )

    return column_transformer.fit(df)


@op
def apply_dataframe_embedder_new(df: pd.DataFrame, transformer: ColumnTransformer):
    """Use :class:`sklearn.compose.ColumnTransformer` to transform input."""
    return FeatureMatrix(matrix=transformer.transform(df))


def dataframe_embedder_factory(vectorizers: dict[str, ColumnVectorizer]):
    """Return a configured op graph to embed an input dataframe."""

    @op
    def train_dataframe_embedder(df: pd.DataFrame):
        """Train :class:`sklearn.compose.ColumnTransformer` on input."""
        column_transformer = ColumnTransformer(
            transformers=[
                (name, column_transform.as_pipeline(), column_transform.columns)
                for name, column_transform in vectorizers.items()
            ],
            transformer_weights={
                name: column_transform.weight
                for name, column_transform in vectorizers.items()
            },
        )

        return column_transformer.fit(df)

    @op
    def apply_dataframe_embedder(df: pd.DataFrame, transformer: ColumnTransformer):
        """Use :class:`sklearn.compose.ColumnTransformer` to transform input."""
        return FeatureMatrix(matrix=transformer.transform(df))

    @graph
    def embed_dataframe(df: pd.DataFrame) -> FeatureMatrix:
        """Train dataframe embedder and apply to input df."""
        transformer = train_dataframe_embedder(df)
        return apply_dataframe_embedder(df, transformer)

    return embed_dataframe


class TextVectorizer(TransformStep):
    """Implement TransformStep for :class:`sklearn.feature_extraction.text.TfidfVectorizer`."""

    name: str = "tfidf_vectorizer"

    #: See sklearn documentation for all options
    options: dict = {"analyzer": "char", "ngram_range": (2, 10)}

    def as_transformer(self):
        """Return configured TfidfVectorizer."""
        return TfidfVectorizer(**self.options)


class CategoricalVectorizer(TransformStep):
    """Implement TransformStep for :class:`sklearn.preprocessing.OneHotEncoder`."""

    name: str = "one_hot_encoder_vectorizer"

    options: dict = {"categories": "auto"}

    def as_transformer(self):
        """Return configured OneHotEncoder."""
        return OneHotEncoder(**self.options)


class NumericalVectorizer(TransformStep):
    """Implement ColumnTransformation for MinMaxScaler."""

    name: str = "numerical_vectorizer"
    options: dict = {}

    def as_transformer(self):
        """Return configured MinMaxScalerConfig."""
        return MinMaxScaler(**self.options)


class NumericalNormalizer(TransformStep):
    """Implement ColumnTransformation for Normalizer."""

    name: str = "numerical_normalizer"
    options: dict = {}

    def as_transformer(self):
        """Return configured NormalizerConfig."""
        return Normalizer(**self.options)


def _apply_cleaning_func(df, function_key: str = None):
    function_transforms = {
        "null_to_zero": lambda df: df.fillna(value=0.0),
        "null_to_empty_str": lambda df: df.fillna(value=""),
        "fix_int_na": lambda df: pudl.helpers.fix_int_na(df, columns=list(df.columns)),
    }

    return function_transforms[function_key](df)


class ColumnCleaner(TransformStep):
    """Implement ColumnTransformation for cleaning functions."""

    name: str = "column_cleaner"
    cleaning_function: str

    def as_transformer(self):
        """Return configured NormalizerConfig."""
        return FunctionTransformer(
            _apply_cleaning_func, kw_args={"function_key": self.cleaning_function}
        )


class NameCleaner(TransformStep):
    """Implement ColumnTransformation for CompanyNameCleaner."""

    name: str = "name_cleaner"
    company_cleaner: CompanyNameCleaner = CompanyNameCleaner()

    def as_transformer(self):
        """Return configured CompanyNameCleaner."""
        return FunctionTransformer(self.company_cleaner.apply_name_cleaning)


def _apply_string_similarity_func(df, function_key: str, col1: str, col2: str):
    function_transforms = {
        "jaro_winkler": lambda df: df.apply(
            lambda row: jaro_winkler_similarity(row[col1], row[col2]), axis=1
        ).to_frame()
    }

    return function_transforms[function_key](df)


class StringSimilarityScorer(TransformStep):
    """Vectorize two string columns with Jaro Winkler similarity."""

    name: str = "string_sim"
    metric: str
    col1: str
    col2: str

    def as_transformer(self):
        """Return configured Jaro Winkler similarity function."""
        return FunctionTransformer(
            _apply_string_similarity_func,
            kw_args={"function_key": self.metric, "col1": self.col1, "col2": self.col2},
        )


def _apply_numeric_similarity_func(
    df,
    function_key: str,
    col1: str,
    col2: str,
    scale: float,
    offset: float,
    origin: float,
    missing_value: float,
    label: str,
):
    def _exp_sim(df, col1, col2, scale, offset, origin, missing_value, label):
        if offset < 0:
            raise ValueError("The offset must be positive.")
        if scale <= 0:
            raise ValueError("The scale must be larger than 0.")
        d = abs(df[col1] - df[col2] - origin).clip(offset, None)
        return (
            2 ** (-(d - offset) / scale).fillna(missing_value).rename(label).to_frame()
        )

    def _linear_sim(df, col1, col2, scale, offset, origin, missing_value, label):
        if offset < 0:
            raise ValueError("The offset must be positive.")
        if scale <= 0:
            raise ValueError("The scale must be larger than 0.")
        d = (abs(df[col1] - df[col2] - origin)).clip(offset, offset + 2 * scale)
        return 1 - (d - offset) / (2 * scale).fillna(missing_value).to_frame()

    def _exact(df, missing_value, label):
        compare = pd.Series(0, index=df[col1])
        compare[df[col1] == df[col2]] = 1
        if missing_value != 0:
            compare[(df[col1].isnull() | df[col2].isnull())] = missing_value
        return compare.rename(label).to_frame()

    function_transforms = {
        "exponential": lambda df: _exp_sim(
            df, col1, col2, scale, offset, origin, missing_value, label
        ),
        "linear": lambda df: _linear_sim(
            df, col1, col2, scale, offset, origin, missing_value, label
        ),
        "exact": lambda df: _exact(df, missing_value, label),
    }
    return function_transforms[function_key](df)


class NumericSimilarityScorer(TransformStep):
    """Vectorize two numeric columns with an expoential similarity score."""

    # TODO: include docstring about what they parameters are
    name: str = "numeric_sim"
    col1: str
    col2: str
    output_name: str
    method: str = "linear"
    scale: float = 1.0
    offset: float = 0.0
    origin: float = 0.0
    missing_value: float = 0.0

    def as_transformer(self):
        """Return configured exponential similarity function."""
        return FunctionTransformer(
            _apply_numeric_similarity_func,
            kw_args={
                "function_key": self.method,
                "col1": self.col1,
                "col2": self.col2,
                "scale": self.scale,
                "offset": self.offset,
                "origin": self.origin,
                "missing_value": self.missing_value,
                "label": self.output_name,
            },
        )
