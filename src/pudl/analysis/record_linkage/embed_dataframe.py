"""Tools for embedding a DataFrame to create feature matrix for models."""
from abc import ABC, abstractmethod
from dataclasses import dataclass

import numpy as np
import pandas as pd
import scipy
from dagster import graph, op
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

    def as_transformer(self):
        """Return configured MinMaxScalerConfig."""
        return MinMaxScaler()


class NumericalNormalizer(TransformStep):
    """Implement ColumnTransformation for Normalizer."""

    name: str = "numerical_normalizer"

    def as_transformer(self):
        """Return configured NormalizerConfig."""
        return Normalizer()


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
