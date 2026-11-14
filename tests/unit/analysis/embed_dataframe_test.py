"""Unit tests for the transform steps in pudl.analysis.record_linkage.embed_dataframe."""

import pandas as pd
import pytest

from pudl.analysis.record_linkage.embed_dataframe import NameCleaner


@pytest.mark.parametrize("num_records", [1, 3])
def test_name_cleaner_returns_series_for_vectorizers_by_default(num_records):
    """Vectorizers need a Series of names, even when there is only one record."""
    df = pd.DataFrame({"name": ["Acme Inc.", "Globex LLC", "Initech"][:num_records]})
    cleaned = NameCleaner().as_transformer().transform(df)
    assert isinstance(cleaned, pd.Series)
    assert len(cleaned) == num_records


def test_name_cleaner_returns_dataframe_when_asked():
    """The cleaning pipeline needs a dataframe with the column of the names."""
    df = pd.DataFrame({"name": ["Acme Inc.", "Globex LLC"]})
    cleaned = NameCleaner(return_as_dframe=True).as_transformer().transform(df)
    assert isinstance(cleaned, pd.DataFrame)
    assert list(cleaned.columns) == ["name"]
    as_series = NameCleaner().as_transformer().transform(df)
    assert isinstance(as_series, pd.Series)
    assert as_series.equals(cleaned["name"])
