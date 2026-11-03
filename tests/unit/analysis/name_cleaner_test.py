"""Unit tests for :mod:`pudl.analysis.record_linkage.name_cleaner`."""

import pandas as pd
import pandas.testing as pdt

from pudl.analysis.record_linkage.name_cleaner import CompanyNameCleaner


def test_clean_series_handles_single_record():
    """A one-record Series is cleaned like any other, and stays a Series."""
    cleaner = CompanyNameCleaner(legal_term_location=2)
    cleaned = cleaner.clean_series(pd.Series(["  ACME Inc.  "], name="name"))
    assert isinstance(cleaned, pd.Series)
    assert cleaned.name == "name"
    assert len(cleaned) == 1


def test_clean_dataframe_single_column_matches_clean_series():
    """Cleaning a one-column dataframe cleans that column, keeping index and name."""
    cleaner = CompanyNameCleaner(legal_term_location=2)
    df = pd.DataFrame({"name": ["ACME Inc.", " Globex  LLC"]}, index=[10, 20])
    cleaned = cleaner.clean_dataframe(df)
    assert isinstance(cleaned, pd.DataFrame)
    pdt.assert_frame_equal(cleaned, cleaner.clean_series(df["name"]).to_frame())


def test_clean_dataframe_cleans_every_column():
    """Each column of a multi-column dataframe is cleaned independently."""
    cleaner = CompanyNameCleaner(legal_term_location=2)
    df = pd.DataFrame(
        {"a": ["ACME Inc.", "Initech"], "b": [" Globex  LLC", "Hooli, Inc."]}
    )
    cleaned = cleaner.clean_dataframe(df)
    assert list(cleaned.columns) == ["a", "b"]
    for col in df.columns:
        pdt.assert_series_equal(cleaned[col], cleaner.clean_series(df[col]))
