import pandas as pd

from pudl.extract import excel
from pudl.extract.eia860m import append_eia860m


def test_append_eia860m_concats_puerto_rico_rows(mocker):
    """Puerto Rico generator sheets from 860m should be concatenated into mainland pages."""
    # Make ExcelMetadata.get_all_pages deterministic for this test
    mocker.patch.object(
        excel.ExcelMetadata,
        "get_all_pages",
        return_value=["generator_existing", "puerto_rico_generator_existing"],
    )

    # mainland eia860 raw data has one row
    eia860_raw_dfs = {"generator_existing": pd.DataFrame({"foo": [1]})}

    # eia860m provides one mainland row and one Puerto Rico row (both include report_date)
    eia860m_raw_dfs = {
        "generator_existing": pd.DataFrame({"foo": [2], "report_date": ["2018-03"]}),
        "puerto_rico_generator_existing": pd.DataFrame(
            {"foo": [3], "report_date": ["2018-03"]}
        ),
    }

    out = append_eia860m(eia860_raw_dfs=eia860_raw_dfs, eia860m_raw_dfs=eia860m_raw_dfs)

    # Expect the mainland page to contain original + mainland 860m + PR rows
    assert "generator_existing" in out
    assert out["generator_existing"]["foo"].tolist() == [1, 2, 3]
    # The report_date column should be dropped by append_eia860m
    assert "report_date" not in out["generator_existing"].columns


def test_append_eia860m_does_not_expose_puerto_rico_pages(mocker):
    """Puerto Rico pages should not be left as top-level pages in the returned dict."""
    mocker.patch.object(
        excel.ExcelMetadata,
        "get_all_pages",
        return_value=["generator_existing", "puerto_rico_generator_existing"],
    )

    eia860_raw_dfs = {"generator_existing": pd.DataFrame({"foo": [10]})}

    eia860m_raw_dfs = {
        "generator_existing": pd.DataFrame({"foo": [20], "report_date": ["2018-03"]}),
        "puerto_rico_generator_existing": pd.DataFrame(
            {"foo": [30], "report_date": ["2018-03"]}
        ),
    }

    out = append_eia860m(eia860_raw_dfs=eia860_raw_dfs, eia860m_raw_dfs=eia860m_raw_dfs)

    # The returned dict should not contain a top-level puerto_rico page key
    assert not any("puerto_rico" in k for k in out)

    # And the mainland page should have been extended by both mainland and PR rows
    assert out["generator_existing"]["foo"].tolist() == [10, 20, 30]
