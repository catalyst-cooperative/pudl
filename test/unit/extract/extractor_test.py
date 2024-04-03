import pandas as pd
import pytest
from dagster import build_op_context

from pudl.extract.extractor import concat_pages, partitions_from_settings_factory
from pudl.settings import DatasetsSettings


@pytest.mark.parametrize(
    "dataset, expected_years",
    (
        ("eia860", set(range(2001, 2022))),
        ("eia861", set(range(2001, 2022))),
        ("eia923", set(range(2001, 2022))),
    ),
)
def test_years_from_settings(dataset, expected_years):
    partitions_from_settings = partitions_from_settings_factory(dataset)

    with build_op_context(
        resources={"dataset_settings": DatasetsSettings()}
    ) as context:
        # Assert actual years are a superset of expected. Instead of doing
        # an equality check, this avoids having to update expected years
        # every time a new year is added to the datasets
        settings_dates = [output.value for output in partitions_from_settings(context)]
        assert {record["year"] for record in settings_dates} >= expected_years


def test_concat_pages():
    pages = ["page1", "page2", "page3"]
    dfs_1 = {page: pd.DataFrame({"df": [1], "page": [page]}) for page in pages}
    dfs_2 = {page: pd.DataFrame({"df": [2], "page": [page]}) for page in pages}

    merged_dfs = concat_pages([dfs_1, dfs_2])
    assert list(merged_dfs.keys()) == pages
    for page in pages:
        pd.testing.assert_frame_equal(
            merged_dfs[page],
            pd.DataFrame({"df": [1, 2], "page": [page, page]}, index=[0, 1]),
        )
