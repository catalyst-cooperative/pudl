"""Unit tests for :mod:`pudl.analysis.record_linkage.eia_ferc1_record_linkage`."""

import pandas as pd

from pudl.analysis.record_linkage.eia_ferc1_record_linkage import add_null_overrides
from pudl.metadata.classes import Resource

# A real record_id_ferc1 pulled from src/pudl/package_data/glue/eia_ferc1_null.csv
NULL_OVERRIDE_RECORD_ID_FERC1 = "f1_gnrt_plant_2008_12_108_0_5"


def test_add_null_overrides_preserves_condensed_columns():
    """Nulling EIA columns shouldn't wipe out condensed FERC1-derived columns.

    ``report_date``, ``report_year``, ``plant_id_pudl``, and ``utility_id_pudl`` are
    condensed in :func:`prettyify_best_matches` to hold a FERC1-derived value even for
    records with no EIA match. Regression test for a bug where these shared columns
    were included in ``eia_cols_to_null`` and got wiped out for every record in
    ``eia_ferc1_null.csv``, producing spurious NULL report dates/years.
    """
    eia_field_names = Resource.from_id("out_eia__yearly_plant_parts").get_field_names()
    eia_only_col = next(
        col
        for col in eia_field_names
        if col not in {"report_year", "report_date", "plant_id_pudl", "utility_id_pudl"}
    )

    connects_ferc1_eia = pd.DataFrame(
        {
            "record_id_ferc1": [NULL_OVERRIDE_RECORD_ID_FERC1, "some_other_record"],
            "record_id_eia": [pd.NA, "some_eia_record"],
            "report_date": pd.to_datetime(["2008-01-01", "2009-01-01"]),
            "report_year": [2008, 2009],
            "plant_id_pudl": [123, 456],
            "utility_id_pudl": [789, 1011],
            "match_type": ["prediction; not in training data", "correct match"],
            eia_only_col: ["some eia value", "another eia value"],
        }
    )

    result = add_null_overrides(connects_ferc1_eia)

    overridden = result.loc[
        result.record_id_ferc1 == NULL_OVERRIDE_RECORD_ID_FERC1
    ].iloc[0]
    assert overridden.match_type == "overridden"
    assert pd.notna(overridden.report_date)
    assert pd.notna(overridden.report_year)
    assert pd.notna(overridden.plant_id_pudl)
    assert pd.notna(overridden.utility_id_pudl)
    assert pd.isna(overridden[eia_only_col])
