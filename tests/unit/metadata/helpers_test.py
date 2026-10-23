"""Unit tests for :mod:`pudl.metadata.helpers`."""

import polars as pl
from shapely.geometry import MultiPolygon, Point, Polygon

from pudl.metadata.helpers import is_valid_wkb


def test_is_valid_wkb() -> None:
    """Parseable WKB and nulls are valid; anything else isn't."""
    polygon = Polygon([(0, 0), (1, 0), (1, 1)])
    values = pl.Series(
        "geometry",
        [
            Point(0, 0).wkb,
            polygon.wkb,
            MultiPolygon([polygon]).wkb,
            None,
            b"not wkb",
            b"",
            polygon.wkb[:20],  # truncated
            Point(0, 0).wkt.encode(),  # WKT is not WKB
        ],
        dtype=pl.Binary,
    )

    result = is_valid_wkb(values)

    assert result.name == "geometry"
    assert result.dtype == pl.Boolean
    assert result.to_list() == [True, True, True, True, False, False, False, False]


def test_is_valid_wkb_empty_series() -> None:
    """An empty Series is trivially valid."""
    assert is_valid_wkb(pl.Series("geometry", [], dtype=pl.Binary)).to_list() == []
