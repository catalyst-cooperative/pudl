"""Spatial operations for demand allocation."""
import warnings
from typing import Callable, Iterable, Literal, Union

import geopandas as gpd
import pandas as pd
from shapely.geometry import GeometryCollection, MultiPolygon, Polygon
from shapely.geometry.base import BaseGeometry


def check_gdf(gdf: gpd.GeoDataFrame) -> None:
    """
    Check that GeoDataFrame contains (Multi)Polygon geometries with non-zero area.

    Args:
        gdf: GeoDataFrame.

    Raises:
        TypeError: Object is not a GeoDataFrame.
        AttributeError: GeoDataFrame has no geometry.
        TypeError: Geometry is not a GeoSeries.
        ValueError: Geometry contains null geometries.
        ValueError: Geometry contains non-(Multi)Polygon geometries.
        ValueError: Geometry contains (Multi)Polygon geometries with zero area.
        ValueError: MultiPolygon contains Polygon geometries with zero area.

    Examples:
        >>> poly = Polygon([(0, 0), (0, 1), (1, 1), (1, 0)])
        >>> zero_poly = Polygon([(0, 0), (0, 0), (0, 0), (0, 0)])
        >>> check_gdf(gpd.GeoDataFrame(geometry=[poly]))
        >>> check_gdf(None)
        Traceback (most recent call last):
          ...
        TypeError: Object is not a GeoDataFrame
        >>> check_gdf(gpd.GeoDataFrame({'x': [0]}))
        Traceback (most recent call last):
          ...
        AttributeError: GeoDataFrame has no geometry
        >>> check_gdf(gpd.GeoDataFrame({'geometry': [0]}))
        Traceback (most recent call last):
          ...
        TypeError: Geometry is not a GeoSeries
        >>> check_gdf(gpd.GeoDataFrame(geometry=[GeometryCollection()]))
        Traceback (most recent call last):
          ...
        ValueError: Geometry contains non-(Multi)Polygon geometries
        >>> check_gdf(gpd.GeoDataFrame(geometry=[zero_poly]))
        Traceback (most recent call last):
          ...
        ValueError: Geometry contains (Multi)Polygon geometries with zero area
        >>> check_gdf(gpd.GeoDataFrame(geometry=[MultiPolygon([poly, zero_poly])]))
        Traceback (most recent call last):
          ...
        ValueError: MultiPolygon contains Polygon geometries with zero area
    """
    if not isinstance(gdf, gpd.GeoDataFrame):
        raise TypeError("Object is not a GeoDataFrame")
    if not hasattr(gdf, "geometry"):
        raise AttributeError("GeoDataFrame has no geometry")
    if not isinstance(gdf.geometry, gpd.GeoSeries):
        raise TypeError("Geometry is not a GeoSeries")
    warnings.filterwarnings("ignore", "GeoSeries.isna", UserWarning)
    if gdf.geometry.isna().any():
        raise ValueError("Geometry contains null geometries")
    if not gdf.geometry.geom_type.isin(["Polygon", "MultiPolygon"]).all():
        raise ValueError("Geometry contains non-(Multi)Polygon geometries")
    if not gdf.geometry.area.all():
        raise ValueError("Geometry contains (Multi)Polygon geometries with zero area")
    is_mpoly = gdf.geometry.geom_type == "MultiPolygon"
    for mpoly in gdf.geometry[is_mpoly]:
        for poly in mpoly:
            if not poly.area:
                raise ValueError(
                    "MultiPolygon contains Polygon geometries with zero area"
                )


def polygonize(geom: BaseGeometry) -> Union[Polygon, MultiPolygon]:
    """
    Convert geometry to (Multi)Polygon.

    Args:
        geom: Geometry to convert to (Multi)Polygon.

    Returns:
        Geometry converted to (Multi)Polygon, with all zero-area components removed.

    Raises:
        ValueError: Geometry has zero area.

    Exmaples:
        A collection with one non-zero-area Polygon is returned as a Polygon.

        >>> poly = Polygon([(0, 0), (0, 1), (1, 1), (1, 0)])
        >>> zero_poly = Polygon([(0, 0), (0, 0), (0, 0), (0, 0)])
        >>> geom = GeometryCollection([poly, zero_poly])
        >>> result = polygonize(geom)
        >>> result.geom_type, result.area
        ('Polygon', 1.0)

        A collection with multiple non-zero-area polygons is returned as a MultiPolygon.

        >>> geom = GeometryCollection([poly, poly])
        >>> result = polygonize(geom)
        >>> result.geom_type, result.area
        ('MultiPolygon', 2.0)

        Zero-area geometries are not permitted.

        >>> result = polygonize(zero_poly)
        Traceback (most recent call last):
          ...
        ValueError: Geometry has zero area
    """
    polys = []
    # Explode geometries to polygons
    if isinstance(geom, GeometryCollection):
        for g in geom:
            if isinstance(g, Polygon):
                polys.append(g)
            elif isinstance(g, MultiPolygon):
                polys.extend(g)
    elif isinstance(geom, MultiPolygon):
        polys.extend(geom)
    elif isinstance(geom, Polygon):
        polys.append(geom)
    # Remove zero-area polygons
    polys = [p for p in polys if p.area]
    if not polys:
        raise ValueError("Geometry has zero area")
    if len(polys) == 1:
        return polys[0]
    return MultiPolygon(polys)


def self_union(gdf: gpd.GeoDataFrame, ratios: Iterable[str] = None) -> gpd.GeoDataFrame:
    """
    Calculate the geometric union of a feature layer with itself.

    Areas of overlap are split into two or more geometrically-identical features:
    one for each of the original overlapping features.
    Each split feature contains the attributes of the original feature.

    Args:
        gdf: GeoDataFrame with non-empty (Multi)Polygon geometries.
        ratios: Names of columns to rescale by the area fraction of the split feature
            relative to the original. By default, the original value is used unchanged.

    Returns:
        GeoDataFrame representing the union of the input features with themselves.
        Its index contains tuples of the index of the original overlapping features.

    Raises:
        ValueError: Only (Multi)Polygon geometries are supported.
        ValueError: Empty Polygon geometries are not supported.
    """
    raise NotImplementedError('Coming soon')


def dissolve(
    gdf: gpd.GeoDataFrame,
    by: Iterable[str],
    func: Union[Callable, str, list, dict],
    how: Union[
        Literal["union", "first"], Callable[[gpd.GeoSeries], BaseGeometry]
    ] = "union",
) -> gpd.GeoDataFrame:
    """
    Dissolve layer by aggregating features based on common attributes.

    Args:
        gdf: GeoDataFrame with non-empty (Multi)Polygon geometries.
        by: Names of columns to group features by.
        func: Aggregation function for data columns (see :meth:`pd.DataFrame.groupby`).
        how: Aggregation function for geometry column.
            Either 'union' (:meth:`gpd.GeoSeries.unary_union`),
            'first' (first geometry in group),
            or a function aggregating multiple geometries into one.

    Returns:
        GeoDataFrame with dissolved geometry and data columns,
        and grouping columns set as the index.

    Examples:
        >>> gpd.options.display_precision = 0
        >>> gdf = gpd.GeoDataFrame({
        ...     'geometry': gpd.GeoSeries([
        ...         Polygon([(0, 0), (0, 1), (3, 1), (3, 0)]),
        ...         Polygon([(3, 0), (3, 1), (4, 1), (4, 0)])
        ...     ]),
        ...     'id': [0, 0],
        ...     'ids': [0, 1],
        ...     'x': [3.0, 1.0]
        ... })
        >>> dissolve(gdf, by='id', func={'ids': tuple, 'x': 'sum'}, how='union')
                                                 geometry     ids    x
        id  ...
        0   POLYGON ((0 0, 0 1, 3 1, 4 1, 4 0, 3 0, 0 0))  (0, 1)  4.0
        >>> dissolve(gdf, by='id', func={'ids': tuple, 'x': 'sum'}, how='first')
                                       geometry     ids    x
        id  ...
        0   POLYGON ((0 0, 0 1, 3 1, 3 0, 0 0))  (0, 1)  4.0
    """
    check_gdf(gdf)
    merges = {"union": lambda x: x.unary_union, "first": lambda x: x.iloc[0]}
    data = gdf.drop(columns=gdf.geometry.name).groupby(by=by).aggregate(func)
    geometry = gdf.groupby(by=by, group_keys=False)[gdf.geometry.name].aggregate(
        merges.get(how, how)
    )
    return gpd.GeoDataFrame(geometry, geometry=gdf.geometry.name, crs=gdf.crs).join(
        data
    )


def overlay(
    *gdfs: gpd.GeoDataFrame,
    how: Literal[
        "intersection", "union", "identity", "symmetric_difference", "difference"
    ] = "intersection",
    ratios: Iterable[str] = None,
) -> gpd.GeoDataFrame:
    """
    Overlay multiple layers incrementally.

    When a feature from one layer overlaps the feature of another layer,
    the area of overlap is split into two geometrically-identical features:
    one for each of the original overlapping features.
    Each split feature contains the attributes of the original feature.

    TODO: To identify the source of output features, the user can ensure that each
    layer contains a column to index by.
    Alternatively, tuples of indices of the overlapping
    feature from each layer (null if none) could be returned as the index.

    Args:
        gdfs: GeoDataFrames with non-empty (Multi)Polygon geometries
            assumed to contain no self-overlaps (see :func:`self_union`).
            Names of (non-geometry) columns cannot be used more than once.
            Any index colums are ignored.
        how: Spatial overlay method (see :func:`gpd.overlay`).
        ratios: Names of columns to rescale by the area fraction of the split feature
            relative to the original. By default, the original value is used unchanged.

    Raises:
        ValueError: Duplicate column names in layers.

    Returns:
        GeoDataFrame with the geometries and attributes resulting from the overlay.

    Examples:
        >>> gpd.options.display_precision = 0
        >>> a = gpd.GeoDataFrame({
        ...     'geometry': gpd.GeoSeries([Polygon([(0, 0), (0, 1), (3, 1), (3, 0)])]),
        ...     'a': [3.0]
        ... })
        >>> b = gpd.GeoDataFrame({
        ...     'geometry': gpd.GeoSeries([Polygon([(2, 0), (2, 1), (4, 1), (4, 0)])]),
        ...     'b': [8.0]
        ... })
        >>> overlay(a, b, how='intersection')
             a    b                             geometry
        0  3.0  8.0  POLYGON ((2 1, 3 1, 3 0, 2 0, 2 1))
        >>> overlay(a, b, how='union')
             a    b                             geometry
        0  3.0  8.0  POLYGON ((2 1, 3 1, 3 0, 2 0, 2 1))
        1  3.0  NaN  POLYGON ((0 0, 0 1, 2 1, 2 0, 0 0))
        2  NaN  8.0  POLYGON ((3 1, 4 1, 4 0, 3 0, 3 1))
        >>> overlay(a, b, how='union', ratios=['a', 'b'])
                                      geometry    a    b
        0  POLYGON ((2 1, 3 1, 3 0, 2 0, 2 1))  1.0  4.0
        1  POLYGON ((0 0, 0 1, 2 1, 2 0, 0 0))  2.0  NaN
        2  POLYGON ((3 1, 4 1, 4 0, 3 0, 3 1))  NaN  4.0
    """
    for gdf in gdfs:
        check_gdf(gdf)
    if ratios is None:
        ratios = []
    # Check for duplicate non-geometry column names
    seen = set()
    duplicates = set(
        c for df in gdfs for c in get_data_columns(df) if c in seen or seen.add(c)
    )
    if duplicates:
        raise ValueError(f"Duplicate column names in layers: {duplicates}")
    # Drop index columns and replace with default index of known name
    # NOTE: Assumes that default index name not already names of columns
    keys = [f"__id{i}__" for i in range(len(gdfs))]
    gdfs = [df.reset_index(drop=True).rename_axis(k) for df, k in zip(gdfs, keys)]
    overlay = None
    for i in range(len(gdfs) - 1):
        a, b = overlay if i else gdfs[i], gdfs[i + 1]
        # Perform overlay with geometry and constant fields
        constants = [
            [c for c in df.columns if c == df.geometry.name or c not in ratios]
            for df in (a, b)
        ]
        overlay = gpd.overlay(
            a[constants[0]].reset_index(), b[constants[1]].reset_index(), how=how
        )
        # For uniform fields, compute area fraction of originals and merge by index
        # new_value = (old_value / old_area) * new_area
        dfs = []
        for j, df in enumerate((a, b)):
            df_ratios = [c for c in df.columns if c != df.geometry.name and c in ratios]
            if df_ratios:
                dfs.append(
                    df[df_ratios]
                    .div(df.area, axis="index")
                    .reindex(overlay[keys[j]])
                    .reset_index(drop=True)
                    .mul(overlay.area, axis="index")
                )
        if dfs:
            # Assumed to be faster than incremental concat
            overlay = pd.concat([overlay] + dfs, axis="columns")
    return overlay.drop(columns=keys)


def get_data_columns(df: pd.DataFrame) -> list:
    """Return list of columns, ignoring geometry."""
    if isinstance(df, gpd.GeoDataFrame) and hasattr(df, "geometry"):
        return [col for col in df.columns if col != df.geometry.name]
    return list(df.columns)


class LayerBuilder:
    """
    Build a layer incrementally.

    Attributes:
        gdf (gpd.GeoDataFrame): GeoDataFrame with non-empty (Multi)Polygon geometries.
        ratios (List[str]): Names of columns to rescale by the area fraction of the
            split feature relative to the original.
            Has no effect on the geometry column.
    """

    def __init__(self, gdf: gpd.GeoDataFrame, ratios: Iterable[str] = None) -> None:
        """
        Initialize a LayerBuilder.

        Args:
            gdf: GeoDataFrame with non-empty (Multi)Polygon geometries.
            ratios: Names of columns (not necessarily present in `gdf`) to rescale by
                the area fraction of the split feature relative to the original.
                Has no effect on the geometry column.
        """
        check_gdf(gdf)
        self.gdf = gdf
        self.ratios = []
        self.add_ratios(ratios)

    def add_ratios(self, ratios: Iterable[str] = None) -> None:
        """Add columns to those treated as ratios when features are split."""
        if ratios is None:
            ratios = []
        self.ratios += [x for x in ratios if x not in self.ratios]

    def join(self, df: pd.DataFrame, ratios: Iterable[str] = None) -> None:
        """
        Join to table on common attributes.

        Args:
            df: Dataframe with attributes to add to the layer.
            ratios: Names of columns to rescale by the area fraction of the
                split feature relative to the original.

        Raises:
            ValueError: Table and layer have no column names in common.
        """
        # Columns present in both existing and new layer (ignoring geometry)
        on = list(set(get_data_columns(df)) & set(get_data_columns(self.gdf)))
        if not on:
            raise ValueError("Table and layer have no column names in common")
        self.gdf = self.gdf.merge(df, how="left", on=on)
        self.add_ratios(ratios)

    def sjoin(
        self,
        gdf: gpd.GeoDataFrame,
        how: Literal["left", "right", "inner"] = "left",
        op: Literal["intersects", "contains", "within"] = "intersects",
        ratios: Iterable[str] = None,
    ) -> None:
        """
        Join to layer on spatial binary predicate.

        Args:
            gdf: GeoDataFrame with non-empty (Multi)Polygon geometries.
            how: Type of join (see :func:`gpd.sjoin`).
            op: Binary predicate
                (http://shapely.readthedocs.io/en/latest/manual.html#binary-predicates).
            ratios: Names of columns to rescale by the area fraction of the
                split feature relative to the original.

        Raises:
            ValueError: Only non-empty (Multi)Polygon geometries are supported.
        """
        check_gdf(gdf)
        # Drop data columns already present in layer
        xcols = list(set(get_data_columns(gdf)) & set(get_data_columns(self.gdf)))
        gdf = gdf.drop(columns=xcols)
        # NOTE: Requires that no column be named 'index_right'.
        self.layer = gpd.sjoin(self.gdf, gdf, how=how, op=op).drop(
            columns=["index_right"]
        )
        self.add_ratios(ratios)

    def overlay(
        self,
        gdf: gpd.GeoDataFrame,
        how: Literal[
            "intersection", "union", "identity", "symmetric_difference", "difference"
        ] = "intersection",
        ratios: Iterable[str] = None,
    ) -> None:
        """
        Overlay onto layer.

        Args:
            gdf: GeoDataFrame with non-empty (Multi)Polygon geometries.
            ratios: Names of columns to rescale by the area fraction of the
                split feature relative to the original.

        ValueError:
            ValueError: Only non-empty (Multi)Polygon geometries are supported.
        """
        check_gdf(gdf)
        # Drop data columns already present in layer
        xcols = list(set(get_data_columns(gdf)) & set(get_data_columns(self.gdf)))
        gdf = gdf.drop(columns=xcols)
        self.add_ratios(ratios)
        self.gdf = overlay(self.gdf, gdf, ratios=self.ratios)

    def dissolve(
        self,
        by: Iterable[str],
        how: Union[
            Literal["union", "first"], Callable[[gpd.GeoSeries], BaseGeometry]
        ] = "union",
        func: Union[Callable, str, list, dict] = None,
    ) -> gpd.GeoDataFrame:
        """
        Dissolve layer by aggregating features based on common attributes.

        See :func:`dissolve`. If `func=None`, ratio columns are summed and non-ratio
        columns are gathered into tuples.
        """
        if func is None:
            func = {
                col: pd.Series.sum if col in self.ratios else pd.Series.tolist
                for col in get_data_columns(self.gdf)
            }
        return dissolve(self.gdf, by=by, how=how, func=func)
