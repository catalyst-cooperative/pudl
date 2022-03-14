"""Spatial operations for demand allocation."""
import itertools
import warnings
from typing import Callable, Iterable, Literal, Union

import geopandas as gpd
import pandas as pd
import shapely.ops
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


def explode(gdf: gpd.GeoDataFrame, ratios: Iterable[str] = None) -> gpd.GeoDataFrame:
    """
    Explode MultiPolygon to multiple Polygon geometries.

    Args:
        gdf: GeoDataFrame with non-zero-area (Multi)Polygon geometries.
        ratios: Names of columns to rescale by the area fraction of the Polygon
            relative to the MultiPolygon.
            If provided, MultiPolygon cannot self-intersect.
            By default, the original value is used unchanged.

    Raises:
        ValueError: Geometry contains self-intersecting MultiPolygon.

    Returns:
        GeoDataFrame with each Polygon as a separate row in the GeoDataFrame.
        The index is the number of the source row in the input GeoDataFrame.

    """
    check_gdf(gdf)
    gdf = gdf.reset_index(drop=True)
    is_mpoly = gdf.geometry.geom_type == "MultiPolygon"
    if ratios and is_mpoly.any():
        union_area = gdf.geometry[is_mpoly].apply(shapely.ops.unary_union).area
        if (union_area != gdf.geometry[is_mpoly].area).any():
            raise ValueError("Geometry contains self-intersecting MultiPolygon")
    result = gdf.explode(index_parts=False)
    if ratios:
        fraction = result.geometry.area.values / gdf.geometry.area[result.index].values
        result[ratios] = result[ratios].multiply(fraction, axis="index")
    return result[gdf.columns]


def self_union(gdf: gpd.GeoDataFrame, ratios: Iterable[str] = None) -> gpd.GeoDataFrame:
    """
    Calculate the geometric union of a feature layer with itself.

    Areas of overlap are split into two or more geometrically-identical features:
    one for each of the original overlapping features.
    Each split feature contains the attributes of the original feature.

    Args:
        gdf: GeoDataFrame with non-zero-area MultiPolygon geometries.
        ratios: Names of columns to rescale by the area fraction of the split feature
            relative to the original. By default, the original value is used unchanged.

    Returns:
        GeoDataFrame representing the union of the input features with themselves.
        Its index contains tuples of the index of the original overlapping features.

    Raises:
        NotImplementedError: MultiPolygon geometries are not yet supported.

    """
    check_gdf(gdf)
    gdf = gdf.reset_index(drop=True)
    is_mpoly = gdf.geometry.geom_type == "MultiPolygon"
    if is_mpoly.any():
        raise NotImplementedError("MultiPolygon geometries are not yet supported")
    # Calculate all pairwise intersections
    # https://nbviewer.jupyter.org/gist/jorisvandenbossche/3a55a16fda9b3c37e0fb48b1d4019e65
    pairs = itertools.combinations(gdf.geometry, 2)
    intersections = gpd.GeoSeries([a.intersection(b) for a, b in pairs])
    # Form polygons from the boundaries of the original polygons and their intersections
    boundaries = pd.concat([gdf.geometry, intersections]).boundary.unary_union
    polygons = gpd.GeoSeries(shapely.ops.polygonize(boundaries))
    # Determine origin of each polygon by a spatial join on representative points
    points = gpd.GeoDataFrame(geometry=polygons.representative_point())
    oids = gpd.sjoin(
        points,
        gdf[["geometry"]],
        how="left",
        predicate="within",
    )["index_right"]
    # Build new dataframe
    columns = get_data_columns(gdf)
    df = gpd.GeoDataFrame(
        data=gdf.loc[oids, columns].reset_index(drop=True),
        geometry=polygons[oids.index].values,
    )
    if ratios:
        fraction = df.area.values / gdf.area[oids].values
        df[ratios] = df[ratios].multiply(fraction, axis="index")
    # Add original row indices to index
    df.index = oids.groupby(oids.index).agg(tuple)[oids.index]
    df.index.name = None
    # Return with original column order
    return df[gdf.columns]


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
