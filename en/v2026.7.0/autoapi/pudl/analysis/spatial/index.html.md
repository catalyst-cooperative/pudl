# pudl.analysis.spatial

Spatial operations for demand allocation.

## Functions

| [`check_gdf`](#pudl.analysis.spatial.check_gdf)(→ None)                     | Check that GeoDataFrame contains (Multi)Polygon geometries with non-zero area.   |
|-----------------------------------------------------------------------------|----------------------------------------------------------------------------------|
| [`polygonize`](#pudl.analysis.spatial.polygonize)(...)                      | Convert geometry to (Multi)Polygon.                                              |
| [`explode`](#pudl.analysis.spatial.explode)(→ geopandas.GeoDataFrame)       | Explode MultiPolygon to multiple Polygon geometries.                             |
| [`self_union`](#pudl.analysis.spatial.self_union)(→ geopandas.GeoDataFrame) | Calculate the geometric union of a feature layer with itself.                    |
| [`dissolve`](#pudl.analysis.spatial.dissolve)(→ geopandas.GeoDataFrame)     | Dissolve layer by aggregating features based on common attributes.               |
| [`overlay`](#pudl.analysis.spatial.overlay)(→ geopandas.GeoDataFrame)       | Overlay multiple layers incrementally.                                           |
| [`get_data_columns`](#pudl.analysis.spatial.get_data_columns)(→ list)       | Return list of columns, ignoring geometry.                                       |

## Module Contents

### pudl.analysis.spatial.check_gdf(gdf: [geopandas.GeoDataFrame](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.html#geopandas.GeoDataFrame)) → [None](https://docs.python.org/3/library/constants.html#None)

Check that GeoDataFrame contains (Multi)Polygon geometries with non-zero area.

* **Parameters:**
  **gdf** – GeoDataFrame.
* **Raises:**
  * [**TypeError**](https://docs.python.org/3/library/exceptions.html#TypeError) – Object is not a GeoDataFrame.
  * [**AttributeError**](https://docs.python.org/3/library/exceptions.html#AttributeError) – GeoDataFrame has no geometry.
  * [**TypeError**](https://docs.python.org/3/library/exceptions.html#TypeError) – Geometry is not a GeoSeries.
  * [**ValueError**](https://docs.python.org/3/library/exceptions.html#ValueError) – Geometry contains null geometries.
  * [**ValueError**](https://docs.python.org/3/library/exceptions.html#ValueError) – Geometry contains non-(Multi)Polygon geometries.
  * [**ValueError**](https://docs.python.org/3/library/exceptions.html#ValueError) – Geometry contains (Multi)Polygon geometries with zero area.
  * [**ValueError**](https://docs.python.org/3/library/exceptions.html#ValueError) – MultiPolygon contains Polygon geometries with zero area.

### pudl.analysis.spatial.polygonize(geom: shapely.geometry.base.BaseGeometry) → shapely.geometry.Polygon | shapely.geometry.MultiPolygon

Convert geometry to (Multi)Polygon.

* **Parameters:**
  **geom** – Geometry to convert to (Multi)Polygon.
* **Returns:**
  Geometry converted to (Multi)Polygon, with all zero-area components removed.
* **Raises:**
  [**ValueError**](https://docs.python.org/3/library/exceptions.html#ValueError) – Geometry has zero area.

### pudl.analysis.spatial.explode(gdf: [geopandas.GeoDataFrame](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.html#geopandas.GeoDataFrame), ratios: [collections.abc.Iterable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None) → [geopandas.GeoDataFrame](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.html#geopandas.GeoDataFrame)

Explode MultiPolygon to multiple Polygon geometries.

* **Parameters:**
  * **gdf** – GeoDataFrame with non-zero-area (Multi)Polygon geometries.
  * **ratios** – Names of columns to rescale by the area fraction of the Polygon
    relative to the MultiPolygon.
    If provided, MultiPolygon cannot self-intersect.
    By default, the original value is used unchanged.
* **Raises:**
  [**ValueError**](https://docs.python.org/3/library/exceptions.html#ValueError) – Geometry contains self-intersecting MultiPolygon.
* **Returns:**
  GeoDataFrame with each Polygon as a separate row in the GeoDataFrame.
  The index is the number of the source row in the input GeoDataFrame.

### pudl.analysis.spatial.self_union(gdf: [geopandas.GeoDataFrame](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.html#geopandas.GeoDataFrame), ratios: [collections.abc.Iterable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None) → [geopandas.GeoDataFrame](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.html#geopandas.GeoDataFrame)

Calculate the geometric union of a feature layer with itself.

Areas of overlap are split into two or more geometrically-identical features:
one for each of the original overlapping features.
Each split feature contains the attributes of the original feature.

* **Parameters:**
  * **gdf** – GeoDataFrame with non-zero-area MultiPolygon geometries.
  * **ratios** – Names of columns to rescale by the area fraction of the split feature
    relative to the original. By default, the original value is used unchanged.
* **Returns:**
  GeoDataFrame representing the union of the input features with themselves.
  Its index contains tuples of the index of the original overlapping features.
* **Raises:**
  [**NotImplementedError**](https://docs.python.org/3/library/exceptions.html#NotImplementedError) – MultiPolygon geometries are not yet supported.

### pudl.analysis.spatial.dissolve(gdf: [geopandas.GeoDataFrame](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.html#geopandas.GeoDataFrame), by: [collections.abc.Iterable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[[str](https://docs.python.org/3/library/stdtypes.html#str)], func: [collections.abc.Callable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable) | [str](https://docs.python.org/3/library/stdtypes.html#str) | [list](https://docs.python.org/3/library/stdtypes.html#list) | [dict](https://docs.python.org/3/library/stdtypes.html#dict), how: Literal['union', 'first'] | [collections.abc.Callable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[[geopandas.GeoSeries](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.html#geopandas.GeoSeries)], shapely.geometry.base.BaseGeometry] = 'union') → [geopandas.GeoDataFrame](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.html#geopandas.GeoDataFrame)

Dissolve layer by aggregating features based on common attributes.

* **Parameters:**
  * **gdf** – GeoDataFrame with non-empty (Multi)Polygon geometries.
  * **by** – Names of columns to group features by.
  * **func** – Aggregation function for data columns (see `pd.DataFrame.groupby()`).
  * **how** – Aggregation function for geometry column.
    Either ‘union’ (`gpd.GeoSeries.union_all()`),
    ‘first’ (first geometry in group),
    or a function aggregating multiple geometries into one.
* **Returns:**
  GeoDataFrame with dissolved geometry and data columns,
  and grouping columns set as the index.

### pudl.analysis.spatial.overlay(\*gdfs: [geopandas.GeoDataFrame](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.html#geopandas.GeoDataFrame), how: Literal['intersection', 'union', 'identity', 'symmetric_difference', 'difference'] = 'intersection', ratios: [collections.abc.Iterable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None) → [geopandas.GeoDataFrame](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.html#geopandas.GeoDataFrame)

Overlay multiple layers incrementally.

When a feature from one layer overlaps the feature of another layer,
the area of overlap is split into two geometrically-identical features:
one for each of the original overlapping features.
Each split feature contains the attributes of the original feature.

TODO: To identify the source of output features, the user can ensure that each
layer contains a column to index by.
Alternatively, tuples of indices of the overlapping
feature from each layer (null if none) could be returned as the index.

* **Parameters:**
  * **gdfs** – GeoDataFrames with non-empty (Multi)Polygon geometries
    assumed to contain no self-overlaps (see [`self_union()`](#pudl.analysis.spatial.self_union)).
    Names of (non-geometry) columns cannot be used more than once.
    Any index columns are ignored.
  * **how** – Spatial overlay method (see `gpd.overlay()`).
  * **ratios** – Names of columns to rescale by the area fraction of the split feature
    relative to the original. By default, the original value is used unchanged.
* **Raises:**
  [**ValueError**](https://docs.python.org/3/library/exceptions.html#ValueError) – Duplicate column names in layers.
* **Returns:**
  GeoDataFrame with the geometries and attributes resulting from the overlay.

### pudl.analysis.spatial.get_data_columns(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [list](https://docs.python.org/3/library/stdtypes.html#list)

Return list of columns, ignoring geometry.
