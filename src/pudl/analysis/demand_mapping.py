"""This module provide demand allocation functions to allocate demand.

A layer contains many features, each of which have an associated geometry and
many attributes. For our purposes, let's allow each attribute to be one of two
types:

constant: The attribute is equal everywhere within the feature geometry (e.g.
identifier, percent area).

When splitting a feature, the attribute value for the resulting features is that
of their parent: e.g. [1] -> [1], [1].

When joining features, the attribute value for the resulting feature must be a
function of its children: e.g. [1], [1] -> [1, 1] (list) or 1 (appropriate
aggregation function, e.g. median or area-weighted mean).

uniform: The attribute is uniformly distributed within the feature geometry
(e.g. count, area).

When splitting a feature, the attribute value for the resulting features is
proportional to their area: e.g. [1] (100% area) -> [0.4] (40% area), [0.6] (60%
area).

When joining features, the attribute value for the resulting feature is the sum
of its children: e.g. [0.4], [0.6] -> [1].

"""
import logging
import pathlib
import zipfile

import geopandas
import numpy as np
import pandas as pd
from geopandas import GeoDataFrame
from shapely.geometry import GeometryCollection, MultiPolygon, Polygon
from shapely.ops import unary_union
from tqdm import tqdm

import pudl

logger = logging.getLogger(__name__)


################################################################################
# Local data acquisition functions specific to the Demand Mapping analysis
################################################################################
def get_hifld_planning_areas_gdf(pudl_settings):
    """
    Obtain Electric Planning Area geometries from HIFLD.

    Download the HIFLD planning area geometries if they are not already available
    locally. Store the data in "local/hifld/electric_planning_areas.gdb" under the PUDL
    datastore's top level directory, as they are not integrated into the core PUDL data
    management. The data is in the geodatabase format.

    Read the data into a goepandas.GeoDataFrame, and convert the data types as needed.
    All columns are assigned nullable types.

    Args:
        pudl_settings (dict): A dictionary of PUDL settings, including the path to the
            datastore.

    Returns:
        geopandas.GeoDataFrame: containing the HIFLD planning area geometries and all
        associated data columns.

    """
    hifld_pa_url = "https://opendata.arcgis.com/datasets/7d35521e3b2c48ab8048330e14a4d2d1_0.gdb"
    hifld_dir = pathlib.Path(pudl_settings["data_dir"]) / "local/hifld"
    hifld_dir.mkdir(parents=True, exist_ok=True)
    hifld_pa_zipfile = hifld_dir / "electric_planning_areas.gdb.zip"
    hifld_pa_gdb_dir = hifld_dir / "electric_planning_areas.gdb"

    if not hifld_pa_gdb_dir.is_dir():
        logger.warning("No Planning Area GeoDB found. Downloading from HIFLD.")
        # Download to appropriate location
        pudl.helpers.download_zip_url(hifld_pa_url, hifld_pa_zipfile)
        # Unzip because we can't use zipfile paths with geopandas
        with zipfile.ZipFile(hifld_pa_zipfile, 'r') as zip_ref:
            zip_ref.extractall(hifld_dir)
            # Grab the UUID based directory name so we can change it:
            extract_root = hifld_dir / \
                pathlib.Path(zip_ref.filelist[0].filename).parent
        extract_root.rename(hifld_pa_gdb_dir)
    else:
        logger.info("We've already got the planning area GeoDB.")

    gdf = (
        geopandas.read_file(hifld_pa_gdb_dir)
        .assign(
            SOURCEDATE=lambda x: pd.to_datetime(x.SOURCEDATE),
            VAL_DATE=lambda x: pd.to_datetime(x.VAL_DATE),
            ID=lambda x: pd.to_numeric(x.ID),
            NAICS_CODE=lambda x: pd.to_numeric(x.NAICS_CODE),
            YEAR=lambda x: pd.to_numeric(x.YEAR),
        )
        .astype({
            "ID": pd.Int64Dtype(),
            "NAME": pd.StringDtype(),
            "COUNTRY": pd.StringDtype(),
            "NAICS_CODE": pd.Int64Dtype(),
            "NAICS_DESC": pd.StringDtype(),
            "SOURCE": pd.StringDtype(),
            "VAL_METHOD": pd.StringDtype(),
            "WEBSITE": pd.StringDtype(),
            "ABBRV": pd.StringDtype(),
            "YEAR": pd.Int64Dtype(),
            "PEAK_LOAD": float,
            "PEAK_RANGE": float,
            "SHAPE_Length": float,
            "SHAPE_Area": float,
        })
    )
    return gdf


################################################################################
# Demand allocation functions
################################################################################
def edit_id_set(row, new_id, id_set):
    """
    Editing ID sets by adding the new geometry ID if required.

    This function edits original ID sets by adding the new geometry ID if
    required. This function is called by another function
    `complete_disjoint_geoms`
    """
    if row["geom_type"] == "geometry_new_int":
        return frozenset(list(row[id_set]) + [new_id])

    else:
        return row[id_set]


def polygonize_geom(geom):
    """
    Remove zero-area geometries from a geometry collection.

    Strip zero-area geometries from a geometrical object.
    (maybe a single geometry object or collection e.g. GeometryCollection)
    This function is called by another function `complete_disjoint_geoms`.
    """
    if isinstance(geom, GeometryCollection):

        new_list = [a for a in list(geom) if isinstance(
            a, Polygon) or isinstance(a, MultiPolygon)]

        if len(new_list) == 1:
            return new_list[0]

        else:
            return MultiPolygon(new_list)

    elif isinstance(geom, MultiPolygon) or isinstance(geom, Polygon):
        return geom

    else:
        return Polygon([])


def extend_gdf(gdf_disjoint, id_col):
    """
    Add duplicates of intersecting geometries to be able to add the constants.

    This function adds rows with duplicate geometries and creates the new `ID`
    column for each of the new rows. This function is called by another function
    `complete_disjoint_geoms`.
    """
    tqdm_max = len(gdf_disjoint)
    ext = pd.DataFrame(columns=list(gdf_disjoint.columns) + [id_col + "_set"])

    for _, row in tqdm(gdf_disjoint.iterrows(), total=tqdm_max):

        num = len(row[id_col])
        data = np.array([list(row[id_col]), [row["geometry"]] * num]).T
        ext_new = pd.DataFrame(data, columns=gdf_disjoint.columns)
        ext_new[id_col + "_set"] = [row[id_col]] * num
        ext = ext.append(ext_new, ignore_index=True)

    return ext


def complete_disjoint_geoms(gdf, attributes):
    """
    Split a self-intersecting layer into distinct non-intersecting geometries.

    Given a GeoDataFrame of multiple geometries, some of which intersect each
    other, this function iterates through the geometries sequentially and
    fragments them into distinct individual pieces, and accordingly allocates
    the `uniform` and `constant` attributes. The intersecting geometries repeat
    the number of times particular `ID` encircles it.

    Args:
        gdf (GeoDataframe): GeoDataFrame consisting of the intersecting
        geometries. Only `POLYGON` and `MULTIPOLYGON` geometries supported.
        Other geometries will be deleted.
        attributes (dict): a dictionary keeping a track of all the types of
        attributes with keys represented by column name, and the values
        representing the type of attribute. One column from the
        attribute dictionary must belong in the GeoDataFrame and should be of
        type `ID` to allow for the intersection to happen. The other two
        possible types are `uniform` and `constant`. The `uniform` type
        attribute disaggregates the data across geometries and the `constant`
        type is propagated as the same value.

    Returns:
        geopandas.GeoDataFrame: GeoDataFrame with all attributes as gdf
        and one extra attribute with name as the `ID` attribute appended by
        "_set" substring. The geometries will not include zero-area geometry
        components.

        attributes: Adds the `ID`+"_set" as a `constant` attribute and returns the
        attributes dictionary
    """
    # ID is the index which will help to identify duplicate geometries
    gdf_ids = [k for k, v in attributes.items() if (
        (k in gdf.columns) and (v == "ID"))][0]
    gdf_constants = [k for k, v in attributes.items() if (
        (k in gdf.columns) and (v == "constant"))]
    gdf_uniforms = [k for k, v in attributes.items() if (
        (k in gdf.columns) and (v == "uniform"))]

    # Check if `ID` column has all unique elements:
    if gdf[gdf_ids].nunique() != len(gdf):
        raise Exception("All ID column elements should be unique")

    # Iterating through each of the geometries
    for row in tqdm((gdf[[gdf_ids, "geometry"]]
                     .reset_index(drop=True)
                     .itertuples()), total=len(gdf)):

        index = row.Index
        if index == 0:
            gdf_disjoint = (pd.DataFrame(row).T
                            .drop(0, axis=1)
                            .rename(columns={1: gdf_ids, 2: "geometry"}))
            gdf_disjoint[gdf_ids] = gdf_disjoint[gdf_ids].apply(
                lambda x: frozenset([x]))
            gdf_disjoint = GeoDataFrame(
                gdf_disjoint, geometry="geometry", crs=gdf.crs)
            gdf_disjoint_cur_union = unary_union(gdf_disjoint["geometry"])

        # Additional geometries
        else:

            # Adding difference and intersections of the old geometries
            # with the new geometry
            gdf_disjoint["geometry_new_diff"] = gdf_disjoint.difference(
                row[2])
            gdf_disjoint["geometry_new_int"] = gdf_disjoint.intersection(
                row[2])
            gdf_disjoint = gdf_disjoint.drop("geometry", axis=1)

            # Stacking all the new geometries in one column
            gdf_disjoint = (gdf_disjoint
                            .set_index(gdf_ids)
                            .stack()
                            .reset_index()
                            .rename(columns={"level_1": "geom_type", 0: "geometry"}))

            # Creating the new gdf_ids sets
            gdf_disjoint[gdf_ids] = gdf_disjoint.apply(
                lambda x: edit_id_set(x, row[1], gdf_ids), axis=1)

            # Adding the new sole geometry's gdf_ids and geometry
            gdf_disjoint = gdf_disjoint.append({
                gdf_ids: frozenset([row[1]]),
                "geom_type": "geometry_new_sole",
                "geometry": row[2].difference(gdf_disjoint_cur_union)
            }, ignore_index=True)

            # Removing geometries which are not polygons
            gdf_disjoint["geometry"] = gdf_disjoint["geometry"].apply(
                polygonize_geom)
            gdf_disjoint = GeoDataFrame(
                gdf_disjoint, geometry="geometry", crs=gdf.crs)

            # Removing zero-area geometries
            gdf_disjoint = gdf_disjoint.drop("geom_type", axis=1)[
                (gdf_disjoint["geometry"].area != 0)]

            # Sum geometry to subtract any new geometry being added
            gdf_disjoint_cur_union = unary_union(
                [gdf_disjoint_cur_union, row[2]])

    gdf_disjoint.reset_index(drop=True, inplace=True)

    # Create duplicate entries to add all constants for self-intersecting geometries
    gdf_disjoint_complete = extend_gdf(gdf_disjoint, gdf_ids)

    # Add gdf's constant values and old geometries areas for allocation
    gdf["_old_ID_area"] = gdf.area

    gdf_disjoint_complete = (gdf_disjoint_complete.merge(
        gdf[[gdf_ids, "_old_ID_area"] + gdf_constants + gdf_uniforms]))
    gdf_disjoint_complete = GeoDataFrame(
        gdf_disjoint_complete, geometry="geometry", crs=gdf.crs)

    # Add gdf's uniform values
    gdf_disjoint_complete["_new_ID_area"] = gdf_disjoint_complete.area
    gdf_disjoint_complete["_area_fraction"] = gdf_disjoint_complete["_new_ID_area"] / \
        gdf_disjoint_complete["_old_ID_area"]

    # Intersecting geometries will have copies of the geometries
    # and the uniform attributes will have different conflicting values
    for uniform in gdf_uniforms:
        gdf_disjoint_complete[uniform] = gdf_disjoint_complete[uniform] * \
            gdf_disjoint_complete["_area_fraction"]

    # delete temporary columns
    del gdf_disjoint_complete["_new_ID_area"]
    del gdf_disjoint_complete["_area_fraction"]
    del gdf_disjoint_complete["_old_ID_area"]
    del gdf["_old_ID_area"]

    # Adding the new attribute
    attributes[gdf_ids + "_set"] = "constant"
    return gdf_disjoint_complete, attributes


def layer_intersection(layer1, layer2, attributes):
    """
    Break two layers, each covering the same area, into disjoint geometries.

    Two GeoDataFrames are combined together in such a fashion that the geometries are
    completely disjoint. The uniform attributes are allocated on the basis of the
    fraction of the area covered by the new geometry compared to the geometry it is
    being split from. There may be non-unique geometries involved in either layer. If
    non-unique geometries are involved in layer 1, layer 2 attributes get counted
    multiple times and are scaled down accordingly and vice-versa.

    For example, in the case of simple geometries A, B and A intersection B (X2) in
    layer 1, and layer 2 containing geometries 1 and 2. The new geometry (1 int A int B)
    will be counted twice, and same for new geometry (2 int A int B). However, the
    allocation of the uniform attribute is done based on the area fraction. So, it is
    divided by the number of times the duplication is occurring.

    The function returns a new GeoDataFrame with all columns from layer1 and
    layer2.

    Args:
        layer1 (geopandas.GeoDataframe): first GeoDataFrame
        layer2 (geopandas.GeoDataframe): second GeoDataFrame
        attributes (dict): a dictionary keeping a track of all the types of
            attributes with keys represented by column names from layer1 and
            layer2, and the values representing the type of attribute. Types
            of attributes include ``constant``, ``uniform`` and ``ID``. If a
            column name ``col`` of type ``ID`` exists, then one column name
            ``col``+``_set`` of type "constant" will exist in the attributes
            dictionary.

    Returns:
        geopandas.GeoDataFrame: New layer consisting all attributes in layer1
        and layer2.

    """
    # separating the uniforms and constant attributes
    layer1_uniforms = [k for k, v in attributes.items() if (
        (k in layer1.columns) and (v == "uniform"))]
    layer2_uniforms = [k for k, v in attributes.items() if (
        (k in layer2.columns) and (v == "uniform"))]

    layer1_constants = [k for k, v in attributes.items() if (
        (k in layer1.columns) and (v != "uniform"))]
    layer2_constants = [k for k, v in attributes.items() if (
        (k in layer2.columns) and (v != "uniform"))]

    # Calculating the intersection layers
    layer_new = geopandas.overlay(layer1, layer2)

    # Calculating the areas for the uniform attribute calculations
    layer1["_layer1_area"] = layer1.area
    layer2["_layer2_area"] = layer2.area
    layer_new["_layernew_area"] = layer_new.area

    # Merging the area layers for uniform attribute disaggregation calculation
    layer_new = (layer_new
                 .merge(layer1[layer1_constants + ["_layer1_area"]])
                 .merge(layer2[layer2_constants + ["_layer2_area"]]))

    # Calculating area fractions to scale the uniforms
    layer_new["_layer1_areafraction"] = layer_new["_layernew_area"] / \
        layer_new["_layer1_area"]
    layer_new["_layer2_areafraction"] = layer_new["_layernew_area"] / \
        layer_new["_layer2_area"]

    # ID columns for scaling uniform values
    layer1_ids = [k for k, v in attributes.items() if (
        (k in layer1.columns) and (v == "ID"))]
    layer2_ids = [k for k, v in attributes.items() if (
        (k in layer2.columns) and (v == "ID"))]

    # Scaling uniform values in the intersecting layer
    # layer 1 multiple intersecting geometries will multiple count layer 2 uniforms
    # layer 2 multiple intersecting geometries will multiple count layer 1 uniforms
    layer_new["_layer1_multi_counts"] = layer_new[[
        col + "_set" for col in layer2_ids]].applymap(len).product(axis=1)
    layer_new["_layer2_multi_counts"] = layer_new[[
        col + "_set" for col in layer1_ids]].applymap(len).product(axis=1)

    # Uniform
    # multiplied by the area fraction and
    # divided by the multiple count that the area was counted for
    for uniform in layer1_uniforms:
        layer_new[uniform] = (layer_new[uniform]
                              * layer_new["_layer1_areafraction"]
                              # Keeping the below line commented
                              # / layer_new["_layer1_multi_counts"]
                              )

    for uniform in layer2_uniforms:
        layer_new[uniform] = (layer_new[uniform]
                              * layer_new["_layer2_areafraction"]
                              # / layer_new["_layer2_multi_counts"]
                              )

    # Deleting layer intermediate calculations
    del layer1["_layer1_area"]
    del layer2["_layer2_area"]
    del layer_new["_layernew_area"]
    del layer_new["_layer1_areafraction"]
    del layer_new["_layer2_areafraction"]
    del layer_new["_layer1_area"]
    del layer_new["_layer2_area"]
    del layer_new["_layer1_multi_counts"]
    del layer_new["_layer2_multi_counts"]

    return layer_new


def flatten(layers, attributes):
    """
    Disaggregate geometries by and propagates relevant data.

    It is assumed that the layers are individual `geopandas.GeoDataFrame`` and have
    three types of columns, which signify the way data is propagated. These types are
    ``ID``, ``constant``, ``uniform``. These types are stored in the dictionary
    ``attributes``. The dictionary has a mapping of all requisite columns in each of the
    layers as keys, and each of the above mentioned types as the values for those keys.
    If an ``ID`` type column is present in a layer, it means that the layer consists of
    intersecting geometries. If this happens, it is passed through the
    ``complete_disjoint_geoms`` function to render it into completely non-overlapping
    geometries. The other attributes are such:

    * ``constant``: The attribute is equal everywhere within the feature geometry
      (e.g. identifier, percent area).

      #. When splitting a feature, the attribute value for the resulting
         features is that of their parent: e.g. [1] -> [1], [1].

      #. When joining features, the attribute value for the resulting feature
         must be a function of its children: e.g. [1], [1] -> [1, 1] (list) or 1
         (appropriate aggregation function, e.g. median or area-weighted mean).

    * ``uniform``: The attribute is uniformly distributed within the feature geometry
      (e.g. count, area).

      #. When splitting a feature, the attribute value for the resulting
         features is proportional to their area: e.g. [1] (100% area) -> [0.4]
         (40% area), [0.6] (60% area).

      #. When joining features, the attribute value for the resulting feature
         is the sum of its children: e.g. [0.4], [0.6] -> [1].

    Args:
        layers (list of geopandas.GeoDataFrame): Polygon feature layers.
        attributes (dict): Attribute names and types ({ name: type, ... }),
            where type is either ``ID``, ``constant`` or ``uniform``.

    Returns:
        geopandas.GeoDataFrame: Polygon feature layer with all attributes named in
        ``attributes``.

    """
    for i, layer in enumerate(layers):

        cols = layer.columns
        type_cols = [attributes.get(col) for col in cols]

        if "ID" in type_cols:
            # New column added and hence attributes dict updated in case of
            # intersecting geometries
            layer, attributes = complete_disjoint_geoms(layer, attributes)

        if i == 0:
            layer_new = layer

        else:
            layer_new = layer_intersection(layer_new, layer, attributes)

    return layer_new


def allocate_and_aggregate(disagg_layer,
                           attributes,
                           timeseries,
                           alloc_exps=None,
                           geo_layer=None,
                           by="respondent_id_ferc714",
                           allocatees="demand",
                           allocators="POPULATION",
                           aggregators=None):
    """
    Aggregate selected columns of the disaggregated layer based on arguments.

    It is assumed that the data, which needs to be disaggregated, is present as
    ``constant`` attributes in the GeoDataFrame. The data is mapped by the ``by``
    columns. So, first the data is disaggregated, according to the allocator columns.
    Then, it is returned if aggregators list is empty. If it is not, then the data is
    aggregated again to the aggregator level.

    Args:
        disagg_layer (geopandas.GeoDataframe): Completely disaggregated GeoDataFrame
        attributes (dict): a dictionary keeping a track of all the types of attributes
            with keys represented by column names from layer1 and layer2, and the values
            representing the type of attribute. Types of attributes include "constant",
            "uniform" and "ID". If a column name ``col`` of type "ID" exists, then one
            column name ``col`` + "_set" of type "constant" will exist in the attributes
            dictionary.
        timeseries (pandas.DataFrame): A dataframe which has the columns present in the
            variable ``by``, which acts as the index. Also, it has the ``allocatees``
            columns, usually indexed as a string type of a ``datetime`` element or some
            other aggregated version of a timeslice.
        alloc_exps (str or list): The exponent to which each column in the
            ``allocators`` column is raised. If it is not assigned, all the exponents
            are considered 1.
        geo_layer (TYPE?): If ``geo_layer`` is ``None``, the function will calculate the
            allocated and aggregated geo_layer by ``aggregators`` column. This is unique
            for every reporting year and aggregators column. So, if there are multiple
            iterations of this function, it is best to save the ``geo_layer``, and
            assign it to the ``geo_layer`` argument in the function to reduce
            time-consuming redundant computation. If the ``geo_layer`` argument is not
            ``None``, the ``geo_layer`` is not returned as an output.
        by (str or list): single column or list of columns according to which the
            constants to be allocated are mentioned (e.g. "Demand" (constant) which
            needs to be allocated is mapped by "id". So, "id" is the ``by`` column)
        allocatees (str or list): single column or list of columns according to which
            the constants to be allocated are mentioned (e.g. "Demand" (constant)
            which needs to be allocated is mapped by "id". So, "demand" is the
            `allocatees` column)
        allocators (str or list): columns by which attribute is weighted and
            allocated
        aggregators (str or list): if empty list, the disaggregated data is
            returned. If aggregators is mentioned, for example REEDs geometries, the
            data is aggregated at that level.


    Returns:
        geopandas.GeoDataFrame: If aggregators is None, the function will return a
        disaggregated GeoDataFrame with all the various allocated demand columns. If
        aggregators is not `None`, the data will be aggregated by the `aggregators`
        column. The geometries span the individual elements of the aggregator columns.

    """
    logger.info("Prep Allocation Data")
    id_cols = [k for k, v in attributes.items() if (
        (k in disagg_layer.columns) and (v == "ID"))]

    id_set_cols = [col + "_set" for col in id_cols]
    disagg_layer["_multi_counts"] = (disagg_layer[id_set_cols]
                                     .applymap(len)
                                     .product(axis=1))

    # Allowing for single and multiple allocators,
    # aggregating columns and allocatees
    def listify(ele):
        if isinstance(ele, list):
            return ele
        else:
            return [ele]
    allocators, allocatees, by = tuple(
        map(listify, [allocators, allocatees, by]))

    if alloc_exps is None:
        alloc_exps = [1] * len(allocators)

    else:
        alloc_exps = listify(alloc_exps)

    for uniform_col in allocators:
        disagg_layer[uniform_col] = disagg_layer[uniform_col] / \
            disagg_layer["_multi_counts"]

    del disagg_layer["_multi_counts"]

    allocators_temp = ["_" + uniform_col + "_exp_" +
                       str(i) for i, uniform_col in enumerate(allocators)]

    logger.info("Raise allocators to appropriate exponents")
    for i, alloc_temp in enumerate(allocators_temp):
        disagg_layer[alloc_temp] = disagg_layer[allocators[i]] ** alloc_exps[i]

    # temp_allocator is product of all allocators in the row
    disagg_layer["temp_allocator"] = disagg_layer[allocators_temp].product(
        axis=1)

    logger.info("Calculate fractional allocation factors for each geometry")
    # the fractional allocation for each row is decided by the multiplier:
    # (temp_allocator/temp_allocator_agg)
    agg_layer = (disagg_layer[by + ["temp_allocator"]]
                 .groupby(by)
                 .sum()
                 .reset_index()
                 .rename(columns={"temp_allocator": "temp_allocator_agg"}))

    # adding temp_allocator_agg column to the disagg_layer
    disagg_layer = disagg_layer.merge(agg_layer)

    logger.info("Allocating demand from demand dataframe")
    demand_allocated_arr = (disagg_layer[by + ["temp_allocator", "temp_allocator_agg"]]
                            .merge(timeseries[by + allocatees])[allocatees].values) * \
        ((disagg_layer["temp_allocator"] /
          disagg_layer["temp_allocator_agg"]).values[:, np.newaxis])

    allocate_layer = pd.concat([disagg_layer, pd.DataFrame(demand_allocated_arr, columns=allocatees)],
                               axis=1)

    if aggregators is not None:

        logger.info("Aggregate data according to level specified")
        aggregators = listify(aggregators)

        if geo_layer is None:
            logger.info("Geo layer being created")
            geo_layer = (allocate_layer[aggregators + ["geometry"]]
                         .dissolve(by=aggregators, as_index=False))

            logger.info("Geo layer merged with aggregated data")
            final_agg_layer = (geo_layer
                               .merge(allocate_layer
                                      .groupby(aggregators)[allocatees]
                                      .sum()
                                      .reset_index())
                               .replace(0, np.nan))

            return final_agg_layer, geo_layer

        else:
            final_agg_layer = (geo_layer
                               .merge(allocate_layer
                                      .groupby(aggregators)[allocatees]
                                      .sum()
                                      .reset_index())
                               .replace(0, np.nan))

            return final_agg_layer

    else:
        logger.info("Complete demand allocation")
        return allocate_layer


################################################################################
# Historical Planning / Balancing Area Geometry Compilation
################################################################################
def categorize_eia_code(eia_codes, ba_ids, util_ids, priority="balancing_authority"):
    """
    Categorize FERC 714 ``eia_codes`` as either balancing authority or utility IDs.

    Most FERC 714 respondent IDs are associated with an ``eia_code`` which refers to
    either a ``balancing_authority_id_eia`` or a ``utility_id_eia`` but no indication
    as to which type of ID each one is. This is further complicated by the fact
    that EIA uses the same numerical ID to refer to the same entity in most but not all
    cases, when that entity acts as both a utility and as a balancing authority.

    This function associates a ``respondent_type`` of ``utility``,
    ``balancing_authority`` or ``pandas.NA`` with each input ``eia_code`` using the
    following rules:
    * If a ``eia_code`` appears only in ``util_ids`` the ``respondent_type`` will be
    ``utility``.
    * If ``eia_code`` appears only in ``ba_ids`` the ``respondent_type`` will be
    assigned ``balancing_authority``.
    * If ``eia_code`` appears in neither set of IDs, ``respondent_type`` will be
    assigned ``pandas.NA``.
    * If ``eia_code`` appears in both sets of IDs, then whichever ``respondent_type``
    has been selected with the ``priority`` flag will be assigned.

    Note that the vast majority of ``balancing_authority_id_eia`` values also show up
    as ``utility_id_eia`` values, but only a small subset of the ``utility_id_eia``
    values are associated with balancing authorities. If you use
    ``priority="utility"`` you should probably also be specifically compiling the list
    of Utility IDs because you know they should take precedence. If you use utility
    priority with all utility IDs

    Args:
        eia_codes (ordered collection of ints): A collection of IDs which may be either
            associated with EIA balancing authorities or utilities, to be categorized.
        ba_ids_eia (ordered collection of ints): A collection of IDs which should be
            interpreted as belonging to EIA Balancing Authorities.
        util_ids_eia (ordered collection of ints): A collection of IDs which should be
            interpreted as belonging to EIA Utilities.
        priorty (str): Which respondent_type to give priority to if the eia_code shows
            up in both util_ids_eia and ba_ids_eia. Must be one of "utility" or
            "balancing_authority". The default is "balanacing_authority".

    Returns:
        pandas.DataFrame: A dataframe containing 2 columns: ``eia_code`` and
        ``respondent_type``.

    """
    if priority == "balancing_authority":
        primary = "balancing_authority"
        secondary = "utility"
    elif priority == "utility":
        primary = "utility"
        secondary = "balancing_authority"
    else:
        raise ValueError(
            f"Invalid respondent type {priority} chosen as priority."
            "Must be either 'utility' or 'balancing_authority'."
        )

    eia_codes = pd.DataFrame(eia_codes, columns=["eia_code"]).drop_duplicates()
    ba_ids = (
        pd.Series(ba_ids, name="balancing_authority_id_eia")
        .drop_duplicates()
        .astype(pd.Int64Dtype())
    )
    util_ids = (
        pd.Series(util_ids, name="utility_id_eia")
        .drop_duplicates()
        .astype(pd.Int64Dtype())
    )

    df = (
        eia_codes
        .merge(ba_ids, left_on="eia_code", right_on="balancing_authority_id_eia", how="left")
        .merge(util_ids, left_on="eia_code", right_on="utility_id_eia", how="left")
    )
    df.loc[df[f"{primary}_id_eia"].notnull(), "respondent_type"] = primary
    df.loc[
        (df[f"{secondary}_id_eia"].notnull())
        & (df[f"{primary}_id_eia"].isnull()), "respondent_type"] = secondary
    df = (
        df.astype({"respondent_type": pd.StringDtype()})
        .loc[:, ["eia_code", "respondent_type"]]
    )
    return df


def has_demand(dhpa, rids):
    """
    Compile a dataframe indicating which respondents reported demand in what years.

    Args:
        dhpa (pandas.DataFrame): The demand_hourly_planning_area_ferc714 table, or
            some subset of it.  Must include the report_year, respondent_id_ferc714,
            demand_mwh, and report_year columns.
        rids (pandas.DataFrame): The respondent_id_ferc714 table, or similar dataframe,
            including a respondent_id_ferc714 column with all of the respondent ID
            values for which you want to check for demand.

    Returns:
        pandas.DataFrame: A dataframe with all 3 columns: respondent_id_ferc714 (int),
        report_year (int), and has_demand (bool). All possible combinations of
        respondent_id_ferc714 (from rids) and report_year (from dhpa) are present, and
        the value of has_demand is True if that respondent ID reported more than zero
        demand in that year.

    """
    # Create an complete 2-column index with all years and rids:
    all_years = (
        dhpa[["report_year"]]
        .drop_duplicates()
        .assign(tmp=1)
    )
    all_rids = (
        rids[["respondent_id_ferc714"]]
        .drop_duplicates()
        .assign(tmp=1)
    )
    all_years_rids = (
        pd.merge(all_years, all_rids)
        .drop("tmp", axis="columns")
    )
    out_df = (
        dhpa.groupby(["respondent_id_ferc714", "report_year"])
        .agg({"demand_mwh": sum})
        .reset_index()
        .assign(has_demand=lambda x: x.demand_mwh > 0.0)
        .rename(columns={"demand_mwh": "total_demand_mwh"})
        .merge(all_years_rids, how="right")
        .assign(has_demand=lambda x: x.has_demand.fillna(False))
        .pipe(pudl.helpers.convert_to_date)
    )
    return out_df


def georef_rids(rids_df, util_geom, ba_geom):
    """
    Add planning area geometries to the FERC 714 respondent_id table.

    Given a respondent_id_ferc714 dataframe with a respondent_type column
    containing only "utility", "balancing_authority" and Null values, merge in
    utility geometries from util_geom on utility_id_eia, and balancing
    authority geometries from ba_geom on balancing_authority_id_eia. Records
    with no respondent_type will end up having a Null geometry.

    Args:
        rids_df (pandas.DataFrame): A respondent_id_ferc714
            dataframe, having at least the respondent_id_ferc714,
            respondent_type, utility_id_eia, and balancing_authority_id_eia
            columns. Note that both of the EIA ID columns must be nullable.
            integers for the merge to work.
        util_geom (geopandas.GeoDataFrame): A geodataframe containing
            report_date, utility_id_eia, and a geometry column. May be
            either a dissolved geometry, or a county-level geometry. If
            the county level geometries are included, at least the
            county_id_fips column should also be included for later use.
            The utility_id_eia column must be a nullable integer type.
        ba_geom (geopandas.GeoDataFrame): A geodataframe containing
            report_date, balancing_authority_id_eia, and a geometry column.
            May be either a dissolved geometry, or a county-level geometry.
            If the county level geometries are included, at least the
            county_id_fips column should also be included for later use.
            The utility_id_eia column must be a nullable integer type. The
            CRS for ba_geom must be the same as util_geom.

    Returns:
        geopandas.GeoDataFrame: A FERC 714 respondent_id table with
        records for each planning area respondent and their associated
        geometries in each year. The CRS will be set to the same CRS
        as the input util_geom.

    """
    if util_geom.crs != ba_geom.crs:
        raise ValueError(
            "Utility and Balancing Authority CRS values don't match!"
        )
    rids_ba = ba_geom.merge(
        rids_df.query("respondent_type=='balancing_authority'"),
        how="right")
    rids_util = util_geom.merge(
        rids_df.query("respondent_type=='utility'"),
        how="right")
    rids_null = rids_df.loc[rids_df.respondent_type.isnull()]
    rids_all = (
        pd.concat([rids_ba, rids_util, rids_null])
        .astype({
            "respondent_id_ferc714": pd.Int64Dtype(),
            "utility_id_eia": pd.Int64Dtype(),
            "balancing_authority_id_eia": pd.Int64Dtype(),
        })
        .set_crs(util_geom.crs)
    )
    return rids_all
