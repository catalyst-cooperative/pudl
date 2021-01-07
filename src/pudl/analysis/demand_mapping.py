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
import calendar
import logging
import pathlib
import zipfile

import geopandas
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import scipy
import seaborn as sns
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
    Editing "id" sets by adding the new geometry id if required.

    This function edits original "id_set" by adding the new geometry "id" if
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

    This function adds rows with duplicate geometries and creates the new `id`
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
    the number of times particular `id` encircles it.

    Args:
        gdf (GeoDataframe): GeoDataFrame consisting of the intersecting
        geometries. Only `POLYGON` and `MULTIPOLYGON` geometries supported.
        Other geometries will be deleted.
        attributes (dict): a dictionary keeping a track of all the types of
        attributes with keys represented by column name, and the values
        representing the type of attribute. One column from the
        attribute dictionary must belong in the GeoDataFrame and should be of
        type `id` to allow for the intersection to happen. The other two
        possible types are `uniform` and `constant`. The `uniform` type
        attribute disaggregates the data across geometries and the `constant`
        type is propagated as the same value.

    Returns:
        geopandas.GeoDataFrame: GeoDataFrame with all attributes as gdf
        and one extra attribute with name as the `id` attribute appended by
        "_set" substring. The geometries will not include zero-area geometry
        components.

        attributes: Adds the `id`+"_set" as a `constant` attribute and returns the
        attributes dictionary
    """
    # ID is the index which will help to identify duplicate geometries
    gdf_ids = [k for k, v in attributes.items() if (
        (k in gdf.columns) and (v == "id"))][0]
    gdf_constants = [k for k, v in attributes.items() if (
        (k in gdf.columns) and (v == "constant"))]
    gdf_uniforms = [k for k, v in attributes.items() if (
        (k in gdf.columns) and (v == "uniform"))]

    # Check if `ID` column has all unique elements:
    if gdf[gdf_ids].nunique() != len(gdf):
        raise Exception("All id column elements should be unique")

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
            of attributes include ``constant``, ``uniform`` and ``id``. If a
            column name ``col`` of type ``id`` exists, then one column name
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
        (k in layer1.columns) and (v == "id"))]
    layer2_ids = [k for k, v in attributes.items() if (
        (k in layer2.columns) and (v == "id"))]

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
    ``id``, ``constant``, ``uniform``. These types are stored in the dictionary
    ``attributes``. The dictionary has a mapping of all requisite columns in each of the
    layers as keys, and each of the above mentioned types as the values for those keys.
    If an ``id`` type column is present in a layer, it means that the layer consists of
    intersecting geometries. If this happens, it is passed through the
    ``complete_disjoint_geoms`` function to render it into completely non-overlapping
    geometries. The other attributes are such:

    * ``constant``: The attribute is equal everywhere within the feature geometry
      (e.g. identifier, percent area).

      -  When splitting a feature, the attribute value for the resulting
         features is that of their parent: e.g. [1] -> [1], [1].

      - When joining features, the attribute value for the resulting feature
        must be a function of its children: e.g. [1], [1] -> [1, 1] (list) or 1
        (appropriate aggregation function, e.g. median or area-weighted mean).

    * ``uniform``: The attribute is uniformly distributed within the feature geometry
      (e.g. count, area).

      - When splitting a feature, the attribute value for the resulting
        features is proportional to their area: e.g. [1] (100% area) -> [0.4]
        (40% area), [0.6] (60% area).

      - When joining features, the attribute value for the resulting feature
        is the sum of its children: e.g. [0.4], [0.6] -> [1].

    Args:
        layers (list of geopandas.GeoDataFrame): Polygon feature layers.
        attributes (dict): Attribute names and types ({ name: type, ... }),
            where type is either ``id``, ``constant`` or ``uniform``.

    Returns:
        geopandas.GeoDataFrame: Polygon feature layer with all attributes named in
        ``attributes``.

    """
    for i, layer in enumerate(layers):

        cols = layer.columns
        type_cols = [attributes.get(col) for col in cols]

        if "id" in type_cols:
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
                           allocatees="demand_mwh",
                           allocators="population",
                           aggregators=None):
    """
    Aggregate selected columns of the disaggregated layer based on arguments.

    It is assumed that the data, which needs to be disaggregated, is present as
    ``constant`` attributes in the GeoDataFrame. The data is mapped by the
    ``by`` columns. So, first the data is disaggregated, according to the
    allocator columns. Then, it is returned if aggregators list is empty. If it
    is not, then the data is aggregated again to the aggregator level.

    Args:
        disagg_layer (geopandas.GeoDataframe): GeoDataFrame with all required
            attribute and geometry layers disaggregated into disjoint sections
            with all attributes distributed to individual disjoint geometries
            based on whether they are constant or uniform.
        attributes (dict): a dictionary keeping a track of all the types of
            attributes with keys represented by column names from all the
            various layers which have been disaggregated into disagg_layer,
            and the dictionary values representing the type of attribute. Types
            of attributes include "constant", "uniform" and "id". If a column
            name ``col`` of type "id" exists, then one column name
            ``col`` + "_set" of type "constant" will exist in the attributes
            dictionary.
        timeseries (pandas.DataFrame): A dataframe which has the columns present
            in the variable ``by``, which acts as the index. Also, it has the
            ``allocatees`` columns, usually indexed as a string type of a
            ``datetime`` element or some other aggregated version of a
            timeslice.
        alloc_exps (str or list): The exponent to which each column in the
            ``allocators`` column is raised. If it is not assigned, all the
            exponents are considered 1.
        geo_layer (TYPE?): If ``geo_layer`` is ``None``, the function will
            calculate the allocated and aggregated geo_layer by ``aggregators``
            column. This is unique for every reporting year and aggregators
            column. So, if there are multiple iterations of this function, it is
            best to save the ``geo_layer``, and assign it to the ``geo_layer``
            argument in the function to reduce time-consuming redundant
            computation. If the ``geo_layer`` argument is not ``None``, the
            ``geo_layer`` is not returned as an output.
        by (str or list): single column or list of columns according to which
            the constants to be allocated are mentioned (e.g. "demand_mwh"
            (constant) which needs to be allocated is mapped by
            "respondent_id_ferc714". So, that's the ``by`` column
        allocatees (str or list): single column or list of columns according to
            which the constants to be allocated are mentioned (e.g. "demand_mwh"
            (constant) which needs to be allocated is mapped by "id". So,
            "demand_mwh" is the `allocatees` column)
        allocators (str or list): columns by which attribute is weighted and
            allocated
        aggregators (str or list): if empty list, the disaggregated data is
            returned. If aggregators is mentioned, for example REEDs geometries,
            the data is aggregated at that level.


    Returns:
        geopandas.GeoDataFrame: If aggregators is None, the function will return
            a disaggregated GeoDataFrame with all the various allocated demand
            columns. If aggregators is not `None`, the data will be aggregated
            by the `aggregators` column. The geometries span the individual
            elements of the aggregator columns.

    """
    logger.info("Prep Allocation Data")
    id_cols = [k for k, v in attributes.items() if (
        (k in disagg_layer.columns) and (v == "id"))]

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


def sales_ratio_by_class_fips(pudl_out):
    """
    Estimate fraction of sales to each customer class by county.

    For each combination of utility_id_eia, report_date, and state found in the EIA 861
    sales table, the relative proportion of electricity sales going to each customer
    class will be calculated. The resulting dataframe is merged with the Service
    Territory table, associating these proprotions with counties (by FIPS code). For
    counties that have more than one estimate in a year, from different utilities, the
    mean of all estimates will be calculated, resulting in a unique estimate of the
    relative proportions of electricity sales to each customer class, in each county,
    in each year.

    Args:
        pudl_out (pudl.output.pudltabl.PudlTabl): A PUDL output object that will be
            used to pull the EIA 861 Sales and Service Territory tables.

    Returns:
        pandas.DataFrame: DataFrame containing unique combinations of ``report_date``
        and ``county_id_fips`` (but not as an index) as well as one column for each
        customer class in the EIA 861 Sales table (residential, commercial, industrial,
        transportation, and other). The sum of the values in each row should be 1.0,
        and the values represent the relative proportions of electricity sales that
        went to each customer class in each county that year.

    """
    sales_by_class = (
        pudl_out.sales_eia861()
        .astype({"customer_class": pd.StringDtype()})
        .groupby(["utility_id_eia", "state", "report_date", "customer_class"], observed=True)
        .agg({"sales_mwh": sum})
        .unstack()
    )
    sales_by_class.columns = sales_by_class.columns.droplevel()
    total_sales_mwh = sales_by_class.sum(axis="columns")
    sales_by_class_fips = (
        sales_by_class
        .divide(total_sales_mwh, axis="index")
        .reset_index()
        .merge(
            pudl_out.service_territory_eia861()[[
                "utility_id_eia",
                "state",
                "report_date",
                "county_id_fips"
            ]]
        )
        .drop(["utility_id_eia", "state"], axis="columns")
        .groupby(["report_date", "county_id_fips"])
        .mean()
        .reset_index()
    )
    return sales_by_class_fips


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


################################################################################
# Demand Data and Error Visualization Functions
################################################################################


def compare_datasets(alloc_demand, actual_demand, demand_columns, select_regions, time_col="utc_datetime", region="pca"):
    """
    Stack allocated and actual demand data together for comparison.

    Given the allocated and actual demand dataframes where both dataframes are
    similarly oriented, i.e. one column specifying all the unique regions, and
    other columns specifying demand data, with the column labelled by the time
    slice it is calculated for, the function will output a single datafram
    which gives a stacked comparison of the actual demand and allocated demand
    at every time interval and for every specified region.

    Args:
        alloc_demand (pandas.DataFrame): A dataframe with the `region` and
            a subset of `demand_columns`. Each column name in the
            `demand_columns` is typically an hourly datetime object refering to
            the time period of demand observed, but can be any other timeslice.
            The `demand_columns` contain the allocated demand as allocated by
            the `allocate_and_aggregate` function. Any columns not present in
            demand_columns will be imputed as np.nan.
        actual_demand (pandas.DataFrame): A similar dataframe as actual_demand,
            but contains actual demand data, which is being compared against.
        demand_columns (list): A list containing column names present in the
            `alloc_demand` dataframe and the `actual_demand` dataframe. If some
            of the columns are not present in either dataframe, those columns
            are instantiated with NaN values.
        select_regions (list): The list of all unique ids whose actual and
            allocated demand is compared. If all regions are to be compared,
            pass `actual_demand[region].unique()` as the argument.
        time_col (str): name that will be given to the time column in the output
            dataframe
        region (str): It is the name of the column, common to both
            `alloc_demand` and `actual_demand` dataframes which refers to the
            unique ID of each region whose demand is being calculated

    Returns:
        pandas.DataFrame: A stacked dataframe which can be utilised for Seaborn
        visualizations to estimate accuracy of allocation and error metrics.

    """
    # Add excepted columns as NaN values
    for col in set(demand_columns).difference(alloc_demand.columns):
        alloc_demand[col] = np.nan

    for col in set(demand_columns).difference(actual_demand.columns):
        actual_demand[col] = np.nan

    # Add column name so that when transformed, the appropriate name is provided
    actual_demand.columns.name = time_col
    alloc_demand.columns.name = time_col

    # Provide NaN values to region ids missing in `alloc_demand`
    missing_region = set(actual_demand[region].unique()).difference(
        alloc_demand[region].unique())
    alloc_demand = alloc_demand.set_index(region)
    alloc_demand = alloc_demand.reindex(
        alloc_demand.index.union(missing_region))
    alloc_demand.index.name = region
    alloc_demand = alloc_demand.reset_index()

    actual_demand_transpose = (actual_demand[actual_demand[region].isin(select_regions)]
                               .set_index(region)[demand_columns]
                               .T
                               .reset_index()
                               [
        [time_col] + select_regions
    ])

    alloc_demand_transpose = (alloc_demand[alloc_demand[region].isin(select_regions)]
                              .set_index(region)[demand_columns]
                              .T
                              .reset_index()
                              [
        [time_col] + select_regions
    ])

    demand_data = actual_demand_transpose.merge(
        alloc_demand_transpose, on=time_col, suffixes=('_measured', '_predicted'), how="outer")

    demand_data = demand_data.set_index(time_col).unstack(
    ).reset_index().rename(columns={0: "Average Hourly Demand (MW)"})
    demand_data[["region", "demand_type"]
                ] = demand_data[region].str.split("_", expand=True)
    demand_data.drop(region, axis=1, inplace=True)

    demand_data = (demand_data
                   .pivot_table(values="Average Hourly Demand (MW)",
                                index=[time_col, "region"],
                                columns="demand_type")
                   .reset_index())

    return demand_data


def corr_fig(compare_data, select_regions=None, suptitle="Parity Plot", s=2, top=0.85):
    """
    Create visualization to compare the allocated and actual demand for every region.

    Uses the output of `compare_datasets` function as input to check
    correlations between actual demand data and the allocated demand data.

    Args:
        compare_data (pandas.DataFrame): This is typically the output of the function
            `compare_datasets`. It has columns named 'alloc', 'actual' and
            'region'.
        select_regions (list): If select_regions is None (default), all regions'
            parity plot will be constructed, else the specific regions mentioned
            in the list will be plotted.
        suptitle (str): The title for the whole image
        s (float): Adjust the size of the markers which are displayed
            in the graph
        top (float): The space by which the top needs to be adjusted to allow
            for the `suptitle` to be adjusted. Ranges typically between 0.85
            and 0.976. Right tuning required

    Returns:
        None: Displays the image

    """
    compare_data = compare_data.dropna()

    mpl.rcdefaults()
    pred = 'predicted'
    actual = "measured"

    if select_regions is not None:
        compare_data = compare_data[compare_data["region"].isin(
            select_regions)]

    else:
        select_regions = list(compare_data["region"].unique())

    g = sns.FacetGrid(compare_data, col="region", col_wrap=3,
                      sharey=False, sharex=False)
    (g.map(sns.regplot, actual, pred, scatter_kws={'alpha': 0.1, 's': s})
     .set_axis_labels('Measured Demand (MW)', 'Predicted Demand (MW)'))

    region_list = compare_data["region"].unique().tolist()

    counter = 0

    for ax in g.axes.flat:

        df_temp = compare_data[compare_data["region"] == region_list[counter]]
        min_max = df_temp.describe().loc[["min", "max"], [
            actual, pred]]

        slope, intercept, r_value, _, _ = scipy.stats.linregress(
            df_temp[actual], df_temp[pred])

        min_lim, max_lim = 0, min_max.max().max()

        ax.plot((min_lim, max_lim), (min_lim, max_lim), ls="--")
        ax.text(max_lim - 10, max_lim - 10, f"y={slope:.2f}x + {intercept:.1f} (R² = {r_value:.2f})",
                horizontalalignment='right', verticalalignment="top")

        ax.set_ylim(min_lim, max_lim)
        ax.set_xlim(min_lim, max_lim)

        counter += 1

    g.fig.suptitle(suptitle)
    # Formula for specifically adjusting the `suptitle`
    # top=(0.8471363 + np.ceil(len(select_regions) / 3) / 44 * 0.126))
    g.fig.subplots_adjust(top=top)
    plt.show()


def error_fig(df_compare, select_regions=None, index_col="region", time_col="utc_datetime"):
    """
    Create visualization to compare the relative and absolute error at various timescales.

    Uses the output of `compare_datasets` function as input to check Root Mean
    Squared Error (RMSE) and Mean Absolute Percentage Error (MAPE) for the hour
    of the day, day of the week and the month of the year.

    Args:
        df_compare (pandas.DataFrame): This is typically the output of the
            function `compare_datasets`. It has columns named 'alloc', 'actual'
            and 'region'.
        select_regions (list): This is the subset of the regions from the
            `index_col` whose error metrics need to be calculated. if
            select_regions is None, the calculation is done for the entire US
            mainland.
        index_col (str): The name of the index column (usually 'region')
        time_col (str): The name of the time column (usually 'utc_datetime')

    Returns:
        None: Displays the image

    """
    def rmse(x):
        return np.sqrt(np.mean(x))

    if select_regions is not None:
        df_compare = df_compare[df_compare[index_col].isin(select_regions)]

    df_compare["Root Mean Squared Error (MWh)"] = (
        df_compare["measured"] - df_compare["predicted"]) ** 2

    df_compare["Mean Absolute Percentage Error (%)"] = np.abs(
        (df_compare["measured"] - df_compare["predicted"]) / df_compare["measured"])

    df_compare["Hour of Day"] = df_compare[time_col].dt.hour
    df_compare["Day of Week"] = df_compare[time_col].apply(
        lambda x: x.weekday())
    df_compare["Month"] = df_compare["utc_datetime"].dt.month - 1
    df_compare["na_predicted"] = df_compare["predicted"].isna().astype(int)
    df_compare["na_measured"] = df_compare["measured"].isna().astype(int)

    fig, ax = plt.subplots(3, 2, figsize=(15, 10))

    for i, col in enumerate(["Hour of Day", "Day of Week", "Month"]):
        sns.barplot(x=col, y="Root Mean Squared Error (MWh)", data=df_compare, ax=ax[i, 0], color="blue",
                    estimator=rmse, ci=None)

    for i, col in enumerate(["Hour of Day", "Day of Week", "Month"]):
        sns.barplot(x=col, y="Mean Absolute Percentage Error (%)", data=df_compare, ax=ax[i, 1], color="blue",
                    estimator=np.mean, ci=None)

    for i in [0, 1]:

        ax[1, i].set_xticklabels([calendar.day_abbr[day]
                                  for day in list(range(7))])

        ax[2, i].set_xticklabels([calendar.month_abbr[month + 1]
                                  for month in list(range(12))])

    fig.tight_layout()
    plt.show()


def regional_demand_profiles(df_compare, select_regions=None, agg=False, time_col="utc_datetime", region_text=None):
    """
    Create visualization to compare the average demand profiles for selected regions.

    Uses the output of `compare_datasets` function as input to plot average
    daily (hour-of-day), weekly (day-of-week) and yearly (month-of-year) demand
    profiles for the selected regions under `select_regions`.

    Args:
        df_compare (pandas.DataFrame): This is typically the output of the function
            `compare_datasets`. It has columns named 'alloc', 'actual' and
            'region'.
        select_regions (list): The list of all unique ids whose actual and
            allocated demand is compared. If not set, all regions will be
            considered.
        agg (bool): If agg is True, all the 'select_regions' will be added and
            compared. If agg is False, then all the regions' data will be
            separately considered.
        time_col (str): The name of the time column (usually 'utc_datetime')
        region_text (str): Works only if agg is True, set custom text label for
            the set of regions being considered. If agg is True, and region_text
            is not set, all the regions will be named separated by a comma.

    Returns:
        None: Displays the image

    """
    if select_regions is not None:
        df_compare = df_compare[df_compare["region"].isin(select_regions)]
        if region_text is None:
            region_text = (",").join(select_regions)

    else:
        select_regions = list(df_compare["region"].unique())
        if region_text is None:
            region_text = "US Mainland"

    if agg is True:
        df_compare.groupby(time_col).agg(np.nansum).replace(0, np.nan)
        df_compare["region"] = region_text
        select_regions = [region_text]

    df_compare["hour_of_day"] = df_compare["utc_datetime"].dt.hour
    df_compare["day_of_week"] = df_compare["utc_datetime"].apply(
        lambda x: x.weekday())
    df_compare["month_of_year"] = df_compare["utc_datetime"].dt.month
    # df_compare = df_compare[df_compare["region"].isin(select_regions)]

    df_compare = (df_compare
                  .set_index([time_col, "region"] + ["hour_of_day", "day_of_week", "month_of_year"])
                  .stack()
                  .reset_index().rename(columns={0: "Average Hourly Demand (MW)"}))
    # display(df_compare)

    df_compare = (df_compare
                  .set_index([time_col, "region"] + ["demand_type", "Average Hourly Demand (MW)"])
                  .stack()
                  .reset_index().rename(columns={0: "Time Interval", "level_4": "time_type"}))

    g = sns.relplot(x="Time Interval", y="Average Hourly Demand (MW)",
                    hue="demand_type", col="time_type", row="region",
                    kind="line", data=df_compare,
                    facet_kws={'sharey': False, 'sharex': False})

    g.set(ylim=(0, None))
    ax = g.axes

    for i in range(len(select_regions)):
        ax[i, 1].set_xticks([0, 1, 2, 3, 4, 5, 6])
        ax[i, 1].set_xticklabels(
            ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'])


def uncovered_area_mismatch(disagg_geom, total_geom, title="Area Coverage (By Planning Area)"):
    """
    Create map visualization to analyze regions covered by FERC714 allocation.

    Uses the final disaggregated layer as input along with the map of the total
    US mainland to display coverage of the planning areas. The US areas which
    have not been allocated demand are shown in gray. Rest are displayed in the
    Viridis colorscheme.

    Args:
        disagg_geom (geopandas.GeoDataFrame): Input the final disaggregated
            geodataframe, which has the column `respondent_id_ferc714_set`. This
            column mentiones the number of overlapping planning areas reporting
            for a particular geometry.
        total_geom (geopandas.GeoDataFrame): This is a geodataframe which has
            the total US mainland map. This will allow display of the regions
            not covered even once as grey.
        title (str): The title for the map visualization

    Returns:
        None: Displays the image

    """
    covered_geom = (disagg_geom[["respondent_id_ferc714_set", "geometry"]]
                    .drop_duplicates()
                    .reset_index(drop=True)
                    )
    covered_geom["num_covered"] = covered_geom["respondent_id_ferc714_set"].apply(
        len)
    max_val = covered_geom["num_covered"].max()

    # extract viridis features
    cmap = plt.cm.viridis  # define the colormap
    cmaplist = [cmap(i) for i in range(cmap.N)]
    # force the first color entry to be grey
    cmaplist[0] = (.5, .5, .5, 1.0)
    # create the new map
    cmap = mpl.colors.LinearSegmentedColormap.from_list(
        'DiscreteViridis', cmaplist, cmap.N)

    # define the bins and normalize
    bounds = np.linspace(-0.5, max_val + 0.5, max_val + 2)
    ticks = [b + 0.5 for b in bounds][:-1]
    norm = mpl.colors.BoundaryNorm(bounds, cmap.N)

    # create the figure
    fig, ax = plt.subplots(figsize=(20, 13))
    ax = total_geom.assign(num=1).dissolve(
        by="num").plot(facecolor="grey", ax=ax)
    covered_geom.plot("num_covered", legend=False, ax=ax, cmap=cmap, norm=norm)
    plt.title(title, fontdict={'fontsize': 25})

    ax.set_xticks([])
    ax.set_yticks([])

    # create a second axes for the colorbar
    ax2 = fig.add_axes([0.97, 0.1, 0.03, 0.8])
    mpl.colorbar.ColorbarBase(ax2, cmap=cmap, norm=norm,
                              spacing='proportional', ticks=ticks,
                              boundaries=bounds, format='%1i')

    plt.show()


def vec_error(vec1, vec2, errtype):
    """
    Calculate error metrics between two vectors vec1 (alloc), and vec2 (actual).

    Takes input of two numpy arrays, vec1 and vec2, and calculates error metric.
    Possible specifications of error metrics include: Mean Squared Error
    ('mse'), Mean Absolute Percentage Error ('mape%') and R2 value ('r2').
    """
    vec1 = np.array(vec1)
    vec2 = np.array(vec2)
    if errtype == "mse":
        return np.nanmean((vec1 - vec2) ** 2)

    elif errtype == "mape%":
        vec1[vec2 == 0] = np.nan
        return np.nanmean(np.abs((vec1 - vec2) / vec2)) * 100

    elif errtype == "r2":
        mask = ~np.isnan(vec1) & ~np.isnan(vec2)
        if vec1[mask].size == 0:
            return np.nan

        else:
            _, _, r_value, _, _ = scipy.stats.linregress(
                vec1[mask], vec2[mask])
            return r_value ** 2


def error_heatmap(alloc_df, actual_df, demand_columns, region_col="pca", error_metric="r2", leap_exception=False):
    """
    Create heatmap of 365X24 dimension to visualize the annual hourly error.

    Uses the output of `allocate_and_aggregate` function as input along with
    actual demand data to plot the annual hourly errors as a heatmap on a 365X24
    grid.

    Args:
        alloc_df (pandas.DataFrame): A dataframe with the `region` and a subset
            of `demand_columns`. Each column name in the `demand_columns` is
            typically an hourly datetime object refering to the time period of
            demand observed, but can be any other timeslice. The
            `demand_columns` contain the allocated demand as allocated by the
            `allocate_and_aggregate` function. Any columns not present in
            `demand_columns` will be imputed as np.nan.
        actual_df (pandas.DataFrame): A similar dataframe as actual_demand,
            but contains actual demand data, which is being compared against.
        region_col (str): The column_name which contains the unique ids for each
            of the regions.
        error_metric (str): Specifies the error metric to be observed in the
            heatmap. Possible error metrics available include: Mean Squared
            Error ('mse'), Mean Absolute Percentage Error ('mape%') and R2 value
            ('r2').
        leap_exception (bool): Specify if the year being analyzed is a leap year
            or not to account for February 29th.

    Returns:
        None: Displays the image

    """
    font = {'size': 12}

    mpl.rc('font', **font)
    demand_columns = list(set(demand_columns)
                          .intersection(set(actual_df.columns)))
    columns_excepted = set(demand_columns).difference(set(alloc_df.columns))

    actual_df = actual_df.sort_values(
        region_col)[[region_col] + demand_columns]

    for col in columns_excepted:
        alloc_df[col] = np.nan

    alloc_df = actual_df[[region_col]].merge(
        alloc_df[[region_col] + demand_columns], how="left")
    hmap = np.empty((365 + int(leap_exception), 24))

    dofw_list = [None] * (365 + int(leap_exception))
    month_label_idx = []
    month_start_idx = []

    for col in demand_columns:
        hmap[col.timetuple().tm_yday - 1, col.hour] = vec_error(np.array(alloc_df[col]),
                                                                np.array(
            actual_df[col]),
            error_metric)
        dofw_list[col.timetuple().tm_yday - 1] = col.weekday()

        if col.day == 1:
            month_start_idx.append(
                (col.timetuple().tm_yday - 1, "-------------"))

        elif col.day == 15:
            month_label_idx.append(
                (col.timetuple().tm_yday, calendar.month_name[col.month] + '            '))

    monday_idx = [(i + 0.5, "(Mon)")
                  for i, v in enumerate(dofw_list) if v == 0]

    df_idx_label = (pd.DataFrame(list(set(month_start_idx))
                                 + list(set(month_label_idx))
                                 + list(set(monday_idx)), columns=['index', 'label'])
                    .sort_values("index"))
    yticks = df_idx_label["index"].tolist()
    yticklabels = df_idx_label["label"].tolist()

    mask = np.isnan(hmap)
    # fig, ax = plt.subplots(figsize=(6, 80))
    fig = plt.figure(figsize=(6, 80))
    ax = fig.add_subplot(111)
    hmap = sns.heatmap(hmap, ax=ax, mask=mask)
    hmap.set_yticks(yticks)
    hmap.set_yticklabels(
        yticklabels, rotation=0)

    hmap.set_xticks([tick + 0.5 for tick in [0, 4, 8, 12, 16, 20]])
    hmap.set_xticklabels([0, 4, 8, 12, 16, 20])

    plt.ylabel("Day of Year")
    plt.xlabel("Hour of Day (UTC Datetime)")

    plt.title(error_metric.upper())
    plt.show()
    mpl.rcdefaults()
