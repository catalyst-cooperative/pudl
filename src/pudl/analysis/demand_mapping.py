"""
Routines for geographically re-sampling regional electricity demand.

Electricity demand is reported by a variety of entities, pertaining to many
different geographic areas. It's often useful to be able to estimate how
demand reported for one geography maps onto another. For instance, taking the
total hourly electricity demand reported with in an ISO region or utility
planning area, and allocating it to individual counties according to their
populations, as reported within US Census tracts.

This process involves 3 different kinds of geometries:

* The demand source geometry, i.e. area associated with the reported demand
* One or more geometries having data associated with them that will be used
  to inform the geographic allocation of the reported demand. E.g. US
  census tract boundaries and their reported populations.
* The output or target geometry, to which the geographically allocated demand
  will be aggregated for final use. E.g. counties, states, NREL ReEDS
  balancing areas, or EPA's IPM regions.

First we calculate the intersection of the reporting demand areas and the
geometires which have data we want to use to allocate demand geographically.

Second, for each of those intersecting areas, we use data associated with the
non-demand geometry to calculate a weighting factor that will determine what
share of the overall demand that area gets.

Third, we use these weights and the demand time series keyed to the IDs of the
demand areas to generate new demand time series for each of the intersecting
geometries.

Finally, we aggregate the demand that has been allocated to the intersecting
areas up to the areas identified within the target output geometry.

"""
import geopandas
import pandas as pd


def create_stacked_intersection_df(gdf_intermediate,
                                   gdf_source,
                                   gdf_intermediate_col="FIPS",
                                   gdf_source_col="ID",
                                   geom_intermediate="geometry",
                                   geom_source="geometry"):
    """
    Build matrix with 1-1 mapping of intermediate and source GeoDataFrame.

    Under standard convention, the source GeoDataFrame consists of the demand
    geometry and is used to map areas to an intermediate GeoDataFrame.

    Example:
        intermediate GeoDataFrame: census tract geometries spanning the US
        source GeoDataFrame: planning area geometries with associated demand.

    The function returns a stacked dataframe with mapping of each intermediate
    geometry column with every non-intersecting source geometry column
    quantifying the intersecting area.

    Args:
        gdf_intermediate (GeoDataframe): intermediate geodataframe
        gdf_source (GeoDataframe): usually source dataframe (i.e. demand)
        gdf_intermediate_col (str): column to identify gdf_intermediate
        gdf_source_col(str): column to identify for gdf_source
        file_save (bool): save file option

    Returns:
        pandas.DataFrame: DataFrame used to map and scale attribute from one
        set of geometries to another.

        Columns:
            intermediate_index: unique index for intermediate geometry
            source_index: unique index for source geometry
            intermediate_intersection_fraction: area as fraction of
            intermediate geometry
            source_intersection_fraction: area as fraction of source geometry

    """
    new_df = geopandas.overlay(
        gdf_intermediate[[gdf_intermediate_col, geom_intermediate]],
        gdf_source[[gdf_source_col, geom_source]],
        how='intersection'
    )
    new_df["area_derived"] = new_df["geometry"].area

    gdf_intermediate["intermediate_area_derived"] = gdf_intermediate[geom_intermediate].area
    gdf_source["source_area_derived"] = gdf_source[geom_source].area

    new_df = (new_df[[gdf_intermediate_col, gdf_source_col, "area_derived", "geometry"]]
              .merge(gdf_intermediate[[gdf_intermediate_col, "intermediate_area_derived"]])
              .merge(gdf_source[[gdf_source_col, "source_area_derived"]]))

    new_df["gdf_intermediate_intersection_fraction"] = new_df["area_derived"] / \
        new_df["intermediate_area_derived"]
    new_df["gdf_source_intersection_fraction"] = new_df["area_derived"] / \
        new_df["source_area_derived"]

    # Deleting temporary columns defined in the original GeoDataFrames
    del gdf_intermediate["intermediate_area_derived"]
    del gdf_source["source_area_derived"]

    new_df = new_df[[gdf_intermediate_col, gdf_source_col,
                     "gdf_intermediate_intersection_fraction",
                     "gdf_source_intersection_fraction"]]

    return new_df


def create_intersection_matrix(gdf_intersection,
                               gdf_intersection_col="gdf_intermediate_intersection_fraction",
                               gdf_intermediate_col="FIPS",
                               gdf_source_col="ID",
                               normalization=True,
                               normalize_axis=1):
    """
    Pivots stacked dataframe to an intersection matrix.

    Inputs the intersection matrix with the area intersection fraction a[i, j]
    for every intermediate ID i and source ID j. Normalization option available
    (To normalize double-counting)

    Args:
        gdf_intersection (pandas.DataFrame): stacked dataframe with every
            one-to-one mapping of intermediate ids.
        gdf_intersection_col (str): name of the column of the area intersection
            fraction. (Usually the fraction along the column you want to
            normalize. Generally makes sense to do it along the distinct and
            disjoint column like census tracts)
        gdf_intermediate_col (str): ID name of intermediate gdf
        gdf_source_col (str): ID name of source gdf
        normalize_axis (int): 1 for normalizing along intermediate axis
            (generally); 0 for normalizing along source axis

    Returns:
        pandas.DataFrame: matrix which consists the intersection value for
        every intermediate geometry i and source geometry j at index i, j

    """
    intersection_matrix = gdf_intersection.pivot_table(
        values=gdf_intersection_col,
        index=gdf_intermediate_col,
        columns=gdf_source_col,
        fill_value=0
    )

    if normalization is True:
        intersection_matrix = intersection_matrix.divide(intersection_matrix.sum(axis=normalize_axis),
                                                         axis=int(not normalize_axis))

    return intersection_matrix


def matrix_linear_scaling(intersection_matrix,
                          gdf_scale,
                          gdf_scale_col="POPULATION",
                          axis_scale=1,
                          normalize=True):
    """
    Scales matrix by a vector with or without normalization.

    Scales the linear mapping matrix by a particular variable e.g. If you want
    to allocate demand by population, scale the area intersection fraction
    matrix by the population of each of the FIPS using matrix_linear_scaling
    once by axis_scale=1, appropriate dataframe, scale_col="POPULATION", then
    allocate demand using matrix_linear_scaling once by axis_scale=0,
    appropriate dataframe, scale_col="demand_mwh"

    Args:
        intersection_matrix (pandas.DataFrame): matrix with every one-one
            mapping of two different geometry IDs
        gdf_scale (pandas.DataFrame): dataframe with appropriate scaling data
        gdf_scale_col (str): name of the column being allocated (needs same
            name in dataframe and matrix)
        axis_scale (int): axis of normalization and demand allocation
            1 if data being multiplied to rows
            0 if data being multiplied to columns
        normalize (bool): normalize along the axis mentioned

    Returns:
        pandas.DataFrame: Intersection matrix scaled by vector (row-wise or
        column-wise)

    """
    if axis_scale == 1:
        unique_index = intersection_matrix.index
        index_name = intersection_matrix.index.name

    else:
        unique_index = intersection_matrix.columns
        index_name = intersection_matrix.columns.name

    if normalize is True:
        intersection_matrix = intersection_matrix.divide(
            intersection_matrix.sum(axis=axis_scale), axis=(1 - axis_scale))

    return intersection_matrix.multiply(
        gdf_scale[gdf_scale[index_name].isin(unique_index)]
        .set_index(index_name)[gdf_scale_col], axis=(1 - axis_scale))


def extract_multiple_tracts_demand_ratios(pop_norm_df, intermediate_ids):
    """
    Extract fraction of target/intermediate geometry demand based on mapping.

    Inputs list of target/intermediate geometry IDs and returns dictionary
    with keys of intersecting source geometry IDs and the demand fraction
    associated with the target/intermediate geometries.

    Example:
        Provide list of tract/county IDs and the scaled intersection matrix
        according to which demand has been allocated (e.g. population mapping).
        It will return dictionary of demand area IDs and the fraction of their
        demand associated with the list of tract/count IDs. Used as intermediate
        step to outputting time series of intermediate/source demands

    Args:
        pop_norm_df (pandas.DataFrame): matrix mapping between source and
            target/intermediate IDs (usually normalized population matrix)
        intermediate_ids (list): list of tract or other intermediate IDs

    Returns:
        dict: Dictionary of keys demand area IDs and the fraction of demand
        allocated to the list of intermediate geometry IDs

    """
    intermediate_demand_ratio_dict = pop_norm_df.loc[intermediate_ids].sum(
    ).to_dict()
    dict_area = pop_norm_df.sum(axis=0).to_dict()
    return {k: v / dict_area[k] for k, v in intermediate_demand_ratio_dict.items() if v != 0}


def extract_time_series_demand_multiple_tracts(ferc_df,
                                               pop_norm_df,
                                               ferc_df_col,
                                               intermediate_ids,
                                               time_col,
                                               demand_col):
    """
    Map time series from source geometry to intermediate/target geometry.

    Inputs time series of demand areas, appropriate scaled intersection mapping
    matrix, source geometry ID column, list of intermediate/source geometry
    IDs, name of datetime column, demand column name in time series. Outputs
    single time series with aggregated demand of the intermediate/target IDs

    Args:
        ferc_df (pandas.DataFrame): dataframe with time-stamped demand data for
            each of the source IDs.
        pop_norm_df (pandas.DataFrame): matrix mapping between source and
            target/intermediate IDs (usually normalized population matrix).
        ferc_df_col (str): name of source ID column.
        intermediate_ids (list): list of tract or other intermediate IDs.
        time_col (str): name of datetime column in ferc_df>
        demand_col (str): name of demand column.

    Returns:
        pandas.DataFrame: A Dataframe with datetime and subsequent demand for
        list of target or intermediate IDs.

    """
    ratio_dict = extract_multiple_tracts_demand_ratios(
        pop_norm_df, intermediate_ids)
    ferc_df_trunc = ferc_df[ferc_df[ferc_df_col].isin(ratio_dict.keys())]
    out_df = ferc_df_trunc.pivot_table(
        index=time_col,
        columns=ferc_df_col,
        values=demand_col
    )
    out_df = out_df.fillna(0)
    out_df = out_df.dot(pd.Series(ratio_dict))
    return out_df
