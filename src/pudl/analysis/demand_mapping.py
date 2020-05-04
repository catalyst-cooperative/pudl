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


def create_stacked_intersection_df(gdf_primary, gdf_secondary, gdf_primary_col="FIPS",
                                   gdf_secondary_col="ID", geom_primary="geometry",
                                   geom_secondary="geometry"):
    """
    Builds matrix with 1-1 mapping of primary and secondary dataframe.

    Returns a stacked dataframe with all the fraction of intersecting areas
    for both geometries. The CRS for both of the geodataframes should be the
    same.

    Inputs:

        gdf_primary (GeoDataframe): primaryer geodataframe
        gdf_secondary (GeoDataframe): secondaryr dataframe
        gdf_primary_col (str): index column for gdf_primary
        gdf_secondary_col(str): index column for gdf_secondary
        file_save (bool): save file option

    Outputs:

        returns new_df: dataframe with columns primary_index, secondary_index,
                primary_intersection_fraction, secondary_intersection_fraction

    """
    new_df = geopandas.overlay(gdf_primary[[gdf_primary_col, geom_primary]], gdf_secondary[[
                               gdf_secondary_col, geom_secondary]], how='intersection')
    new_df["area_derived"] = new_df["geometry"].area

    gdf_primary["primary_area_derived"] = gdf_primary[geom_primary].area
    gdf_secondary["secondary_area_derived"] = gdf_secondary[geom_secondary].area

    new_df = (new_df[[gdf_primary_col, gdf_secondary_col, "area_derived", "geometry"]]
              .merge(gdf_primary[[gdf_primary_col, "primary_area_derived"]])
              .merge(gdf_secondary[[gdf_secondary_col, "secondary_area_derived"]]))

    new_df["gdf_primary_intersection_fraction"] = new_df["area_derived"] / \
        new_df["primary_area_derived"]
    new_df["gdf_secondary_intersection_fraction"] = new_df["area_derived"] / \
        new_df["secondary_area_derived"]

    new_df = new_df[[gdf_primary_col, gdf_secondary_col, "gdf_primary_intersection_fraction",
                     "gdf_secondary_intersection_fraction"]]

    return new_df


def create_intersection_matrix(gdf_intersection, gdf_intersection_col="gdf_primary_intersection_fraction",
                               gdf_primary_col="FIPS", gdf_secondary_col="ID", normalization=True, normalize_axis=1):
    """
    Converts stacked dataframe to an intersection matrix.

    Provides the intersection matrix with the area intersection fraction a[i, j] for every primary id i
    and secondary id j. Normalization option available (To normalize double-counting)

    Inputs:

        gdf_intersection: stacked dataframe with every one-one mapping of primary ids
        gdf_intersection_col: usually the area intersection fraction. (Usually the fraction along the
        type (primary/secondary) you want to normalize)

        gdf_primary_col: index of primary gdf
        gdf_secondary_col: index of secondary gdf

        normalize_axis: 1 for normalizing along primary axis (generally)
                        0 for noramlizing along secondary axis

    Outputs:

        returns: intersection_matrix

    """
    intersection_matrix = gdf_intersection.pivot_table(values=gdf_intersection_col,
                                                       index=gdf_primary_col,
                                                       columns=gdf_secondary_col,
                                                       fill_value=0)

    if normalization is True:
        intersection_matrix = intersection_matrix.divide(intersection_matrix.sum(axis=normalize_axis),
                                                         axis=int(not normalize_axis))

    return intersection_matrix


def matrix_linear_scaling(intersection_matrix, gdf_scale, gdf_scale_col="POPULATION", axis_scale=1,
                          normalize=True):
    """
    Scales matrix by a vector with or without normalization.

    Scales the linear mapping matrix by a particular variable e.g. If you want to allocate demand by
    population, scale the area intersection fraction matrix by the population of each of the FIPS
    using matrix_linear_scaling once by axis_scale=1, appropriate dataframe, scale_col="POPULATION",
    then allocate demand using matrix_linear_scaling once by axis_scale=0, appropriate dataframe,
    scale_col="demand_mwh"

    Inputs:

        intersection_matrix: matrix with every one-one mapping of primary ids
        gdf_scale: dataframe with the appropriate scaling data
        gdf_scale_col: the column being allocated (needs same name in dataframe and matrix)

        axis_scale: scale of normalization and demand allocation
                    1 if data being multiplied to rows
                    0 if data being multiplied to columns

        normalize: normalize along the axis mentioned

    Outputs:

        returns scaled matrix


    """
    if axis_scale == 1:
        unique_index = intersection_matrix.index
        index_name = intersection_matrix.index.name

    else:
        unique_index = intersection_matrix.columns
        index_name = intersection_matrix.columns.name

    if normalize is True:

        intersection_matrix = intersection_matrix.divide(intersection_matrix.sum(axis=axis_scale),
                                                         axis=int(not axis_scale))

    return intersection_matrix.multiply(gdf_scale[gdf_scale[index_name].isin(unique_index)]
                                        .set_index(index_name)[gdf_scale_col], axis=int(not axis_scale))


def extract_multiple_tracts_demand_ratios(pop_norm_df, tract_ids):
    """
    Extracts fraction of primary geometry demand based on secondary geometry.

    Inputs list of tract/county IDs and provide mapping based on which demand is
    extrapolated (here, population)

    Inputs:

        pop_norm_df: matrix mapping
        tract_ids: list of tract IDs

    Outputs:

        dictionary of demand area and the fraction of demand it serves


    """
    tract_demand_ratio_dict = pop_norm_df.loc[tract_ids].sum().to_dict()
    dict_area = pop_norm_df.sum(axis=0).to_dict()
    return {k: v / dict_area[k] for k, v in tract_demand_ratio_dict.items() if v != 0}


def extract_time_series_demand_multiple_tracts(ferc_df, pop_norm_df, ferc_df_col, tract_ids):
    """
    Works with dictionary calculation in fn above to provide time series of allocated demand.

    Inputs time series of demand areas, list of tract/county IDs, and mapping matrix
    to provide time series of allocated demand

    Inputs:

        pop_norm_df: matrix mapping
        tract_ids: list of tract IDs

    Outputs:

        dictionary of demand area and the fraction of demand it serves


    """
    ratio_dict = extract_multiple_tracts_demand_ratios(pop_norm_df, tract_ids)
    ferc_df_trunc = ferc_df[ferc_df[ferc_df_col].isin(ratio_dict.keys())]
    return ferc_df_trunc.pivot_table(index='local_time',
                                     columns='eia_code',
                                     values='demand_mwh').fillna(0).dot(pd.Series(ratio_dict))
