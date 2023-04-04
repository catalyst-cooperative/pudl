"""A collection of denormalized EIA assets."""
import pandas as pd
from dagster import asset

import pudl
from pudl.metadata.fields import apply_pudl_dtypes
from pudl.output.eia860 import fill_in_missing_ba_codes

logger = pudl.logging_helpers.get_logger(__name__)


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def pu_eia(
    denorm_plants_eia: pd.DataFrame,
    denorm_utilities_eia: pd.DataFrame,
):
    """Create a dataframe of plant and utility IDs and names from EIA 860.

    Returns a pandas dataframe with the following columns:
    - report_date (in which data was reported)
    - plant_name_eia (from EIA entity)
    - plant_id_eia (from EIA entity)
    - plant_id_pudl
    - utility_id_eia (from EIA860)
    - utility_name_eia (from EIA860)
    - utility_id_pudl

    Args:
        pudl_engine (sqlalchemy.engine.Engine): SQLAlchemy connection engine
            for the PUDL DB.
        start_date (date-like): date-like object, including a string of the
            form 'YYYY-MM-DD' which will be used to specify the date range of
            records to be pulled.  Dates are inclusive.
        end_date (date-like): date-like object, including a string of the
            form 'YYYY-MM-DD' which will be used to specify the date range of
            records to be pulled.  Dates are inclusive.

    Returns:
        pandas.DataFrame: A DataFrame containing plant and utility IDs and
        names from EIA 860.
    """
    # Contains the one-to-one mapping of EIA plants to their operators
    plants_eia = denorm_plants_eia.drop(
        [
            "utility_id_pudl",
            "city",
            "state",  # Avoid dupes in merge
            "zip_code",
            "street_address",
            "utility_name_eia",
        ],
        axis="columns",
    ).dropna(
        subset=["utility_id_eia"]
    )  # Drop unmergable records

    # to avoid duplicate columns on the merge...
    out_df = pd.merge(
        plants_eia,
        denorm_utilities_eia,
        how="left",
        on=["report_date", "utility_id_eia"],
    )

    out_df = (
        out_df.loc[
            :,
            [
                "report_date",
                "plant_id_eia",
                "plant_name_eia",
                "plant_id_pudl",
                "utility_id_eia",
                "utility_name_eia",
                "utility_id_pudl",
            ],
        ]
        .dropna(subset=["report_date", "plant_id_eia", "utility_id_eia"])
        .pipe(apply_pudl_dtypes, group="eia")
    )
    return out_df


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_utilities_eia(
    utilities_entity_eia: pd.DataFrame,
    utilities_eia860: pd.DataFrame,
    utilities_eia: pd.DataFrame,
):
    """Pull all fields from the EIA Utilities table.

    Args:
        utilities_entity_eia: EIA utility entity table.
        utilities_eia860: EIA 860 annual utility table.
        utilities_eia: Associations between EIA utilities and pudl utility IDs.

    Returns:
        A DataFrame containing all the fields of the EIA 860 Utilities table.
    """
    utilities_eia = utilities_eia[["utility_id_eia", "utility_id_pudl"]]
    out_df = pd.merge(
        utilities_entity_eia, utilities_eia860, how="left", on=["utility_id_eia"]
    )
    out_df = pd.merge(out_df, utilities_eia, how="left", on=["utility_id_eia"])
    out_df = (
        out_df.assign(report_date=lambda x: pd.to_datetime(x.report_date))
        .dropna(subset=["report_date", "utility_id_eia"])
        .pipe(apply_pudl_dtypes, group="eia")
    )
    first_cols = [
        "report_date",
        "utility_id_eia",
        "utility_id_pudl",
        "utility_name_eia",
    ]
    out_df = pudl.helpers.organize_cols(out_df, first_cols)
    return out_df


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_plants_eia(
    plants_entity_eia: pd.DataFrame,
    plants_eia860: pd.DataFrame,
    plants_eia: pd.DataFrame,
    utilities_eia: pd.DataFrame,
):
    """Pull all fields from the EIA Plants tables.

    Args:
        plants_entity_eia: EIA plant entity table.
        plants_eia860: EIA 860 annual plant attribute table.
        plants_eia: Associations between EIA plants and pudl utility IDs.

    Returns:
        pandas.DataFrame: A DataFrame containing all the fields of the EIA 860
        Plants table.
    """
    plants_eia860 = plants_eia860.assign(
        report_date=lambda x: pd.to_datetime(x.report_date)
    )

    plants_eia = plants_eia[["plant_id_eia", "plant_id_pudl"]]

    out_df = (
        pd.merge(plants_entity_eia, plants_eia860, how="left", on=["plant_id_eia"])
        .merge(plants_eia, how="left", on=["plant_id_eia"])
        .merge(utilities_eia, how="left", on=["utility_id_eia"])
        .dropna(subset=["report_date", "plant_id_eia"])
        .pipe(fill_in_missing_ba_codes)
        .pipe(apply_pudl_dtypes, group="eia")
    )
    return out_df


# @asset(compute_kind="Python")
# def denorm_generators_eia(
#     generators_eia860: pd.DataFrame,
#     generators_entity_eia: pd.DataFrame,
#     plants_entity_eia: pd.DataFrame,
#     pu_eia: pd.DataFrame,
#     boiler_generator_assn_eia860: pd.DataFrame,
#     unit_ids: bool = False,
#     fill_tech_desc: bool = True,
# ):
#     """Pull all fields from the EIA Utilities table.

#     Args:
#         utilities_entity_eia: EIA utility entity table.
#         utilities_eia860: EIA 860 annual utility table.
#         utilities_eia: Associations between EIA utilities and pudl utility IDs.

#     Returns:
#         A DataFrame containing all the fields of the EIA 860 Utilities table.
#     """
#     # Almost all the info we need will come from here.

#     out_df = pd.merge(
#         generators_eia860, plants_entity_eia, how="left", on=["plant_id_eia"]
#     )
#     out_df = pd.merge(
#         out_df,
#         generators_entity_eia,
#         how="left",
#         on=["plant_id_eia", "generator_id"],
#     )

#     out_df.report_date = pd.to_datetime(out_df.report_date)

#     # Bring in some generic plant & utility information:
#     pu_eia = pu_eia.drop(["plant_name_eia", "utility_id_eia"], axis="columns")
#     out_df = pd.merge(out_df, pu_eia, on=["report_date", "plant_id_eia"], how="left")

#     # Merge in the unit_id_pudl assigned to each generator in the BGA process
#     # Pull the BGA table and make it unit-generator only:
#     out_df = pd.merge(
#         out_df,
#         boiler_generator_assn_eia860[
#             [
#                 "report_date",
#                 "plant_id_eia",
#                 "generator_id",
#                 "unit_id_pudl",
#                 "bga_source",
#             ]
#         ].drop_duplicates(),
#         on=["report_date", "plant_id_eia", "generator_id"],
#         how="left",
#         validate="m:1",
#     )

#     # In order to be able to differentiate between single and multi-fuel
#     # plants, we need to count how many different simple energy sources there
#     # are associated with plant's generators. This allows us to do the simple
#     # lumping of an entire plant's fuel & generation if its primary fuels
#     # are homogeneous, and split out fuel & generation by fuel if it is
#     # hetereogeneous.
#     ft_count = (
#         out_df[["plant_id_eia", "fuel_type_code_pudl", "report_date"]]
#         .drop_duplicates()
#         .groupby(["plant_id_eia", "report_date"])
#         .count()
#     )
#     ft_count = ft_count.reset_index()
#     ft_count = ft_count.rename(columns={"fuel_type_code_pudl": "fuel_type_count"})
#     out_df = (
#         pd.merge(out_df, ft_count, how="left", on=["plant_id_eia", "report_date"])
#         .dropna(subset=["report_date", "plant_id_eia", "generator_id"])
#         .pipe(apply_pudl_dtypes, group="eia")
#     )
#     # Augment those base unit_id_pudl values using heuristics, see below.
#     if unit_ids:
#         logger.info("Assigning pudl unit ids")
#         out_df = assign_unit_ids(out_df)

#     if fill_tech_desc:
#         logger.info("Filling technology type")
#         out_df = fill_generator_technology_description(out_df)

#     first_cols = [
#         "report_date",
#         "plant_id_eia",
#         "plant_id_pudl",
#         "plant_name_eia",
#         "utility_id_eia",
#         "utility_id_pudl",
#         "utility_name_eia",
#         "generator_id",
#     ]

#     # Re-arrange the columns for easier readability:
#     out_df = (
#         pudl.helpers.organize_cols(out_df, first_cols)
#         .sort_values(["report_date", "plant_id_eia", "generator_id"])
#         .pipe(apply_pudl_dtypes, group="eia")
#     )

#     return out_df


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_boilers_eia(
    boilers_eia860: pd.DataFrame,
    boilers_entity_eia: pd.DataFrame,
    plants_entity_eia: pd.DataFrame,
    pu_eia: pd.DataFrame,
    boiler_generator_assn_eia860: pd.DataFrame,
) -> pd.DataFrame:
    """Pull all fields reported in the EIA boilers tables.

    Merge in other useful fields including the latitude & longitude of the
    plant that the boilers are part of, canonical plant & operator names and
    the PUDL IDs of the plant and operator, for merging with other PUDL data
    sources.

    Returns:
        A DataFrame containing boiler attributes from EIA 860.
    """
    out_df = pd.merge(
        boilers_eia860, plants_entity_eia, how="left", on=["plant_id_eia"]
    )

    out_df = pd.merge(
        out_df, boilers_entity_eia, how="left", on=["plant_id_eia", "boiler_id"]
    )

    out_df.report_date = pd.to_datetime(out_df.report_date)

    # Bring in some generic plant & utility information:
    out_df = pd.merge(
        out_df,
        pu_eia.drop(["plant_name_eia"], axis="columns"),
        on=["report_date", "plant_id_eia"],
        how="left",
    )

    # Merge in the unit_id_pudl assigned to each boiler in the BGA process
    # Pull the BGA table and make it unit-boiler only:
    out_df = pd.merge(
        out_df,
        boiler_generator_assn_eia860[
            [
                "report_date",
                "plant_id_eia",
                "boiler_id",
                "unit_id_pudl",
            ]
        ].drop_duplicates(),
        on=["report_date", "plant_id_eia", "boiler_id"],
        how="left",
        validate="m:1",
    )

    first_cols = [
        "report_date",
        "plant_id_eia",
        "plant_id_pudl",
        "plant_name_eia",
        "utility_id_eia",
        "utility_id_pudl",
        "utility_name_eia",
        "boiler_id",
    ]

    # Re-arrange the columns for easier readability:
    out_df = (
        pudl.helpers.organize_cols(out_df, first_cols)
        .sort_values(["report_date", "plant_id_eia", "boiler_id"])
        .pipe(apply_pudl_dtypes, group="eia")
    )

    return out_df
