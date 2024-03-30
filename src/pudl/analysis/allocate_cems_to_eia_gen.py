"""Allocate CEMS emissions data to EIA generators and fuel types.

# This module takes the outputs from the allocate_net_gen module adds CO2, SO2, and
# NOx emissions. The outputs are the following tables:
# - Emissions by EIA generator
# - Emissions by EIA generator and fuel type
# - Maybe something with ownership?
"""
# import logging

# # Useful high-level external modules.
# import pandas as pd
# import numpy as np

# from pudl.helpers import allocate_cols

# # from pudl.metadata.fields import apply_pudl_dtypes
# from pudl.analysis.epacamd_eia import filter_crosswalk, make_subplant_ids

# logger = logging.getLogger(__name__)

# ENERGY_SOURCE_CO2_EMISSIONS_FACTORS = {  # tones per mmtbu
#     "AB": 0.13026,
#     "BFG": 0.30239,
#     "BIT": 0.10296,
#     "BLQ": 0.11083,
#     "DFO": 0.08166,
#     "GEO": None,
#     "JF": 0.07961,
#     "KER": 0.08289,
#     "LFG": 0.0635,
#     "LIG": 0.10622,
#     "MSW": 0.09998,
#     "MWH": 0,
#     "NG": 0.05844,
#     "NUC": 0,
#     "OBG": 0.0635,
#     "OBL": 0.09257,
#     "OBS": 0.1163,
#     "OG": 0.05844,
#     "OTH": None,
#     "PC": 0.11289,
#     "PG": 0.06775,
#     "PUR": 0,
#     "RC": 0.10529,
#     "RFO": 0.08159,
#     "SC": None,
#     "SG": None,
#     "SGC": 0.05844,
#     "SGP": 0.05844,
#     "SLW": 0.09257,
#     "SUB": 0.10695,
#     "SUN": 0,
#     "TDF": 0.09477,
#     "WAT": 0,
#     "WC": 0.10529,
#     "WDL": 0.09257,
#     "WDS": 0.1034,
#     "WH": 0,
#     "WND": 0,
#     "WO": 0.08525,
# }


# # # Top level functions


# def allocate_cems_by_gen(
#     epacamd_eia: pd.DataFrame,
#     annualized_cems_df: pd.DataFrame,
#     gen_fuel_by_generator_eia923: pd.DataFrame,
#     gens_eia860: pd.DataFrame,
# ) -> pd.DataFrame:
#     """NOT WORKING RN....NEED TO DO SINGLE FUEL/MULTI FUEL STUFF

#     First we use the merge_cems_eia function to use the crosswalk to merge CEMS data
#     with the desired generator data from 923. Then we merge with the generator data
#     from eia860 to get the capacity.

#     Merging CEMS and EIA may create some generator duplication due to the complicated
#     m:m relationship between EPA units and EIA generators. For example, the Barry plant
#     in Alabama has two generators (among many) A1ST and A2ST that are each linked to two
#     emissions units from EPA: 6A, 6B and 7A, 7B respectively. These emissions units each
#     also feed into other generators from Barry. This relationship is called
#     many-to-many. When we combine CEMS and EIA this creates duplicate rows for the two
#     Barry generators. We can't aggregate yet, however, because the emissions units are
#     tied to more than one generator.

#     Now, we allocate the emissions associated with each emissions unit to their
#     respective portion of generator using the `allocate_cols` function from helpers,
#     then we can aggregate by generator for the final output.

#     Args:
#         epacamd_eia: The crosswalk between EPA and EIA data.
#         cems_df: CEMS data aggregated by year, plant, and emissions_unit_id
#         gen_fuel_by_generator_eia923: One of the outputs from the allocate_net_gen
#             analysis module.

#     Returns:
#         A pandas DataFrame with Emissions data associated with each EIA generator.

#     """
#     emissions_by_generator = (
#         # Merge CEMS with EIA and add capacity_mw from 860 gens table
#         merge_cems_eia(epacamd_eia, annualized_cems_df, gen_fuel_by_generator_eia923)
#         .merge(
#             gens_eia860[["report_date", "plant_id_eia", "generator_id", "capacity_mw"]],
#             on=["report_date", "plant_id_eia", "generator_id"],
#             how="left",
#         )
#         # Now allocate emissions!
#         .pipe(
#             allocate_cols,
#             by=["report_date", "plant_id_eia", "emissions_unit_id_epa"],
#             data_and_allocator_cols={
#                 "co2_mass_tons": ["net_generation_mwh", "capacity_mw"],
#                 "so2_mass_lbs": ["net_generation_mwh", "capacity_mw"],
#                 "nox_mass_lbs": ["net_generation_mwh", "capacity_mw"],
#             },
#         )
#         # Merging creates some amount of record duplication so now groupby generator
#         # and aggregate
#         .groupby(["report_date", "plant_id_eia", "generator_id"])
#         .sum(min_count=1)
#         .reset_index()
#     )

#     return emissions_by_generator


# def allocate_cems_by_gen_energy_source(
#     epacamd_eia: pd.DataFrame,
#     annualized_cems_df: pd.DataFrame,
#     gen_fuel_by_generator_energy_source_eia923: pd.DataFrame,
# ) -> pd.DataFrame:
#     """Allocate CEMS emissions data by EIA generator and energy source."""
#     # Merge CEMS and EIA data with the crosswalk
#     cems_eia_df = merge_cems_eia(
#         epacamd_eia, annualized_cems_df, gen_fuel_by_generator_energy_source_eia923
#     )
#     # Allocate emissions by generator energy source
#     cems_by_gen_energy_source = single_fuel_allocate(cems_eia_df)
#     return cems_by_gen_energy_source


# # Helper functions
# def merge_cems_eia(
#     epacamd_eia: pd.DataFrame, cems_df: pd.DataFrame, eia_gens_df: pd.DataFrame
# ) -> pd.DataFrame:
#     """Use the crosswalk to combine CEMS and EIA data.

#     Args:
#         epacamd_eia: The crosswalk between EPA and EIA data.
#         cems_df: EPACEMS data aggregated by year, plant, and emissions_unit_id_epa
#         eia_gens_df: The EIA table of your choice that contains report_year,
#             plant_id_eia, and generator_id fields.

#     Returns:
#         A pandas DataFrame that merges CEMS and EIA data.

#     """
#     logger.info("Merging CEMS and EIA")
#     filtered_crosswalk = filter_crosswalk(epacamd_eia, cems_df)
#     crosswalk_with_subplant_ids = make_subplant_ids(filtered_crosswalk)
#     eia_gens_cems_merge = (
#         eia_gens_df.merge(
#             crosswalk_with_subplant_ids,
#             how="left",
#             left_on=["plant_id_eia", "generator_id"],
#             right_on=["plant_id_eia", "generator_id_epa"],
#         )
#         .assign(year=lambda x: x.report_date.dt.year.astype("Int64"))
#         .merge(
#             cems_df, how="left", on=["year", "plant_id_eia", "emissions_unit_id_epa"]
#         )
#         .drop(columns=["year"])
#     )

#     return eia_gens_cems_merge


# def single_fuel_allocate(cems_eia_df: pd.DataFrame) -> pd.DataFrame:
#     """Allocate emissions to generators for gen-emissions groups with one fuel type.

#     Allocating emissions can only be done simply when the emissions source is
#     associated with one type of fuel and the generators that are associated with that
#     source of emissions are associated with other emissions sources that also draw
#     from one fuel. I know it's a mouthfull. Basically if you had smokestack 1
#     connected to generators A and B and smokestack 2 connected to gennerators B and C
#     you can only allocate emissions here if A, B, and C are associated with the same,
#     single fuel. If generator C is associated with a different fuel, then the
#     emissions from smokestack 2 cannot be seperated by net generation between gens B
#     and C because different fuels lead to different emissions. Emissions from
#     smokestack 1 are also affected because they feed into generator B which also
#     recieves from smokestack 2. Because we can't calculate the emissions breakdown for
#     smokestack 2, generator B is left with only emissions from smokestack 1. That's
#     only a portion of the emissions that it should account for. Therefore, the whole
#     "unit" is compromised. Subplant id identifies these emissions-generator units.
#     We'll find the ones that are 100% the same fuel and allocate emissions for those.
#     The others we can calculate emissions for.

#     """
#     # Spliting into groups with and without cems data
#     logger.info("Splitting into groups with and without cems data")
#     with_cems = cems_eia_df[cems_eia_df["emissions_unit_id_epa"].notna()].copy()
#     without_cems = cems_eia_df[cems_eia_df["emissions_unit_id_epa"].isna()].copy()
#     # Create multi-fuel groups
#     logger.info("Creating multi-fuel groups")
#     with_cems["multi_fuel_emissions_group"] = (
#         with_cems.groupby(["report_date", "plant_id_eia", "subplant_id"])
#         .energy_source_code.transform(lambda x: x.nunique() > 1)
#         .fillna(False)
#     )
#     # Split the table by single vs. multi-fuel emissions units
#     single_fuels = with_cems[~with_cems["multi_fuel_emissions_group"]].copy()
#     multi_fuels = with_cems[with_cems["multi_fuel_emissions_group"]].copy()
#     # Allocate emissions for single-fuel groups
#     logger.info("Allocating emissions for single-fuel groups")
#     single_fuels_allocate = allocate_cols(
#         single_fuels,
#         by=["report_date", "plant_id_eia", "emissions_unit_id_epa"],
#         data_and_allocator_cols={
#             "co2_mass_tons": ["net_generation_mwh"],
#             "so2_mass_lbs": ["net_generation_mwh"],
#             "nox_mass_lbs": ["net_generation_mwh"],
#             "gross_load_mw": ["net_generation_mwh"],
#         },
#     )
#     # Add flags to show how accurate the emissions allocation process is
#     # Start by flagging rows where the subplant id consists of one generator. If one
#     # emissions unit connects to one generator (and vice versa) then we know the
#     # emissions reported to CEMS are the emissions associated with this generator, no
#     # allocation needed. Because this is already the single_fuels table, we know that
#     # one generator means one fuel.
#     logger.info("Adding flags to show emissions allocation accuracy")
#     one_to_one_index = (
#         single_fuels_allocate.groupby(["report_date", "plant_id_eia", "subplant_id"])[
#             "generator_id"
#         ]
#         .filter(lambda x: x.count() == 1)
#         .index
#     )
#     single_fuels_allocate.loc[
#         one_to_one_index, "emissions_allocation_accuracy"
#     ] = "no_allocation_needed"
#     # Add another flag to show the rest of the single-fuel emissions allocation
#     single_fuels_allocate = single_fuels_allocate.assign(
#         emissions_allocation_accuracy=lambda x: x.emissions_allocation_accuracy.fillna(
#             "single_fuel_allocation"
#         )
#     )
#     # Reset emissions for multi-fuel_groups
#     # Right now this is redundant because of the issues with groupby.sum() described
#     # below. But I'm keeping it here for now just so the emissions values at least get
#     # zeroed out before becoming NA.
#     logger.info("Resetting emissions for multi-fuel_groups")
#     multi_fuels_remove = multi_fuels.assign(
#         co2_mass_tons=np.nan,
#         so2_mass_lbs=np.nan,
#         nox_mass_lbs=np.nan,
#         gross_load_mw=np.nan,
#     )
#     # Recombine the single and multi-fuel tables as well as the non-cems table
#     logger.info("Recombining single and multi fuel groups")
#     single_multi_fuel_combine = pd.concat(
#         [single_fuels_allocate, multi_fuels_remove, without_cems], ignore_index=True
#     )
#     # Make sure no rows have been dropped in concat
#     if len(cems_eia_df) != len(single_multi_fuel_combine):
#         raise AssertionError(
#             "Recombined single and multi-fuel table has fewer rows than the original"
#         )
#     # Aggregate up to the year, plant, gen, energy source level
#     # cems_by_gen_energy_source = _agg_to_year_gen_energy(single_multi_fuel_combine)

#     return single_multi_fuel_combine  # cems_by_gen_energy_source


# def _agg_to_year_gen_energy(single_multi_fuel_combine: pd.DataFrame) -> pd.DataFrame:
#     """Aggregate allocated cems data to the year, plant, gen, energy source level.

#     This is the final groupby generator-fuel type. The values in this table already
#     pertain to various levels of aggregation. The net_generation_mwh, prime_mover_code,
#     and fuel_consumed come from the eia generators table. When we combined CEMS with eia
#     there were some duplicate eia rows due to some EIA records associating with more
#     than one emissions unit. The values from these columns should therefore not be
#     summed together. This breakdown and generator duplication is necessary for the
#     allocate_cols function to work properly, so all of the emissions and gross load
#     allocations can be summed.

#     """
#     logger.info("Aggregating by generator and energy source")
#     cems_by_gen_energy_source = (
#         single_multi_fuel_combine.groupby(
#             ["report_date", "plant_id_eia", "generator_id", "energy_source_code"]
#         )
#         .agg(
#             {
#                 "subplant_id": "first",
#                 "prime_mover_code": "first",
#                 "net_generation_mwh": "first",
#                 "fuel_consumed_mmbtu": "first",
#                 "multi_fuel_emissions_group": "first",
#                 "co2_mass_tons": "sum",
#                 "so2_mass_lbs": "sum",
#                 "nox_mass_lbs": "sum",
#                 "gross_load_mw": "sum",
#                 "emissions_allocation_accuracy": "first",
#             }
#         )
#         .reset_index()
#         .assign(
#             co2_mass_tons_calc=lambda x: x.fuel_consumed_mmbtu
#             * x.energy_source_code.map(ENERGY_SOURCE_CO2_EMISSIONS_FACTORS)
#         )
#     )
#     # Right now, pandas does not enable groupby().sum(skipna=False) so when the
#     # emissions are aggregated, all the NA values becoem 0. We don't want this,
#     # because there is a big different between NA emissions and 0 emissions. Here, we
#     # set all the emissions that we haven't allocated (i.e., anything that's not a
#     # single-fuel group) to NA.
#     cems_by_gen_energy_source.loc[
#         (cems_by_gen_energy_source["multi_fuel_emissions_group"].isin([True, None])),
#         ["co2_mass_tons", "so2_mass_lbs", "nox_mass_lbs", "gross_load_mw"],
#     ] = np.nan
#     # Also make sure cases where net gen is NA show NA emissions
#     cems_by_gen_energy_source.loc[
#         (cems_by_gen_energy_source["multi_fuel_emissions_group"].isin([False]))
#         & (cems_by_gen_energy_source["net_generation_mwh"].isna()),
#         ["co2_mass_tons", "so2_mass_lbs", "nox_mass_lbs", "gross_load_mw"],
#     ] = np.nan

#     return cems_by_gen_energy_source


# def _add_emissions_units_column(cems_eia_df, cems_by_gen_energy_source):
#     """Add column to year, generator agg that includes emissions unit id.

#     This column isn't necessary, but it's helpful if you want to get a full picture
#     of what is going on. Because of the m:m relationship between emissions id and
#     generator id, there may be more than one value in this column making it somewhat
#     difficult to work with. The idea, however, is that you could look at this and, on
#     a case by case basis, better understand how the emissions are allocated.

#     """
#     cems_eia_df.groupby(["report_date", "plant_id", "generator_id"])[
#         "emissions_unit_id_epa"
#     ].apply(lambda x: list(x.unique())).reset_index()
