"""Prepare the inputs to the FERC1 to EIA record linkage model."""

import importlib
from typing import Literal

import pandas as pd

import pudl
from pudl.analysis.plant_parts_eia import match_to_single_plant_part

logger = pudl.logging_helpers.get_logger(__name__)


class InputManager:
    """Class to prepare inputs for linking FERC1 and EIA."""

    def __init__(
        self,
        plants_all_ferc1: pd.DataFrame,
        fbp_ferc1: pd.DataFrame,
        plant_parts_eia: pd.DataFrame,
    ):
        """Initialize inputs manager that gets inputs for linking FERC and EIA.

        Args:
            plants_all_ferc1: Table of all of the FERC1-reporting plants.
            fbp_ferc1: Table of the fuel reported aggregated to the FERC1 plant-level.
            plant_parts_eia: The EIA plant parts list.
            start_date: Start date that corresponds to the tables passed in.
            end_date: End date that corresponds to the tables passed in.
        """
        self.plant_parts_eia = plant_parts_eia.set_index("record_id_eia")
        self.plants_all_ferc1 = plants_all_ferc1
        self.fbp_ferc1 = fbp_ferc1

        self.start_date = min(plant_parts_eia.report_date)
        self.end_date = max(plant_parts_eia.report_date)
        if (
            yrs_len := len(yrs_range := range(self.start_date.year, self.end_date.year))
        ) < 3:
            logger.warning(
                f"Your attempting to fit a model with only {yrs_len} years "
                f"({yrs_range}). This will probably result in overfitting. In order to"
                " best judge the results, use more years of data."
            )

        # generate empty versions of the inputs.. this let's this class check
        # whether or not the compiled inputs exist before compilnig
        self.plant_parts_eia_true = None
        self.plants_ferc1 = None
        self.train_df = None
        self.train_ferc1 = None
        self.train_eia = None

    def get_plant_parts_eia_true(self, clobber: bool = False) -> pd.DataFrame:
        """Get the EIA plant-parts with only the unique granularities."""
        if self.plant_parts_eia_true is None or clobber:
            self.plant_parts_eia_true = (
                pudl.analysis.plant_parts_eia.plant_parts_eia_distinct(
                    self.plant_parts_eia
                )
            )
        return self.plant_parts_eia_true

    def get_plants_ferc1(self, clobber: bool = False) -> pd.DataFrame:
        """Prepare FERC1 plants data for record linkage with EIA plant-parts.

        This method merges two internally cached dataframes (``self.plants_all_ferc1``
        and ``self.fuel_by_plant_ferc1`` (originally obtained from
        :ref:`out_ferc1__yearly_all_plants` and
        :ref:`out_ferc1__yearly_steam_plants_fuel_by_plant_sched402`) respectively) and
        ensures that key columns are have the same names and dtypes as the analogous
        EIA columns so that they can be used in the FERC-EIA record linkage model
        easily.

        Returns:
            A cleaned table of FERC1 plants plant records with fuel cost data.
        """
        if clobber or self.plants_ferc1 is None:
            fbp_cols_to_use = [
                "report_year",
                "utility_id_ferc1",
                "plant_name_ferc1",
                "utility_id_pudl",
                "fuel_cost",
                "fuel_mmbtu",
                "primary_fuel_by_mmbtu",
            ]

            logger.info("Preparing the FERC1 tables.")
            self.plants_ferc1 = (
                self.plants_all_ferc1.merge(
                    self.fbp_ferc1[fbp_cols_to_use],
                    on=[
                        "report_year",
                        "utility_id_ferc1",
                        "utility_id_pudl",
                        "plant_name_ferc1",
                    ],
                    how="left",
                )
                .pipe(pudl.helpers.convert_cols_dtypes, "ferc1")
                .assign(
                    installation_year=lambda x: (
                        x.installation_year.astype("float")
                    ),  # need for comparison vectors
                    plant_id_report_year=lambda x: (
                        x.plant_id_pudl.astype(str) + "_" + x.report_year.astype(str)
                    ),
                    plant_id_report_year_util_id=lambda x: (
                        x.plant_id_report_year + "_" + x.utility_id_pudl.astype(str)
                    ),
                    fuel_cost_per_mmbtu=lambda x: (x.fuel_cost / x.fuel_mmbtu),
                    unit_heat_rate_mmbtu_per_mwh=lambda x: (
                        x.fuel_mmbtu / x.net_generation_mwh
                    ),
                )
                .rename(
                    columns={
                        "record_id": "record_id_ferc1",
                        "opex_plants": "opex_plant",
                        "fuel_cost": "total_fuel_cost",
                        "fuel_mmbtu": "total_mmbtu",
                        "opex_fuel_per_mwh": "fuel_cost_per_mwh",
                        "primary_fuel_by_mmbtu": "fuel_type_code_pudl",
                    }
                )
                .set_index("record_id_ferc1")
            )
        return self.plants_ferc1

    def get_train_df(self) -> pd.DataFrame:
        """Get the training connections.

        Prepare them if the training data hasn't been connected to FERC data yet.
        """
        if self.train_df is None:
            self.train_df = prep_train_connections(
                ppe=self.plant_parts_eia,
                start_date=self.start_date,
                end_date=self.end_date,
            )
        return self.train_df

    def get_train_records(
        self,
        dataset_df: pd.DataFrame,
        dataset_id_col: Literal["record_id_eia", "record_id_ferc1"],
    ) -> pd.DataFrame:
        """Generate a set of known connections from a dataset using training data.

        This method grabs only the records from the the datasets (EIA or FERC)
        that we have in our training data.

        Args:
            dataset_df: either FERC1 plants table (result of :meth:`get_plants_ferc1`) or
                EIA plant-parts (result of :meth:`get_plant_parts_eia_true`).
            dataset_id_col: Identifying column name. Either ``record_id_eia`` for
                ``plant_parts_eia_true`` or ``record_id_ferc1`` for ``plants_ferc1``.
        """
        known_df = (
            pd.merge(
                dataset_df,
                self.get_train_df().reset_index()[[dataset_id_col]],
                left_index=True,
                right_on=[dataset_id_col],
            )
            .drop_duplicates(subset=[dataset_id_col])
            .set_index(dataset_id_col)
            .astype({"total_fuel_cost": float, "total_mmbtu": float})
        )
        return known_df

    # Note: Is there a way to avoid these little shell methods? I need a
    # standard way to access
    def get_train_eia(self, clobber: bool = False) -> pd.DataFrame:
        """Get the known training data from EIA."""
        if clobber or self.train_eia is None:
            self.train_eia = self.get_train_records(
                self.get_plant_parts_eia_true(), dataset_id_col="record_id_eia"
            )
        return self.train_eia

    def get_train_ferc1(self, clobber: bool = False) -> pd.DataFrame:
        """Get the known training data from FERC1."""
        if clobber or self.train_ferc1 is None:
            self.train_ferc1 = self.get_train_records(
                self.get_plants_ferc1(), dataset_id_col="record_id_ferc1"
            )
        return self.train_ferc1

    def execute(self, clobber: bool = False):
        """Compile all the inputs.

        This method is only run if/when you want to ensure all of the inputs are
        generated all at once. While using :class:`InputManager`, it is preferred to
        access each input dataframe or index via their ``get_`` method instead of
        accessing the attribute.
        """
        # grab the FERC table we are trying
        # to connect to self.plant_parts_eia_true
        self.plants_ferc1 = self.get_plants_ferc1(clobber=clobber)
        self.plant_parts_eia_true = self.get_plant_parts_eia_true(clobber=clobber)

        # we want both the df version and just the index; skl uses just the
        # index and we use the df in merges and such
        self.train_df = self.get_train_df()

        # generate the list of the records in the EIA and FERC records that
        # exist in the training data
        self.train_eia = self.get_train_eia(clobber=clobber)
        self.train_ferc1 = self.get_train_ferc1(clobber=clobber)
        return


def restrict_train_connections_on_date_range(
    train_df: pd.DataFrame,
    id_col: Literal["record_id_eia", "record_id_ferc1"],
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> pd.DataFrame:
    """Restrict the training data based on the date ranges of the input tables.

    The training data for this model spans the full PUDL date range. We don't want to
    add training data from dates that are outside of the range of the FERC and EIA data
    we are attempting to match. So this function restricts the training data based on
    start and end dates.

    The training data is only the record IDs, which contain the report year inside them.
    This function compiles a regex using the date range to grab only training records
    which contain the years in the date range followed by and preceded by ``_`` - in
    the format of ``record_id_eia``and ``record_id_ferc1``. We use that extracted year
    to determine
    """
    # filter training data by year range
    # first get list of all years to grab from training data
    date_range_years_str = "|".join(
        [
            f"{year}"
            for year in pd.date_range(start=start_date, end=end_date, freq="YS").year
        ]
    )
    logger.info(f"Restricting training data on years: {date_range_years_str}")
    train_df = train_df.assign(
        year_in_date_range=lambda x: x[id_col].str.extract(
            r"_{1}" + f"({date_range_years_str})" + "_{1}"
        )
    )
    # pd.drop returns a copy, so no need to copy this portion of train_df
    return train_df.loc[train_df.year_in_date_range.notnull()].drop(
        columns=["year_in_date_range"]
    )


def prep_train_connections(
    ppe: pd.DataFrame, start_date: pd.Timestamp, end_date: pd.Timestamp
) -> pd.DataFrame:
    """Get and prepare the training connections for the model.

    We have stored training data, which consists of records with ids
    columns for both FERC and EIA. Those id columns serve as a connection
    between ferc1 plants and the EIA plant-parts. These connections
    indicate that a ferc1 plant records is reported at the same granularity
    as the connected EIA plant-parts record.

    Arguments:
        ppe: The EIA plant parts. Records from this dataframe will be connected to the
            training data records. This needs to be the full EIA plant parts, not just
            the distinct/true granularities because the training data could contain
            non-distinct records and this function reassigns those to their distinct
            counterparts.
        start_date: Beginning date for records from the training data. Should match the
            start date of ``ppe``. Default is None and all the training data will be used.
        end_date: Ending date for records from the training data. Should match the end
            date of ``ppe``. Default is None and all the training data will be used.

    Returns:
        A dataframe of training connections which has a MultiIndex of ``record_id_eia``
        and ``record_id_ferc1``.
    """
    ppe_cols = [
        "true_gran",
        "appro_part_label",
        "appro_record_id_eia",
        "plant_part",
        "ownership_dupe",
    ]
    # Read in one_to_many csv and join corresponding plant_match_ferc1 parts to FERC IDs
    one_to_many = (
        pd.read_csv(
            importlib.resources.files("pudl.package_data.glue")
            / "eia_ferc1_one_to_many.csv"
        )
        .pipe(pudl.helpers.cleanstrings_snake, ["record_id_eia"])
        .drop_duplicates(subset=["record_id_ferc1", "record_id_eia"])
    )

    # Get the 'm' generator IDs 1:m
    one_to_many_single = match_to_single_plant_part(
        multi_gran_df=ppe.loc[ppe.index.isin(one_to_many.record_id_eia)].reset_index(),
        ppe=ppe.reset_index(),
        part_name="plant_gen",
        cols_to_keep=["plant_part"],
    )[["record_id_eia", "record_id_eia_plant_gen"]].rename(
        columns={"record_id_eia_plant_gen": "gen_id"}
    )
    one_to_many = (
        one_to_many.merge(
            one_to_many_single,  # Match plant parts to generators
            on="record_id_eia",
            how="left",
            validate="1:m",
        )
        .drop_duplicates("gen_id")
        .merge(  # Match generators to ferc1_generator_agg_id
            ppe["ferc1_generator_agg_id"].reset_index(),
            left_on="gen_id",
            right_on="record_id_eia",
            how="left",
            validate="1:1",
        )
        .dropna(subset=["ferc1_generator_agg_id"])
        .drop(["record_id_eia_x", "record_id_eia_y"], axis=1)
        .merge(  # Match ferc1_generator_agg_id to new faked plant part record_id_eia
            ppe.loc[
                ppe.plant_part == "plant_match_ferc1",
                ["ferc1_generator_agg_id"],
            ].reset_index(),
            on="ferc1_generator_agg_id",
            how="left",
            validate="m:1",
        )
        .drop(["ferc1_generator_agg_id", "gen_id"], axis=1)
        .drop_duplicates(subset=["record_id_ferc1", "record_id_eia"])
        .set_index("record_id_ferc1")
    )

    train_df = (
        pd.read_csv(
            importlib.resources.files("pudl.package_data.glue") / "eia_ferc1_train.csv"
        )
        .pipe(pudl.helpers.cleanstrings_snake, ["record_id_eia"])
        .drop_duplicates(subset=["record_id_ferc1", "record_id_eia"])
        .set_index("record_id_ferc1")
    )
    logger.info(f"Updating {len(one_to_many)} training records with 1:m plant parts.")
    train_df.update(one_to_many)  # Overwrite FERC records with faked 1:m parts.
    train_df = (
        # we want to ensure that the records are associated with a
        # "true granularity" - which is a way we filter out whether or
        # not each record in the EIA plant-parts is actually a
        # new/unique collection of plant parts
        # once the true_gran is dealt with, we also need to convert the
        # records which are ownership dupes to reflect their "total"
        # ownership counterparts
        train_df.reset_index()
        .pipe(
            restrict_train_connections_on_date_range,
            id_col="record_id_eia",
            start_date=start_date,
            end_date=end_date,
        )
        .merge(
            ppe[ppe_cols].reset_index(),
            how="left",
            on=["record_id_eia"],
            indicator=True,
        )
    )
    not_in_ppe = train_df[train_df._merge == "left_only"]
    if not not_in_ppe.empty:
        raise AssertionError(
            "Not all training data is associated with EIA records.\n"
            "record_id_ferc1's of bad training data records are: "
            f"{list(not_in_ppe.reset_index().record_id_ferc1)}"
        )
    train_df = (
        train_df.assign(
            plant_part=lambda x: x["appro_part_label"],
            record_id_eia=lambda x: x["appro_record_id_eia"],
        )
        .pipe(pudl.analysis.plant_parts_eia.reassign_id_ownership_dupes)
        .fillna(
            value={
                "record_id_eia": pd.NA,
            }
        )
        .set_index(  # sklearn wants a MultiIndex to do the stuff
            [
                "record_id_ferc1",
                "record_id_eia",
            ]
        )
    )
    train_df = train_df.drop(columns=ppe_cols + ["_merge"])
    return train_df
