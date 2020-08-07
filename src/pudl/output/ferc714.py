"""Functions & classes for compiling derived aspects of the FERC Form 714 data."""
import pandas as pd

import pudl


################################################################################
# Helper functions
################################################################################
def add_dates(rids_ferc714, report_dates):
    """
    Broadcast respondent data across dates.

    Args:
        rids_ferc714 (pandas.DataFrame): A simple FERC 714 Respondent ID dataframe,
            without any date information.
        report_dates (ordered collection of datetime): Dates for which each respondent
            should be given a record.

    Raises:
        ValueError: if a ``report_date`` column exists in ``rids_ferc714``.

    Returns:
        pandas.DataFrame: Dataframe having all the same columns as the input
        ``rids_ferc714`` with the addition of a ``report_date`` column, but with all
        records associated with each ``respondent_id_ferc714`` duplicated on a per-date
        basis.

    """
    if "report_date" in rids_ferc714.columns:
        raise ValueError("report_date already present, can't be added again!")
    # Create DataFrame with all report_date and respondent_id_ferc714 combos
    dates_rids_df = (
        pd.DataFrame(
            index=pd.MultiIndex.from_product([
                report_dates, rids_ferc714.respondent_id_ferc714.unique()],
                names=["report_date", "respondent_id_ferc714"]
            )
        ).reset_index()
    )
    return pd.merge(rids_ferc714, dates_rids_df, on="respondent_id_ferc714")


class Respondents(object):
    """
    A class coordinating compilation of data related to FERC 714 Respondents.

    The FERC 714 Respondents themselves are not complex as they are reported, but
    various ambiguities and the need to associate service territories with them mean
    there are a lot of different derived aspects related to them which we repeatedly
    need to compile in a self consistent way. This class allows you to choose several
    parameters for that compilation, and then easily access the resulting derived
    tabular outputs.

    Some of these derived attributes are computationally expensive, and so they are
    cached internally. You can force a new computation in most cases by using
    ``update=True`` in the access methods. However, this functionality isn't totally
    implemented because we're still depending on the interim ETL processes for the FERC
    714 and EIA 861 data, and we don't want to trigger whole new ETL runs every time
    a derived value is updated.

    Attributes:
        pudl_out (pudl.output.pudltabl.PudlTabl): The PUDL output object which should be
            used to obtain PUDL data.
        pudl_settings (dict or None): A dictionary of settings indicating where data
            related to PUDL can be found. Needed to obtain US Census DP1 data which
            has the county geometries.
        ba_ids (ordered collection or None): EIA IDs that should be treated as referring
            to balancing authorities in respondent categorization process. If None, all
            known values of ``balancing_authority_id_eia`` will be used.
        util_ids (ordered collection or None): EIA IDs that should be treated as
            referring to utilities in respondent categorization process. If None, all
            known values of ``utility_id_eia`` will be used.
        priority (str): Which type of entity should take priority in the categorization
            of FERC 714 respondents. Must be either ``utility`` or
            ``balancing_authority.`` The default is ``balancing_authority``.
        limit_by_state (bool): Whether to limit respondent service territories to the
            states where they have documented activity in the EIA 861. Currently this
            is only implemented for Balancing Authorities.

    """

    def __init__(
        self,
        pudl_out,
        pudl_settings=None,
        ba_ids=None,
        util_ids=None,
        priority="balancing_authority",
        limit_by_state=True,
    ):
        """Set respondent compilation parameters."""
        self.pudl_out = pudl_out

        if pudl_settings is None:
            pudl_settings = pudl.workspace.setup.get_defaults()
        self.pudl_settings = pudl_settings

        if ba_ids is None:
            ba_ids = (
                self.pudl_out.balancing_authority_eia861()
                .balancing_authority_id_eia.dropna().unique()
            )
        self.ba_ids = ba_ids

        if util_ids is None:
            util_ids = (
                pudl.analysis.service_territory
                .get_all_utils(self.pudl_out).utility_id_eia
            )
        self.util_ids = util_ids

        self.priority = priority
        self.limit_by_state = limit_by_state
        self._categorized = None
        self._annualized = None
        self._demand_summary = None
        self._fipsified = None
        self._counties_gdf = None
        self._respondents_gdf = None

    def annualize(self, update=False):
        """
        Broadcast respondent data across all years with reported demand.

        The FERC 714 Respondent IDs and names are reported in their own table,
        without any refence to individual years, but much of the information we are
        associating with them varies annually. This method creates an annualized
        version of the respondent table, with each respondent having an entry
        corresponding to every year in which hourly demand was reported in the FERC 714
        dataset as a whole -- this necessarily means that many of the respondents will
        end up having entries for years in which they reported no demand, and that's
        fine.  They can be filtered later.
        """
        if update or self._annualized is None:
            # Calculate the total demand per respondent, per year:
            report_dates = (
                self.pudl_out.demand_hourly_pa_ferc714()
                .report_date.unique()
            )
            self._annualized = (
                self.pudl_out.respondent_id_ferc714()
                .pipe(add_dates, report_dates)
                .pipe(
                    pudl.helpers.convert_cols_dtypes,
                    data_source="ferc714",
                    name="FERC 714 Respondents Annualized",
                )
            )
        return self._annualized

    def categorize(self, update=False):
        """
        Annualized respondents with ``respondent_type`` assigned if possible.

        Categorize each respondent as either a ``utility`` or a ``balancing_authority``
        using the parameters stored in the instance of the class. While categorization
        can also be done without annualizing, this function annualizes as well, since
        we are adding the ``respondent_type`` in order to be able to compile service
        territories for the respondent, which vary annually.
        """
        if update or self._categorized is None:
            rids_ferc714 = self.pudl_out.respondent_id_ferc714()
            categorized = (
                pudl.analysis.demand_mapping.categorize_eia_code(
                    rids_ferc714.eia_code.dropna().unique(),
                    ba_ids=self.ba_ids,
                    util_ids=self.util_ids,
                    priority=self.priority)
                .merge(self.annualize(update=update), how="right")
            )
            # Names, ids, and codes for BAs identified as FERC 714 respondents
            # NOTE: this is not *strictly* correct, because the EIA BAs are not
            # eternal and unchanging.  There's at least one case in which the BA
            # associated with a given ID had a code and name change between years
            # after it changed hands. However, not merging on report_date in
            # addition to the balancing_authority_id_eia / eia_code fields ensures
            # that all years are populated for all BAs, which keeps them analogous
            # to the Utiliies in structure. Sooo.... it's fine for now.
            ba_respondents = (
                categorized.query("respondent_type=='balancing_authority'")
                .merge(
                    self.pudl_out.balancing_authority_eia861()[[
                        "balancing_authority_id_eia",
                        "balancing_authority_code_eia",
                        "balancing_authority_name_eia",
                    ]].drop_duplicates(subset=["balancing_authority_id_eia", ]),
                    how="left",
                    left_on="eia_code",
                    right_on="balancing_authority_id_eia",
                )
            )
            # Names and ids for Utils identified as FERC 714 respondents
            util_respondents = (
                categorized.query("respondent_type=='utility'")
                .merge(
                    pudl.analysis.service_territory.get_all_utils(self.pudl_out),
                    how="left", left_on="eia_code", right_on="utility_id_eia"
                )
            )
            self._categorized = (
                pd.concat([
                    ba_respondents,
                    util_respondents,
                    # Uncategorized respondents w/ no respondent_type:
                    categorized[categorized.respondent_type.isnull()]
                ])
                .pipe(
                    pudl.helpers.convert_cols_dtypes,
                    data_source="ferc714",
                    name="FERC 714 Respondents Categorized",
                )
                .pipe(
                    pudl.helpers.convert_cols_dtypes,
                    data_source="eia",
                    name="FERC 714 Respondents Categorized",
                )
            )
        return self._categorized

    def summarize_demand(self, update=False):
        """
        Compile annualized, categorized respondents with yearly demand totals.

        Calculate the total reported electricity demand associated with each respondent,
        and add that value to the annualized, categorized respodnent dataframe. This
        is in a ``demand_annual_mwh`` column.
        """
        if update or self._demand_summary is None:
            demand_annual = (
                pd.merge(
                    self.annualize(update=update),
                    self.pudl_out.demand_hourly_pa_ferc714()
                    .loc[:, ["report_date", "respondent_id_ferc714", "demand_mwh"]],
                    how="left")
                .groupby(["report_date", "respondent_id_ferc714"])
                .agg({"demand_mwh": sum})
                .rename(columns={"demand_mwh": "demand_annual_mwh"})
                .reset_index()
            )
            # Merge respondent categorizations into the annual demand
            self._demand_summary = (
                pd.merge(demand_annual, self.categorize(update=update), how="left")
                .pipe(
                    pudl.helpers.convert_cols_dtypes,
                    data_source="ferc714",
                    name="FERC 714 Respondents Demand Summary",
                )
                .pipe(
                    pudl.helpers.convert_cols_dtypes,
                    data_source="eia",
                    name="FERC 714 Respondents Demand Summary",
                )
            )
        return self._demand_summary

    def fipsify(self, update=False):
        """
        Annual respondents with the county FIPS IDs for their service territories.

        Given the ``respondent_type`` associated with each respondent (either
        ``utility`` or ``balancing_authority``) compile a list of counties that are part
        of their service territory on an annual basis, and merge those into the
        annualized respondent table. This results in a very long dataframe, since there
        are thousands of counties and many of them are served by more than one entity.

        Currently respondents categorized as ``utility`` will include any county that
        appears in the ``service_territory_eia861`` table in association with that
        utility ID in each year, while for ``balancing_authority`` respondents, some
        counties can be excluded based on state (if ``self.limit_by_state==True``).
        """
        if update or self._fipsified is None:
            categorized = self.categorize(update=update)
            # Generate the BA:FIPS relation:
            ba_counties = pd.merge(
                categorized.query("respondent_type=='balancing_authority'"),
                pudl.analysis.service_territory.get_territory_fips(
                    ids=categorized.balancing_authority_id_eia.unique(),
                    assn=self.pudl_out.balancing_authority_assn_eia861(),
                    assn_col="balancing_authority_id_eia",
                    st_eia861=self.pudl_out.service_territory_eia861(),
                    limit_by_state=self.limit_by_state),
                on=["report_date", "balancing_authority_id_eia"],
                how="left",
            )
            # Generate the Util:FIPS relation:
            util_counties = pd.merge(
                categorized.query("respondent_type=='utility'"),
                pudl.analysis.service_territory.get_territory_fips(
                    ids=categorized.utility_id_eia.unique(),
                    assn=self.pudl_out.utility_assn_eia861(),
                    assn_col="utility_id_eia",
                    st_eia861=self.pudl_out.service_territory_eia861(),
                    limit_by_state=self.limit_by_state,
                ),
                on=["report_date", "utility_id_eia"],
                how="left",
            )
            self._fipsified = (
                pd.concat([
                    ba_counties,
                    util_counties,
                    categorized[categorized.respondent_type.isnull()]
                ])
                .pipe(
                    pudl.helpers.convert_cols_dtypes,
                    data_source="ferc714",
                    name="FERC 714 Respondents FIPSified",
                )
                .pipe(
                    pudl.helpers.convert_cols_dtypes,
                    data_source="eia",
                    name="FERC 714 Respondents FIPSified",
                )
            )
        return self._fipsified

    def georef_counties(self, update=False):
        """
        Annual respondents with all associated county-level geometries.

        Given the county FIPS codes associated with each respondent in each year,
        pull in associated geometries from the US Census DP1 dataset, so we can do
        spatial analyses. This keeps each county record independent -- so there will
        be many records for each respondent in each year. This is fast, and still good
        for mapping, and retains all of the FIPS IDs so you can also still do ID based
        analyses.
        """
        if update or self._counties_gdf is None:
            census_counties = pudl.analysis.service_territory.get_census2010_gdf(
                pudl_settings=self.pudl_settings, layer="county")
            self._counties_gdf = (
                pudl.analysis.service_territory.add_geometries(
                    self.fipsify(update=update), census_gdf=census_counties)
                .pipe(
                    pudl.helpers.convert_cols_dtypes,
                    data_source="ferc714",
                    name="FERC 714 Respondents with County Geometries",
                )
                .pipe(
                    pudl.helpers.convert_cols_dtypes,
                    data_source="eia",
                    name="FERC 714 Respondents with County Geometries",
                )
            )
        return self._counties_gdf

    def georef_respondents(self, update=False):
        """
        Annual respondents with a single all-encompassing geometry for each year.

        Given the county FIPS codes associated with each responent in each year, compile
        a geometry for the respondent's entire service territory annually. This results
        in just a single record per respondent per year, but is computationally
        expensive and you lose the information about what all counties are associated
        with the respondent in that year. But it's useful for merging in other annual
        data like total demand, so you can see which respondent-years have both reported
        demand and decent geometries, calculate their areas to see if something changed
        from year to year, etc.
        """
        if update or self._respondents_gdf is None:
            census_counties = pudl.analysis.service_territory.get_census2010_gdf(
                pudl_settings=self.pudl_settings, layer="county")
            self._respondents_gdf = (
                pudl.analysis.service_territory.add_geometries(
                    self.fipsify(update=update),
                    census_gdf=census_counties,
                    dissolve=True,
                    dissolve_by=["report_date", "respondent_id_ferc714"])
                .merge(self.summarize_demand(update=update)[[
                    "report_date", "respondent_id_ferc714", "demand_annual_mwh"]])
                .pipe(
                    pudl.helpers.convert_cols_dtypes,
                    data_source="ferc714",
                    name="FERC 714 Respondents with Dissolved Geometries",
                )
                .pipe(
                    pudl.helpers.convert_cols_dtypes,
                    data_source="eia",
                    name="FERC 714 Respondents with Dissolved Geometries",
                )
            )
        return self._respondents_gdf
