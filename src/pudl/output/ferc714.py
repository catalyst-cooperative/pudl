"""Functions & classes for compiling derived aspects of the FERC Form 714 data."""
from functools import cached_property
from typing import Any, Dict, List

import numpy as np
import pandas as pd

import pudl

ASSOCIATIONS: List[Dict[str, Any]] = [
    # MISO: Midwest Indep System Operator
    {'id': 56669, 'from': 2011, 'to': [2009, 2010]},
    # SWPP: Southwest Power Pool
    {'id': 59504, 'from': 2014, 'to': [2006, 2009], 'exclude': ['NE']},
    {'id': 59504, 'from': 2014, 'to': [2010, 2013]},
    # LGEE: LG&E and KU Services Company
    {'id': 11249, 'from': 2014, 'to': [2006, 2013]},
    # (no code): Entergy
    {'id': 12506, 'from': 2012, 'to': [2013, 2013]},
    # (no code): American Electric Power Co Inc
    {'id': 829, 'from': 2008, 'to': [2009, 2013]},
]
"""
Adjustments to balancing authority-utility associations from EIA 861.

The changes are applied locally to EIA 861 tables.

* `id` (int): EIA balancing authority identifier (`balancing_authority_id_eia`).
* `from` (int): Reference year, to use as a template for target years.
* `to` (List[int]): Target years, in the closed interval format [minimum, maximum].
  Rows in `balancing_authority_eia861` are added (if missing) for every target year
  with the attributes from the reference year.
  Rows in `balancing_authority_assn_eia861` are added (or replaced, if existing)
  for every target year with the utility associations from the reference year.
  Rows in `service_territory_eia861` are added (if missing) for every target year
  with the nearest year's associated utilities' counties.
* `exclude` (Optional[List[str]]): Utilities to exclude, by state (two-letter code).
  Rows are excluded from `balancing_authority_assn_eia861` with target year and state.
"""

UTILITIES: List[Dict[str, Any]] = [
    # (no code): Pacific Gas & Electric Co
    {'id': 14328, 'reassign': True},
    # (no code): San Diego Gas & Electric Co
    {'id': 16609, 'reassign': True},
    # (no code): Dayton Power & Light Co
    {'id': 4922, 'reassign': True},
    # (no code): Consumers Energy Company
    # NOTE: 2003-2006 parent to 40211, which is never child to parent BA (12427),
    # (and 40211 never reports in service_territory_eia861) so don't reassign.
    {'id': 4254},
]
"""
Balancing authorities to treat as utilities in associations from EIA 861.

The changes are applied locally to EIA 861 tables.

* `id` (int): EIA balancing authority (BA) identifier (`balancing_authority_id_eia`).
  Rows for `id` are removed from `balancing_authority_eia861`.
* `reassign` (Optional[bool]): Whether to reassign utilities to parent BAs.
  Rows for `id` as BA in `balancing_authority_assn_eia861` are removed.
  Utilities assigned to `id` for a given year are reassigned
  to the BAs for which `id` is an associated utility.
* `replace` (Optional[bool]): Whether to remove rows where `id` is a utility in
  `balancing_authority_assn_eia861`. Applies only if `reassign=True`.
"""

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
                self.balancing_authority_eia861
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

    @cached_property
    def balancing_authority_eia861(self) -> pd.DataFrame:
        """Modified balancing_authority_eia861 table."""
        df = self.pudl_out.balancing_authority_eia861()
        index = ['balancing_authority_id_eia', 'report_date']
        dfi = df.set_index(index)
        # Prepare reference rows
        keys = [(fix['id'], pd.Timestamp(fix['from'], 1, 1)) for fix in ASSOCIATIONS]
        refs = dfi.loc[keys].reset_index().to_dict('records')
        # Build table of new rows
        # Insert row for each target balancing authority-year pair
        # missing from the original table, using the reference year as a template.
        rows = []
        for ref, fix in zip(refs, ASSOCIATIONS):
            for year in range(fix['to'][0], fix['to'][1] + 1):
                key = (fix['id'], pd.Timestamp(year, 1, 1))
                if key not in dfi.index:
                    rows.append({**ref, 'report_date': key[1]})
        # Append to original table
        df = df.append(pd.DataFrame(rows))
        # Remove balancing authorities treated as utilities
        mask = df['balancing_authority_id_eia'].isin([util['id'] for util in UTILITIES])
        return df[~mask]

    @cached_property
    def balancing_authority_assn_eia861(self) -> pd.DataFrame:
        """Modified balancing_authority_assn_eia861 table."""
        df = self.pudl_out.balancing_authority_assn_eia861()
        # Prepare reference rows
        refs = []
        for fix in ASSOCIATIONS:
            mask = df['balancing_authority_id_eia'].eq(fix['id']).to_numpy(bool)
            mask[mask] = df['report_date'][mask].eq(pd.Timestamp(fix['from'], 1, 1))
            ref = df[mask]
            if 'exclude' in fix:
                # Exclude utilities by state
                mask = ref['state'].isin(fix['exclude'])
                ref = ref[mask]
            refs.append(ref)
        # Buid table of new rows
        # Insert (or overwrite) rows for each target balancing authority-year pair,
        # using the reference year as a template.
        replaced = np.zeros(df.shape[0], dtype=bool)
        tables = []
        for ref, fix in zip(refs, ASSOCIATIONS):
            for year in range(fix['to'][0], fix['to'][1] + 1):
                key = fix['id'], pd.Timestamp(year, 1, 1)
                mask = df['balancing_authority_id_eia'].eq(key[0]).to_numpy(bool)
                mask[mask] = df['report_date'][mask].eq(key[1])
                tables.append(ref.assign(report_date=key[1]))
                replaced |= mask
        # Append to original table with matching rows removed
        df = df[~replaced].append(pd.concat(tables))
        # Remove balancing authorities treated as utilities
        mask = np.zeros(df.shape[0], dtype=bool)
        tables = []
        for util in UTILITIES:
            is_parent = df['balancing_authority_id_eia'].eq(util['id'])
            mask |= is_parent
            # Associated utilities are reassigned to parent balancing authorities
            if 'reassign' in util and util['reassign']:
                # Ignore when entity is child to itself
                is_child = ~is_parent & df['utility_id_eia'].eq(util['id'])
                # Build table associating parents to children of entity
                table = df[is_child].merge(
                    df[is_parent & ~df['utility_id_eia'].eq(util['id'])],
                    left_on=['report_date', 'utility_id_eia'],
                    right_on=['report_date', 'balancing_authority_id_eia']
                ).drop(
                    columns=[
                        'utility_id_eia_x', 'state_x', 'balancing_authority_id_eia_y'
                    ]
                ).rename(
                    columns={
                        'balancing_authority_id_eia_x': 'balancing_authority_id_eia',
                        'utility_id_eia_y': 'utility_id_eia',
                        'state_y': 'state'
                    }
                )
                tables.append(table)
                if 'replace' in util and util['replace']:
                    mask |= is_child
        return df[~mask].append(pd.concat(tables)).drop_duplicates()

    @cached_property
    def service_territory_eia861(self) -> pd.DataFrame:
        """Modified service_territory_eia861 table."""
        index = ['utility_id_eia', 'state', 'report_date']
        # Select relevant balancing authority-utility associations
        assn = self.balancing_authority_assn_eia861
        selected = np.zeros(assn.shape[0], dtype=bool)
        for fix in ASSOCIATIONS:
            years = [fix['from'], *range(fix['to'][0], fix['to'][1] + 1)]
            dates = [pd.Timestamp(year, 1, 1) for year in years]
            mask = assn['balancing_authority_id_eia'].eq(fix['id']).to_numpy(bool)
            mask[mask] = assn['report_date'][mask].isin(dates)
            selected |= mask
        # Reformat as unique utility-state-year
        assn = assn[selected][index].drop_duplicates()
        # Select relevant service territories
        df = self.pudl_out.service_territory_eia861()
        mdf = assn.merge(df, how='left')
        # Drop utility-state with no counties for all years
        grouped = mdf.groupby(['utility_id_eia', 'state'])['county_id_fips']
        mdf = mdf[grouped.transform('count').gt(0)]
        # Fill missing utility-state-year with nearest year with counties
        grouped = mdf.groupby(index)['county_id_fips']
        missing = mdf[grouped.transform('count').eq(0)].to_dict('records')
        has_county = mdf['county_id_fips'].notna()
        tables = []
        for row in missing:
            mask = (
                mdf['utility_id_eia'].eq(row['utility_id_eia']) &
                mdf['state'].eq(row['state']) &
                has_county
            )
            years = mdf['report_date'][mask].drop_duplicates()
            # Match to nearest year
            idx = (years - row['report_date']).abs().idxmin()
            mask &= mdf['report_date'].eq(years[idx])
            tables.append(mdf[mask].assign(report_date=row['report_date']))
        return pd.concat([df] + tables)

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
                    self.balancing_authority_eia861[[
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
        Compile annualized, categorized respondents and summarize values.

        Calculated summary values include:
        * Total reported electricity demand per respondent (``demand_annual_mwh``)
        * Reported per-capita electrcity demand (``demand_annual_per_capita_mwh``)
        * Population density (``population_density_km2``)
        * Demand density (``demand_density_mwh_km2``)

        These metrics are helpful identifying suspicious changes in the compiled annual
        geometries for the planning areas.

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
                .merge(
                    self.georef_counties(update=update)
                    .groupby(["report_date", "respondent_id_ferc714"])
                    .agg({"population": sum,
                          "area_km2": sum})
                    .reset_index())
                .assign(
                    population_density_km2=lambda x: x.population / x.area_km2,
                    demand_annual_per_capita_mwh=lambda x: x.demand_annual_mwh / x.population,
                    demand_density_mwh_km2=lambda x: x.demand_annual_mwh / x.area_km2,
                )
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
                    assn=self.balancing_authority_assn_eia861,
                    assn_col="balancing_authority_id_eia",
                    st_eia861=self.service_territory_eia861,
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
                    st_eia861=self.service_territory_eia861,
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
                pudl_settings=self.pudl_settings, layer="county", ds=self.pudl_out.ds)
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
