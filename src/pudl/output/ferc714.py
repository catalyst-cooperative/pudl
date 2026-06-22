"""Functions & classes for compiling derived aspects of the FERC Form 714 data.

For a narrative overview of the timeseries imputation process, see the documentation
at :doc:`/methodology/timeseries_imputation`
"""

from typing import Any

import geopandas as gpd  # noqa: ICN002
import numpy as np
import pandas as pd
from dagster import Field, asset
from pydantic import BaseModel, RootModel, model_validator

import pudl.analysis.service_territory
import pudl.logging_helpers
from pudl.analysis.service_territory import utility_ids_all_eia
from pudl.analysis.timeseries_cleaning import (
    ImputeTimeseriesSettings,
    impute_timeseries_asset_factory,
)
from pudl.metadata.fields import apply_pudl_dtypes

logger = pudl.logging_helpers.get_logger(__name__)


class BaFix(BaseModel):
    """A single BA data repair entry for one target year.

    * ``id``: EIA balancing authority identifier (``balancing_authority_id_eia``).
    * ``source_year``: The year whose data is used as a template for the repair.
    * ``exclude_states``: Optional list of two-letter state codes. Utility associations
      for utilities in these states are omitted when copying from ``source_year``.
    """

    model_config = {"frozen": True, "extra": "forbid"}

    id: int
    source_year: int
    exclude_states: list[str] = []


class BaFixMap(RootModel[dict[int, list[BaFix]]]):
    """Validated mapping of target years to lists of :class:`BaFix` repair entries.

    Raises ``pydantic.ValidationError`` for malformed entries (wrong types,
    unknown keys) and ``ValueError`` for cross-entry constraint violations:
    duplicate BA IDs within the same target year, and self-referential fixes
    where ``source_year == target_year``.  Instantiated once at module load as
    the :data:`BA_FIXES` constant.
    """

    @model_validator(mode="after")
    def _check_structure(self) -> "BaFixMap":
        for target_year, fixes in self.root.items():
            ba_ids = [f.id for f in fixes]
            if len(ba_ids) != len(set(ba_ids)):
                dupes = sorted({x for x in ba_ids if ba_ids.count(x) > 1})
                raise ValueError(
                    f"Duplicate BA IDs {dupes} in BA_FIXES[{target_year}]: "
                    "each BA must appear at most once per target year."
                )
            for fix in fixes:
                if fix.source_year == target_year:
                    raise ValueError(
                        f"BA {fix.id} in BA_FIXES[{target_year}]: "
                        f"source_year ({fix.source_year}) must not equal target_year."
                    )
        return self

    def items(self):
        """Iterate over (target_year, fixes) pairs, delegating to the root dict."""
        return self.root.items()


BA_FIXES = BaFixMap.model_validate(
    {
        # Each key is a target year that needs repair. Each value is the list of
        # per-BA fixes to apply for that year.  A fix copies the BA's utility
        # associations from source_year into target_year, replacing any rows that
        # already exist for that (BA, target_year) pair.
        #
        # When either target_year or source_year is absent from the available
        # EIA-861 data (e.g. in the fast ETL), the fix is skipped.  If
        # target_year IS present but source_year is NOT, a warning is logged.
        2006: [
            # SWPP: Southwest Power Pool (Nebraska utilities excluded — reported separately)
            {"id": 59504, "source_year": 2014, "exclude_states": ["NE"]},
            # LGEE: LG&E and KU Services Company
            {"id": 11249, "source_year": 2014},
            # PJM: PJM Interconnection LLC
            {"id": 14725, "source_year": 2011},
            # SPS: Southwestern Public Service
            {"id": 17718, "source_year": 2010},
            # Nevada Power Company
            {"id": 13407, "source_year": 2009},
        ],
        2007: [
            # SWPP: Southwest Power Pool (Nebraska utilities excluded)
            {"id": 59504, "source_year": 2014, "exclude_states": ["NE"]},
            # LGEE: LG&E and KU Services Company
            {"id": 11249, "source_year": 2014},
            # PJM: PJM Interconnection LLC
            {"id": 14725, "source_year": 2011},
            # SPS: Southwestern Public Service
            {"id": 17718, "source_year": 2010},
            # Nevada Power Company
            {"id": 13407, "source_year": 2009},
        ],
        2008: [
            # SWPP: Southwest Power Pool (Nebraska utilities excluded)
            {"id": 59504, "source_year": 2014, "exclude_states": ["NE"]},
            # LGEE: LG&E and KU Services Company
            {"id": 11249, "source_year": 2014},
            # PJM: PJM Interconnection LLC
            {"id": 14725, "source_year": 2011},
            # SPS: Southwestern Public Service
            {"id": 17718, "source_year": 2010},
            # Nevada Power Company
            {"id": 13407, "source_year": 2009},
        ],
        2009: [
            # MISO: Midwest Independent System Operator
            {"id": 56669, "source_year": 2011},
            # SWPP: Southwest Power Pool (Nebraska utilities excluded)
            {"id": 59504, "source_year": 2014, "exclude_states": ["NE"]},
            # LGEE: LG&E and KU Services Company
            {"id": 11249, "source_year": 2014},
            # AEP: American Electric Power Co Inc
            {"id": 829, "source_year": 2008},
            # PJM: PJM Interconnection LLC
            {"id": 14725, "source_year": 2011},
            # SPS: Southwestern Public Service
            {"id": 17718, "source_year": 2010},
        ],
        2010: [
            # MISO: Midwest Independent System Operator
            {"id": 56669, "source_year": 2011},
            # SWPP: Southwest Power Pool
            {"id": 59504, "source_year": 2014},
            # LGEE: LG&E and KU Services Company
            {"id": 11249, "source_year": 2014},
            # AEP: American Electric Power Co Inc
            {"id": 829, "source_year": 2008},
            # PJM: PJM Interconnection LLC
            {"id": 14725, "source_year": 2011},
        ],
        2011: [
            # SWPP: Southwest Power Pool
            {"id": 59504, "source_year": 2014},
            # LGEE: LG&E and KU Services Company
            {"id": 11249, "source_year": 2014},
            # AEP: American Electric Power Co Inc
            {"id": 829, "source_year": 2008},
        ],
        2012: [
            # SWPP: Southwest Power Pool
            {"id": 59504, "source_year": 2014},
            # LGEE: LG&E and KU Services Company
            {"id": 11249, "source_year": 2014},
            # BANC: Balancing Authority of Northern California
            {"id": 16534, "source_year": 2013},
            # AEP: American Electric Power Co Inc
            {"id": 829, "source_year": 2008},
        ],
        2013: [
            # SWPP: Southwest Power Pool
            {"id": 59504, "source_year": 2014},
            # LGEE: LG&E and KU Services Company
            {"id": 11249, "source_year": 2014},
            # Entergy
            {"id": 12506, "source_year": 2012},
            # AEP: American Electric Power Co Inc
            {"id": 829, "source_year": 2008},
        ],
        2014: [
            # Nevada Power Company (now NV Energy)
            {"id": 13407, "source_year": 2013},
        ],
        2015: [
            # Nevada Power Company (now NV Energy)
            {"id": 13407, "source_year": 2013},
        ],
        2016: [
            # Nevada Power Company (now NV Energy)
            {"id": 13407, "source_year": 2013},
        ],
        2017: [
            # Nevada Power Company (now NV Energy)
            {"id": 13407, "source_year": 2013},
        ],
        2018: [
            # Nevada Power Company (now NV Energy)
            {"id": 13407, "source_year": 2013},
        ],
        2019: [
            # Nevada Power Company (now NV Energy)
            {"id": 13407, "source_year": 2013},
        ],
    }
)
"""Spot-fixes for known EIA-861 balancing authority ID errors, keyed by target year.

Each key is a year whose reported BA-utility associations are known to be
incorrect.  Each value is a list of :class:`BaFix` entries describing which BA
to fix and which year's data to use as the authoritative template.

The fixes are applied by three functions:
:func:`filled_core_eia861__yearly_balancing_authority`,
:func:`filled_core_eia861__assn_balancing_authority`, and
:func:`filled_service_territory_eia861`.  Each function:

* Silently skips a target year that is not present in the available EIA-861 data.
* Logs a warning and skips a fix when the target year IS present but the
  ``source_year`` is NOT, since the repair cannot be applied correctly.

To add a new fix: identify the target year(s), the affected BA's
``balancing_authority_id_eia``, and a ``source_year`` whose data is correct.
If certain states should be excluded from the copy, add ``exclude_states``.
"""

UTILITIES: list[dict[str, Any]] = [
    # (no code): Pacific Gas & Electric Co
    {"id": 14328, "reassign": True},
    # (no code): San Diego Gas & Electric Co
    {"id": 16609, "reassign": True},
    # (no code): Dayton Power & Light Co
    {"id": 4922, "reassign": True},
    # (no code): Consumers Energy Company
    # NOTE: 2003-2006 parent to 40211, which is never child to parent BA (12427),
    # (and 40211 never reports in core_eia861__yearly_service_territory) so don't reassign.
    {"id": 4254},
]
"""Balancing authorities to treat as utilities in associations from EIA 861.

The changes are applied locally to EIA 861 tables.

* ``id`` (int): EIA balancing authority (BA) identifier (``balancing_authority_id_eia``).
  Rows for ``id`` are removed from ``core_eia861__yearly_balancing_authority``.
* ``reassign`` (Optional[bool]): Whether to reassign utilities to parent BAs.
  Rows for ``id`` as BA in ``core_eia861__assn_balancing_authority`` are removed.
  Utilities assigned to ``id`` for a given year are reassigned
  to the BAs for which ``id`` is an associated utility.
* ``replace`` (Optional[bool]): Whether to remove rows where ``id`` is a utility in
  ``core_eia861__assn_balancing_authority``. Applies only if ``reassign=True``.
"""

################################################################################
# Helper functions
################################################################################


def categorize_eia_code(
    eia_codes: list[int],
    ba_ids: list[int],
    util_ids: list[int],
    priority: str = "balancing_authority",
) -> pd.DataFrame:
    """Categorize FERC 714 ``eia_codes`` as either balancing authority or utility IDs.

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
        eia_codes: A collection of IDs which may be either
            associated with EIA balancing authorities or utilities, to be categorized.
        ba_ids_eia: A collection of IDs which should be
            interpreted as belonging to EIA Balancing Authorities.
        util_ids_eia: A collection of IDs which should be
            interpreted as belonging to EIA Utilities.
        priority: Which respondent_type to give priority to if the eia_code shows
            up in both util_ids_eia and ba_ids_eia. Must be one of "utility" or
            "balancing_authority". The default is "balancing_authority".

    Returns:
        A DataFrame containing 2 columns: ``eia_code`` and ``respondent_type``.
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

    codes_df = pd.DataFrame({"eia_code": eia_codes}).drop_duplicates()
    ba_series = (
        pd.Series(ba_ids, name="balancing_authority_id_eia")
        .drop_duplicates()
        .convert_dtypes()
    )
    util_series = (
        pd.Series(util_ids, name="utility_id_eia").drop_duplicates().convert_dtypes()
    )

    df = codes_df.merge(
        ba_series, left_on="eia_code", right_on="balancing_authority_id_eia", how="left"
    ).merge(util_series, left_on="eia_code", right_on="utility_id_eia", how="left")
    df.loc[df[f"{primary}_id_eia"].notnull(), "respondent_type"] = primary
    df.loc[
        (df[f"{secondary}_id_eia"].notnull()) & (df[f"{primary}_id_eia"].isnull()),
        "respondent_type",
    ] = secondary
    df = df.astype({"respondent_type": pd.StringDtype()}).loc[
        :, ["eia_code", "respondent_type"]
    ]
    return df


#################################################################################
# Functions to compute analysis assets.
################################################################################


def _apply_exclude_states(
    ref: pd.DataFrame, fix: BaFix, target_year: int
) -> pd.DataFrame:
    """Filter source rows by exclude_states, raising if all rows are eliminated.

    An empty result after filtering means the exclude_states list is broader than
    the actual data, which would silently drop all associations for the (BA,
    target_year) pair.  That is almost certainly an erroneous specification.
    """
    if not fix.exclude_states:
        return ref
    filtered = ref[~ref["state"].isin(fix.exclude_states)]
    if filtered.empty and not ref.empty:
        raise AssertionError(
            f"BA {fix.id} target_year={target_year}: "
            f"exclude_states={fix.exclude_states!r} removed all "
            f"{len(ref)} source rows from source_year={fix.source_year}. "
            "Verify the exclude_states list is not too broad."
        )
    return filtered


def filled_core_eia861__yearly_balancing_authority(
    core_eia861__yearly_balancing_authority: pd.DataFrame,
) -> pd.DataFrame:
    """Modified core_eia861__yearly_balancing_authority table.

    For each entry in :data:`BA_FIXES`, adds a row for the target year if
    one is missing, copying all attributes from the source year.  Silently
    skips any target year absent from the data.  Logs a warning and skips if
    the target year is present but the source year is not.  Also removes
    balancing authorities that are manually categorised as utilities via
    :data:`UTILITIES`.
    """
    df = core_eia861__yearly_balancing_authority
    index = ["balancing_authority_id_eia", "report_date"]
    dfi = df.set_index(index)
    eia861_years = set(df["report_date"].dt.year)

    new_rows: list[dict[str, Any]] = []
    for target_year, fixes in BA_FIXES.items():
        if target_year not in eia861_years:
            continue
        target_date = pd.Timestamp(target_year, 1, 1)
        for fix in fixes:
            source_year = fix.source_year
            if source_year not in eia861_years:
                logger.warning(
                    f"Skipping BA repair for id={fix.id} target_year={target_year}: "
                    f"source_year={source_year} not in available EIA-861 data. "
                    "If running the full ETL, verify the fix spec is correct."
                )
                continue
            target_key = (fix.id, target_date)
            if target_key in dfi.index:
                continue
            source_key = (fix.id, pd.Timestamp(source_year, 1, 1))
            if source_key not in dfi.index:
                logger.warning(
                    f"Skipping BA repair for id={fix.id} target_year={target_year}: "
                    f"source row {source_key} not found in BA table."
                )
                continue
            ref = dfi.loc[[source_key]].reset_index().to_dict("records")[0]
            new_rows.append({**ref, "report_date": target_date})

    if new_rows:
        df = pd.concat(
            [df, apply_pudl_dtypes(pd.DataFrame(new_rows), group="eia")],
            axis="index",
        )
    mask = df["balancing_authority_id_eia"].isin([util["id"] for util in UTILITIES])
    return apply_pudl_dtypes(df[~mask], group="eia")


def filled_core_eia861__assn_balancing_authority(
    core_eia861__assn_balancing_authority: pd.DataFrame,
) -> pd.DataFrame:
    """Modified core_eia861__assn_balancing_authority table.

    For each entry in :data:`BA_FIXES`, replaces all existing rows for
    the (BA, target_year) pair with rows copied from the source year (optionally
    filtered by :attr:`BaFix.exclude_states`).  Silently skips target years
    absent from the data; logs a warning when the target year is present but
    the source year is not.  Also removes balancing authorities listed in
    :data:`UTILITIES` and, when ``reassign=True``, re-parents their child
    utilities to the grandparent BAs.
    """
    df = core_eia861__assn_balancing_authority
    eia861_years = set(df["report_date"].dt.year)

    replaced = np.zeros(df.shape[0], dtype=bool)
    new_tables: list[pd.DataFrame] = []

    for target_year, fixes in BA_FIXES.items():
        if target_year not in eia861_years:
            continue
        target_date = pd.Timestamp(target_year, 1, 1)
        for fix in fixes:
            source_year = fix.source_year
            if source_year not in eia861_years:
                logger.warning(
                    f"Skipping BA association repair for id={fix.id} "
                    f"target_year={target_year}: source_year={source_year} not in "
                    "available EIA-861 data. If running the full ETL, verify the "
                    "fix spec is correct."
                )
                continue
            source_date = pd.Timestamp(source_year, 1, 1)
            # Collect source-year association rows for this BA
            ref = df[
                df["balancing_authority_id_eia"].eq(fix.id)
                & df["report_date"].eq(source_date)
            ]
            ref = _apply_exclude_states(ref, fix, target_year)
            # Mark existing target-year rows for removal
            target_mask = df["balancing_authority_id_eia"].eq(fix.id) & df[
                "report_date"
            ].eq(target_date)
            replaced |= target_mask.to_numpy(bool)
            new_tables.append(ref.assign(report_date=target_date))

    if new_tables:
        new_rows = apply_pudl_dtypes(pd.concat(new_tables), group="eia")
        df = pd.concat([df[~replaced], new_rows], axis="index")
    else:
        df = df[~replaced]

    # Remove BAs that are really utilities; optionally re-parent their children
    removal_mask = np.zeros(df.shape[0], dtype=bool)
    reassignment_tables: list[pd.DataFrame] = []
    for util in UTILITIES:
        is_parent = df["balancing_authority_id_eia"].eq(util["id"])
        removal_mask |= is_parent
        if util.get("reassign"):
            # Rows where util["id"] is a utility (child) under some other BA
            is_child = ~is_parent & df["utility_id_eia"].eq(util["id"])
            # Link each grandchild utility to the grandparent BAs
            table = (
                df[is_child]
                .merge(
                    df[is_parent & ~df["utility_id_eia"].eq(util["id"])],
                    left_on=["report_date", "utility_id_eia"],
                    right_on=["report_date", "balancing_authority_id_eia"],
                )
                .drop(
                    columns=[
                        "utility_id_eia_x",
                        "state_x",
                        "balancing_authority_id_eia_y",
                    ]
                )
                .rename(
                    columns={
                        "balancing_authority_id_eia_x": "balancing_authority_id_eia",
                        "utility_id_eia_y": "utility_id_eia",
                        "state_y": "state",
                    }
                )
            )
            reassignment_tables.append(table)
            if util.get("replace"):
                removal_mask |= is_child
    return (
        pd.concat([df[~removal_mask]] + reassignment_tables)
        .drop_duplicates()
        .pipe(apply_pudl_dtypes, group="eia")
    )


def filled_service_territory_eia861(
    filled_assn: pd.DataFrame,
    core_eia861__yearly_service_territory: pd.DataFrame,
) -> pd.DataFrame:
    """Modified core_eia861__yearly_service_territory table.

    Selects utility-state-year combinations that are relevant to the BA
    repairs in :data:`BA_FIXES` (covering both source and target years),
    merges them with the service territory to obtain county FIPS data, drops
    utility-state pairs that have no county data in *any* year, and fills
    records missing counties with data from the nearest available year.

    Args:
        filled_assn: The already-repaired BA-utility association table, as
            returned by :func:`filled_core_eia861__assn_balancing_authority`.
        core_eia861__yearly_service_territory: The raw service territory table.
    """
    index = ["utility_id_eia", "state", "report_date"]
    eia861_years = set(core_eia861__yearly_service_territory["report_date"].dt.year)

    # Collect (ba_id, date) pairs relevant to any BA_FIXES fix so we can
    # select the matching rows from the filled association table.
    relevant: set[tuple] = set()
    for target_year, fixes in BA_FIXES.items():
        for fix in fixes:
            if target_year in eia861_years:
                relevant.add((fix.id, pd.Timestamp(target_year, 1, 1)))
            if fix.source_year in eia861_years:
                relevant.add((fix.id, pd.Timestamp(fix.source_year, 1, 1)))

    selected = np.zeros(filled_assn.shape[0], dtype=bool)
    for ba_id, date in relevant:
        selected |= (
            filled_assn["balancing_authority_id_eia"].eq(ba_id)
            & filled_assn["report_date"].eq(date)
        ).to_numpy(bool)

    # Unique utility-state-year triples relevant to any fix
    assn = filled_assn[selected][index].drop_duplicates()
    # Left-join to service territory: missing counties become NaN
    mdf = assn.merge(core_eia861__yearly_service_territory, how="left")
    # Drop utility-state pairs that never have any county data
    grouped = mdf.groupby(["utility_id_eia", "state"])["county_id_fips"]
    mdf = mdf[grouped.transform("count").gt(0)]
    # Fill utility-state-year triples missing counties from the nearest year
    grouped = mdf.groupby(index)["county_id_fips"]
    missing = mdf[grouped.transform("count").eq(0)].to_dict("records")
    has_county = mdf["county_id_fips"].notna()
    fill_tables: list[pd.DataFrame] = []
    for row in missing:
        mask = (
            mdf["utility_id_eia"].eq(row["utility_id_eia"])
            & mdf["state"].eq(row["state"])
            & has_county
        )
        years = mdf["report_date"][mask].drop_duplicates()
        idx = (years - row["report_date"]).abs().idxmin()
        mask &= mdf["report_date"].eq(years[idx])
        fill_tables.append(mdf[mask].assign(report_date=row["report_date"]))
    return pd.concat([core_eia861__yearly_service_territory] + fill_tables).pipe(
        apply_pudl_dtypes, group="eia"
    )


@asset(
    compute_kind="Python",
    required_resource_keys={"global_data_config"},
)
def _out_ferc714__annualized_respondents(
    context,
    core_ferc714__respondent_id: pd.DataFrame,
) -> pd.DataFrame:
    """Broadcast respondent data across all years with reported demand.

    The FERC 714 Respondent IDs and names are reported in their own table, without any
    reference to individual years, but much of the information we are associating with
    them varies annually. This method creates an annualized version of the respondent
    table, with each respondent having an entry corresponding to every year for which
    FERC 714 has been processed. This means that many of the respondents will end up
    having entries for years in which they reported no demand, and that's fine.
    They can be filtered later.
    """
    if "report_date" in core_ferc714__respondent_id.columns:
        raise AssertionError("report_date already present, can't be added again!")

    ferc714_data_config = context.resources.global_data_config.pudl.ferc714
    report_dates = pd.DataFrame(
        {"report_date": pd.to_datetime(sorted(ferc714_data_config.years), format="%Y")}
    )
    return core_ferc714__respondent_id.merge(report_dates, how="cross")


@asset(
    config_schema={
        "priority": Field(
            str,
            default_value="balancing_authority",
            description=(
                "Which type of entity should take priority in the categorization of "
                "FERC 714 respondents. Must be either ``utility`` or "
                "``balancing_authority.`` The default is ``balancing_authority``."
            ),
        ),
    },
    compute_kind="Python",
)
def _out_ferc714__categorized_respondents(
    context,
    core_ferc714__respondent_id: pd.DataFrame,
    out_eia__yearly_utilities: pd.DataFrame,
    core_eia861__yearly_service_territory: pd.DataFrame,
    core_eia861__yearly_balancing_authority: pd.DataFrame,
    _out_ferc714__annualized_respondents: pd.DataFrame,
) -> pd.DataFrame:
    """Annualized respondents with ``respondent_type`` assigned if possible.

    Categorize each respondent as either a ``utility`` or a ``balancing_authority``
    using the parameters stored in the instance of the class. While categorization
    can also be done without annualizing, this function annualizes as well, since we
    are adding the ``respondent_type`` in order to be able to compile service
    territories for the respondent, which vary annually.
    """
    priority = context.op_config["priority"]

    logger.info("Categorizing EIA codes associated with FERC-714 Respondents.")

    bal_auth = filled_core_eia861__yearly_balancing_authority(
        core_eia861__yearly_balancing_authority
    )
    utilids_all_eia = utility_ids_all_eia(
        out_eia__yearly_utilities, core_eia861__yearly_service_territory
    )

    categorized = categorize_eia_code(
        core_ferc714__respondent_id.eia_code.dropna().unique(),
        ba_ids=bal_auth.balancing_authority_id_eia.dropna().unique(),
        util_ids=utilids_all_eia.utility_id_eia,
        priority=priority,
    )
    logger.info(
        "Merging categorized EIA codes with annualized FERC-714 Respondent data."
    )
    categorized = pd.merge(
        categorized, _out_ferc714__annualized_respondents, how="right"
    )
    # Names, ids, and codes for BAs identified as FERC 714 respondents
    # NOTE: this is not *strictly* correct, because the EIA BAs are not
    # eternal and unchanging.  There's at least one case in which the BA
    # associated with a given ID had a code and name change between years
    # after it changed hands. However, not merging on report_date in
    # addition to the balancing_authority_id_eia / eia_code fields ensures
    # that all years are populated for all BAs, which keeps them analogous
    # to the Utilities in structure. Sooo.... it's fine for now.
    logger.info("Selecting FERC-714 Balancing Authority respondents.")
    ba_respondents = categorized.query("respondent_type=='balancing_authority'")
    logger.info(
        "Merging FERC-714 Balancing Authority respondents with BA id/code/name "
        "information from EIA-861."
    )
    ba_respondents = pd.merge(
        ba_respondents,
        bal_auth[
            [
                "balancing_authority_id_eia",
                "balancing_authority_code_eia",
                "balancing_authority_name_eia",
            ]
        ].drop_duplicates(
            subset=[
                "balancing_authority_id_eia",
            ]
        ),
        how="left",
        left_on="eia_code",
        right_on="balancing_authority_id_eia",
    )
    logger.info("Selecting names and IDs for FERC-714 Utility respondents.")
    util_respondents = categorized.query("respondent_type=='utility'")
    logger.info("Merging FERC-714 Utility respondents with service territory.")
    util_respondents = pd.merge(
        util_respondents,
        utilids_all_eia,
        how="left",
        left_on="eia_code",
        right_on="utility_id_eia",
    )
    logger.info(
        "Concatenating categorized FERC-714 respondents:"
        f"{len(ba_respondents)} BA records, "
        f"{len(util_respondents)} utility records, "
        f"{sum(categorized.respondent_type.isnull())} uncategorized records."
    )
    categorized = pd.concat(
        [
            ba_respondents,
            util_respondents,
            # Uncategorized respondents w/ no respondent_type:
            categorized[categorized.respondent_type.isnull()],
        ]
    )
    categorized = apply_pudl_dtypes(categorized)
    return categorized


@asset(
    config_schema={
        "limit_by_state": Field(
            bool,
            default_value=True,
            description=(
                "Whether to limit respondent service territories to the states where "
                "they have documented activity in the EIA 861. Currently this is only "
                "implemented for Balancing Authorities."
            ),
        ),
    },
    compute_kind="pandas",
    io_manager_key="pudl_io_manager",
)
def out_ferc714__respondents_with_fips(
    context,
    _out_ferc714__categorized_respondents: pd.DataFrame,
    core_eia861__assn_balancing_authority: pd.DataFrame,
    core_eia861__yearly_service_territory: pd.DataFrame,
    core_eia861__assn_utility: pd.DataFrame,
) -> pd.DataFrame:
    """Annual respondents with the county FIPS IDs for their service territories.

    Given the ``respondent_type`` associated with each respondent (either ``utility`` or
    ``balancing_authority``) compile a list of counties that are part of their service
    territory on an annual basis, and merge those into the annualized respondent table.
    This results in a very long dataframe, since there are thousands of counties and
    many of them are served by more than one entity.

    Currently respondents categorized as ``utility`` will include any county that
    appears in the ``core_eia861__yearly_service_territory`` table in association with
    that utility ID in each year, while for ``balancing_authority`` respondents, some
    counties can be excluded based on state (if ``limit_by_state==True``).

    """
    assn = filled_core_eia861__assn_balancing_authority(
        core_eia861__assn_balancing_authority
    )
    st_eia861 = filled_service_territory_eia861(
        assn, core_eia861__yearly_service_territory
    )

    # Generate the BA:FIPS relation:
    ba_counties = pd.merge(
        _out_ferc714__categorized_respondents.query(
            "respondent_type=='balancing_authority'"
        ),
        pudl.analysis.service_territory.get_territory_fips(
            ids=_out_ferc714__categorized_respondents.balancing_authority_id_eia.unique(),
            assn=assn,
            assn_col="balancing_authority_id_eia",
            core_eia861__yearly_service_territory=st_eia861,
            limit_by_state=context.op_config["limit_by_state"],
        ),
        on=["report_date", "balancing_authority_id_eia"],
        how="left",
    )
    # Generate the Util:FIPS relation:
    util_counties = pd.merge(
        _out_ferc714__categorized_respondents.query("respondent_type=='utility'"),
        pudl.analysis.service_territory.get_territory_fips(
            ids=_out_ferc714__categorized_respondents.utility_id_eia.unique(),
            assn=core_eia861__assn_utility,
            assn_col="utility_id_eia",
            core_eia861__yearly_service_territory=st_eia861,
            limit_by_state=context.op_config["limit_by_state"],
        ),
        on=["report_date", "utility_id_eia"],
        how="left",
    )
    logger.info(
        "Concatenating georeferenced FERC-714 respondents:"
        f"{len(ba_counties)} BA records, "
        f"{len(util_counties)} utility records, "
        f"{sum(_out_ferc714__categorized_respondents.respondent_type.isnull())} uncategorized records."
    )
    fipsified = pd.concat(
        [
            ba_counties,
            util_counties,
            _out_ferc714__categorized_respondents[
                _out_ferc714__categorized_respondents.respondent_type.isnull()
            ],
        ]
    ).pipe(apply_pudl_dtypes)
    return fipsified


@asset(
    op_tags={"memory-use": "high"},
    compute_kind="pandas",
)
def _out_ferc714__georeferenced_counties(
    out_ferc714__respondents_with_fips: pd.DataFrame,
    out_censusdp1tract__counties: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """Annual respondents with all associated county-level geometries.

    Given the county FIPS codes associated with each respondent in each year, pull in
    associated geometries from the US Census DP1 dataset, so we can do spatial analyses.
    This keeps each county record independent -- so there will be many records for each
    respondent in each year. This is fast, and still good for mapping, and retains all
    of the FIPS IDs so you can also still do ID based analyses.
    """
    return gpd.GeoDataFrame(
        pudl.analysis.service_territory.add_geometries(
            out_ferc714__respondents_with_fips,
            census_gdf=out_censusdp1tract__counties,
        ).pipe(apply_pudl_dtypes)
    )


@asset(
    compute_kind="pandas",
    io_manager_key="geoparquet_io_manager",
)
def out_ferc714__georeferenced_respondents(
    out_ferc714__respondents_with_fips: pd.DataFrame,
    out_ferc714__summarized_demand: pd.DataFrame,
    out_censusdp1tract__counties: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """Annual respondents with a single all-encompassing geometry for each year.

    Given the county FIPS codes associated with each responent in each year, compile a
    geometry for the respondent's entire service territory annually. This results in
    just a single record per respondent per year, but is computationally expensive and
    you lose the information about what all counties are associated with the respondent
    in that year. But it's useful for merging in other annual data like total demand, so
    you can see which respondent-years have both reported demand and decent geometries,
    calculate their areas to see if something changed from year to year, etc.
    """
    return gpd.GeoDataFrame(
        pudl.analysis.service_territory.add_geometries(
            out_ferc714__respondents_with_fips,
            census_gdf=out_censusdp1tract__counties,
            dissolve=True,
            dissolve_by=["report_date", "respondent_id_ferc714"],
        )
        .merge(
            out_ferc714__summarized_demand[
                ["report_date", "respondent_id_ferc714", "demand_annual_mwh"]
            ]
        )
        .pipe(apply_pudl_dtypes)
    )


@asset(
    io_manager_key="pudl_io_manager",
    op_tags={"memory-use": "high"},
    compute_kind="pandas",
)
def out_ferc714__summarized_demand(
    _out_ferc714__annualized_respondents: pd.DataFrame,
    out_ferc714__hourly_planning_area_demand: pd.DataFrame,
    _out_ferc714__categorized_respondents: pd.DataFrame,
    _out_ferc714__georeferenced_counties: gpd.GeoDataFrame,
) -> pd.DataFrame:
    """Compile annualized, categorized respondents and summarize values.

    Calculated summary values include:
    * Total reported electricity demand per respondent (``demand_annual_mwh``)
    * Reported per-capita electrcity demand (``demand_annual_per_capita_mwh``)
    * Population density (``population_density_km2``)
    * Demand density (``demand_density_mwh_km2``)

    These metrics are helpful identifying suspicious changes in the compiled annual
    geometries for the planning areas.
    """
    demand_annual = (
        pd.merge(
            _out_ferc714__annualized_respondents,
            out_ferc714__hourly_planning_area_demand.loc[
                :, ["report_date", "respondent_id_ferc714", "demand_imputed_pudl_mwh"]
            ],
            on=["report_date", "respondent_id_ferc714"],
            how="left",
        )
        .groupby(["report_date", "respondent_id_ferc714"], as_index=False)[
            ["demand_imputed_pudl_mwh"]
        ]
        .sum(min_count=1)
        .rename(columns={"demand_imputed_pudl_mwh": "demand_annual_mwh"})
        .merge(
            _out_ferc714__georeferenced_counties.groupby(
                ["report_date", "respondent_id_ferc714"], as_index=False
            )[["population", "area_km2"]].sum(min_count=1),
            on=["report_date", "respondent_id_ferc714"],
            how="left",
        )
        .assign(
            population_density_km2=lambda x: x.population / x.area_km2,
            demand_annual_per_capita_mwh=lambda x: x.demand_annual_mwh / x.population,
            demand_density_mwh_km2=lambda x: x.demand_annual_mwh / x.area_km2,
        )
    )
    # Merge respondent categorizations into the annual demand
    demand_summary = pd.merge(
        demand_annual, _out_ferc714__categorized_respondents, how="left"
    ).pipe(apply_pudl_dtypes)
    return demand_summary


imputed_hourly_planning_area_demand_assets = impute_timeseries_asset_factory(
    input_asset_name="core_ferc714__hourly_planning_area_demand",
    output_asset_name="out_ferc714__hourly_planning_area_demand",
    years_from_context=lambda context: (
        context.resources.global_data_config.pudl.ferc714.years
    ),
    value_col="demand_mwh",
    imputed_value_col="demand_imputed_pudl_mwh",
    id_col="respondent_id_ferc714",
    op_tags={"dagster/priority": 10},
    settings=ImputeTimeseriesSettings(min_data_fraction=0.7),
)
