"""Unit tests for pudl.output.ferc714 EIA-861 data repair functions.

Tests focus on the three fill functions that apply manual spot-fixes to
EIA-861 balancing authority and service territory tables:
  - filled_core_eia861__yearly_balancing_authority
  - filled_core_eia861__assn_balancing_authority
  - filled_service_territory_eia861

These tests use real BA IDs from the BA_FIXES constant so that they
exercise the actual fix specifications.  All three functions are pure
DataFrame → DataFrame transforms with no external dependencies.

Selected BA IDs used in tests:
  - 56669 (MISO): source_year=2011, target_years=[2009, 2010]
  - 59504 (SWPP): source_year=2014, target_years=[2006..2013];
      exclude_states=["NE"] for 2006-2009
  - 13407 (Nevada Power): source_year=2013, target_years=[2014..2019]

Selected UTILITIES IDs used in tests:
  - 4254  (Consumers Energy): no reassign — just removed
  - 14328 (Pacific G&E):      reassign=True — children move to parent BA
"""

import pandas as pd
import pytest

from pudl.output.ferc714 import (
    BaFixMap,
    filled_core_eia861__assn_balancing_authority,
    filled_core_eia861__yearly_balancing_authority,
    filled_service_territory_eia861,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ba_yearly(*rows: dict) -> pd.DataFrame:
    """Build a minimal core_eia861__yearly_balancing_authority DataFrame.

    Each dict may have keys: id, year, code (optional), name (optional).
    """
    return pd.DataFrame(
        {
            "balancing_authority_id_eia": pd.array(
                [r["id"] for r in rows], dtype="Int64"
            ),
            "report_date": [pd.Timestamp(f"{r['year']}-01-01") for r in rows],
            "balancing_authority_code_eia": pd.array(
                [r.get("code", "XX") for r in rows], dtype=pd.StringDtype()
            ),
            "balancing_authority_name_eia": pd.array(
                [r.get("name", "Dummy") for r in rows], dtype=pd.StringDtype()
            ),
        }
    )


def _ba_assn(*rows: dict) -> pd.DataFrame:
    """Build a minimal core_eia861__assn_balancing_authority DataFrame.

    Each dict may have keys: ba_id, util_id, state, year.
    """
    return pd.DataFrame(
        {
            "balancing_authority_id_eia": pd.array(
                [r["ba_id"] for r in rows], dtype="Int64"
            ),
            "utility_id_eia": pd.array([r["util_id"] for r in rows], dtype="Int64"),
            "state": pd.array([r["state"] for r in rows], dtype=pd.StringDtype()),
            "report_date": [pd.Timestamp(f"{r['year']}-01-01") for r in rows],
        }
    )


def _service_territory(*rows: dict) -> pd.DataFrame:
    """Build a minimal core_eia861__yearly_service_territory DataFrame.

    Each dict may have keys: util_id, state, year, county_id_fips (optional).
    """
    return pd.DataFrame(
        {
            "utility_id_eia": pd.array([r["util_id"] for r in rows], dtype="Int64"),
            "state": pd.array([r["state"] for r in rows], dtype=pd.StringDtype()),
            "report_date": [pd.Timestamp(f"{r['year']}-01-01") for r in rows],
            "county_id_fips": pd.array(
                [r.get("county_id_fips", pd.NA) for r in rows],
                dtype=pd.StringDtype(),
            ),
        }
    )


# ---------------------------------------------------------------------------
# Tests for filled_core_eia861__yearly_balancing_authority
# ---------------------------------------------------------------------------


class TestFilledYearlyBalancingAuthority:
    def test_adds_missing_target_year_row(self):
        """When source year present and target year absent, new row is added.

        MISO (56669): source=2011, target=[2009, 2010].
        Input has (56669, 2011) and a dummy row to put 2009 into eia861_years.
        Expects (56669, 2009) to be added with MISO's code/name.
        """
        df = _ba_yearly(
            {"id": 56669, "year": 2011, "code": "MISO", "name": "Midwest ISO"},
            {"id": 99999, "year": 2009, "code": "XX", "name": "Dummy"},
        )
        result = filled_core_eia861__yearly_balancing_authority(df)
        result_keys = set(
            zip(
                result["balancing_authority_id_eia"],
                result["report_date"].dt.year,
                strict=False,
            )
        )
        assert (56669, 2009) in result_keys, "MISO 2009 row should be added"
        # 2010 not in eia861_years, so should not be added
        assert (56669, 2010) not in result_keys, (
            "MISO 2010 should not be added (2010 not in data)"
        )

    def test_added_row_copies_source_attributes(self):
        """The added row for the target year inherits the source year's code/name."""
        df = _ba_yearly(
            {"id": 56669, "year": 2011, "code": "MISO", "name": "Midwest ISO"},
            {"id": 99999, "year": 2009},
        )
        result = filled_core_eia861__yearly_balancing_authority(df)
        added = result[
            (result["balancing_authority_id_eia"] == 56669)
            & (result["report_date"].dt.year == 2009)
        ]
        assert len(added) == 1
        assert added.iloc[0]["balancing_authority_code_eia"] == "MISO"
        assert added.iloc[0]["balancing_authority_name_eia"] == "Midwest ISO"

    def test_does_not_overwrite_existing_target_year_row(self):
        """If the target BA-year already exists, it is left unchanged."""
        df = _ba_yearly(
            {
                "id": 56669,
                "year": 2011,
                "code": "MISO_2011",
                "name": "Midwest ISO 2011",
            },
            {
                "id": 56669,
                "year": 2009,
                "code": "MISO_2009",
                "name": "Midwest ISO 2009",
            },
            {"id": 99999, "year": 2009},
        )
        result = filled_core_eia861__yearly_balancing_authority(df)
        existing = result[
            (result["balancing_authority_id_eia"] == 56669)
            & (result["report_date"].dt.year == 2009)
        ]
        assert len(existing) == 1, "Should not duplicate an existing target row"
        assert existing.iloc[0]["balancing_authority_code_eia"] == "MISO_2009"

    def test_skips_fix_when_target_year_not_in_data(self):
        """No row is added if the target year is not present in eia861_years at all."""
        # Input only has 2011 — 2009 and 2010 are not in eia861_years
        df = _ba_yearly(
            {"id": 56669, "year": 2011, "code": "MISO", "name": "Midwest ISO"}
        )
        result = filled_core_eia861__yearly_balancing_authority(df)
        result_years = set(result["report_date"].dt.year)
        assert 2009 not in result_years
        assert 2010 not in result_years

    def test_removes_utilities_ba_rows(self):
        """BAs listed in UTILITIES are removed from the result table."""
        df = _ba_yearly(
            # 4254 is Consumers Energy — in UTILITIES, no reassign
            {"id": 4254, "year": 2020, "code": "CE", "name": "Consumers Energy"},
            {"id": 56669, "year": 2020, "code": "MISO", "name": "Midwest ISO"},
        )
        result = filled_core_eia861__yearly_balancing_authority(df)
        assert 4254 not in result["balancing_authority_id_eia"].to_numpy()
        assert 56669 in result["balancing_authority_id_eia"].to_numpy()

    def test_noop_when_no_relevant_years_in_data(self):
        """Fast-ETL case: no fixes applied when years don't overlap BA_FIXES."""
        # Fast ETL uses years like 2020, 2024 — none of the BA_FIXES fixes apply
        df = _ba_yearly(
            {"id": 56669, "year": 2020, "code": "MISO", "name": "Midwest ISO"},
            {"id": 99999, "year": 2024, "code": "XX", "name": "Dummy"},
        )
        result = filled_core_eia861__yearly_balancing_authority(df)
        # Row count may differ due to UTILITIES removal, but no new rows are added
        original_ids = set(
            zip(
                df["balancing_authority_id_eia"],
                df["report_date"].dt.year,
                strict=False,
            )
        )
        result_ids = set(
            zip(
                result["balancing_authority_id_eia"],
                result["report_date"].dt.year,
                strict=False,
            )
        )
        # Any rows present in result but not original must be from UTILITIES reassignment,
        # not from BA_FIXES fixes (since no BA_FIXES fixes apply here)
        added = result_ids - original_ids
        assert all(year not in range(2006, 2020) for _, year in added), (
            "No BA_FIXES fix rows should be added for fast ETL years"
        )


# ---------------------------------------------------------------------------
# Tests for filled_core_eia861__assn_balancing_authority
# ---------------------------------------------------------------------------


class TestFilledAssnBalancingAuthority:
    def test_replaces_existing_target_year_assn_rows(self):
        """Existing assn rows for a target year are replaced with source year rows.

        MISO (56669): source=2011, target=2009.
        The 2009 assn rows are replaced by the 2011 rows (with date changed to 2009).
        """
        df = _ba_assn(
            # Source rows (2011)
            {"ba_id": 56669, "util_id": 100, "state": "IL", "year": 2011},
            {"ba_id": 56669, "util_id": 200, "state": "MN", "year": 2011},
            # Existing wrong rows for 2009 (to be replaced)
            {"ba_id": 56669, "util_id": 999, "state": "WI", "year": 2009},
            # Dummy row to put 2009 in eia861_years
            {"ba_id": 99999, "util_id": 1, "state": "TX", "year": 2009},
        )
        result = filled_core_eia861__assn_balancing_authority(df)
        miso_2009 = result[
            (result["balancing_authority_id_eia"] == 56669)
            & (result["report_date"].dt.year == 2009)
        ]
        # Old wrong row should be gone
        assert 999 not in miso_2009["utility_id_eia"].to_numpy()
        # Source year rows should be present with target date
        assert set(miso_2009["utility_id_eia"].to_numpy()) == {100, 200}

    def test_adds_target_year_assn_rows_when_absent(self):
        """Source year assn rows are copied to a target year that has none."""
        df = _ba_assn(
            {"ba_id": 56669, "util_id": 100, "state": "IL", "year": 2011},
            {"ba_id": 99999, "util_id": 1, "state": "TX", "year": 2009},
        )
        result = filled_core_eia861__assn_balancing_authority(df)
        miso_2009 = result[
            (result["balancing_authority_id_eia"] == 56669)
            & (result["report_date"].dt.year == 2009)
        ]
        assert len(miso_2009) == 1
        assert miso_2009.iloc[0]["utility_id_eia"] == 100
        assert miso_2009.iloc[0]["state"] == "IL"

    def test_skips_fix_when_target_year_not_in_data(self):
        """No rows are added when the target year is absent from eia861_years."""
        df = _ba_assn({"ba_id": 56669, "util_id": 100, "state": "IL", "year": 2011})
        result = filled_core_eia861__assn_balancing_authority(df)
        # 2009 is not in eia861_years, so no MISO 2009 rows
        years_present = set(result["report_date"].dt.year)
        assert 2009 not in years_present

    def test_exclude_states_filters_associations(self):
        """exclude_states removes utilities in those states from copied rows.

        SWPP (59504): source=2014, target=[2006..2009], exclude_states=["NE"].
        Nebraska utilities should not appear in the 2009 (or any 2006-2009) target.
        """
        df = _ba_assn(
            # Source rows (2014)
            {"ba_id": 59504, "util_id": 100, "state": "KS", "year": 2014},
            {"ba_id": 59504, "util_id": 200, "state": "NE", "year": 2014},
            # Dummy row to put 2009 in eia861_years
            {"ba_id": 99999, "util_id": 1, "state": "TX", "year": 2009},
        )
        result = filled_core_eia861__assn_balancing_authority(df)
        swpp_2009 = result[
            (result["balancing_authority_id_eia"] == 59504)
            & (result["report_date"].dt.year == 2009)
        ]
        assert 100 in swpp_2009["utility_id_eia"].to_numpy(), (
            "KS utility should be included"
        )
        assert 200 not in swpp_2009["utility_id_eia"].to_numpy(), (
            "NE utility should be excluded"
        )

    def test_removes_utility_ba_without_reassign(self):
        """BAs in UTILITIES without reassign are removed, children unchanged.

        4254 (Consumers Energy) has no 'reassign', so rows where it is the BA
        are removed, but rows where it appears as a utility are left alone.
        """
        df = _ba_assn(
            # 4254 acting as a BA
            {"ba_id": 4254, "util_id": 100, "state": "MI", "year": 2020},
            # Some other BA where 4254 is a utility (should be unchanged)
            {"ba_id": 56669, "util_id": 4254, "state": "MI", "year": 2020},
        )
        result = filled_core_eia861__assn_balancing_authority(df)
        assert 4254 not in result["balancing_authority_id_eia"].to_numpy()
        # 4254 as utility should still be present (no reassign)
        child_rows = result[result["utility_id_eia"] == 4254]
        assert len(child_rows) == 1
        assert child_rows.iloc[0]["balancing_authority_id_eia"] == 56669

    def test_reassigns_utility_ba_children_to_parent(self):
        """BAs in UTILITIES with reassign=True have children moved to parent BAs.

        14328 (Pacific G&E) has reassign=True.
        Its children (utilities it manages as a BA) are reassigned to 14328's own
        parent BAs.
        """
        df = _ba_assn(
            # 14328 is a utility under parent BA 12345
            {"ba_id": 12345, "util_id": 14328, "state": "CA", "year": 2020},
            # 14328 has utility 56669 under it
            {"ba_id": 14328, "util_id": 56669, "state": "CA", "year": 2020},
            # 14328 also appears as its own utility (should be ignored in reassign)
            {"ba_id": 14328, "util_id": 14328, "state": "CA", "year": 2020},
        )
        result = filled_core_eia861__assn_balancing_authority(df)
        # 14328 should no longer appear as a BA
        assert 14328 not in result["balancing_authority_id_eia"].to_numpy()
        # 56669 should now be under parent BA 12345
        reassigned = result[
            (result["balancing_authority_id_eia"] == 12345)
            & (result["utility_id_eia"] == 56669)
        ]
        assert len(reassigned) == 1, "Child utility should be reassigned to parent BA"

    def test_noop_when_no_relevant_years_in_data(self):
        """Fast-ETL case: no BA_FIXES fixes applied for years 2020/2024."""
        df = _ba_assn(
            {"ba_id": 56669, "util_id": 100, "state": "IL", "year": 2020},
            {"ba_id": 56669, "util_id": 100, "state": "IL", "year": 2024},
        )
        result = filled_core_eia861__assn_balancing_authority(df)
        # MISO rows should pass through unchanged (no UTILITIES entries match 56669)
        miso_rows = result[result["balancing_authority_id_eia"] == 56669]
        assert len(miso_rows) == 2


# ---------------------------------------------------------------------------
# Tests for filled_service_territory_eia861
# ---------------------------------------------------------------------------


class TestFilledServiceTerritoryEia861:
    def test_fills_missing_county_data_from_nearest_year(self):
        """Missing county data for a target year is filled from the nearest year.

        MISO (56669): source=2011, target=2009.
        Utility 100 in CA has county "06001" in 2011 but no county data in 2009.
        The 2009 entry should be filled from 2011.
        """
        # Pre-filled assn: utility 100 under MISO in CA for both years
        filled_assn = _ba_assn(
            {"ba_id": 56669, "util_id": 100, "state": "CA", "year": 2009},
            {"ba_id": 56669, "util_id": 100, "state": "CA", "year": 2011},
        )
        # Service territory: county data only for 2011
        st = _service_territory(
            {"util_id": 100, "state": "CA", "year": 2009, "county_id_fips": pd.NA},
            {"util_id": 100, "state": "CA", "year": 2011, "county_id_fips": "06001"},
        )
        result = filled_service_territory_eia861(filled_assn, st)
        # Should have filled 2009 from 2011
        filled_2009 = result[
            (result["utility_id_eia"] == 100)
            & (result["state"] == "CA")
            & (result["report_date"].dt.year == 2009)
            & result["county_id_fips"].notna()
        ]
        assert len(filled_2009) == 1
        assert filled_2009.iloc[0]["county_id_fips"] == "06001"

    def test_no_fill_for_utility_state_with_no_county_data_ever(self):
        """A utility-state pair with no county data in any year is not filled.

        The original NA rows pass through unchanged, but no additional fill
        rows are appended (because there's no year to copy from).
        """
        filled_assn = _ba_assn(
            {"ba_id": 56669, "util_id": 100, "state": "CA", "year": 2009},
            {"ba_id": 56669, "util_id": 100, "state": "CA", "year": 2011},
            # Utility 200 in NV: never has county data
            {"ba_id": 56669, "util_id": 200, "state": "NV", "year": 2009},
            {"ba_id": 56669, "util_id": 200, "state": "NV", "year": 2011},
        )
        st = _service_territory(
            {"util_id": 100, "state": "CA", "year": 2009, "county_id_fips": pd.NA},
            {"util_id": 100, "state": "CA", "year": 2011, "county_id_fips": "06001"},
            {"util_id": 200, "state": "NV", "year": 2009, "county_id_fips": pd.NA},
            {"util_id": 200, "state": "NV", "year": 2011, "county_id_fips": pd.NA},
        )
        result = filled_service_territory_eia861(filled_assn, st)
        nv_rows = result[(result["utility_id_eia"] == 200) & (result["state"] == "NV")]
        # Original NA rows pass through but no fill rows are added — county data stays NA
        assert nv_rows["county_id_fips"].isna().all(), (
            "NV rows should have no county data"
        )
        # No extra rows appended beyond the original two
        assert len(nv_rows) == 2, (
            "No fill rows should have been added for utility 200 in NV"
        )

    def test_original_service_territory_preserved(self):
        """Non-repair rows in the original service territory are always returned."""
        filled_assn = _ba_assn(
            {"ba_id": 56669, "util_id": 100, "state": "CA", "year": 2011},
        )
        # Include a utility-state row NOT associated with any BA_FIXES fix
        st = _service_territory(
            {"util_id": 100, "state": "CA", "year": 2011, "county_id_fips": "06001"},
            {"util_id": 999, "state": "TX", "year": 2011, "county_id_fips": "48001"},
        )
        result = filled_service_territory_eia861(filled_assn, st)
        # The unrelated row should still be in the output
        tx_row = result[(result["utility_id_eia"] == 999) & (result["state"] == "TX")]
        assert len(tx_row) >= 1
        assert tx_row.iloc[0]["county_id_fips"] == "48001"

    def test_noop_for_fast_etl_years(self):
        """Fast-ETL case: no additional rows added when no BA_FIXES fixes apply."""
        # 2020 and 2024 have no BA_FIXES entries — nothing to fill
        filled_assn = _ba_assn(
            {"ba_id": 56669, "util_id": 100, "state": "IL", "year": 2020},
            {"ba_id": 56669, "util_id": 100, "state": "IL", "year": 2024},
        )
        st = _service_territory(
            {"util_id": 100, "state": "IL", "year": 2020, "county_id_fips": "17031"},
            {"util_id": 100, "state": "IL", "year": 2024, "county_id_fips": "17031"},
        )
        result = filled_service_territory_eia861(filled_assn, st)
        # No extra rows should be added
        assert len(result) == len(st)


# ---------------------------------------------------------------------------
# Tests for BaFixMap validation
# ---------------------------------------------------------------------------


class TestBaFixMap:
    def test_rejects_duplicate_ba_in_same_target_year(self):
        """Two entries with the same BA id in one target year raise ValueError."""
        with pytest.raises(ValueError, match="Duplicate"):
            BaFixMap.model_validate(
                {
                    2009: [
                        {"id": 56669, "source_year": 2011},
                        {"id": 56669, "source_year": 2012},
                    ]
                }
            )

    def test_rejects_self_referential_source_year(self):
        """source_year == target_year raises ValueError."""
        with pytest.raises(ValueError, match="source_year"):
            BaFixMap.model_validate({2009: [{"id": 56669, "source_year": 2009}]})

    def test_rejects_unknown_field(self):
        """An unrecognised field name raises a Pydantic ValidationError."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            BaFixMap.model_validate(
                {2009: [{"id": 56669, "source_year": 2011, "typo": True}]}
            )

    def test_accepts_valid_spec(self):
        """A well-formed spec passes without raising."""
        BaFixMap.model_validate(
            {
                2009: [{"id": 56669, "source_year": 2011}],
                2010: [
                    {"id": 56669, "source_year": 2011},
                    {"id": 59504, "source_year": 2014, "exclude_states": ["NE"]},
                ],
            }
        )


# ---------------------------------------------------------------------------
# Tests for the exclude_states guard in filled_core_eia861__assn_balancing_authority
# ---------------------------------------------------------------------------


class TestExcludeStatesGuard:
    def test_raises_when_exclude_states_removes_all_source_rows(self):
        """AssertionError fires when exclude_states eliminates every source row.

        SWPP (59504) for target_year=2009 uses source_year=2014 with
        exclude_states=["NE"].  If every 2014 source row is from NE the guard
        should fire rather than silently dropping all rows.
        """
        df = _ba_assn(
            # SWPP 2014 source rows — all from NE (every row will be excluded)
            {"ba_id": 59504, "util_id": 100, "state": "NE", "year": 2014},
            {"ba_id": 59504, "util_id": 200, "state": "NE", "year": 2014},
            # Dummy row to put 2009 in eia861_years
            {"ba_id": 99999, "util_id": 1, "state": "TX", "year": 2009},
        )
        with pytest.raises(AssertionError, match="exclude_states"):
            filled_core_eia861__assn_balancing_authority(df)
