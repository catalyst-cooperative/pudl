"""Unit tests for allocation of net generation."""

from io import StringIO

import pandas as pd
import pytest

from pudl.analysis import allocate_gen_fuel
from pudl.metadata.dtypes import apply_pudl_dtypes

# `identify_retiring_generators`/`identify_newly_operating_generators` (via the shared
# `_identify_transitioning_generators` helper in `allocate_gen_fuel.py`) flag a
# generator as transitioning mid-year if ANY of three conditions hold. Tests below
# that exercise one of these are labeled "condition A/B/C" by this scheme:
#
# - Condition A: `report_date` already reflects the generator's actual transition
#   date (`generator_retirement_date`/`generator_operating_date`) this report_year.
# - Condition B: it reports generator-specific data in the more granular `g` table.
# - Condition C: it has non-zero data in the less granular `gf` table for a PM/ESC
#   combo that is unique to it at its plant.
#
# See `_identify_transitioning_generators`'s docstring for the authoritative version.

# Reusable input files...

# inputs for example 1:
#  multi-generator-plant with one primary fuel type that fully reports to the
#  generation_eia923 table


def test_distribute_annually_reported_data_to_months_if_annual():
    """Test :func:`distribute_annually_reported_data_to_months_if_annual`."""
    annual_2021 = 22_222.0
    annual_2020 = 20_202.0
    bf_with_monthly_annual_mix = pd.read_csv(
        StringIO(
            f"""plant_id_eia,report_date,boiler_id,energy_source_code,prime_mover_code,fuel_consumed_mmbtu
    41,2021-01-01,a,NG,GT,1.0
    41,2021-02-01,a,NG,GT,2.0
    41,2021-03-01,a,NG,GT,3.0
    41,2021-04-01,a,NG,GT,4.0
    41,2021-05-01,a,NG,GT,5.0
    41,2021-06-01,a,NG,GT,6.0
    41,2021-07-01,a,NG,GT,6.0
    41,2021-08-01,a,NG,GT,5.0
    41,2021-09-01,a,NG,GT,4.0
    41,2021-10-01,a,NG,GT,3.0
    41,2021-11-01,a,NG,GT,2.0
    41,2021-12-01,a,NG,GT,1.0
    41,2020-01-01,a,NG,GT,2.0
    41,2020-02-01,a,NG,GT,3.0
    41,2020-03-01,a,NG,GT,4.0
    41,2020-04-01,a,NG,GT,5.0
    41,2020-05-01,a,NG,GT,6.0
    41,2020-06-01,a,NG,GT,7.0
    41,2020-07-01,a,NG,GT,7.0
    41,2020-08-01,a,NG,GT,6.0
    41,2020-09-01,a,NG,GT,5.0
    41,2020-10-01,a,NG,GT,4.0
    41,2020-11-01,a,NG,GT,3.0
    41,2020-12-01,a,NG,GT,2.0
    200,2021-01-01,B1,SUB,ST,{annual_2021}
    200,2021-02-01,B1,SUB,ST,
    200,2021-03-01,B1,SUB,ST,
    200,2021-04-01,B1,SUB,ST,
    200,2021-05-01,B1,SUB,ST,
    200,2021-06-01,B1,SUB,ST,
    200,2021-07-01,B1,SUB,ST,
    200,2021-08-01,B1,SUB,ST,
    200,2021-09-01,B1,SUB,ST,
    200,2021-10-01,B1,SUB,ST,
    200,2021-11-01,B1,SUB,ST,
    200,2021-12-01,B1,SUB,ST,
    200,2020-01-01,B1,BIT,ST,0.0
    200,2020-02-01,B1,BIT,ST,0.0
    200,2020-03-01,B1,BIT,ST,0.0
    200,2020-04-01,B1,BIT,ST,0.0
    200,2020-05-01,B1,BIT,ST,0.0
    200,2020-06-01,B1,BIT,ST,0.0
    200,2020-07-01,B1,BIT,ST,0.0
    200,2020-08-01,B1,BIT,ST,0.0
    200,2020-09-01,B1,BIT,ST,0.0
    200,2020-10-01,B1,BIT,ST,0.0
    200,2020-11-01,B1,BIT,ST,0.0
    200,2020-12-01,B1,BIT,ST,{annual_2020}"""
        )
    ).pipe(apply_pudl_dtypes, field_namespace="eia")

    out = allocate_gen_fuel.distribute_annually_reported_data_to_months_if_annual(
        df=bf_with_monthly_annual_mix,
        key_columns=allocate_gen_fuel.IDX_B_PM_ESC,
        data_column_name="fuel_consumed_mmbtu",
        freq="MS",
    )

    out = out.sort_values(["plant_id_eia", "report_date"]).reset_index(drop=True)
    yearly_out = out[out["plant_id_eia"] == 200]
    report_years = pd.to_datetime(yearly_out.report_date).dt.year
    fuel_2020 = yearly_out[report_years == 2020]["fuel_consumed_mmbtu"]
    fuel_2021 = yearly_out[report_years == 2021]["fuel_consumed_mmbtu"]

    assert (fuel_2020 == annual_2020 / 12).all()
    assert (fuel_2021 == annual_2021 / 12).all()

    monthly_in = bf_with_monthly_annual_mix[
        bf_with_monthly_annual_mix["plant_id_eia"] == 41
    ].sort_values("report_date", ignore_index=True)
    monthly_out = out[out["plant_id_eia"] == 41]
    # the function we are testing spreads annual data into monthly data; the
    # plant that reports monthly should have its data completely untouched.
    pd.testing.assert_frame_equal(monthly_in, monthly_out)


# Test data constants

# Base generators EIA860 data
GENS_EIA860_BASE = pd.read_csv(
    StringIO(
        """report_date,plant_id_eia,generator_id,prime_mover_code,unit_id_pudl,capacity_mw,fuel_type_count,operational_status,generator_retirement_date,generator_operating_date,energy_source_code_1,energy_source_code_2,energy_source_code_3,energy_source_code_4,energy_source_code_5,energy_source_code_6,energy_source_code_7,planned_energy_source_code_1,startup_source_code_1,startup_source_code_2,startup_source_code_3,startup_source_code_4
2019-01-01,8023,1,ST,1,556.0,1,existing,nan,2000-01-01,SUB,BIT,null,null,nan,nan,nan,nan,DFO,nan,nan,nan
2019-01-01,8023,2,ST,2,556.0,1,existing,nan,2000-01-01,SUB,SUB,BIT,nan,nan,nan,nan,DFO,nan,nan,nan
"""
    ),
).pipe(apply_pudl_dtypes, field_namespace="eia")

# Base boiler fuel EIA923 data
BOILER_FUEL_EIA923_BASE = pd.read_csv(
    StringIO(
        """report_date,plant_id_eia,boiler_id,energy_source_code,prime_mover_code,fuel_consumed_mmbtu
2019-01-01,8023,1,DFO,ST,17853.519999999997
2019-01-01,8023,1,RC,ST,27681065.276
2019-01-01,8023,2,DFO,ST,17712.999999999996
2019-01-01,8023,2,RC,ST,29096935.279
"""
    ),
).pipe(apply_pudl_dtypes, field_namespace="eia")

# Base generation EIA923 data
GEN_EIA923_BASE = pd.read_csv(
    StringIO(
        """report_date,plant_id_eia,generator_id,net_generation_mwh
2019-01-01,8023,1,2606737.0
2019-01-01,8023,2,2759826.0
"""
    ),
).pipe(apply_pudl_dtypes, field_namespace="eia")

# Base boiler generator association EIA860 data
BOILER_GENERATOR_ASSN_EIA860_BASE = pd.read_csv(
    StringIO(
        """plant_id_eia,boiler_id,generator_id,report_date
8023,1,1,2019-01-01
8023,2,2,2019-01-01
"""
    ),
).pipe(apply_pudl_dtypes, field_namespace="eia")

# Base generation fuel EIA923 data
GENERATION_FUEL_EIA923_BASE = pd.read_csv(
    StringIO(
        """report_date,plant_id_eia,energy_source_code,prime_mover_code,net_generation_mwh,fuel_consumed_mmbtu,fuel_consumed_for_electricity_mmbtu
2019-01-01,8023,DFO,ST,3369.286,35566.0,35566.0
2019-01-01,8023,RC,ST,5363193.71,56777578.0,56777578.0
2019-01-01,8023,SUB,ST,10000.0, 100000.0,100000.0
"""
    ),
).pipe(apply_pudl_dtypes, field_namespace="eia")

# Generation fuel EIA923 data with extra energy source code
GENERATION_FUEL_EIA923_EXTRA_ESC = pd.read_csv(
    StringIO(
        """report_date,plant_id_eia,energy_source_code,prime_mover_code,net_generation_mwh,fuel_consumed_mmbtu,fuel_consumed_for_electricity_mmbtu
2019-01-01,8023,DFO,ST,3369.286,35566.0,35566.0
2019-01-01,8023,RC,ST,5363193.71,56777578.0,56777578.0
2019-01-01,8023,SUB,ST,10000.0, 100000.0,100000.0
"""
    ),
).pipe(apply_pudl_dtypes, field_namespace="eia")

# Boiler fuel EIA923 data with extra prime mover
BOILER_FUEL_EIA923_EXTRA_PM = BOILER_FUEL_EIA923_BASE.copy()
BOILER_FUEL_EIA923_EXTRA_PM.loc[0, "prime_mover_code"] = "CT"


def _report_periods(df: pd.DataFrame, fmt: str = "%Y-%m") -> list[str]:
    """Sorted list of ``report_date`` strings, for compact test assertions.

    ``pd.to_datetime`` gives type checkers a concretely-dated return type to hang
    the ``.dt`` accessor off of, unlike a bare ``df.report_date`` column access.
    """
    return sorted(pd.to_datetime(df.report_date).dt.strftime(fmt).tolist())


def _with_parsed_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Apply ``convert_dtypes`` and parse any date columns a gen_assoc fixture has.

    Converts ``report_date`` and any ``generator_retirement_date`` /
    ``generator_operating_date`` columns present to datetimes.
    """
    df = df.convert_dtypes()
    date_cols = [
        col
        for col in (
            "report_date",
            "generator_retirement_date",
            "generator_operating_date",
        )
        if col in df.columns
    ]
    return df.assign(
        **{col: (lambda x, col=col: pd.to_datetime(x[col])) for col in date_cols}
    )


def _gen_assoc_df(data: dict) -> pd.DataFrame:
    """Build a ``gen_assoc``-shaped test fixture from column data."""
    return _with_parsed_dates(pd.DataFrame(data))


def _read_gen_assoc(csv_text: str) -> pd.DataFrame:
    """Read a ``gen_assoc`` test fixture from CSV text."""
    return _with_parsed_dates(pd.read_csv(StringIO(csv_text)))


def get_ratio_from_bf_and_allocated_by_boiler(
    bf: pd.DataFrame,
    allocated: pd.DataFrame,
    bga: pd.DataFrame,
    boiler_id_to_check: str,
    energy_source_code_to_check: str,
) -> tuple[float, float]:
    """Helper function to calculate the ratio of a boiler's fuel consumption."""
    # what gen is this boiler associated with? needed for masking in the allocated tbl
    generator_id_to_check = bga.loc[
        (bga.boiler_id == boiler_id_to_check), "generator_id"
    ]

    def sum_of_fuel_consumed_mmbtu_by_esc(
        df: pd.DataFrame, energy_source_code_to_check: str
    ) -> float:
        return df[
            (df.energy_source_code == energy_source_code_to_check)
        ].fuel_consumed_mmbtu.sum()

    ratio_bf = bf[
        (bf.energy_source_code == energy_source_code_to_check)
        & (bf.boiler_id == boiler_id_to_check)
    ].fuel_consumed_mmbtu.sum() / sum_of_fuel_consumed_mmbtu_by_esc(
        bf, energy_source_code_to_check
    )
    ratio_allocated = allocated.loc[
        (allocated.energy_source_code == energy_source_code_to_check)
        & allocated.generator_id.isin(generator_id_to_check)
    ].fuel_consumed_mmbtu.sum() / sum_of_fuel_consumed_mmbtu_by_esc(
        allocated, energy_source_code_to_check
    )
    return ratio_bf, ratio_allocated


# Main assumptions about how allocate_gen_fuel_by_generators should behave
# TODO: if we figure out how to do test data generation, these would be good
#       candidates for property-based testing


@pytest.mark.parametrize(
    "gf,bf",
    [
        (GENERATION_FUEL_EIA923_BASE, BOILER_FUEL_EIA923_BASE),
        (GENERATION_FUEL_EIA923_EXTRA_ESC, BOILER_FUEL_EIA923_BASE),
        (GENERATION_FUEL_EIA923_BASE, BOILER_FUEL_EIA923_EXTRA_PM),
    ],
)
def test_allocate_gen_fuel_sums_match(gf, bf):
    """Test that fuel consumption sums match between input and output."""

    gf_selected, bf_selected, gen, bga, gens = allocate_gen_fuel.select_input_data(
        gf=gf,
        bf=bf,
        gen=GEN_EIA923_BASE,
        bga=BOILER_GENERATOR_ASSN_EIA860_BASE,
        gens=GENS_EIA860_BASE,
    )
    allocated = allocate_gen_fuel.allocate_gen_fuel_by_generator_energy_source(
        gf=gf_selected,
        bf=bf_selected,
        gen=gen,
        bga=bga,
        gens=gens,
        freq="YS",
    )

    assert gf.fuel_consumed_mmbtu.sum() == allocated.fuel_consumed_mmbtu.sum()


@pytest.mark.parametrize(
    "gf",
    [GENERATION_FUEL_EIA923_BASE, GENERATION_FUEL_EIA923_EXTRA_ESC],
)
def test_allocate_gen_fuel_dfo_ratios_match(gf):
    """Test that DFO fuel ratios match between boiler and allocated data."""

    gf_selected, bf, gen, bga, gens = allocate_gen_fuel.select_input_data(
        gf=gf,
        bf=BOILER_FUEL_EIA923_BASE,
        gen=GEN_EIA923_BASE,
        bga=BOILER_GENERATOR_ASSN_EIA860_BASE,
        gens=GENS_EIA860_BASE,
    )
    allocated = allocate_gen_fuel.allocate_gen_fuel_by_generator_energy_source(
        gf=gf_selected, bf=bf, gen=gen, bga=bga, gens=gens, freq="YS"
    )

    assert gf.fuel_consumed_mmbtu.sum() == allocated.fuel_consumed_mmbtu.sum()
    ratio_bf, ratio_allocated = get_ratio_from_bf_and_allocated_by_boiler(
        bf, allocated, bga, boiler_id_to_check="1", energy_source_code_to_check="DFO"
    )
    assert ratio_bf == ratio_allocated


# Implementation and special cases


def test_add_missing_energy_source():
    """Test adding missing energy source codes to generators."""
    gf, bf, _, _, gens = allocate_gen_fuel.select_input_data(
        gf=GENERATION_FUEL_EIA923_EXTRA_ESC,
        bf=BOILER_FUEL_EIA923_BASE,
        gen=GEN_EIA923_BASE,
        bga=BOILER_GENERATOR_ASSN_EIA860_BASE,
        gens=GENS_EIA860_BASE,
    )
    gens = allocate_gen_fuel.add_missing_energy_source_codes_to_gens(gens, gf, bf)
    # assert that the missing energy source code is RC
    assert gens.energy_source_code_8.unique() == "RC"


def test_allocate_bf_data_to_gens_drops_pm_code():
    """Test that non-matching prime mover codes are dropped."""
    _, bf, _, bga, gens = allocate_gen_fuel.select_input_data(
        gf=GENERATION_FUEL_EIA923_BASE,
        bf=BOILER_FUEL_EIA923_EXTRA_PM,
        gen=GEN_EIA923_BASE,
        bga=BOILER_GENERATOR_ASSN_EIA860_BASE,
        gens=GENS_EIA860_BASE,
    )
    bf_by_gens = allocate_gen_fuel.allocate_bf_data_to_gens(bf, gens, bga)
    # allocate_bf_data_to_gens quietly drops and records with non-matching PM codes.
    assert "CT" not in bf_by_gens.prime_mover_code.unique()

    # The CT record is no longer in the output & the total fuel_consumed_mmbtu is
    # missing the CT fuel
    assert bf_by_gens.fuel_consumed_mmbtu.sum() == (
        bf.fuel_consumed_mmbtu.sum()
        - bf[(bf.prime_mover_code == "CT")].fuel_consumed_mmbtu.sum()
    )


def test_allocate_gen_fuel_by_generator_drops_pm_data():
    """Test that prime mover data not in BGA is handled correctly."""
    gf, bf, gen, bga, gens = allocate_gen_fuel.select_input_data(
        gf=GENERATION_FUEL_EIA923_BASE,
        bf=BOILER_FUEL_EIA923_EXTRA_PM,
        gen=GEN_EIA923_BASE,
        bga=BOILER_GENERATOR_ASSN_EIA860_BASE,
        gens=GENS_EIA860_BASE,
    )

    allocated = allocate_gen_fuel.allocate_gen_fuel_by_generator_energy_source(
        gf=gf,
        bf=bf,
        gen=gen,
        bga=bga,
        gens=gens,
        freq="YS",
    )

    # the data associated with the PM code from BF that's not in the BGA is
    # zeroed out, which shows up in the ratios.

    # TODO: what should we do about generators with multiple prime movers?
    #       they're likely typos, since there's only one PRIME mover, but...
    (
        ratio_bf,
        ratio_allocated,
    ) = get_ratio_from_bf_and_allocated_by_boiler(
        bf, allocated, bga, boiler_id_to_check="1", energy_source_code_to_check="DFO"
    )
    assert ratio_bf != ratio_allocated


def test_identify_retiring_generators():
    """Ensure identify_retiring_generators grabs all months from the year a generator is retiring."""
    # i added a few records from the year before and after the retiring year to make sure those are not included in the output
    gena_retiring = _read_gen_assoc(
        """plant_id_eia,generator_id,report_date,operational_status,generator_retirement_date,net_generation_mwh_g_tbl,fuel_consumed_mmbtu_gf_tbl,net_generation_mwh_gf_tbl,gf_unique_to_gen
50937,GENA,2021-12-01,existing,,,0.0,,TRUE
50937,GENA,2022-01-01,retired,2022-09-01,,85.0,,TRUE
50937,GENA,2022-02-01,retired,2022-09-01,,91.0,,TRUE
50937,GENA,2022-03-01,retired,2022-09-01,,278.0,,TRUE
50937,GENA,2022-04-01,retired,2022-09-01,,127.0,,TRUE
50937,GENA,2022-05-01,retired,2022-09-01,,79.0,,TRUE
50937,GENA,2022-06-01,retired,2022-09-01,,85.0,,TRUE
50937,GENA,2022-07-01,retired,2022-09-01,,91.0,,TRUE
50937,GENA,2022-08-01,retired,2022-09-01,,85.0,,TRUE
50937,GENA,2022-09-01,retired,2022-09-01,,48.0,,TRUE
50937,GENA,2022-10-01,retired,2022-09-01,,67.0,,TRUE
50937,GENA,2022-11-01,retired,2022-09-01,,67.0,,TRUE
50937,GENA,2022-12-01,retired,2022-09-01,,,,TRUE
50937,GENA,2023-01-01,retired,2022-09-01,,,,TRUE
"""
    )
    expected_retiring = _read_gen_assoc(
        """plant_id_eia,generator_id,report_date,operational_status,generator_retirement_date,net_generation_mwh_g_tbl,fuel_consumed_mmbtu_gf_tbl,net_generation_mwh_gf_tbl,gf_unique_to_gen
50937,GENA,2022-01-01,retired,2022-09-01,,85.0,,TRUE
50937,GENA,2022-02-01,retired,2022-09-01,,91.0,,TRUE
50937,GENA,2022-03-01,retired,2022-09-01,,278.0,,TRUE
50937,GENA,2022-04-01,retired,2022-09-01,,127.0,,TRUE
50937,GENA,2022-05-01,retired,2022-09-01,,79.0,,TRUE
50937,GENA,2022-06-01,retired,2022-09-01,,85.0,,TRUE
50937,GENA,2022-07-01,retired,2022-09-01,,91.0,,TRUE
50937,GENA,2022-08-01,retired,2022-09-01,,85.0,,TRUE
50937,GENA,2022-09-01,retired,2022-09-01,,48.0,,TRUE
50937,GENA,2022-10-01,retired,2022-09-01,,67.0,,TRUE
50937,GENA,2022-11-01,retired,2022-09-01,,67.0,,TRUE
50937,GENA,2022-12-01,retired,2022-09-01,,,,TRUE
"""
    )

    out = allocate_gen_fuel.identify_retiring_generators(gena_retiring)
    pd.testing.assert_frame_equal(expected_retiring, out, check_exact=False)


def _make_tiny_plant_example(
    report_date,
    retirement_date,
    existing_pm,
    retiring_pm,
    retiring_net_generation_mwh_g_tbl="",
    retiring_net_generation_mwh_gf_tbl="",
):
    """Make a tiny two generator plant with a retiring and existing generator."""
    tiny_plant = _read_gen_assoc(
        f"""plant_id_eia,generator_id,report_date,operational_status,prime_mover_code,energy_source_code,generator_retirement_date,net_generation_mwh_g_tbl,fuel_consumed_mmbtu_gf_tbl,net_generation_mwh_gf_tbl
1,A,{report_date},existing,NG,{existing_pm},,,,85
1,B,{report_date},retired,NG,{retiring_pm},{retirement_date},{retiring_net_generation_mwh_g_tbl},,{retiring_net_generation_mwh_gf_tbl}
"""
    )
    # add the uniqueness label
    out_labeled = allocate_gen_fuel._label_gf_unique_to_gen(tiny_plant)
    pd.testing.assert_frame_equal(
        out_labeled,
        tiny_plant.assign(gf_unique_to_gen=existing_pm != retiring_pm),
    )
    return out_labeled


def test_identify_retiring_generators_mixed_pm_esc_some_gen():
    plant1_mixed_some_gen = _make_tiny_plant_example(
        report_date="2022-10-01",
        existing_pm="GT",
        retiring_pm="IC",
        retirement_date="2022-09-01",
        retiring_net_generation_mwh_g_tbl=1,
    )
    # condition B: since its unique and there is some generation, the retiring gen
    # should be ID-ed as retiring
    assert allocate_gen_fuel.identify_retiring_generators(
        plant1_mixed_some_gen
    ).generator_id.to_numpy() == ["B"]

    # Let's try that again but with gen being reported in the less granular gf table
    plant1_mixed_some_gen = _make_tiny_plant_example(
        report_date="2022-10-01",
        existing_pm="GT",
        retiring_pm="IC",
        retirement_date="2022-09-01",
        retiring_net_generation_mwh_gf_tbl=1,
    )
    # condition C: since its unique and there is some generation - even from the less
    # granular gf table, the retiring gen should be ID-ed as retiring
    assert allocate_gen_fuel.identify_retiring_generators(
        plant1_mixed_some_gen
    ).generator_id.to_numpy() == ["B"]


def test_identify_retiring_generators_mixed_pm_esc_no_gen():
    plant1_mixed_no_gen = _make_tiny_plant_example(
        report_date="2022-10-01",
        existing_pm="GT",
        retiring_pm="IC",
        retirement_date="2022-09-01",
        retiring_net_generation_mwh_g_tbl="",
    )
    # neither condition B nor C is satisfied (no reported generation at all), so
    # there is nothing to allocate and this will not be ID-ed as retiring
    assert allocate_gen_fuel.identify_retiring_generators(plant1_mixed_no_gen).empty


def test_identify_retiring_generators_same_pm_esc():
    plant1_same = _make_tiny_plant_example(
        report_date="2022-10-01",
        existing_pm="GT",
        retiring_pm="GT",
        retirement_date="2022-09-01",
        retiring_net_generation_mwh_gf_tbl=1,
    )
    # condition C does not apply when a retiring pm/esc combo is the same as another:
    # even with gf-table generation reported, it will **not** be flagged as retiring,
    # bc all the generation from that pm/esc combo should be allocated to its
    # non-retiring brethren
    assert allocate_gen_fuel.identify_retiring_generators(plant1_same).empty

    # we can extra try same situation with no generation - still should not show up
    plant1_same_nada = _make_tiny_plant_example(
        report_date="2022-10-01",
        existing_pm="GT",
        retiring_pm="GT",
        retirement_date="2022-09-01",
    )
    assert allocate_gen_fuel.identify_retiring_generators(plant1_same_nada).empty

    plant1_same_g = _make_tiny_plant_example(
        report_date="2022-10-01",
        existing_pm="GT",
        retiring_pm="GT",
        retirement_date="2022-09-01",
        retiring_net_generation_mwh_g_tbl=1,
    )
    # condition B still applies even when a retiring pm/esc combo is the same as
    # another: g-table generation is generator-specific, so it will be flagged as
    # retiring regardless. that gen should go to that gen then... even if retiring.
    assert allocate_gen_fuel.identify_retiring_generators(
        plant1_same_g
    ).generator_id.to_numpy() == ["B"]


def test_identify_retiring_generators_non_monotonic_status():
    """A generator that goes ``retired -> existing -> retired`` again should be
    flagged as retiring in both of its retired stretches, independently.

    ``identify_retiring_generators`` already scopes its checks to ``report_year``
    (fixed in #3690, before the sibling ``identify_proposed_plants`` multiyear bug was
    found), so this isn't expected to fail -- but it's worth locking in explicitly,
    since real EIA-860M data shows generators cycling through operational statuses
    non-monotonically (e.g. plant 314 in the published data goes from "retired" in
    2009 back to reporting "existing" generators in later years).
    """
    gen_assoc = _read_gen_assoc(
        """plant_id_eia,generator_id,report_date,operational_status,generator_retirement_date,net_generation_mwh_g_tbl,fuel_consumed_mmbtu_gf_tbl,net_generation_mwh_gf_tbl,gf_unique_to_gen
50937,GENA,2021-12-01,existing,,,,,TRUE
50937,GENA,2022-01-01,retired,2021-12-01,,85.0,,TRUE
50937,GENA,2022-02-01,retired,2021-12-01,,91.0,,TRUE
50937,GENA,2022-12-01,retired,2021-12-01,,60.0,,TRUE
50937,GENA,2023-01-01,existing,,,,,TRUE
50937,GENA,2023-02-01,existing,,,,,TRUE
50937,GENA,2024-01-01,retired,2023-12-01,,70.0,,TRUE
50937,GENA,2024-02-01,retired,2023-12-01,,75.0,,TRUE
"""
    )

    out = allocate_gen_fuel.identify_retiring_generators(gen_assoc)

    # both the 2022 and 2024 retiring stretches should be kept, but not the 2021 or
    # 2023 "existing" months in between.
    assert _report_periods(out) == [
        "2022-01",
        "2022-02",
        "2022-12",
        "2024-01",
        "2024-02",
    ]
    assert (out.operational_status == "retired").all()


def test_identify_newly_operating_generators_mid_year_operating_date():
    """Condition A: a generator whose confirmed operating date has already passed
    this year should be kept for the whole report_year, even with no reported data
    at all.

    This is the new capability enabled by plumbing ``generator_operating_date``
    into this module -- previously ``identify_newly_operating_generators`` relied
    purely on data presence (g-table or unique gf-table reporting), so a generator
    that's genuinely already operating but hasn't shown up in either data table yet
    would have been invisible to it.
    """
    gen_assoc = _read_gen_assoc(
        """plant_id_eia,generator_id,report_date,operational_status,generator_operating_date,gf_unique_to_gen,net_generation_mwh_g_tbl,net_generation_mwh_gf_tbl
12345,GEN1,2023-01-01,proposed,2023-06-01,False,,
12345,GEN1,2023-06-01,proposed,2023-06-01,False,,
12345,GEN1,2023-12-01,proposed,2023-06-01,False,,
"""
    )

    out = allocate_gen_fuel.identify_newly_operating_generators(gen_assoc)

    # the whole report_year is kept, including December, despite no data ever
    # having been reported for this generator.
    assert _report_periods(out) == [
        "2023-01",
        "2023-06",
        "2023-12",
    ]


def test_identify_newly_operating_generators_g_tbl_or_gf_unique_to_gen():
    """A proposed generator reporting generator-specific g-table data should be
    kept, mirroring ``identify_retiring_generators``'s condition B. So should one
    with non-zero gf-table generation for a PM/ESC combo unique to it, mirroring
    condition C.
    """
    g_tbl_data = _read_gen_assoc(
        """plant_id_eia,generator_id,report_date,operational_status,generator_operating_date,gf_unique_to_gen,net_generation_mwh_g_tbl,net_generation_mwh_gf_tbl
23456,GEN1,2023-03-01,proposed,,False,15,
"""
    )
    assert len(allocate_gen_fuel.identify_newly_operating_generators(g_tbl_data)) == 1

    gf_unique_to_gen = _read_gen_assoc(
        """plant_id_eia,generator_id,report_date,operational_status,generator_operating_date,gf_unique_to_gen,net_generation_mwh_g_tbl,net_generation_mwh_gf_tbl
23456,GEN1,2023-03-01,proposed,,True,,25
"""
    )
    assert (
        len(allocate_gen_fuel.identify_newly_operating_generators(gf_unique_to_gen))
        == 1
    )


def test_identify_newly_operating_generators_sweeps_whole_generator_year():
    """A generator reporting real data in only one month of a report_year should
    have every month of that report_year kept, matching
    ``identify_retiring_generators``'s "seed then sweep the whole generator-year"
    behavior. Previously ``identify_newly_operating_generators`` was a flat row-by-row
    filter with no such sweep-in, so a generator's other months could be dropped.
    """
    gen_assoc = _read_gen_assoc(
        """plant_id_eia,generator_id,report_date,operational_status,generator_operating_date,gf_unique_to_gen,net_generation_mwh_g_tbl,net_generation_mwh_gf_tbl
34567,GEN1,2023-01-01,proposed,,False,,
34567,GEN1,2023-06-01,proposed,,False,45,
34567,GEN1,2023-12-01,proposed,,False,,
"""
    )

    out = allocate_gen_fuel.identify_newly_operating_generators(gen_assoc)

    assert _report_periods(out) == [
        "2023-01",
        "2023-06",
        "2023-12",
    ]


PLANT_LEVEL_CASES = pytest.mark.parametrize(
    "identify_fn,status,transition_date_col,transition_date",
    [
        pytest.param(
            allocate_gen_fuel.identify_proposed_plants,
            "proposed",
            "generator_operating_date",
            "2030-01-01",
            id="proposed",
        ),
        pytest.param(
            allocate_gen_fuel.identify_retired_plants,
            "retired",
            "generator_retirement_date",
            "2020-01-01",
            id="retired",
        ),
    ],
)
"""Shared parametrization for the ``identify_proposed_plants`` /
``identify_retired_plants`` mirror-image test cases below.

``transition_date`` is chosen far enough from the 2023-2024 report_dates used in
these tests to safely satisfy each direction's "anomalous report" condition
(``report_date < generator_operating_date`` for proposed, ``report_date >
generator_retirement_date`` for retired) without falling within any of the
report_years under test.
"""


@PLANT_LEVEL_CASES
def test_identify_plants_excludes_phantom_null_months(
    identify_fn, status, transition_date_col, transition_date
):
    """Within an otherwise-flagged plant-year, a month where nothing was reported
    at all should be excluded from the output. ``identify_proposed_plants`` and
    ``identify_retired_plants`` should behave identically here.
    """
    gen_assoc = _gen_assoc_df(
        {
            "plant_id_eia": [1, 1],
            "generator_id": ["GEN1", "GEN1"],
            "report_date": ["2023-01-01", "2023-02-01"],
            "operational_status": [status, status],
            transition_date_col: [transition_date, transition_date],
            "net_generation_mwh_g_tbl": [pd.NA, pd.NA],
            "net_generation_mwh_gf_tbl": [150, pd.NA],
        }
    )

    out = identify_fn(gen_assoc)

    assert _report_periods(out) == ["2023-01"]


@pytest.mark.parametrize(
    "identify_fn,csv_text,expected_periods,expected_status",
    [
        pytest.param(
            allocate_gen_fuel.identify_proposed_plants,
            """plant_id_eia,generator_id,report_date,operational_status,net_generation_mwh_gf_tbl,net_generation_mwh_g_tbl,generator_operating_date
45678,GEN1,2023-01-01,proposed,100,,2025-01-01
45678,GEN1,2023-02-01,proposed,110,,2025-01-01
45678,GEN1,2024-01-01,proposed,120,,2025-01-01
45678,GEN1,2024-02-01,proposed,130,,2025-01-01
45678,GEN1,2025-01-01,existing,140,,2025-01-01
45678,GEN1,2025-02-01,existing,150,,2025-01-01
""",
            ["2023-01", "2023-02", "2024-01", "2024-02"],
            "proposed",
            id="proposed",
        ),
        pytest.param(
            allocate_gen_fuel.identify_retired_plants,
            # real EIA-860M data: plant 314 is entirely "retired" in 2009 but has
            # "existing" generators in many later years (2010-2026).
            """plant_id_eia,generator_id,report_date,operational_status,generator_retirement_date,net_generation_mwh_g_tbl,net_generation_mwh_gf_tbl
314,OLD1,2009-01-01,retired,2008-01-01,,50
314,OLD1,2009-02-01,retired,2008-01-01,,60
314,NEW1,2014-01-01,existing,,,70
314,NEW1,2014-02-01,existing,,,80
""",
            ["2009-01", "2009-02"],
            "retired",
            id="retired",
        ),
    ],
)
def test_identify_plants_multiyear_status_change(
    identify_fn, csv_text, expected_periods, expected_status
):
    """A plant that transitions status across the years spanned by a multi-year
    ``gen_assoc`` should keep its genuinely-``expected_status`` years.

    Regression test for the bug described in :issue:`5440` and :pr:`5419`
    (``identify_proposed_plants``) and its sibling bug in
    ``identify_retired_plants``, found while reviewing the fix: both functions
    checked whether a plant's operational_status was uniformly one status across
    the *entire* input frame, so a plant with mixed statuses across its history
    would never pass the check for *any* of its years, silently dropping
    legitimate generation/fuel data for years it genuinely was
    ``expected_status``-but-reporting.
    """
    gen_assoc = _read_gen_assoc(csv_text)

    out = identify_fn(gen_assoc)

    assert _report_periods(out) == expected_periods
    # none of the later "existing" months, which aren't this function's concern,
    # should be kept
    assert (out.operational_status == expected_status).all()


@PLANT_LEVEL_CASES
def test_identify_plants_mixed_status_same_year(
    identify_fn, status, transition_date_col, transition_date
):
    """A plant with both ``status`` and "existing" generators in the *same* year
    should be excluded, since the gf-reported generation can't be reliably
    attributed to just one of them. Confirms the multi-year fix doesn't regress
    this within-year behavior, for either direction.
    """
    gen_assoc = _gen_assoc_df(
        {
            "plant_id_eia": [1, 1],
            "generator_id": ["GEN1", "GEN2"],
            "report_date": ["2024-01-01", "2024-01-01"],
            "operational_status": [status, "existing"],
            transition_date_col: [transition_date, pd.NA],
            "net_generation_mwh_g_tbl": [pd.NA, pd.NA],
            "net_generation_mwh_gf_tbl": [50, 60],
        }
    )

    assert identify_fn(gen_assoc).empty


@pytest.mark.parametrize(
    "identify_fn,csv_text,expected_periods,expected_status",
    [
        pytest.param(
            allocate_gen_fuel.identify_proposed_plants,
            """plant_id_eia,generator_id,report_date,operational_status,net_generation_mwh_gf_tbl,net_generation_mwh_g_tbl,generator_operating_date
56401,GEN2,2005-01-01,proposed,10,,2020-01-01
56401,GEN2,2006-01-01,proposed,20,,2020-01-01
56401,GEN2,2007-01-01,proposed,30,,2020-01-01
56401,GEN2,2008-01-01,existing,40,,2020-01-01
56401,GEN2,2009-01-01,existing,50,,2020-01-01
56401,GEN2,2010-01-01,proposed,60,,2020-01-01
56401,GEN2,2011-01-01,proposed,70,,2020-01-01
""",
            ["2005", "2006", "2007", "2010", "2011"],
            "proposed",
            id="proposed",
        ),
        pytest.param(
            allocate_gen_fuel.identify_retired_plants,
            """plant_id_eia,generator_id,report_date,operational_status,generator_retirement_date,net_generation_mwh_g_tbl,net_generation_mwh_gf_tbl
56789,GEN1,2015-01-01,retired,2010-01-01,,10
56789,GEN1,2016-01-01,retired,2010-01-01,,20
56789,GEN1,2018-01-01,existing,,,30
56789,GEN1,2019-01-01,existing,,,40
56789,GEN1,2021-01-01,retired,2020-06-01,,50
56789,GEN1,2022-01-01,retired,2020-06-01,,60
""",
            ["2015", "2016", "2021", "2022"],
            "retired",
            id="retired",
        ),
    ],
)
def test_identify_plants_non_monotonic_status(
    identify_fn, csv_text, expected_periods, expected_status
):
    """A generator that flips status and back (e.g. ``proposed -> existing ->
    proposed``) should keep both genuinely-``expected_status`` stretches,
    independently, regardless of order.

    This isn't hypothetical: real EIA-860M data for plant 56401/generator GEN2
    shows exactly this pattern (proposed 2005-2007, existing 2008-2009, proposed
    again 2010-2016) -- a planned unit apparently came online, then reverted to
    "proposed" in later reporting. The per-year scoping added by the
    multiyear-status fix should handle this correctly regardless of how many
    times, or in which direction, the status flips.
    """
    gen_assoc = _read_gen_assoc(csv_text)

    out = identify_fn(gen_assoc)

    assert _report_periods(out, "%Y") == expected_periods
    assert (out.operational_status == expected_status).all()


@PLANT_LEVEL_CASES
def test_identify_plants_all_null_or_zero_generation(
    identify_fn, status, transition_date_col, transition_date
):
    """A plant-year that is entirely ``status`` but reports no non-zero gf
    generation should not be picked up, since there's nothing to allocate.
    Confirms the per-year "notnull and nonzero" gate still applies even though
    it's scoped to report_year rather than the whole input frame, for either
    direction.
    """
    gen_assoc = _gen_assoc_df(
        {
            "plant_id_eia": [1, 1, 1, 1],
            "generator_id": ["GEN1"] * 4,
            "report_date": ["2023-01-01", "2023-02-01", "2024-01-01", "2024-02-01"],
            "operational_status": [status] * 4,
            transition_date_col: [transition_date] * 4,
            "net_generation_mwh_g_tbl": [pd.NA] * 4,
            "net_generation_mwh_gf_tbl": [pd.NA, 0, 100, 110],
        }
    )

    out = identify_fn(gen_assoc)

    # 2023 has no non-null/non-zero gf generation for any month, so the plant-year
    # never qualifies as an "entirely `status` plant with reported data" and is
    # dropped entirely; 2024 does qualify and is kept in full.
    assert _report_periods(out) == ["2024-01", "2024-02"]


@PLANT_LEVEL_CASES
def test_identify_plants_unknown_transition_date(
    identify_fn, status, transition_date_col, transition_date
):
    """A plant-year that is entirely ``status`` with an *unknown* transition date
    should still be caught if it reports real, unambiguous gf-table generation.

    Regression test: an unknown transition date can never disprove the "anomalous
    report" condition, so it must not be *required* for a candidate to be flagged.
    This isn't hypothetical: real EIA-860M data for plant 63622 (generators OES01
    and OES02, both permanently "proposed" with no ``generator_operating_date`` on
    record at all, since they haven't started operating) hits exactly this case.
    An earlier version of this shared-helper refactor required a *known*
    transition date to seed a candidate plant-year, which silently dropped
    plant 63622's real reported generation -- caught by comparing against the
    nightly build after this refactor's ETL run.
    """
    gen_assoc = _gen_assoc_df(
        {
            "plant_id_eia": [1, 1],
            "generator_id": ["GEN1", "GEN1"],
            "report_date": ["2022-01-01", "2022-02-01"],
            "operational_status": [status, status],
            transition_date_col: [pd.NA, pd.NA],
            "net_generation_mwh_g_tbl": [pd.NA, pd.NA],
            "net_generation_mwh_gf_tbl": [0.1875, 0.166],
        }
    )

    out = identify_fn(gen_assoc)

    assert _report_periods(out) == ["2022-01", "2022-02"]


def test_remove_inactive_generators_composability_independent_transitions():
    """End-to-end check that ``identify_proposed_plants`` and
    ``identify_newly_operating_generators`` compose correctly within
    ``remove_inactive_generators`` when multiple generators (at different plants)
    transition from proposed to existing independently, across multiple years.

    Plant 67890 is an entirely new plant: both of its generators are proposed
    together in 2023 and become existing together in 2024. This is the
    plant-level, multi-year transition that ``identify_proposed_plants`` exists to
    protect (its 2023 data must survive despite the plant's later 2024 "existing"
    status).

    Plant 78901 is an already-existing plant (GEN2 has been "existing" the whole
    time) that adds a *single* new generator (GEN1) in 2023, which then becomes
    existing itself in 2024. Because GEN2 is "existing" in the same years GEN1 is
    "proposed", plant 78901 never qualifies as "entirely proposed" in any year, so
    ``identify_proposed_plants`` correctly ignores it — GEN1's 2023 data is instead
    the responsibility of ``identify_newly_operating_generators``, which keeps it
    because GEN1 reports generator-specific data in the g table.

    Together, no legitimate data should be lost for either plant.
    """
    gen_assoc = _read_gen_assoc(
        """plant_id_eia,generator_id,report_date,operational_status,prime_mover_code,energy_source_code,generator_retirement_date,generator_operating_date,net_generation_mwh_g_tbl,net_generation_mwh_gf_tbl,fuel_consumed_mmbtu_gf_tbl
67890,GEN1,2023-01-01,proposed,ST,NG,,2024-01-01,,100,
67890,GEN2,2023-01-01,proposed,ST,NG,,2024-01-01,,100,
67890,GEN1,2023-02-01,proposed,ST,NG,,2024-01-01,,110,
67890,GEN2,2023-02-01,proposed,ST,NG,,2024-01-01,,110,
67890,GEN1,2024-01-01,existing,ST,NG,,2024-01-01,,120,
67890,GEN2,2024-01-01,existing,ST,NG,,2024-01-01,,120,
67890,GEN1,2024-02-01,existing,ST,NG,,2024-01-01,,130,
67890,GEN2,2024-02-01,existing,ST,NG,,2024-01-01,,130,
78901,GEN2,2023-01-01,existing,GT,NG,,,,200,
78901,GEN2,2023-02-01,existing,GT,NG,,,,210,
78901,GEN2,2024-01-01,existing,GT,NG,,,,220,
78901,GEN2,2024-02-01,existing,GT,NG,,,,230,
78901,GEN1,2023-01-01,proposed,CT,DFO,,,50,,
78901,GEN1,2023-02-01,proposed,CT,DFO,,,60,,
78901,GEN1,2024-01-01,existing,CT,DFO,,,70,,
78901,GEN1,2024-02-01,existing,CT,DFO,,,80,,
"""
    )

    out = allocate_gen_fuel.remove_inactive_generators(gen_assoc)

    # nothing should be lost: every input row has a legitimate reason to be kept.
    assert len(out) == len(gen_assoc)

    # plant 67890's entirely-proposed 2023 months survive despite becoming an
    # entirely-existing plant in 2024 (the identify_proposed_plants fix).
    plant_67890_2023 = out[
        (out.plant_id_eia == 67890) & (pd.to_datetime(out.report_date).dt.year == 2023)
    ]
    assert len(plant_67890_2023) == 4
    assert (plant_67890_2023.operational_status == "proposed").all()

    # plant 78901's GEN1 is proposed alongside an already-existing GEN2, so it's
    # picked up by identify_newly_operating_generators rather than
    # identify_proposed_plants, in both the years it's proposed and once it
    # becomes existing.
    plant_78901_gen1 = out[(out.plant_id_eia == 78901) & (out.generator_id == "GEN1")]
    assert len(plant_78901_gen1) == 4


@pytest.mark.parametrize(
    "identify_fn,status,transition_date_col,transition_date,report_dates",
    [
        pytest.param(
            allocate_gen_fuel.identify_proposed_plants,
            "proposed",
            "generator_operating_date",
            "2022-06-01",
            ["2022-01-01", "2022-02-01"],
            id="proposed",
        ),
        pytest.param(
            allocate_gen_fuel.identify_retired_plants,
            "retired",
            "generator_retirement_date",
            "2022-09-01",
            ["2022-10-01", "2022-11-01"],
            id="retired",
        ),
    ],
)
def test_identify_plants_excludes_mid_year_transition(
    identify_fn, status, transition_date_col, transition_date, report_dates
):
    """A plant transitioning status *during* the report_year (rather than having
    already transitioned before it began) should be excluded from
    ``identify_proposed_plants``/``identify_retired_plants`` -- that's
    ``identify_newly_operating_generators``/``identify_retiring_generators``'s
    responsibility instead, and double-counting would inflate the plant-level data
    with months that are already handled elsewhere.
    """
    gen_assoc = _gen_assoc_df(
        {
            "plant_id_eia": [1] * len(report_dates),
            "generator_id": ["GEN1"] * len(report_dates),
            "report_date": report_dates,
            "operational_status": [status] * len(report_dates),
            transition_date_col: [transition_date] * len(report_dates),
            "net_generation_mwh_g_tbl": [pd.NA] * len(report_dates),
            "net_generation_mwh_gf_tbl": [85, 90],
        }
    )

    assert identify_fn(gen_assoc).empty
