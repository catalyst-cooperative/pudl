"""Unit tests for allocation of net generation.

Note: ``identify_retiring_generators`` and ``identify_newly_operating_generators`` (via
the shared ``_identify_transitioning_generators`` function) flag a generator as
transitioning mid-year if ANY of three conditions hold. Tests below that exercise one of
these are labeled "condition A/B/C" by this scheme:

* Condition A: `report_date` already reflects the generator's actual transition
  date (`generator_retirement_date`/`generator_operating_date`) this report_year.
* Condition B: it reports generator-specific data in the more granular `g` table.
* Condition C: it has non-zero data in the less granular `gf` table for a PM/ESC
  combo that is unique to it at its plant.

See the :func:`pudl.analysis.allocate_gen_fuel._identify_transitioning_generators`
docstring for a complete explanation.
"""

from io import StringIO

import pandas as pd
import pytest

from pudl.analysis import allocate_gen_fuel
from pudl.metadata.dtypes import apply_pudl_dtypes

# ================================================================================
# Reusable input files...
# ================================================================================

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


# Main assumptions about how allocate_gen_fuel_by_generator_energy_source should behave
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
    flagged as retiring in both retired stretches, independently.

    ``identify_retiring_generators`` already scopes checks to ``report_year`` (fixed in
    #3690, before the sibling ``identify_proposed_groups`` bug was found), so this isn't
    expected to fail. Real EIA-860M data shows generators cycling non-monotonically
    (e.g. plant 314 goes "retired" in 2009, back to "existing" in later years).
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
    kept (mirroring condition B), as should one with non-zero gf-table generation
    for a PM/ESC combo unique to it (condition C).
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
    have every month of that year kept, matching ``identify_retiring_generators``'s
    "seed then sweep" behavior.
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
            allocate_gen_fuel.identify_proposed_groups,
            "proposed",
            "generator_operating_date",
            "2030-01-01",
            id="proposed",
        ),
        pytest.param(
            allocate_gen_fuel.identify_retired_groups,
            "retired",
            "generator_retirement_date",
            "2020-01-01",
            id="retired",
        ),
    ],
)
"""Shared parametrization for the mirror-image ``identify_proposed_groups`` /
``identify_retired_groups`` test cases below.

``transition_date`` is chosen far enough from the 2023-2024 report_dates to satisfy each
direction's "anomalous report" condition (``report_date < generator_operating_date`` for
proposed, ``report_date > generator_retirement_date`` for retired) without falling
within any tested report_year.
"""


@PLANT_LEVEL_CASES
def test_identify_plants_excludes_phantom_null_months(
    identify_fn, status, transition_date_col, transition_date
):
    """Within an otherwise-flagged plant-year, a month with nothing reported at
    all should be excluded. Both ``identify_proposed_groups`` and
    ``identify_retired_groups`` should behave identically here.
    """
    gen_assoc = _gen_assoc_df(
        {
            "plant_id_eia": [1, 1],
            "generator_id": ["GEN1", "GEN1"],
            "report_date": ["2023-01-01", "2023-02-01"],
            "operational_status": [status, status],
            "prime_mover_code": ["ST", "ST"],
            "energy_source_code": ["NG", "NG"],
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
            allocate_gen_fuel.identify_proposed_groups,
            """plant_id_eia,generator_id,report_date,operational_status,prime_mover_code,energy_source_code,net_generation_mwh_gf_tbl,net_generation_mwh_g_tbl,generator_operating_date
45678,GEN1,2023-01-01,proposed,ST,NG,100,,2025-01-01
45678,GEN1,2023-02-01,proposed,ST,NG,110,,2025-01-01
45678,GEN1,2024-01-01,proposed,ST,NG,120,,2025-01-01
45678,GEN1,2024-02-01,proposed,ST,NG,130,,2025-01-01
45678,GEN1,2025-01-01,existing,ST,NG,140,,2025-01-01
45678,GEN1,2025-02-01,existing,ST,NG,150,,2025-01-01
""",
            ["2023-01", "2023-02", "2024-01", "2024-02"],
            "proposed",
            id="proposed",
        ),
        pytest.param(
            allocate_gen_fuel.identify_retired_groups,
            # real EIA-860M data: plant 314 is entirely "retired" in 2009 but has
            # "existing" generators in many later years (2010-2026).
            """plant_id_eia,generator_id,report_date,operational_status,prime_mover_code,energy_source_code,generator_retirement_date,net_generation_mwh_g_tbl,net_generation_mwh_gf_tbl
314,OLD1,2009-01-01,retired,ST,NG,2008-01-01,,50
314,OLD1,2009-02-01,retired,ST,NG,2008-01-01,,60
314,NEW1,2014-01-01,existing,ST,NG,,,70
314,NEW1,2014-02-01,existing,ST,NG,,,80
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
    """A plant whose status changes across years should keep only the years matching
    ``expected_status``.

    Regression test for :issue:`5440` / :pr:`5419` (``identify_proposed_groups``) and
    its sibling bug in ``identify_retired_groups``: both checked for a uniform status
    across the *entire* input frame, so a plant with mixed status across its history
    never passed the check for any year, silently dropping legitimate data.
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
    """A PM/ESC group with both ``status`` and "existing" generators in the same
    year is excluded, since the gf-reported generation can't be attributed to just
    one of them. GEN1 and GEN2 deliberately share one PM/ESC group, to exercise
    this mixed-status exclusion rather than the (group-scoped) cross-group check.
    Confirms the multi-year fix doesn't regress this within-year behavior.
    """
    gen_assoc = _gen_assoc_df(
        {
            "plant_id_eia": [1, 1],
            "generator_id": ["GEN1", "GEN2"],
            "report_date": ["2024-01-01", "2024-01-01"],
            "operational_status": [status, "existing"],
            "prime_mover_code": ["ST", "ST"],
            "energy_source_code": ["NG", "NG"],
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
            allocate_gen_fuel.identify_proposed_groups,
            """plant_id_eia,generator_id,report_date,operational_status,prime_mover_code,energy_source_code,net_generation_mwh_gf_tbl,net_generation_mwh_g_tbl,generator_operating_date
56401,GEN2,2005-01-01,proposed,ST,NG,10,,2020-01-01
56401,GEN2,2006-01-01,proposed,ST,NG,20,,2020-01-01
56401,GEN2,2007-01-01,proposed,ST,NG,30,,2020-01-01
56401,GEN2,2008-01-01,existing,ST,NG,40,,2020-01-01
56401,GEN2,2009-01-01,existing,ST,NG,50,,2020-01-01
56401,GEN2,2010-01-01,proposed,ST,NG,60,,2020-01-01
56401,GEN2,2011-01-01,proposed,ST,NG,70,,2020-01-01
""",
            ["2005", "2006", "2007", "2010", "2011"],
            "proposed",
            id="proposed",
        ),
        pytest.param(
            allocate_gen_fuel.identify_retired_groups,
            """plant_id_eia,generator_id,report_date,operational_status,prime_mover_code,energy_source_code,generator_retirement_date,net_generation_mwh_g_tbl,net_generation_mwh_gf_tbl
56789,GEN1,2015-01-01,retired,ST,NG,2010-01-01,,10
56789,GEN1,2016-01-01,retired,ST,NG,2010-01-01,,20
56789,GEN1,2018-01-01,existing,ST,NG,,,30
56789,GEN1,2019-01-01,existing,ST,NG,,,40
56789,GEN1,2021-01-01,retired,ST,NG,2020-06-01,,50
56789,GEN1,2022-01-01,retired,ST,NG,2020-06-01,,60
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
    proposed``) should keep both ``expected_status`` stretches independently.

    Real EIA-860M data for plant 56401/GEN2 exhibits this pattern (proposed 2005-2007,
    existing 2008-2009, proposed again 2010-2016).
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
    generation isn't picked up, since there's nothing to allocate. Confirms the
    "notnull and nonzero" condition still applies per-year in either direction.
    """
    gen_assoc = _gen_assoc_df(
        {
            "plant_id_eia": [1, 1, 1, 1],
            "generator_id": ["GEN1"] * 4,
            "report_date": ["2023-01-01", "2023-02-01", "2024-01-01", "2024-02-01"],
            "operational_status": [status] * 4,
            "prime_mover_code": ["ST"] * 4,
            "energy_source_code": ["NG"] * 4,
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
    """A plant-year entirely ``status`` with an *unknown* transition date should
    still be caught if it reports real, unambiguous gf-table generation -- an
    unknown date can never disprove the "anomalous report" condition, so it must
    not be required to flag a candidate.

    EIA-860M plant 63622 (generators OES01/OES02, permanently "proposed" with no
    ``generator_operating_date`` on record) exercises this.
    """
    gen_assoc = _gen_assoc_df(
        {
            "plant_id_eia": [1, 1],
            "generator_id": ["GEN1", "GEN1"],
            "report_date": ["2022-01-01", "2022-02-01"],
            "operational_status": [status, status],
            "prime_mover_code": ["ST", "ST"],
            "energy_source_code": ["NG", "NG"],
            transition_date_col: [pd.NA, pd.NA],
            "net_generation_mwh_g_tbl": [pd.NA, pd.NA],
            "net_generation_mwh_gf_tbl": [0.1875, 0.166],
        }
    )

    out = identify_fn(gen_assoc)

    assert _report_periods(out) == ["2022-01", "2022-02"]


def test_remove_inactive_generators_composability_independent_transitions():
    """End-to-end check that ``identify_proposed_groups`` and
    ``identify_newly_operating_generators`` compose correctly within
    ``remove_inactive_generators``, across two independent multi-year transitions.

    Plant 67890 is an entirely new plant: both generators are proposed together
    in 2023 and become existing together in 2024 -- the plant-level transition
    ``identify_proposed_groups`` exists to protect.

    Plant 78901 already has an existing generator (GEN2) and adds a new one
    (GEN1) in 2023. Since GEN2 is "existing" throughout, plant 78901 never
    qualifies as "entirely proposed", so ``identify_proposed_groups`` ignores it;
    ``identify_newly_operating_generators`` picks up GEN1 instead, via its
    generator-specific g-table data.

    No legitimate data should be lost for either plant.
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
    # entirely-existing plant in 2024 (the identify_proposed_groups fix).
    plant_67890_2023 = out[
        (out.plant_id_eia == 67890) & (pd.to_datetime(out.report_date).dt.year == 2023)
    ]
    assert len(plant_67890_2023) == 4
    assert (plant_67890_2023.operational_status == "proposed").all()

    # plant 78901's GEN1 is proposed alongside an already-existing GEN2, so it's
    # picked up by identify_newly_operating_generators rather than
    # identify_proposed_groups, in both the years it's proposed and once it
    # becomes existing.
    plant_78901_gen1 = out[(out.plant_id_eia == 78901) & (out.generator_id == "GEN1")]
    assert len(plant_78901_gen1) == 4


@pytest.mark.parametrize(
    "identify_fn,status,transition_date_col,transition_date,report_dates",
    [
        pytest.param(
            allocate_gen_fuel.identify_proposed_groups,
            "proposed",
            "generator_operating_date",
            "2022-06-01",
            ["2022-01-01", "2022-02-01"],
            id="proposed",
        ),
        pytest.param(
            allocate_gen_fuel.identify_retired_groups,
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
    ``identify_proposed_groups``/``identify_retired_groups`` -- that's
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
            "prime_mover_code": ["ST"] * len(report_dates),
            "energy_source_code": ["NG"] * len(report_dates),
            transition_date_col: [transition_date] * len(report_dates),
            "net_generation_mwh_g_tbl": [pd.NA] * len(report_dates),
            "net_generation_mwh_gf_tbl": [85, 90],
        }
    )

    assert identify_fn(gen_assoc).empty


# =====================================================================================
# Coverage-matrix tests for remove_inactive_generators()
# =====================================================================================
#
# "Rescue" means being kept in remove_inactive_generators's output despite an
# operational_status of "proposed"/"retired", because there's reported gf-table data
# that needs to be allocated to *some* generator rather than silently dropped.
#
# "Self-rescue" is condition A in _identify_transitioning_generators: a generator's
# own transition date shows its report_date is stale-or-mid-year, so it's kept
# individually, regardless of any other generator.
#
# "Plant-level rescue" (_identify_entirely_transitioned_groups) is the alternative path:
# it keeps an entire PM/ESC group at once when none of its generators self-rescue -- but
# any generator with a "triggering" transition date disqualifies its own group from
# taking that path (see below for a definition of what "triggering" means).

_TRANSITION_DATE_COL = {
    "proposed": "generator_operating_date",
    "retired": "generator_retirement_date",
}
_TRANSITION_DATE_ROLES = {
    # report_date (2024-07-01) >= operating_date, and operating_date < 2025-01-01.
    "proposed": {"triggering": "2020-01-01", "fallback": "2025-06-01"},
    # report_date (2024-07-01) <= retirement_date, and retirement_date >= 2024-01-01.
    "retired": {"triggering": "2024-09-01", "fallback": "2020-01-01"},
}
_COVERAGE_REPORT_DATE = "2024-07-01"


def _dates_for(status: str, transition_date: str) -> tuple[str, str]:
    """Return ``(generator_retirement_date, generator_operating_date)`` CSV fields.

    ``remove_inactive_generators`` unconditionally calls both the retired-side and
    proposed-side identify functions, regardless of which status a fixture is
    exercising, so both date columns must always be present -- only the one
    matching ``status`` is populated, the other stays blank.
    """
    if status == "retired":
        return transition_date, ""
    return "", transition_date


@pytest.mark.parametrize("status", ["proposed", "retired"])
def test_remove_inactive_generators_cross_group_transition_does_not_lose_data(
    status,
):
    """A "triggering" transition date in one PM/ESC group must not block the
    plant-level rescue for a different, "fallback" PM/ESC group at the same plant.

    A "triggering" date (see ``_TRANSITION_DATE_ROLES``) both self-rescues its own
    generator via ``_identify_transitioning_generators``'s condition A and disqualifies
    its own PM/ESC group from ``_identify_entirely_transitioned_groups``'s plant-level
    rescue. A "fallback" date does neither, so a fallback generator's survival depends
    entirely on that plant-level rescue applying to its group.

    Axis swept: ``status`` (``proposed``/``retired``) -- the two mirror-image directions
    of the transition logic. Generator roles are fixed by construction rather than
    parametrized: GEN_TRIGGER (prime mover CT) is always "triggering" in its own PM/ESC
    group, and GEN_A/GEN_B (prime mover ST, sharing a different PM/ESC group) are always
    "fallback". One fixed pairing is enough to prove per-group scoping -- sweeping how
    roles combine *within* a shared group is
    ``test_remove_inactive_generators_shared_group_heterogeneous_timing_no_loss``'s job,
    below.

    A concrete example: plant_id_eia 2835 previously lost ~93 GWh of generation in 2015
    this way -- a mid-2015 ST/SUB retirement blocked the plant-level rescue for four
    long-retired ST/BIT generators in a different PM/ESC group.
    """
    trigger_ret, trigger_op = _dates_for(
        status, _TRANSITION_DATE_ROLES[status]["triggering"]
    )
    fallback_ret, fallback_op = _dates_for(
        status, _TRANSITION_DATE_ROLES[status]["fallback"]
    )
    # GEN_A and GEN_B get distinct values so a row's *value* (not just its
    # generator_id) confirms which generator's data actually survived.
    gen_a_net_gen, gen_a_fuel = 500, 5_000
    gen_b_net_gen, gen_b_fuel = 600, 6_000

    gen_assoc = _read_gen_assoc(f"""plant_id_eia,generator_id,report_date,operational_status,prime_mover_code,energy_source_code,generator_retirement_date,generator_operating_date,net_generation_mwh_g_tbl,net_generation_mwh_gf_tbl,fuel_consumed_mmbtu_gf_tbl
1,GEN_TRIGGER,{_COVERAGE_REPORT_DATE},{status},CT,NG,{trigger_ret},{trigger_op},,,
1,GEN_A,{_COVERAGE_REPORT_DATE},{status},ST,DFO,{fallback_ret},{fallback_op},,{gen_a_net_gen},{gen_a_fuel}
1,GEN_B,{_COVERAGE_REPORT_DATE},{status},ST,DFO,{fallback_ret},{fallback_op},,{gen_b_net_gen},{gen_b_fuel}
""")

    out = allocate_gen_fuel.remove_inactive_generators(gen_assoc)

    # All three generators must survive: GEN_TRIGGER individually self-rescues via
    # condition A, and GEN_A/GEN_B's shared group is untouched by a transition
    # belonging to a different PM/ESC group.
    assert set(out.generator_id) == {"GEN_TRIGGER", "GEN_A", "GEN_B"}

    # The surviving rows must carry their original, undamaged reported values, not
    # just a matching generator_id.
    gen_a_row = out.loc[out.generator_id == "GEN_A"].iloc[0]
    assert gen_a_row.net_generation_mwh_gf_tbl == gen_a_net_gen
    assert gen_a_row.fuel_consumed_mmbtu_gf_tbl == gen_a_fuel
    gen_b_row = out.loc[out.generator_id == "GEN_B"].iloc[0]
    assert gen_b_row.net_generation_mwh_gf_tbl == gen_b_net_gen
    assert gen_b_row.fuel_consumed_mmbtu_gf_tbl == gen_b_fuel


@pytest.mark.parametrize("status", ["proposed", "retired"])
@pytest.mark.parametrize(
    "position_a,position_b",
    [
        ("triggering", "triggering"),
        ("triggering", "fallback"),
        ("triggering", "null"),
        ("fallback", "fallback"),
        ("fallback", "null"),
        ("null", "null"),
    ],
)
def test_remove_inactive_generators_shared_group_heterogeneous_timing_no_loss(
    status, position_a, position_b
):
    """Two generators *sharing one* PM/ESC group never lose the group's reported
    generation fuel table value outright, no matter how their individual transition-date
    roles differ -- unlike the cross-group case above, where role placement doesn't
    matter outside the group.

    Axes swept:

    * ``status`` (``proposed``/``retired``).
    * ``position_a``/``position_b``: each generator's transition-date role,
      independently, one of "triggering" (self-rescues via condition A, and would
      disqualify this group from the plant-level rescue), "fallback" (does neither), or
      "null" (no transition date at all -- behaves like "fallback" for both mechanisms,
      since comparisons against NaT are always False). Only 6 of the 3x3 combinations
      are listed, since GEN_A and GEN_B are interchangeable within one shared group.

    Unlike the cross-group test above, role placement is parametrized here because it's
    the interaction *within* one group that matters: if either generator is "triggering"
    it self-rescues regardless of its sibling -- and, because a triggering transition
    disqualifies the *whole* group from the plant-level rescue, exactly the triggering
    generator(s) survive and no others. If neither is "triggering", nothing disqualifies
    the group, so the plant-level rescue saves both instead. This also means the
    surviving generator_id set differs by case, which the assertions below check
    explicitly rather than just checking that *something* survived.
    """
    dates = {**_TRANSITION_DATE_ROLES[status], "null": ""}
    ret_a, op_a = _dates_for(status, dates[position_a])
    ret_b, op_b = _dates_for(status, dates[position_b])
    # GEN_A and GEN_B get distinct values so a row's *value* (not just its
    # generator_id) confirms which generator's data actually survived.
    gen_a_net_gen, gen_a_fuel = 500, 5_000
    gen_b_net_gen, gen_b_fuel = 600, 6_000

    gen_assoc = _read_gen_assoc(f"""plant_id_eia,generator_id,report_date,operational_status,prime_mover_code,energy_source_code,generator_retirement_date,generator_operating_date,net_generation_mwh_g_tbl,net_generation_mwh_gf_tbl,fuel_consumed_mmbtu_gf_tbl
1,GEN_A,{_COVERAGE_REPORT_DATE},{status},ST,DFO,{ret_a},{op_a},,{gen_a_net_gen},{gen_a_fuel}
1,GEN_B,{_COVERAGE_REPORT_DATE},{status},ST,DFO,{ret_b},{op_b},,{gen_b_net_gen},{gen_b_fuel}
""")

    out = allocate_gen_fuel.remove_inactive_generators(gen_assoc)

    # A "triggering" generator self-rescues regardless of its sibling, but also
    # disqualifies the whole shared group from the plant-level rescue -- so if
    # either position is "triggering", only the triggering generator(s) survive.
    # If neither is, nothing disqualifies the group, so the plant-level rescue
    # saves both.
    triggering = {
        gen_id
        for gen_id, position in [("GEN_A", position_a), ("GEN_B", position_b)]
        if position == "triggering"
    }
    expected_survivors = triggering or {"GEN_A", "GEN_B"}
    assert set(out.generator_id) == expected_survivors

    # The surviving row(s) must carry their original, undamaged reported values.
    expected_values = {
        "GEN_A": (gen_a_net_gen, gen_a_fuel),
        "GEN_B": (gen_b_net_gen, gen_b_fuel),
    }
    for gen_id in expected_survivors:
        row = out.loc[out.generator_id == gen_id].iloc[0]
        expected_net_gen, expected_fuel = expected_values[gen_id]
        assert row.net_generation_mwh_gf_tbl == expected_net_gen
        assert row.fuel_consumed_mmbtu_gf_tbl == expected_fuel
