"""Unit tests for allocation of net generation."""

from io import StringIO

import pandas as pd
import pytest

from pudl.analysis import allocate_gen_fuel
from pudl.metadata.fields import apply_pudl_dtypes

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
    ).pipe(apply_pudl_dtypes, group="eia")

    out = allocate_gen_fuel.distribute_annually_reported_data_to_months_if_annual(
        df=bf_with_monthly_annual_mix,
        key_columns=allocate_gen_fuel.IDX_B_PM_ESC,
        data_column_name="fuel_consumed_mmbtu",
        freq="MS",
    )

    out = out.sort_values(["plant_id_eia", "report_date"]).reset_index(drop=True)
    yearly_out = out[out["plant_id_eia"] == 200]
    fuel_2020 = yearly_out[yearly_out.report_date.dt.year == 2020][
        "fuel_consumed_mmbtu"
    ]
    fuel_2021 = yearly_out[yearly_out.report_date.dt.year == 2021][
        "fuel_consumed_mmbtu"
    ]

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
        """report_date,plant_id_eia,generator_id,prime_mover_code,unit_id_pudl,capacity_mw,fuel_type_count,operational_status,generator_retirement_date,energy_source_code_1,energy_source_code_2,energy_source_code_3,energy_source_code_4,energy_source_code_5,energy_source_code_6,energy_source_code_7,planned_energy_source_code_1,startup_source_code_1,startup_source_code_2,startup_source_code_3,startup_source_code_4
2019-01-01,8023,1,ST,1,556.0,1,existing,nan,SUB,BIT,null,null,nan,nan,nan,nan,DFO,nan,nan,nan
2019-01-01,8023,2,ST,2,556.0,1,existing,nan,SUB,SUB,BIT,nan,nan,nan,nan,DFO,nan,nan,nan
"""
    ),
).pipe(apply_pudl_dtypes, group="eia")

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
).pipe(apply_pudl_dtypes, group="eia")

# Base generation EIA923 data
GEN_EIA923_BASE = pd.read_csv(
    StringIO(
        """report_date,plant_id_eia,generator_id,net_generation_mwh
2019-01-01,8023,1,2606737.0
2019-01-01,8023,2,2759826.0
"""
    ),
).pipe(apply_pudl_dtypes, group="eia")

# Base boiler generator association EIA860 data
BOILER_GENERATOR_ASSN_EIA860_BASE = pd.read_csv(
    StringIO(
        """plant_id_eia,boiler_id,generator_id,report_date
8023,1,1,2019-01-01
8023,2,2,2019-01-01
"""
    ),
).pipe(apply_pudl_dtypes, group="eia")

# Base generation fuel EIA923 data
GENERATION_FUEL_EIA923_BASE = pd.read_csv(
    StringIO(
        """report_date,plant_id_eia,energy_source_code,prime_mover_code,net_generation_mwh,fuel_consumed_mmbtu,fuel_consumed_for_electricity_mmbtu
2019-01-01,8023,DFO,ST,3369.286,35566.0,35566.0
2019-01-01,8023,RC,ST,5363193.71,56777578.0,56777578.0
2019-01-01,8023,SUB,ST,10000.0, 100000.0,100000.0
"""
    ),
).pipe(apply_pudl_dtypes, group="eia")

# Generation fuel EIA923 data with extra energy source code
GENERATION_FUEL_EIA923_EXTRA_ESC = pd.read_csv(
    StringIO(
        """report_date,plant_id_eia,energy_source_code,prime_mover_code,net_generation_mwh,fuel_consumed_mmbtu,fuel_consumed_for_electricity_mmbtu
2019-01-01,8023,DFO,ST,3369.286,35566.0,35566.0
2019-01-01,8023,RC,ST,5363193.71,56777578.0,56777578.0
2019-01-01,8023,SUB,ST,10000.0, 100000.0,100000.0
"""
    ),
).pipe(apply_pudl_dtypes, group="eia")

# Boiler fuel EIA923 data with extra prime mover
BOILER_FUEL_EIA923_EXTRA_PM = BOILER_FUEL_EIA923_BASE.copy()
BOILER_FUEL_EIA923_EXTRA_PM.loc[0, "prime_mover_code"] = "CT"


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
    gena_retiring = (
        pd.read_csv(
            StringIO(
                """plant_id_eia,generator_id,report_date,operational_status,generator_retirement_date,net_generation_mwh_g_tbl,fuel_consumed_mmbtu_gf_tbl
50937,GENA,2021-12-01,existing,,,0.0
50937,GENA,2022-01-01,retired,2022-09-01,,85.0
50937,GENA,2022-02-01,retired,2022-09-01,,91.0
50937,GENA,2022-03-01,retired,2022-09-01,,278.0
50937,GENA,2022-04-01,retired,2022-09-01,,127.0
50937,GENA,2022-05-01,retired,2022-09-01,,79.0
50937,GENA,2022-06-01,retired,2022-09-01,,85.0
50937,GENA,2022-07-01,retired,2022-09-01,,91.0
50937,GENA,2022-08-01,retired,2022-09-01,,85.0
50937,GENA,2022-09-01,retired,2022-09-01,,48.0
50937,GENA,2022-10-01,retired,2022-09-01,,67.0
50937,GENA,2022-11-01,retired,2022-09-01,,67.0
50937,GENA,2022-12-01,retired,2022-09-01,,
50937,GENA,2023-01-01,retired,2022-09-01,,
"""
            )
        )
        .convert_dtypes()
        .assign(report_date=lambda x: pd.to_datetime(x.report_date))
    )
    expected_retiring = (
        pd.read_csv(
            StringIO("""plant_id_eia,generator_id,report_date,operational_status,generator_retirement_date,net_generation_mwh_g_tbl,fuel_consumed_mmbtu_gf_tbl
50937,GENA,2022-01-01,retired,2022-09-01,,85.0
50937,GENA,2022-02-01,retired,2022-09-01,,91.0
50937,GENA,2022-03-01,retired,2022-09-01,,278.0
50937,GENA,2022-04-01,retired,2022-09-01,,127.0
50937,GENA,2022-05-01,retired,2022-09-01,,79.0
50937,GENA,2022-06-01,retired,2022-09-01,,85.0
50937,GENA,2022-07-01,retired,2022-09-01,,91.0
50937,GENA,2022-08-01,retired,2022-09-01,,85.0
50937,GENA,2022-09-01,retired,2022-09-01,,48.0
50937,GENA,2022-10-01,retired,2022-09-01,,67.0
50937,GENA,2022-11-01,retired,2022-09-01,,67.0
50937,GENA,2022-12-01,retired,2022-09-01,,
""")
        )
        .convert_dtypes()
        .assign(report_date=lambda x: pd.to_datetime(x.report_date))
    )
    out = allocate_gen_fuel.identify_retiring_generators(gena_retiring)
    pd.testing.assert_frame_equal(expected_retiring, out, check_exact=False)
