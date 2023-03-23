"""Tests for timeseries anomalies detection and imputation."""
import pandas as pd

import pudl.analysis.plant_parts_eia

GENS_MEGA = pd.DataFrame(
    {
        "plant_id_eia": [1, 1, 1, 1],
        "report_date": ["2020-01-01", "2020-01-01", "2020-01-01", "2020-01-01"],
        "utility_id_eia": [111, 111, 111, 111],
        "generator_id": ["a", "b", "c", "d"],
        "prime_mover_code": ["ST", "GT", "CT", "CA"],
        "energy_source_code_1": ["BIT", "NG", "NG", "NG"],
        "ownership_record_type": [
            "total",
            "total",
            "total",
            "total",
        ],
        "operational_status_pudl": ["operating", "operating", "operating", "operating"],
        "capacity_mw": [400, 50, 125, 75],
    }
).astype({"report_date": "datetime64[ns]"})


def test_plant_ag():
    """Test aggregation of the plant-part part list by plant.

    The only data col we are testing here is capacity_mw.
    """
    # test aggregation by plant
    plant_ag_out = (
        pudl.analysis.plant_parts_eia.PlantPart(part_name="plant")
        .ag_part_by_own_slice(GENS_MEGA, sum_cols=["capacity_mw"], wtavg_dict={})
        .convert_dtypes()
    )

    plant_ag_expected = (
        pd.DataFrame(
            {
                "plant_id_eia": [1],
                "report_date": ["2020-01-01"],
                "operational_status_pudl": ["operating"],
                "utility_id_eia": [111],
                "ownership_record_type": ["total"],
                "capacity_mw": [650.0],
            }
        )
        .astype({"report_date": "datetime64[ns]"})
        .convert_dtypes()
    )

    pd.testing.assert_frame_equal(plant_ag_out, plant_ag_expected)


def test_prime_fuel_ag():
    """Test aggregation of the plant-part part list by prime fuel.

    The only data col we are testing here is capacity_mw.
    """
    # test aggregation by plant prime fuel
    plant_primary_fuel_ag_out = (
        pudl.analysis.plant_parts_eia.PlantPart(part_name="plant_prime_fuel")
        .ag_part_by_own_slice(GENS_MEGA, sum_cols=["capacity_mw"], wtavg_dict={})
        .convert_dtypes()
    )

    plant_primary_fuel_ag_expected = (
        pd.DataFrame(
            {
                "plant_id_eia": 1,
                "energy_source_code_1": ["BIT", "NG"],
                "report_date": "2020-01-01",
                "operational_status_pudl": "operating",
                "utility_id_eia": 111,
                "ownership_record_type": "total",
                "capacity_mw": [400.0, 250.0],
            }
        )
        .astype({"report_date": "datetime64[ns]"})
        .convert_dtypes()
    )

    pd.testing.assert_frame_equal(
        plant_primary_fuel_ag_out, plant_primary_fuel_ag_expected
    )


def test_prime_mover_ag():
    """Test aggregation of the plant-part part list by prime mover.

    The only data col we are testing here is capacity_mw.
    """
    # test aggregation by plant prime mover
    plant_prime_mover_ag_out = (
        pudl.analysis.plant_parts_eia.PlantPart(part_name="plant_prime_mover")
        .ag_part_by_own_slice(GENS_MEGA, sum_cols=["capacity_mw"], wtavg_dict={})
        .convert_dtypes()
    )

    plant_prime_mover_ag_expected = (
        pd.DataFrame(
            {
                "plant_id_eia": 1,
                "prime_mover_code": ["CA", "CT", "GT", "ST"],
                "report_date": "2020-01-01",
                "operational_status_pudl": "operating",
                "utility_id_eia": 111,
                "ownership_record_type": "total",
                "capacity_mw": [75.0, 125.0, 50.0, 400.0],
            }
        )
        .astype({"report_date": "datetime64[ns]"})
        .convert_dtypes()
    )

    pd.testing.assert_frame_equal(
        plant_prime_mover_ag_out, plant_prime_mover_ag_expected
    )


def test_plant_gen_ag():
    """Test aggregation of the plant-part part list by generator.

    The only data col we are testing here is capacity_mw.
    """
    # test aggregation by plant gen
    plant_gen_ag_out = (
        pudl.analysis.plant_parts_eia.PlantPart(part_name="plant_gen")
        .ag_part_by_own_slice(GENS_MEGA, sum_cols=["capacity_mw"], wtavg_dict={})
        .convert_dtypes()
    )

    plant_gen_ag_expected = (
        pd.DataFrame(
            {
                "plant_id_eia": 1,
                "generator_id": ["a", "b", "c", "d"],
                "report_date": "2020-01-01",
                "operational_status_pudl": "operating",
                "utility_id_eia": 111,
                "ownership_record_type": "total",
                "capacity_mw": [400.0, 50.0, 125.0, 75.0],
            }
        )
        .astype({"report_date": "datetime64[ns]"})
        .convert_dtypes()
    )

    pd.testing.assert_frame_equal(plant_gen_ag_out, plant_gen_ag_expected)


def test_make_mega_gen_tbl():
    """Test the creation of the mega generator table.

    Integrates ownership with generators.
    """
    # one plant with three generators
    mcoe = pd.DataFrame(
        {
            "plant_id_eia": 1,
            "report_date": "2020-01-01",
            "generator_id": ["a", "b", "c"],
            "utility_id_eia": [111, 111, 111],
            "unit_id_pudl": 1,
            "prime_mover_code": ["CT", "CT", "CA"],
            "technology_description": "Natural Gas Fired Combined Cycle",
            "operational_status": "existing",
            "generator_retirement_date": pd.NA,
            "capacity_mw": [50, 50, 100],
            "generator_operating_date": "2001-12-01",
        }
    ).astype(
        {
            "generator_retirement_date": "datetime64[ns]",
            "report_date": "datetime64[ns]",
            "generator_operating_date": "datetime64[ns]",
        }
    )
    # one record for every owner of each generator
    df_own_eia860 = pd.DataFrame(
        {
            "plant_id_eia": 1,
            "report_date": "2020-01-01",
            "generator_id": ["a", "b", "c", "c"],
            "utility_id_eia": 111,
            "owner_utility_id_eia": [111, 111, 111, 888],
            "fraction_owned": [1, 1, 0.75, 0.25],
        }
    ).astype({"report_date": "datetime64[ns]"})

    out = pudl.analysis.plant_parts_eia.MakeMegaGenTbl().execute(
        mcoe, df_own_eia860, slice_cols=["capacity_mw"]
    )

    out_expected = (
        pd.DataFrame(
            {
                "plant_id_eia": 1,
                "report_date": "2020-01-01",
                "generator_id": ["a", "b", "c", "c", "a", "b", "c", "c"],
                "unit_id_pudl": 1,
                "prime_mover_code": ["CT", "CT", "CA", "CA", "CT", "CT", "CA", "CA"],
                "technology_description": "Natural Gas Fired Combined Cycle",
                "operational_status": "existing",
                "generator_retirement_date": pd.NaT,
                "capacity_mw": [50.0, 50.0, 75.0, 25.0, 50.0, 50.0, 100.0, 100.0],
                "generator_operating_date": "2001-12-01",
                "ferc_acct_name": "Other",
                "generator_operating_year": 2001,
                "operational_status_pudl": "operating",
                "capacity_eoy_mw": [50, 50, 100, 100, 50, 50, 100, 100],
                "fraction_owned": [1.00, 1.00, 0.75, 0.25, 1.00, 1.00, 1.00, 1.00],
                "utility_id_eia": [111, 111, 111, 888, 111, 111, 111, 888],
                "ownership_record_type": [
                    "owned",
                    "owned",
                    "owned",
                    "owned",
                    "total",
                    "total",
                    "total",
                    "total",
                ],
            }
        )
        .astype(
            {
                "generator_retirement_date": "datetime64[ns]",
                "report_date": "datetime64[ns]",
                "generator_operating_date": "datetime64[ns]",
                "generator_operating_year": "Int64",
                "utility_id_eia": "Int64",  # convert to pandas Int64 instead of numpy int64
            }
        )
        .set_index([[0, 1, 2, 3, 0, 1, 2, 3]])
    )

    pd.testing.assert_frame_equal(out, out_expected)


def test_scale_by_ownership():
    """Test the scale_by_ownership method."""
    dtypes = {"report_date": "datetime64[ns]", "utility_id_eia": pd.Int64Dtype()}
    own_ex1 = pd.DataFrame(
        {
            "plant_id_eia": [1, 1, 1, 1],
            "report_date": ["2019-01-01", "2019-01-01", "2019-01-01", "2019-01-01"],
            "generator_id": ["a", "a", "b", "b"],
            "utility_id_eia": [3, 3, 3, 3],
            "owner_utility_id_eia": [3, 4, 3, 4],
            "fraction_owned": [0.7, 0.3, 0.1, 0.9],
        },
    ).astype(dtypes)

    gens_mega_ex1 = pd.DataFrame(
        {
            "plant_id_eia": [1, 1],
            "report_date": [
                "2019-01-01",
                "2019-01-01",
            ],
            "generator_id": [
                "a",
                "b",
            ],
            "utility_id_eia": [
                3,
                3,
            ],
            "total_fuel_cost": [4500, 1250],
            "net_generation_mwh": [10000, 5000],
            "capacity_mw": [100, 50],
            "capacity_eoy_mw": [100, 50],
            "total_mmbtu": [9000, 7800],
        },
    ).astype(dtypes)

    out_ex1 = pd.DataFrame(
        {
            "plant_id_eia": [
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
            ],
            "report_date": [
                "2019-01-01",
                "2019-01-01",
                "2019-01-01",
                "2019-01-01",
                "2019-01-01",
                "2019-01-01",
                "2019-01-01",
                "2019-01-01",
            ],
            "generator_id": [
                "a",
                "a",
                "b",
                "b",
                "a",
                "a",
                "b",
                "b",
            ],
            "total_fuel_cost": [
                4500 * 0.7,
                4500 * 0.3,
                1250 * 0.1,
                1250 * 0.9,
                4500,
                4500,
                1250,
                1250,
            ],
            "net_generation_mwh": [
                10000 * 0.7,
                10000 * 0.3,
                5000 * 0.1,
                5000 * 0.9,
                10000,
                10000,
                5000,
                5000,
            ],
            "capacity_mw": [
                100 * 0.7,
                100 * 0.3,
                50 * 0.1,
                50 * 0.9,
                100,
                100,
                50,
                50,
            ],
            "capacity_eoy_mw": [
                100 * 0.7,
                100 * 0.3,
                50 * 0.1,
                50 * 0.9,
                100,
                100,
                50,
                50,
            ],
            "total_mmbtu": [
                9000 * 0.7,
                9000 * 0.3,
                7800 * 0.1,
                7800 * 0.9,
                9000,
                9000,
                7800,
                7800,
            ],
            "fraction_owned": [0.7, 0.3, 0.1, 0.9, 1, 1, 1, 1],
            "utility_id_eia": [3, 4, 3, 4, 3, 4, 3, 4],
            "ownership_record_type": [
                "owned",
                "owned",
                "owned",
                "owned",
                "total",
                "total",
                "total",
                "total",
            ],
        },
    ).astype(dtypes)

    out = (
        pudl.analysis.plant_parts_eia.MakeMegaGenTbl()
        .scale_by_ownership(gens_mega=gens_mega_ex1, own_eia860=own_ex1)
        .reset_index(drop=True)
    )

    pd.testing.assert_frame_equal(out_ex1, out)


def test_label_true_grans():
    """Test the labeling of true granularities in the plant part list."""
    plant_part_list_input = pd.DataFrame(
        {
            "report_date": ["2020-01-01"] * 7,
            "record_id_eia": [
                "plant_3",
                "unit_a",
                "unit_b",
                "gen_1",
                "gen_2",
                "gen_3",
                "tech_nat_gas",
            ],
            "plant_id_eia": [3] * 7,
            "plant_part": [
                "plant",
                "plant_unit",
                "plant_unit",
                "plant_gen",
                "plant_gen",
                "plant_gen",
                "plant_technology",
            ],
            "generator_id": [None, None, None, 1, 2, 3, None],
            "unit_id_pudl": [None, "A", "B", "A", "B", "B", None],
            "technology_description": ["nat_gas"] * 7,
            "operational_status_pudl": [None] * 7,
            "utility_id_eia": [None] * 7,
            "ownership_record_type": [None] * 7,
            "prime_mover_code": [None] * 7,
            "ferc_acct_name": [None] * 7,
            "energy_source_code_1": [None] * 7,
            "generator_operating_year": [None] * 7,
            "installation_year": [None] * 7,
            "construction_year": [None] * 7,
        }
    ).astype({"report_date": "datetime64[ns]"})

    true_grans = pd.DataFrame(
        {
            "true_gran": [True, True, True, False, True, True, False],
            "appro_record_id_eia": [
                "plant_3",
                "unit_a",
                "unit_b",
                "unit_a",
                "gen_2",
                "gen_3",
                "plant_3",
            ],
            "appro_part_label": [
                "plant",
                "plant_unit",
                "plant_unit",
                "plant_unit",
                "plant_gen",
                "plant_gen",
                "plant",
            ],
        }
    ).astype({"appro_part_label": "string"})

    expected_out = pd.concat([plant_part_list_input, true_grans], axis=1)

    out = pudl.analysis.plant_parts_eia.TrueGranLabeler().execute(plant_part_list_input)

    pd.testing.assert_frame_equal(expected_out, out)
