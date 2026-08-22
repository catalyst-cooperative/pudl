import pytest


@pytest.mark.script_launch_mode("inprocess")
@pytest.mark.usefixtures("prebuilt_outputs")
def test_update_tables(dbt_target: str, script_runner):
    """Run update-tables. Should detect everything already exists, and do nothing.

    This test relies on the prebuilt outputs so the Parquet files are available.
    """
    args = [
        "dbt_helper",
        "update-tables",
        # "--schema",  # Uncomment when we have schema-preserving updates
        "all",
    ]
    if dbt_target == "etl-full":
        args.append("--row-counts")
    ret = script_runner.run(
        args,
        print_result=True,
    )
    assert ret.success
