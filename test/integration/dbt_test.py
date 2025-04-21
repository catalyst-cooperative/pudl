import logging

import pytest

from pudl.io_managers import PudlMixedFormatIOManager

logger = logging.getLogger(__name__)


@pytest.mark.script_launch_mode("inprocess")
def test_dbt_helper(pudl_io_manager: PudlMixedFormatIOManager, script_runner):
    """Run add-tables. Should detect everything already exists, and do nothing.

    The dependency on pudl_io_manager is necessary because it ensures that the dbt
    tests don't run until after the ETL has completed and the Parquet files are
    available.
    """
    ret = script_runner.run(
        [
            "dbt_helper",
            "add-tables",
            "--etl-fast",
            "--use-local-tables",
            "all",
        ],
        print_result=True,
    )
    assert ret.success
