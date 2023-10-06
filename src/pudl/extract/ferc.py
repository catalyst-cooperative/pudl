"""Hooks to integrate ferc to sqlite functionality into dagster graph."""


import pandas as pd
from dagster import Field, op

import pudl
from pudl.extract.ferc1 import Ferc1DbfExtractor
from pudl.extract.ferc2 import Ferc2DbfExtractor
from pudl.extract.ferc6 import Ferc6DbfExtractor
from pudl.extract.ferc60 import Ferc60DbfExtractor
from pudl.workspace.setup import PudlPaths

logger = pudl.logging_helpers.get_logger(__name__)


@op(
    config_schema={
        "clobber": Field(
            bool, description="Clobber existing ferc1 database.", default_value=False
        ),
    },
    required_resource_keys={"ferc_to_sqlite_settings", "datastore"},
)
def dbf2sqlite(context) -> None:
    """Clone the FERC Form 1 Visual FoxPro databases into SQLite."""
    # TODO(rousik): this thin wrapper seems to be somewhat quirky. Maybe there's a way
    # to make the integration # between the class and dagster better? Investigate.
    logger.info(f"dbf2sqlite settings: {context.resources.ferc_to_sqlite_settings}")

    extractors = [
        Ferc1DbfExtractor,
        Ferc2DbfExtractor,
        Ferc6DbfExtractor,
        Ferc60DbfExtractor,
    ]
    for xclass in extractors:
        xclass(
            datastore=context.resources.datastore,
            settings=context.resources.ferc_to_sqlite_settings,
            clobber=context.op_config["clobber"],
            output_path=PudlPaths().output_dir,
        ).execute()


def use_latest_filing_for_context(
    table: pd.DataFrame, unique_cols: list[str]
) -> pd.DataFrame:
    """Get facts from the latest filing that reported for each context.

    We treat two XBRL contexts that are the same except for their IDs as the same context.
    """
    strict_deduped = table.drop_duplicates()
    logger.debug(
        f"Dropped {len(table) - len(strict_deduped)} completely duplicated rows."
    )
    chrono_order = table.sort_values("publication_time")
    inter_filing_deduped = chrono_order.drop_duplicates(
        subset=[
            c for c in table.columns if c not in {"publication_time", "filing_name"}
        ],
        keep="last",
    )
    logger.debug(
        f"Dropped {len(chrono_order) - len(inter_filing_deduped)} rows that were duplicated across filings."
    )

    deduped_by_context = inter_filing_deduped.drop_duplicates(
        subset=unique_cols,
        keep="last",
    )
    logger.debug(
        f"Dropped {len(inter_filing_deduped) - len(deduped_by_context)} rows for contexts that were updated in later filings."
    )

    return deduped_by_context
