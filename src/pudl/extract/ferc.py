"""Hooks to integrate ferc to sqlite functionality into dagster graph."""


from dagster import Field, op

import pudl
from pudl.extract.ferc1 import Ferc1DbfExtractor
from pudl.extract.ferc2 import Ferc2DbfExtractor
from pudl.extract.ferc6 import Ferc6DbfExtractor
from pudl.extract.ferc60 import Ferc60DbfExtractor
from pudl.helpers import EnvVar

logger = pudl.logging_helpers.get_logger(__name__)


@op(
    config_schema={
        "pudl_output_path": Field(
            EnvVar(
                env_var="PUDL_OUTPUT",
            ),
            description="Path of directory to store the database in.",
            default_value=None,
        ),
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
            output_path=context.op_config["pudl_output_path"],
        ).execute()
