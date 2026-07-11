"""CLI for generating FERC SQLite provenance requirements for caching.

This tool generates a JSON representation of the inputs that affect the
materialization of FERC SQLite assets. This representation can be hashed
in CI to detect when the cached SQLite databases need to be rebuilt.
"""

import json
import sys
from dataclasses import asdict

import click

from pudl.dagster.provenance import FercSqliteProvenance, get_xbrl_extractor_version
from pudl.settings import Ferc1DataConfig, Ferc714DataConfig
from pudl.workspace.datastore import ZenodoDoiSettings


def get_provenance(dataset: str, data_format: str) -> dict:
    """Return the current provenance requirements for a FERC SQLite asset.

    Args:
        dataset: The name of the dataset (e.g. 'ferc1', 'ferc714').
        data_format: The data format ('dbf' or 'xbrl').

    Returns:
        A dictionary containing the provenance requirements.
    """
    doi_settings = ZenodoDoiSettings()
    xbrl_version = get_xbrl_extractor_version()

    if dataset == "ferc1":
        years = Ferc1DataConfig().years
    elif dataset == "ferc714":
        years = Ferc714DataConfig().years
    else:
        raise click.BadParameter(f"Unsupported FERC dataset: {dataset}")

    if data_format not in ("dbf", "xbrl"):
        raise click.BadParameter(
            f"Unsupported data format: {data_format}. Must be 'dbf' or 'xbrl'."
        )
    if dataset == "ferc714" and data_format == "dbf":
        raise click.BadParameter("FERC Form 714 is only available in XBRL format.")

    prov = FercSqliteProvenance(
        dataset=dataset,
        data_format=data_format,
        zenodo_doi=doi_settings.get_doi(dataset),
        years=years,
        ferc_xbrl_extractor_version=xbrl_version,
    )
    return asdict(prov)


@click.command(
    context_settings={"help_option_names": ["-h", "--help"]},
)
@click.argument(
    "dataset",
    type=click.Choice(["ferc1", "ferc714"]),
)
@click.argument(
    "data_format",
    type=click.Choice(["dbf", "xbrl"]),
)
def main(dataset: str, data_format: str) -> int:
    """Generate a JSON representation of the provenance for a FERC SQLite asset.

    This output can be hashed to detect changes in raw data versions (DOIs),
    data configurations (years), or extractor versions.
    """
    provenance = get_provenance(dataset, data_format)
    click.echo(json.dumps(provenance, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
