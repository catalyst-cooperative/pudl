"""Script to check each DOI in zenodo_dois.yml against Zenodo's /versions/latest endpoint.

If there is a more current DOI, update to the latest one. This can be used to avoid
having to hand-update DOI values, and eventually to auto-update records that don't
require hand mapping to extract in PUDL.
"""

import importlib
import sys
from pathlib import Path

import click
import requests
import yaml

from pudl.logging_helpers import get_logger

logger = get_logger(__name__)


def get_latest_record_id(record_id: str) -> tuple[str | None, str | None]:
    """Query https://zenodo.org/api/records/{recordid}/versions/latest for latest record."""
    url = f"https://zenodo.org/api/records/{record_id}/versions/latest"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    # Latest version returns record metadata with the current latest record ID
    latest_id = data.get("id")
    latest_doi = data.get("doi")

    return str(latest_id), latest_doi


def update_yaml_dois(
    yaml_file: Path, datasets: list[str] | None = None
) -> dict[str, dict]:
    """Check all DOIs and update to latest record versions."""
    updates = {}

    with Path(yaml_file).open() as f:
        data = yaml.safe_load(f)

    for dataset_name, current_doi in data.items():
        if datasets is None or dataset_name in datasets:
            # Extract record ID from DOI (e.g, grab 123456 from 10.5281/zenodo.123456)
            record_id = current_doi.split("zenodo.")[-1].split("/")[0]

            latest_id, latest_doi = get_latest_record_id(record_id)

            if latest_id and latest_id != record_id:
                # Reconstruct DOI with latest record ID
                new_doi = f"10.5281/zenodo.{latest_id}"
                logger.info(
                    f"{dataset_name}: Updating DOI from {current_doi} to {new_doi}"
                )

                # Update the DOI
                data[dataset_name] = new_doi

                updates[dataset_name] = {
                    "old_doi": current_doi,
                    "new_doi": new_doi,
                }

            elif latest_id == record_id:
                logger.info(f"{dataset_name}: DOI already current.")
            else:
                raise AssertionError(
                    f"Expected new or same record ID, got {latest_id}."
                )

    # Write back only if changes
    if updates:
        with Path(yaml_file).open("w") as f:
            yaml.dump(data, f)
        print(f"\n✅ Updated {yaml_file} with {len(updates)} newer record versions")


@click.command(
    context_settings={"help_option_names": ["-h", "--help"]},
)
@click.option(
    "--datasets",
    type=str,
    default=None,
    help="Specific datasets to update, separated by a comma (e.g. phmsagas,epacamd_eia). If none are specified, all are updated.",
)
def main(datasets: str | None = None):
    """Auto-update Zenodo DOIs to the latest value."""
    if datasets:
        datasets = datasets.split(",")
    yaml_file = importlib.resources.files("pudl.package_data.settings").joinpath(
        "zenodo_dois.yml"
    )
    if not yaml_file.exists():
        logger.warn(f"❌ File not found: {yaml_file}")
        sys.exit(1)

    logger.info(
        f"Checking {yaml_file} for newer Zenodo record versions {'of ' + ', '.join(datasets) if datasets else ''}"
    )

    update_yaml_dois(yaml_file, datasets)


if __name__ == "__main__":
    sys.exit(main())
