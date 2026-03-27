"""Script to check each DOI in zenodo_dois.yml against Zenodo's /versions/latest endpoint.

If there is a more current DOI, update to the latest one. This can be used to avoid
having to hand-update DOI values, and eventually to auto-update records that don't
require hand mapping to extract in PUDL.
"""

import re
import sys
from pathlib import Path

import click
import requests
import yaml

from pudl.logging_helpers import get_logger

logger = get_logger(__name__)


def get_latest_record_id(record_id: str) -> tuple[str | None, str | None]:
    """Get ID of the latest version of any Zenodo record.

    Given the ID of any Zenodo record, this will return the record ID and DOI of the
    latest version associated with the same concept DOI.
    """
    url = f"https://zenodo.org/api/records/{record_id}/versions/latest"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    # Latest version returns record metadata with the current latest record ID
    latest_id = data.get("id")
    latest_doi = data.get("doi")

    return str(latest_id), latest_doi


def update_yaml_dois(yaml_file: Path, datasets: tuple[str, ...]) -> dict[str, dict]:
    """Check all DOIs and update to latest record versions."""
    updates = {}

    with Path(yaml_file).open() as f:
        data = yaml.safe_load(f)

    for dataset_name, current_doi in data.items():
        if dataset_name in datasets:
            # Extract record ID from DOI (e.g, grab 123456 from 10.5281/zenodo.123456)
            if (match := re.search(r"^10\.5281/zenodo\.(\d+)$", current_doi)) is None:
                raise ValueError(
                    f"Unexpected Zenodo DOI format for {dataset_name}: {current_doi}"
                )
            record_id = match.group(1)

            latest_id, latest_doi = get_latest_record_id(record_id)

            if latest_id and latest_id != record_id:
                # Reconstruct DOI with latest record ID
                logger.info(
                    f"{dataset_name}: Updating DOI from {current_doi} to {latest_doi}"
                )

                # Update the DOI
                data[dataset_name] = latest_doi

                updates[dataset_name] = {
                    "old_doi": current_doi,
                    "new_doi": latest_doi,
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

    return updates


@click.command(
    context_settings={"help_option_names": ["-h", "--help"]},
)
@click.argument(
    "datasets",
    nargs=-1,
)
def main(datasets: tuple[str, ...]):  # pragma: no cover
    """Auto-update Zenodo DOIs to the latest value."""
    # Deferred to keep --help fast; see pudl/scripts/__init__.py for rationale.
    from pudl.workspace.datastore import get_zenodo_dois_path  # noqa: PLC0415

    if not datasets:  # If no datasets to update
        logger.warning("No datasets provided, nothing will be updated.")
        sys.exit(0)

    yaml_file = get_zenodo_dois_path()
    if not yaml_file.exists():
        logger.warning(f"❌ File not found: {yaml_file}")
        sys.exit(1)

    logger.info(
        f"Checking {yaml_file} for newer Zenodo record versions {'of ' + ', '.join(datasets) if datasets else ''}"
    )

    update_yaml_dois(yaml_file, datasets)


if __name__ == "__main__":
    sys.exit(main())
