"""Distribute PUDL ETL outputs to cloud storage and update git branches.

This module handles distribution of completed ETL builds to public cloud storage
(GCS and S3), git branch updates, Zenodo releases, and Cloud Run deployments.
"""

import logging
import shutil
import zipfile
from pathlib import Path

logger = logging.getLogger(__name__)


def prepare_outputs_for_distribution(output_dir: Path) -> None:
    """Prepare ETL outputs for distribution.

    Takes raw ETL output structure and produces distribution-ready outputs:
    - Moves parquet files from parquet/ subdirectory to root
    - Compresses SQLite databases with maximum compression
    - Creates parquet archive (no compression, already compressed)
    - Removes test databases and temporary directories

    Args:
        output_dir: Directory containing ETL outputs to prepare.
    """
    output_dir = Path(output_dir)
    logger.info(f"Preparing outputs in {output_dir} for distribution")

    # Move parquet files to root and clean up subdirectory
    parquet_dir = output_dir / "parquet"
    if parquet_dir.exists():
        for parquet_file in parquet_dir.glob("*.parquet"):
            shutil.move(str(parquet_file), str(output_dir / parquet_file.name))

        datapackage = parquet_dir / "pudl_parquet_datapackage.json"
        if datapackage.exists():
            shutil.move(str(datapackage), str(output_dir / datapackage.name))

        shutil.rmtree(parquet_dir)

    # Remove test database
    test_db = output_dir / "pudl_dbt_tests.duckdb"
    if test_db.exists():
        test_db.unlink()

    # Compress SQLite databases
    sqlite_files = list(output_dir.glob("*.sqlite"))
    for sqlite_file in sqlite_files:
        zip_path = output_dir / f"{sqlite_file.name}.zip"
        with zipfile.ZipFile(
            zip_path, "w", zipfile.ZIP_DEFLATED, compresslevel=9
        ) as zf:
            zf.write(sqlite_file, arcname=sqlite_file.name)
        sqlite_file.unlink()
        logger.info(f"Compressed {sqlite_file.name}")

    # Create parquet archive (store mode, no compression)
    parquet_files = list(output_dir.glob("*.parquet"))
    if parquet_files:
        archive_path = output_dir / "pudl_parquet.zip"
        with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_STORED) as zf:
            for parquet_file in parquet_files:
                zf.write(parquet_file, arcname=parquet_file.name)

            datapackage = output_dir / "pudl_parquet_datapackage.json"
            if datapackage.exists():
                zf.write(datapackage, arcname=datapackage.name)

        logger.info(f"Created parquet archive: {archive_path}")

    logger.info("Output preparation complete")
