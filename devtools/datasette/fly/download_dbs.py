"""Download SQLite databases from public AWS bucket to /data volume."""

import argparse
import logging
import urllib.request
from pathlib import Path


logging.basicConfig(format="%(asctime)s %(message)s", level=logging.INFO)


def main(args):
    """Main entry-point.

    Assumes that we only want files from dev, and that there is a volume mounted on /data.
    """
    base_url = "https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/dev/"
    save_path = Path("/data")
    existing_files = list(save_path.glob("*.sqlite"))
    logging.info(f"Found {existing_files} in /data, deleting")
    for f in existing_files:
        f.unlink()
    for file_name in args.files:
        url = base_url + file_name
        dest = save_path / file_name
        logging.info(f"Downloading {url} to {dest}...")
        urllib.request.urlretrieve(url, dest)
        logging.info(f"Finished downloading {url} to {dest}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download files from S3 Bucket")
    parser.add_argument(
        "files", metavar="F", type=str, nargs="+", help="Files to be downloaded"
    )
    args = parser.parse_args()
    main(args)
