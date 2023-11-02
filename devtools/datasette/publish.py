import json
import logging
import secrets
from pathlib import Path
from subprocess import check_call, check_output

from pudl.metadata.classes import DatasetteMetadata
from pudl.workspace.setup import PudlPaths

logging.basicConfig(format="%(asctime)s %(message)s", level=logging.INFO)

DOCKERFILE_TEMPLATE = """
FROM python:3.11.0-slim-bullseye
COPY . /app
WORKDIR /app

RUN apt-get update
RUN apt-get install -y zstd

ENV DATASETTE_SECRET '{datasette_secret}'
RUN pip install -U datasette datasette-cluster-map datasette-vega datasette-block-robots
ENV PORT 8080
EXPOSE 8080

CMD ["/bin/bash", "-c", "shopt -s nullglob && find /data/ -name '*.sqlite' -delete && mv all_dbs.tar.zst /data && zstd -f -d /data/all_dbs.tar.zst -o /data/all_dbs.tar && tar -xf /data/all_dbs.tar --directory /data && datasette serve --host 0.0.0.0 /data/*.sqlite --cors --inspect-file inspect-data.json --metadata metadata.yml --setting sql_time_limit_ms 5000 --port $PORT"]

"""


def make_dockerfile(datasets):
    dataset_names = " ".join(datasets)
    datasette_secret = secrets.token_hex(16)
    return DOCKERFILE_TEMPLATE.format(
        datasette_secret=datasette_secret, datasets=dataset_names
    )


def inspect_data(datasets, pudl_out):
    inspect_output = json.loads(
        check_output(
            [  # noqa: S603
                "datasette",
                "inspect",
            ]
            + [str(pudl_out / ds) for ds in datasets]
        )
    )

    for dataset in inspect_output:
        name = Path(inspect_output[dataset]["file"]).name
        new_filepath = Path("/data") / name
        inspect_output[dataset]["file"] = str(new_filepath)
    return inspect_output


def metadata(pudl_out) -> str:
    return DatasetteMetadata.from_data_source_ids(pudl_out).to_yaml()


def main():
    fly_dir = Path(__file__).parent.absolute() / "fly"
    docker_path = fly_dir / "Dockerfile"
    inspect_path = fly_dir / "inspect-data.json"
    metadata_path = fly_dir / "metadata.yml"

    pudl_out = PudlPaths().pudl_output
    datasets = [str(p.name) for p in pudl_out.glob("*.sqlite")]
    logging.info("Inspecting DBs for datasette...")
    inspect_output = inspect_data(datasets, pudl_out)
    with inspect_path.open("w") as f:
        f.write(json.dumps(inspect_output))

    logging.info("Writing metadata...")
    with metadata_path.open("w") as f:
        f.write(metadata(pudl_out))

    logging.info("Writing Dockerfile...")
    with docker_path.open("w") as f:
        f.write(make_dockerfile(datasets))

    logging.info(f"Compressing databases at {datasets}...")
    check_call(
        ["tar", "-a", "-czvf", fly_dir / "all_dbs.tar.zst"] + datasets,
        cwd=pudl_out,
    )

    check_call(["flyctl", "deploy"], cwd=fly_dir)


if __name__ == "__main__":
    main()
