"""Benchmark bulk-transfer options for the FERC EQR deployment.

Compares tuned ``s3fs`` / ``gcsfs`` (what ``test-eqr-fsspec`` uses) against the
``boto3`` CRT client and ``gcloud storage`` (what ``fast-ferceqr-deploy`` used), for:

* local -> S3 upload
* local -> GCS upload
* S3 server-side copy within a bucket (the promote / snapshot step)

Each case is run on a single ~3.5 GiB transactions file (per-file throughput) and on
``--n-parallel`` files at once through a thread pool (what the deploy actually does).
Wall time, throughput, and process CPU utilization are recorded; CPU well above 100%
means multiple cores are doing work, while ~100% on a slow case points at a single
thread (e.g. the shared fsspec IO loop) as the bottleneck.

Meant to run on a GCP Batch VM in the same configuration as the real builds (see
the temporary ``build-deploy-ferceqr`` workflow edit on this branch). Only writes
beneath the scratch prefixes given on the command line, and deletes them when done.
"""

import contextlib
import json
import os
import shutil
import subprocess
import time
import traceback
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import click
import fsspec

MiB = 1024**2
GiB = 1024**3

RESULTS: list[dict] = []


def log(msg: str) -> None:
    """Print a timestamped progress line (flushed so Batch logs stream)."""
    print(f"{time.strftime('%H:%M:%S')} {msg}", flush=True)  # noqa: T201


def timed(name: str, mode: str, nbytes: int, fn: Callable[[], None]) -> None:
    """Run *fn*, recording wall time, throughput and CPU utilization."""
    log(f"START {name} [{mode}] ({nbytes / GiB:.1f} GiB)")
    t0, c0 = time.perf_counter(), os.times()
    error = None
    try:
        fn()
    except Exception:
        error = traceback.format_exc()
        log(f"FAILED {name} [{mode}]:\n{error}")
    wall = time.perf_counter() - t0
    c1 = os.times()
    cpu = (c1.user - c0.user) + (c1.system - c0.system)
    cpu += (c1.children_user - c0.children_user) + (
        c1.children_system - c0.children_system
    )
    rec = {
        "case": name,
        "mode": mode,
        "gib": round(nbytes / GiB, 2),
        "wall_s": round(wall, 1),
        "MB_per_s": round(nbytes / 1e6 / wall, 1),
        "cpu_pct": round(100 * cpu / wall),
        "error": error,
    }
    RESULTS.append(rec)
    log(
        f"DONE  {name} [{mode}] {rec['wall_s']}s {rec['MB_per_s']} MB/s "
        f"cpu={rec['cpu_pct']}%"
    )


def run_parallel(fn: Callable, args: list[tuple]) -> None:
    """Call *fn* on every args tuple in a thread pool with one worker per item."""
    with ThreadPoolExecutor(max_workers=len(args)) as ex:
        for fut in [ex.submit(fn, *a) for a in args]:
            fut.result()


def s3fs_fs(
    pool: int | None,
    max_concurrency: int | None = None,
    read_timeout: int | None = None,
):
    """Build an uncached s3fs filesystem with the given connection tuning."""
    kwargs = {"skip_instance_cache": True}
    config_kwargs = {}
    if pool:
        config_kwargs["max_pool_connections"] = pool
    if read_timeout:
        config_kwargs["read_timeout"] = read_timeout
    if config_kwargs:
        kwargs["config_kwargs"] = config_kwargs
    if max_concurrency:
        kwargs["max_concurrency"] = max_concurrency
    return fsspec.filesystem("s3", **kwargs)


def safe_rm(fs, path: str) -> None:
    """Recursively delete *path*, tolerating a case that failed before writing it."""
    with contextlib.suppress(FileNotFoundError):
        fs.rm(path, recursive=True)


def boto3_client():
    """Build a boto3 S3 client in the region of AWS_DEFAULT_REGION."""
    import boto3

    return boto3.client("s3")


def crt_upload(client, src: Path, uri: str) -> None:
    """Upload *src* with the CRT transfer manager (settings from the old branch)."""
    from boto3.s3.transfer import TransferConfig, create_transfer_manager

    bucket, _, key = uri.removeprefix("s3://").partition("/")
    cfg = TransferConfig(
        preferred_transfer_client="crt",
        multipart_threshold=64 * MiB,
        multipart_chunksize=64 * MiB,
        max_concurrency=64,
    )
    with create_transfer_manager(client, cfg) as mgr:
        mgr.upload(str(src), bucket, key).result()


def crt_copy(client, src_uri: str, dst_uri: str) -> None:
    """Server-side copy with the tuned multipart manager (old-branch settings)."""
    from boto3.s3.transfer import TransferConfig, create_transfer_manager

    sb, _, sk = src_uri.removeprefix("s3://").partition("/")
    db, _, dk = dst_uri.removeprefix("s3://").partition("/")
    cfg = TransferConfig(
        multipart_threshold=512 * MiB,
        multipart_chunksize=512 * MiB,
        max_concurrency=128,
    )
    with create_transfer_manager(client, cfg) as mgr:
        mgr.copy({"Bucket": sb, "Key": sk}, db, dk).result()


def gcloud_cp(srcs: list[Path], dest_prefix: str) -> None:
    """Upload *srcs* into *dest_prefix* with a single ``gcloud storage cp``."""
    cmd = ["gcloud", "storage", "cp", *map(str, srcs), f"{dest_prefix}/"]
    subprocess.run(cmd, check=True, capture_output=True, text=True)  # noqa: S603


def bench_s3_upload(files: list[Path], scratch: str) -> None:
    """S3 upload: s3fs at several tunings vs. the CRT client."""
    # (label, pool connections, max_concurrency per file, chunksize)
    configs = [
        ("s3fs-default", None, None, None),
        ("s3fs-pool64", 64, None, None),
        ("s3fs-pool64-conc32-64MiB", 64, 32, 64 * MiB),
        ("s3fs-pool256-conc64-64MiB", 256, 64, 64 * MiB),
        ("s3fs-pool512-conc64-128MiB", 512, 64, 128 * MiB),
    ]
    for label, pool, conc, chunk in configs:
        for mode, subset in (("single", files[:1]), ("parallel", files)):
            fs = s3fs_fs(pool, conc)
            put_kwargs = {"chunksize": chunk} if chunk else {}
            dst = f"{scratch}/up/{label}/{mode}"

            def one(f: Path, fs=fs, dst=dst, put_kwargs=put_kwargs) -> None:
                fs.put(str(f), f"{dst}/{f.name}", **put_kwargs)

            timed(
                f"s3-upload {label}",
                mode,
                sum(f.stat().st_size for f in subset),
                lambda subset=subset, one=one: run_parallel(
                    one, [(f,) for f in subset]
                ),
            )
            safe_rm(fs, f"{scratch}/up/{label}/{mode}")

    client = boto3_client()
    for mode, subset in (("single", files[:1]), ("parallel", files)):
        dst = f"{scratch}/up/crt/{mode}"
        timed(
            "s3-upload boto3-crt-64MiB-x64",
            mode,
            sum(f.stat().st_size for f in subset),
            lambda subset=subset, dst=dst: run_parallel(
                lambda f: crt_upload(client, f, f"{dst}/{f.name}"),
                [(f,) for f in subset],
            ),
        )


def bench_s3_copy(files: list[Path], scratch: str) -> None:
    """S3 server-side copy: s3fs (default and ``block=``) vs. tuned multipart copy."""
    client = boto3_client()
    src_prefix = f"{scratch}/copy_src"
    for f in files:  # source objects, uploaded fast; not part of the timing
        crt_upload(client, f, f"{src_prefix}/{f.name}")
    sizes = {f.name: f.stat().st_size for f in files}

    # (label, s3fs cp kwargs, read_timeout). s3fs' default read_timeout of 15 s
    # kills a single UploadPartCopy of several GiB, so large blocks need more.
    configs = [
        ("s3fs-cp-default", {}, None),
        ("s3fs-cp-block512MiB", {"block": 512 * MiB}, None),
        ("s3fs-cp-block1GiB-rt600", {"block": 1 * GiB}, 600),
        ("s3fs-cp-block5GiB-rt600", {"block": 5 * GiB - 1}, 600),
    ]
    for label, cp_kwargs, read_timeout in configs:
        for mode, names in (
            ("single", list(sizes)[:1]),
            ("parallel", list(sizes)),
        ):
            fs = s3fs_fs(256, read_timeout=read_timeout)
            dst = f"{scratch}/copy_dst/{label}/{mode}"
            timed(
                f"s3-copy {label}",
                mode,
                sum(sizes[n] for n in names),
                lambda fs=fs, dst=dst, names=names, cp_kwargs=cp_kwargs: run_parallel(
                    lambda n: fs.cp(f"{src_prefix}/{n}", f"{dst}/{n}", **cp_kwargs),
                    [(n,) for n in names],
                ),
            )
            safe_rm(fs, dst)

    for mode, names in (("single", list(sizes)[:1]), ("parallel", list(sizes))):
        dst = f"{scratch}/copy_dst/crt/{mode}"
        timed(
            "s3-copy boto3-tm-512MiB-x128",
            mode,
            sum(sizes[n] for n in names),
            lambda dst=dst, names=names: run_parallel(
                lambda n: crt_copy(client, f"{src_prefix}/{n}", f"{dst}/{n}"),
                [(n,) for n in names],
            ),
        )


def bench_gcs_upload(files: list[Path], scratch: str) -> None:
    """GCS upload: gcsfs at several chunk sizes vs. ``gcloud storage cp``."""
    for label, chunk in [
        ("gcsfs-default-50MiB", None),
        ("gcsfs-chunk256MiB", 256 * MiB),
    ]:
        for mode, subset in (("single", files[:1]), ("parallel", files)):
            fs = fsspec.filesystem("gcs", skip_instance_cache=True)
            put_kwargs = {"chunksize": chunk} if chunk else {}
            dst = f"{scratch}/up/{label}/{mode}"

            def one(f: Path, fs=fs, dst=dst, put_kwargs=put_kwargs) -> None:
                fs.put(str(f), f"{dst}/{f.name}", **put_kwargs)

            timed(
                f"gcs-upload {label}",
                mode,
                sum(f.stat().st_size for f in subset),
                lambda subset=subset, one=one: run_parallel(
                    one, [(f,) for f in subset]
                ),
            )
            safe_rm(fs, dst)

    for mode, subset in (("single", files[:1]), ("parallel", files)):
        dst = f"{scratch}/up/gcloud/{mode}"
        timed(
            "gcs-upload gcloud-storage-cp",
            mode,
            sum(f.stat().st_size for f in subset),
            lambda subset=subset, dst=dst: gcloud_cp(subset, dst),
        )


def bench_combined(files: list[Path], s3_scratch: str, gcs_scratch: str) -> None:
    """Upload to S3 and GCS at the same time, as the real deploy does."""
    nbytes = 2 * sum(f.stat().st_size for f in files)

    def fsspec_case(label: str, pool, conc, chunk) -> None:
        s3 = s3fs_fs(pool, conc)
        gcs = fsspec.filesystem("gcs", skip_instance_cache=True)
        s3_kwargs = {"chunksize": chunk} if chunk else {}
        s3_dst = f"{s3_scratch}/combined/{label}"
        gcs_dst = f"{gcs_scratch}/combined/{label}"

        def run() -> None:
            with ThreadPoolExecutor(max_workers=2 * len(files)) as ex:
                futs = []
                for f in files:
                    futs.append(
                        ex.submit(s3.put, str(f), f"{s3_dst}/{f.name}", **s3_kwargs)
                    )
                    futs.append(ex.submit(gcs.put, str(f), f"{gcs_dst}/{f.name}"))
                for fut in futs:
                    fut.result()

        timed(f"combined {label}", "s3+gcs", nbytes, run)
        safe_rm(s3, s3_dst)
        safe_rm(gcs, gcs_dst)

    fsspec_case("fsspec-default", None, None, None)
    fsspec_case("fsspec-pool256-conc64-64MiB", 256, 64, 64 * MiB)

    client = boto3_client()
    s3_dst = f"{s3_scratch}/combined/native"
    gcs_dst = f"{gcs_scratch}/combined/native"

    def run_native() -> None:
        with ThreadPoolExecutor(max_workers=len(files) + 1) as ex:
            futs = [
                ex.submit(crt_upload, client, f, f"{s3_dst}/{f.name}") for f in files
            ]
            futs.append(ex.submit(gcloud_cp, files, gcs_dst))
            for fut in futs:
                fut.result()

    timed("combined boto3-crt+gcloud", "s3+gcs", nbytes, run_native)
    safe_rm(fsspec.filesystem("s3", skip_instance_cache=True), s3_dst)
    safe_rm(fsspec.filesystem("gcs", skip_instance_cache=True), gcs_dst)


@click.command()
@click.option(
    "--source", default="gs://test.catalyst.coop/ferceqr/core_ferceqr__transactions"
)
@click.option("--s3-scratch", required=True, help="s3:// prefix we may write/delete.")
@click.option("--gcs-scratch", required=True, help="gs:// prefix we may write/delete.")
@click.option("--n-parallel", default=4, show_default=True)
@click.option("--workdir", default="/tmp/ferceqr_bench")  # noqa: S108
@click.option(
    "--only",
    type=click.Choice(["s3-upload", "s3-copy", "gcs-upload", "combined"]),
    multiple=True,
)
@click.option("--results", "results_uri", required=True, help="gs:// URI for JSON.")
def main(source, s3_scratch, gcs_scratch, n_parallel, workdir, only, results_uri):
    """Run the benchmarks and upload a JSON summary."""
    run_id = uuid.uuid4().hex[:8]
    s3_scratch = f"{s3_scratch.rstrip('/')}/{run_id}"
    gcs_scratch = f"{gcs_scratch.rstrip('/')}/{run_id}"
    workdir = Path(workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    only = set(only) or {"s3-upload", "s3-copy", "gcs-upload", "combined"}

    gcs = fsspec.filesystem("gcs")
    names = sorted(
        (p for p in gcs.ls(source, detail=True) if p["type"] == "file"),
        key=lambda p: -p["size"],
    )[:n_parallel]
    log(f"Fetching {len(names)} source files to {workdir}")
    files = []
    for info in names:
        dest = workdir / Path(info["name"]).name
        subprocess.run(  # noqa: S603
            ["gcloud", "storage", "cp", f"gs://{info['name']}", str(dest)],  # noqa: S607
            check=True,
        )
        files.append(dest)

    try:
        sections = {
            "s3-upload": lambda: bench_s3_upload(files, s3_scratch),
            "s3-copy": lambda: bench_s3_copy(files, s3_scratch),
            "gcs-upload": lambda: bench_gcs_upload(files, gcs_scratch),
            "combined": lambda: bench_combined(files, s3_scratch, gcs_scratch),
        }
        for name, run_section in sections.items():
            if name not in only:
                continue
            try:
                run_section()
            except Exception:
                log(f"SECTION {name} aborted:\n{traceback.format_exc()}")
    finally:
        log("Cleaning up scratch prefixes")
        for scratch in (s3_scratch, gcs_scratch):
            try:
                fs = fsspec.filesystem(
                    scratch.split("://")[0], skip_instance_cache=True
                )
                if fs.exists(scratch):
                    fs.rm(scratch, recursive=True)
            except Exception:
                log(f"cleanup of {scratch} failed:\n{traceback.format_exc()}")
        shutil.rmtree(workdir, ignore_errors=True)
        out = Path("/tmp/results.json")  # noqa: S108
        out.write_text(json.dumps(RESULTS, indent=2))
        subprocess.run(  # noqa: S603
            ["gcloud", "storage", "cp", str(out), results_uri],  # noqa: S607
            check=False,
        )
        log("RESULTS\n" + json.dumps(RESULTS, indent=2))


if __name__ == "__main__":
    main()
