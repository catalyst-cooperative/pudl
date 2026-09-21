#!/usr/bin/bash
# Run the FERC EQR transfer benchmark inside the pudl-etl container on a Batch VM.
# Expects BATCH_JOB_ID and GCP_BILLING_PROJECT (and AWS credentials) in the
# environment. Temporary: used only by the benchmark takeover of the
# build-deploy-ferceqr workflow.
set -uo pipefail

RESULTS_BUCKET="gs://test.catalyst.coop/_bench"

# The pixi-provided gcloud lacks the gcloud-crc32c component, and without a TTY it
# cannot prompt to install it, so `gcloud storage cp` refuses to run. Never prompt,
# and skip hash checks when the fast implementation is unavailable.
export CLOUDSDK_CORE_DISABLE_PROMPTS=1
export CLOUDSDK_STORAGE_CHECK_HASHES=if_fast_else_skip

gcloud config set project "$GCP_BILLING_PROJECT" || exit 1

python devtools/ferceqr_transfer_benchmark.py \
    --s3-scratch s3://pudl.catalyst.coop/._ferceqr_bench \
    --gcs-scratch "$RESULTS_BUCKET" \
    --n-parallel 4 \
    --results "${RESULTS_BUCKET}/results-${BATCH_JOB_ID}.json" \
    2>&1 | tee /tmp/bench.log
rc=${PIPESTATUS[0]}

gcloud storage cp /tmp/bench.log "${RESULTS_BUCKET}/log-${BATCH_JOB_ID}.log"
exit "$rc"
