#!/usr/bin/env bash
# fix-pixi-shebangs.sh — runs INSIDE the container via postCreateCommand.
#
# The pudl-pixi Docker volume persists across container rebuilds. If the
# container workspace folder name changes (e.g. from a different worktree),
# all Python scripts in .pixi/envs/*/bin/ will have shebangs pointing to the
# old workspace path, making them unexecutable. This script patches them
# in-place without re-downloading or re-extracting packages.

set -euo pipefail

WORKSPACE="$(cd "$(dirname "$0")/.." && pwd)"
PIXI_ENVS="${WORKSPACE}/.pixi/envs"

if [ ! -d "${PIXI_ENVS}" ]; then
    echo "fix-pixi-shebangs.sh: no .pixi/envs directory found, nothing to fix." >&2
    exit 0
fi

# Find scripts in bin/ dirs whose shebang points to a different workspace path.
# grep -l: files containing any /workspaces/ shebang
# xargs grep -L: from those, exclude files already pointing to the correct path
STALE=$(find "${PIXI_ENVS}" -path "*/bin/*" -type f -print0 \
    | xargs -0 grep -lZ "^#!/workspaces/" 2>/dev/null \
    | xargs -0 grep -L "^#!${PIXI_ENVS}/" 2>/dev/null \
    || true)

if [ -z "${STALE}" ]; then
    echo "fix-pixi-shebangs.sh: all shebangs are current." >&2
    exit 0
fi

echo "${STALE}" | xargs sed -i \
    "1s|^#!/workspaces/[^/]*/.pixi/envs/|#!${PIXI_ENVS}/|"

echo "fix-pixi-shebangs.sh: patched $(echo "${STALE}" | wc -l) script(s)." >&2
