#!/usr/bin/env bash
# fix-git-worktree.sh — runs INSIDE the container via postCreateCommand.
#
# The workspace's .git file references the host's absolute path to the main
# git directory (e.g. /Users/zane/code/catalyst/pudl/.git).  That path does
# not exist inside the container, but the main git directory IS mounted at
# /workspaces/pudl.git.
#
# This script creates a symlink inside the container at the path the .git file
# expects, pointing to the container mount.  The .git file itself is never
# modified, so host git is unaffected.

set -euo pipefail

# Capture the canonical path to the repository root within the container
WORKSPACE="$(cd "$(dirname "$0")/.." && pwd)"
CONTAINER_GIT_MOUNT="/workspaces/pudl.git"
GIT_FILE="${WORKSPACE}/.git"

if [ ! -d "${CONTAINER_GIT_MOUNT}/objects" ]; then
    echo "fix-git-worktree.sh: ${CONTAINER_GIT_MOUNT} is not mounted — git will be unavailable." >&2
    exit 0
fi

# The .git file contains: "gitdir: <host-path>/.git/worktrees/<name>"
# Two levels up from worktrees/<name> gives the host's main .git path.
CURRENT_GITDIR="$(sed 's/^gitdir: //' "${GIT_FILE}")"
HOST_MAIN_GIT="$(dirname "$(dirname "${CURRENT_GITDIR}")")"

# Create a symlink at the host path inside the container filesystem so that
# git can follow the reference in .git without us touching that file.
sudo mkdir -p "$(dirname "${HOST_MAIN_GIT}")"
sudo ln -sfn "${CONTAINER_GIT_MOUNT}" "${HOST_MAIN_GIT}"
echo "fix-git-worktree.sh: symlinked ${HOST_MAIN_GIT} → ${CONTAINER_GIT_MOUNT}" >&2

# Sanity check.
if git -C "${WORKSPACE}" rev-parse HEAD >/dev/null 2>&1; then
    echo "fix-git-worktree.sh: git rev-parse HEAD → $(git -C "${WORKSPACE}" rev-parse HEAD)" >&2
else
    echo "fix-git-worktree.sh: git not working (container may need git ≥ 2.48 for relativeWorktrees)." >&2
fi
