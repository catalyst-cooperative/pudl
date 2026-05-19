# Devcontainer Paths

This devcontainer expects the host to provide three directories before the container starts:

- `PUDL_INPUT`
- `PUDL_OUTPUT`
- `DAGSTER_HOME`

The simplest way to pass them in is to export the paths in the shell that launches VS Code, then reopen the repository in the container.

```bash
mkdir -p "$HOME/pudl-dev/input" "$HOME/pudl-dev/output" "$HOME/pudl-dev/dagster-home"
export PUDL_INPUT="$HOME/pudl-dev/input"
export PUDL_OUTPUT="$HOME/pudl-dev/output"
export DAGSTER_HOME="$HOME/pudl-dev/dagster-home"
code /Users/zane/code/catalyst/pudl-worktrees/devcontainer
```

The repository itself uses the normal Dev Containers workspace mount for whatever folder you open in VS Code.
The exact in-container path is chosen by the Dev Containers extension, typically under `/workspaces/`.

The extra host directories are bind-mounted separately and then exposed to PUDL as:

- `PUDL_INPUT=/workspaces/pudl-input`
- `PUDL_OUTPUT=/workspaces/pudl-output`
- `DAGSTER_HOME=/workspaces/dagster-home`

That keeps the Git worktree handling on the standard Dev Containers path while still giving PUDL stable paths for its input, output, and Dagster state directories.

Notes:

- `DAGSTER_HOME` is intentionally left as a plain directory with no checked-in `dagster.yaml`, so local development uses Dagster's default SQLite-backed instance storage.
- If VS Code is launched from the macOS GUI instead of from a shell, it may not inherit these environment variables. In that case, the easiest fix is to launch `code` from a shell where the variables are already exported.
