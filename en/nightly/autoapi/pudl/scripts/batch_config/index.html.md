# pudl.scripts.batch_config

Generate a Google Batch Job configuration file.

This runs on bare GitHub Actions runners, without the full pixi/pudl environment – the
inline script metadata above lets `uv run` install just the handful of dependencies
this script actually needs (stdlib plus `click`) into an ephemeral environment, rather
than requiring a full pudl install first.

The `--container-*` flags are named after their equivalents in `gcloud compute
instances update-container`.

## Attributes

| [`logger`](#pudl.scripts.batch_config.logger)               |    |
|-----------------------------------------------------------------------|----|
| [`DEFAULT_MACHINE_TYPE`](#pudl.scripts.batch_config.DEFAULT_MACHINE_TYPE) |    |
| [`DEFAULT_DISK_GB`](#pudl.scripts.batch_config.DEFAULT_DISK_GB)      |    |
| [`DEFAULT_DISK_TYPE`](#pudl.scripts.batch_config.DEFAULT_DISK_TYPE)    |    |

## Functions

| [`_parse_container_env`](#pudl.scripts.batch_config._parse_container_env)(→ dict[str, str])   | Parse --container-env KEY=VALUE pairs into a dict.                        |
|-------------------------------------------------------------------------------------------|---------------------------------------------------------------------------|
| [`_lookup_machine_spec`](#pudl.scripts.batch_config._lookup_machine_spec)(→ tuple[int, int])  | Return `(cpuMilli, memoryMib)` for a real GCE machine type, via `gcloud`. |
| [`to_config`](#pudl.scripts.batch_config.to_config)(→ dict[str, Any])              | Munge arguments into a configuration dictionary.                          |
| [`main`](#pudl.scripts.batch_config.main)(→ None)                             | Generate a Batch configuration file.                                      |

## Module Contents

### pudl.scripts.batch_config.logger

### pudl.scripts.batch_config.DEFAULT_MACHINE_TYPE *= 'c4d-standard-8'*

### pudl.scripts.batch_config.DEFAULT_DISK_GB *= 250*

### pudl.scripts.batch_config.DEFAULT_DISK_TYPE *= 'hyperdisk-balanced'*

### pudl.scripts.batch_config.\_parse_container_env(container_env: [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[str](https://docs.python.org/3/library/stdtypes.html#str), ...]) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)]

Parse –container-env KEY=VALUE pairs into a dict.

Raises if the same key is given more than once.

### pudl.scripts.batch_config.\_lookup_machine_spec(machine_type: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[int](https://docs.python.org/3/library/functions.html#int), [int](https://docs.python.org/3/library/functions.html#int)]

Return `(cpuMilli, memoryMib)` for a real GCE machine type, via `gcloud`.

Batch’s `computeResource.cpuMilli`/`memoryMib` default to 2000/2000 (2 vCPU, 2
GB) if left unset – regardless of the machine type pinned in `allocationPolicy`.
This looks up the real values and fills them in so the job doesn’t lie about the
resources it has available.

### pudl.scripts.batch_config.to_config(, container_image: [str](https://docs.python.org/3/library/stdtypes.html#str), container_env: [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[str](https://docs.python.org/3/library/stdtypes.html#str), ...], container_command: [str](https://docs.python.org/3/library/stdtypes.html#str), container_arg: [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[str](https://docs.python.org/3/library/stdtypes.html#str), ...], machine_type: [str](https://docs.python.org/3/library/stdtypes.html#str), cpu_milli: [int](https://docs.python.org/3/library/functions.html#int), memory_mib: [int](https://docs.python.org/3/library/functions.html#int), disk_gb: [int](https://docs.python.org/3/library/functions.html#int), disk_type: [str](https://docs.python.org/3/library/stdtypes.html#str), batch_job_id: [str](https://docs.python.org/3/library/stdtypes.html#str), pipeline: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]

Munge arguments into a configuration dictionary.

### pudl.scripts.batch_config.main(container_image: [str](https://docs.python.org/3/library/stdtypes.html#str), container_command: [str](https://docs.python.org/3/library/stdtypes.html#str), container_env: [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[str](https://docs.python.org/3/library/stdtypes.html#str), ...], container_arg: [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[str](https://docs.python.org/3/library/stdtypes.html#str), ...], machine_type: [str](https://docs.python.org/3/library/stdtypes.html#str), disk_gb: [int](https://docs.python.org/3/library/functions.html#int), disk_type: [str](https://docs.python.org/3/library/stdtypes.html#str), batch_job_id: [str](https://docs.python.org/3/library/stdtypes.html#str), pipeline: [str](https://docs.python.org/3/library/stdtypes.html#str), output: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)) → [None](https://docs.python.org/3/library/constants.html#None)

Generate a Batch configuration file.
