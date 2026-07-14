# pudl.scripts.check_path_permissions

Check read, write, and delete permissions for local or remote paths.

## Attributes

| [`NOISY_BACKEND_LOGGERS`](#pudl.scripts.check_path_permissions.NOISY_BACKEND_LOGGERS)   |    |
|-----------------------------------------------------------------------------------------|----|

## Exceptions

| [`PathPermissionError`](#pudl.scripts.check_path_permissions.PathPermissionError)   | Permission check failure annotated with the stage that failed.   |
|-------------------------------------------------------------------------------------|------------------------------------------------------------------|

## Classes

| [`PermissionCheck`](#pudl.scripts.check_path_permissions.PermissionCheck)   | Named permission-check stages used by the CLI and its reports.   |
|-----------------------------------------------------------------------------|------------------------------------------------------------------|
| [`CheckReport`](#pudl.scripts.check_path_permissions.CheckReport)           | Structured result for a single permission check.                 |
| [`PathReport`](#pudl.scripts.check_path_permissions.PathReport)             | Structured result for all permission checks against one path.    |
| [`PathCheckReport`](#pudl.scripts.check_path_permissions.PathCheckReport)   | Top-level report for a CLI invocation across one or more paths.  |

## Functions

| [`_get_ferceqr_deployment_paths`](#pudl.scripts.check_path_permissions._get_ferceqr_deployment_paths)(→ list[upath.UPath])   | Return resolved FERC EQR deployment targets as fully configured UPath objects.   |
|------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------|
| [`_build_upath`](#pudl.scripts.check_path_permissions._build_upath)(→ upath.UPath)                                           | Return a `UPath` with anon and any per-target options baked in.                  |
| [`_suppress_backend_tracebacks`](#pudl.scripts.check_path_permissions._suppress_backend_tracebacks)(→ Any)                   | Temporarily silence noisy storage-backend exception logging.                     |
| [`_ensure_directory_like_path`](#pudl.scripts.check_path_permissions._ensure_directory_like_path)(→ None)                    | Raise if the provided path points at an existing file/object.                    |
| [`check_read_access`](#pudl.scripts.check_path_permissions.check_read_access)(→ None)                                        | Raise if the given path cannot be read as a directory-like location.             |
| [`check_write_access`](#pudl.scripts.check_path_permissions.check_write_access)(→ None)                                      | Raise if a canary file cannot be written, read back, and deleted.                |
| [`_record_check_output`](#pudl.scripts.check_path_permissions._record_check_output)(→ None)                                  | Record a result message in the summary and optionally print it.                  |
| [`_record_check_outcome`](#pudl.scripts.check_path_permissions._record_check_outcome)(→ None)                                | Set the check outcome and record the message in one step.                        |
| [`_run_check`](#pudl.scripts.check_path_permissions._run_check)(→ None)                                                      | Run a single permission check and update the structured summary.                 |
| [`_check_single_path`](#pudl.scripts.check_path_permissions._check_single_path)(→ PathReport)                                | Run the requested checks for one path and return a structured summary.           |
| [`main`](#pudl.scripts.check_path_permissions.main)(→ int)                                                                   | Check path permissions using UPath for local filesystems and cloud buckets.      |

## Module Contents

### pudl.scripts.check_path_permissions.NOISY_BACKEND_LOGGERS *= ('fsspec', 'gcsfs', 'gcsfs.retry', 's3fs')*

### *class* pudl.scripts.check_path_permissions.PermissionCheck

Bases: [`enum.StrEnum`](https://docs.python.org/3/library/enum.html#enum.StrEnum)

Named permission-check stages used by the CLI and its reports.

#### READ *= 'read'*

#### WRITE *= 'write'*

#### DELETE *= 'delete'*

### *exception* pudl.scripts.check_path_permissions.PathPermissionError(message: [str](https://docs.python.org/3/library/stdtypes.html#str))

Bases: `click.ClickException`

Permission check failure annotated with the stage that failed.

#### check *: [PermissionCheck](#pudl.scripts.check_path_permissions.PermissionCheck)*

#### message *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### \_\_post_init_\_() → [None](https://docs.python.org/3/library/constants.html#None)

Initialize the underlying Click exception message.

### *class* pudl.scripts.check_path_permissions.CheckReport

Structured result for a single permission check.

#### requested *: [bool](https://docs.python.org/3/library/functions.html#bool)*

#### success *: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### messages *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= []*

#### errors *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= []*

### *class* pudl.scripts.check_path_permissions.PathReport

Structured result for all permission checks against one path.

#### path *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### resolved_path *: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)*

#### anon *: [bool](https://docs.python.org/3/library/functions.html#bool)*

#### checks *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[PermissionCheck](#pudl.scripts.check_path_permissions.PermissionCheck), [CheckReport](#pudl.scripts.check_path_permissions.CheckReport)]*

#### success *: [bool](https://docs.python.org/3/library/functions.html#bool)* *= False*

### *class* pudl.scripts.check_path_permissions.PathCheckReport

Top-level report for a CLI invocation across one or more paths.

#### paths *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]*

#### anon *: [bool](https://docs.python.org/3/library/functions.html#bool)*

#### results *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[PathReport](#pudl.scripts.check_path_permissions.PathReport)]*

#### success *: [bool](https://docs.python.org/3/library/functions.html#bool)* *= False*

### pudl.scripts.check_path_permissions.\_get_ferceqr_deployment_paths(anon: [bool](https://docs.python.org/3/library/functions.html#bool) = False) → [list](https://docs.python.org/3/library/stdtypes.html#list)[upath.UPath]

Return resolved FERC EQR deployment targets as fully configured UPath objects.

Each target is constructed with its YAML-defined `storage_options`, then the
global `--anon` flag is applied on top (so it can override per-target settings
if needed).

* **Parameters:**
  **anon** – Whether to force anonymous access for `gs://` or `s3://` targets.

### pudl.scripts.check_path_permissions.\_build_upath(path: [str](https://docs.python.org/3/library/stdtypes.html#str), anon: [bool](https://docs.python.org/3/library/functions.html#bool), storage_options: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any] | [None](https://docs.python.org/3/library/constants.html#None) = None) → upath.UPath

Return a `UPath` with anon and any per-target options baked in.

### pudl.scripts.check_path_permissions.\_suppress_backend_tracebacks() → Any

Temporarily silence noisy storage-backend exception logging.

### pudl.scripts.check_path_permissions.\_ensure_directory_like_path(path: upath.UPath) → [None](https://docs.python.org/3/library/constants.html#None)

Raise if the provided path points at an existing file/object.

### pudl.scripts.check_path_permissions.check_read_access(path: upath.UPath) → [None](https://docs.python.org/3/library/constants.html#None)

Raise if the given path cannot be read as a directory-like location.

### pudl.scripts.check_path_permissions.check_write_access(path: upath.UPath) → [None](https://docs.python.org/3/library/constants.html#None)

Raise if a canary file cannot be written, read back, and deleted.

### pudl.scripts.check_path_permissions.\_record_check_output(, check_name: [PermissionCheck](#pudl.scripts.check_path_permissions.PermissionCheck), message: [str](https://docs.python.org/3/library/stdtypes.html#str), json_output: [bool](https://docs.python.org/3/library/functions.html#bool), summary: [PathReport](#pudl.scripts.check_path_permissions.PathReport), is_error: [bool](https://docs.python.org/3/library/functions.html#bool) = False) → [None](https://docs.python.org/3/library/constants.html#None)

Record a result message in the summary and optionally print it.

### pudl.scripts.check_path_permissions.\_record_check_outcome(, check_name: [PermissionCheck](#pudl.scripts.check_path_permissions.PermissionCheck), success: [bool](https://docs.python.org/3/library/functions.html#bool), message: [str](https://docs.python.org/3/library/stdtypes.html#str), json_output: [bool](https://docs.python.org/3/library/functions.html#bool), summary: [PathReport](#pudl.scripts.check_path_permissions.PathReport)) → [None](https://docs.python.org/3/library/constants.html#None)

Set the check outcome and record the message in one step.

### pudl.scripts.check_path_permissions.\_run_check(, action: [PermissionCheck](#pudl.scripts.check_path_permissions.PermissionCheck), resolved_path: upath.UPath, json_output: [bool](https://docs.python.org/3/library/functions.html#bool), summary: [PathReport](#pudl.scripts.check_path_permissions.PathReport)) → [None](https://docs.python.org/3/library/constants.html#None)

Run a single permission check and update the structured summary.

### pudl.scripts.check_path_permissions.\_check_single_path(, path: upath.UPath, read_requested: [bool](https://docs.python.org/3/library/functions.html#bool), write_requested: [bool](https://docs.python.org/3/library/functions.html#bool), json_output: [bool](https://docs.python.org/3/library/functions.html#bool), anon: [bool](https://docs.python.org/3/library/functions.html#bool)) → [PathReport](#pudl.scripts.check_path_permissions.PathReport)

Run the requested checks for one path and return a structured summary.

### pudl.scripts.check_path_permissions.main(ctx: click.Context, paths: [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[str](https://docs.python.org/3/library/stdtypes.html#str), Ellipsis], read_requested: [bool](https://docs.python.org/3/library/functions.html#bool), write_requested: [bool](https://docs.python.org/3/library/functions.html#bool), json_output: [bool](https://docs.python.org/3/library/functions.html#bool), anon: [bool](https://docs.python.org/3/library/functions.html#bool), check_ferceqr_deployment_paths: [bool](https://docs.python.org/3/library/functions.html#bool)) → [int](https://docs.python.org/3/library/functions.html#int)

Check path permissions using UPath for local filesystems and cloud buckets.
