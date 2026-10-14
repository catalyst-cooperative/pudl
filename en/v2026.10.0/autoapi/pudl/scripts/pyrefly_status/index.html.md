# pudl.scripts.pyrefly_status

Report on the current status of pyrefly typing in the repo.

Meant for the weekly baseline-ratcheting workflow: pick the module or error kind
with the biggest count, fix it, then shrink `.pyrefly-baseline.json`.

## Attributes

| [`_COVERAGE_COLUMNS`](#pudl.scripts.pyrefly_status._COVERAGE_COLUMNS)   |    |
|----------------------------------------------------------------------|----|

## Functions

| [`_run_pyrefly`](#pudl.scripts.pyrefly_status._run_pyrefly)(→ dict)               | Run a pyrefly subcommand and return its JSON stdout, parsed.             |
|-------------------------------------------------------------------------------------|--------------------------------------------------------------------------|
| [`_collect_live_errors`](#pudl.scripts.pyrefly_status._collect_live_errors)(→ list[dict]) | Run pyrefly directly and return its full, per-occurrence error list.     |
| [`_collect_coverage_report`](#pudl.scripts.pyrefly_status._collect_coverage_report)(→ dict)   | Run `pyrefly coverage report` on `src/` and return its parsed JSON.      |
| [`_print_counts`](#pudl.scripts.pyrefly_status._print_counts)(→ None)              |                                                                          |
| [`_relative_path`](#pudl.scripts.pyrefly_status._relative_path)(→ str)              | Display a coverage report path relative to the repo root when possible.  |
| [`_print_coverage_table`](#pudl.scripts.pyrefly_status._print_coverage_table)(→ None)      |                                                                          |
| [`main`](#pudl.scripts.pyrefly_status.main)(→ None)                       | Report on the current status of pyrefly typing in the repo.              |
| [`errors`](#pudl.scripts.pyrefly_status.errors)(→ None)                     | Summarize remaining pyrefly type errors by module and/or error kind.     |
| [`coverage`](#pudl.scripts.pyrefly_status.coverage)(→ None)                   | Summarize pyrefly coverage report's per-module type coverage as a table. |

## Module Contents

### pudl.scripts.pyrefly_status.\_run_pyrefly(args: [list](https://docs.python.org/3/builtins/stdtypes.html#list)[[str](https://docs.python.org/3/builtins/stdtypes.html#str)]) → [dict](https://docs.python.org/3/builtins/stdtypes.html#dict)

Run a pyrefly subcommand and return its JSON stdout, parsed.

Raises `click.ClickException` if pyrefly can’t be invoked or produces
unparsable output.

### pudl.scripts.pyrefly_status.\_collect_live_errors() → [list](https://docs.python.org/3/builtins/stdtypes.html#list)[[dict](https://docs.python.org/3/builtins/stdtypes.html#dict)]

Run pyrefly directly and return its full, per-occurrence error list.

### pudl.scripts.pyrefly_status.\_collect_coverage_report() → [dict](https://docs.python.org/3/builtins/stdtypes.html#dict)

Run `pyrefly coverage report` on `src/` and return its parsed JSON.

Restricted to `src/` since test modules aren’t expected to be fully typed. The
returned dict has `module_reports` (a list, one entry per module) and
`summary` (aggregate counts and coverage across all of `src/`).

### pudl.scripts.pyrefly_status.\_print_counts(errors: [list](https://docs.python.org/3/builtins/stdtypes.html#list)[[dict](https://docs.python.org/3/builtins/stdtypes.html#dict)], key: [str](https://docs.python.org/3/builtins/stdtypes.html#str), header: [str](https://docs.python.org/3/builtins/stdtypes.html#str), top: [int](https://docs.python.org/3/builtins/functions.html#int) | [None](https://docs.python.org/3/builtins/constants.html#None)) → [None](https://docs.python.org/3/builtins/constants.html#None)

### pudl.scripts.pyrefly_status.\_relative_path(path: [str](https://docs.python.org/3/builtins/stdtypes.html#str)) → [str](https://docs.python.org/3/builtins/stdtypes.html#str)

Display a coverage report path relative to the repo root when possible.

### pudl.scripts.pyrefly_status.\_COVERAGE_COLUMNS *= ['typable', 'typed', 'any', 'untyped', 'coverage']*

### pudl.scripts.pyrefly_status.\_print_coverage_table(report: [dict](https://docs.python.org/3/builtins/stdtypes.html#dict), sort_by: [str](https://docs.python.org/3/builtins/stdtypes.html#str), top: [int](https://docs.python.org/3/builtins/functions.html#int) | [None](https://docs.python.org/3/builtins/constants.html#None)) → [None](https://docs.python.org/3/builtins/constants.html#None)

### pudl.scripts.pyrefly_status.main() → [None](https://docs.python.org/3/builtins/constants.html#None)

Report on the current status of pyrefly typing in the repo.

Meant for the weekly baseline-ratcheting workflow: pick the module or error kind
with the biggest count, fix it, then shrink .pyrefly-baseline.json.

### pudl.scripts.pyrefly_status.errors(by: [str](https://docs.python.org/3/builtins/stdtypes.html#str), top: [int](https://docs.python.org/3/builtins/functions.html#int) | [None](https://docs.python.org/3/builtins/constants.html#None)) → [None](https://docs.python.org/3/builtins/constants.html#None)

Summarize remaining pyrefly type errors by module and/or error kind.

Runs pyrefly itself to get an exact, per-occurrence error count.

### pudl.scripts.pyrefly_status.coverage(sort_by: [str](https://docs.python.org/3/builtins/stdtypes.html#str), top: [int](https://docs.python.org/3/builtins/functions.html#int) | [None](https://docs.python.org/3/builtins/constants.html#None)) → [None](https://docs.python.org/3/builtins/constants.html#None)

Summarize pyrefly coverage report’s per-module type coverage as a table.

Sorted by ascending coverage (worst first) by default, so the least-typed
modules – the best ratcheting targets – show up at the top.
