# pudl.scripts.pyrefly_diff_baseline

Diff the pyrefly baseline against a git ref by content, not by raw JSON text.

## Functions

| [`_load_baseline_entries`](#pudl.scripts.pyrefly_diff_baseline._load_baseline_entries)(→ set[tuple[str, str, str]])   | Load baseline error entries from a git ref, or the working tree if `ref` is None.   |
|--------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------|
| [`main`](#pudl.scripts.pyrefly_diff_baseline.main)(→ None)                                          | Diff a pyrefly baseline against a git ref by (file, code, description).             |

## Module Contents

### pudl.scripts.pyrefly_diff_baseline.\_load_baseline_entries(ref: [str](https://docs.python.org/3/builtins/stdtypes.html#str) | [None](https://docs.python.org/3/builtins/constants.html#None), baseline_path: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)) → [set](https://docs.python.org/3/builtins/stdtypes.html#set)[[tuple](https://docs.python.org/3/builtins/stdtypes.html#tuple)[[str](https://docs.python.org/3/builtins/stdtypes.html#str), [str](https://docs.python.org/3/builtins/stdtypes.html#str), [str](https://docs.python.org/3/builtins/stdtypes.html#str)]]

Load baseline error entries from a git ref, or the working tree if `ref` is None.

### pudl.scripts.pyrefly_diff_baseline.main(ref: [str](https://docs.python.org/3/builtins/stdtypes.html#str), baseline_path: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)) → [None](https://docs.python.org/3/builtins/constants.html#None)

Diff a pyrefly baseline against a git ref by (file, code, description).

Run this after `pixi run pyrefly-prune-baseline` and `pixi run
pyrefly-update-baseline` to sanity-check the regenerated baseline before committing
it: the “fixed” list should match what you intentionally fixed, and the “newly
baselined” list should only contain pre-existing issues you’re deliberately
deferring – not something your own change introduced.

A pyrefly version bump can also surface “newly baselined” entries that are purely
description-text reformatting of an existing error rather than a real new one; check
the file and error code before assuming it’s a regression.
