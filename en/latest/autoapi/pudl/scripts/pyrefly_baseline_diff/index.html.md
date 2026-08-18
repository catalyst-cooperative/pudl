# pudl.scripts.pyrefly_baseline_diff

Diff the pyrefly baseline against a git ref by content, not by line number.

Raw JSON diffs of `.pyrefly-baseline.json` are dominated by line/column churn from
ordinary code changes and pyrefly version bumps – a single `--update-baseline` run
can touch hundreds of lines without any error actually appearing or disappearing. This
diffs baseline entries by `(file, error code, description)` instead, so the output
only shows errors that were genuinely fixed or newly introduced.

## Functions

| [`_load_baseline_entries`](#pudl.scripts.pyrefly_baseline_diff._load_baseline_entries)(→ set[tuple[str, str, str]])   | Load baseline error entries from a git ref, or the working tree if `ref` is None.   |
|--------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------|
| [`main`](#pudl.scripts.pyrefly_baseline_diff.main)(→ None)                                          | Diff a pyrefly baseline against a git ref by (file, code, description).             |

## Module Contents

### pudl.scripts.pyrefly_baseline_diff.\_load_baseline_entries(ref: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None), baseline_path: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)) → [set](https://docs.python.org/3/library/stdtypes.html#set)[[tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)]]

Load baseline error entries from a git ref, or the working tree if `ref` is None.

### pudl.scripts.pyrefly_baseline_diff.main(ref: [str](https://docs.python.org/3/library/stdtypes.html#str), baseline_path: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)) → [None](https://docs.python.org/3/library/constants.html#None)

Diff a pyrefly baseline against a git ref by (file, code, description).

Run this after `pyrefly check --baseline .pyrefly-baseline.json --update-baseline`
to sanity-check the regenerated baseline before committing it: the “fixed” list
should match what you intentionally fixed, and the “newly baselined” list should
only contain pre-existing issues you’re deliberately deferring – not something
your own change introduced.
