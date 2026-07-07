# pudl.scripts.update_zenodo_dois

Script to check each DOI in zenodo_dois.yml against Zenodo’s /versions/latest endpoint.

If there is a more current DOI, update to the latest one. This can be used to avoid
having to hand-update DOI values, and eventually to auto-update records that don’t
require hand mapping to extract in PUDL.

## Attributes

| [`logger`](#pudl.scripts.update_zenodo_dois.logger)   |    |
|-------------------------------------------------------|----|

## Functions

| [`get_latest_record_id`](#pudl.scripts.update_zenodo_dois.get_latest_record_id)(→ tuple[str | None, str | None])   | Get ID of the latest version of any Zenodo record.   |
|--------------------------------------------------------------------------------------------------------------------|------------------------------------------------------|
| [`update_yaml_dois`](#pudl.scripts.update_zenodo_dois.update_yaml_dois)(→ dict[str, dict])                         | Check all DOIs and update to latest record versions. |
| [`main`](#pudl.scripts.update_zenodo_dois.main)(→ None)                                                            | Auto-update Zenodo DOIs to the latest value.         |

## Module Contents

### pudl.scripts.update_zenodo_dois.logger

### pudl.scripts.update_zenodo_dois.get_latest_record_id(record_id: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None), [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)]

Get ID of the latest version of any Zenodo record.

Given the ID of any Zenodo record, this will return the record ID and DOI of the
latest version associated with the same concept DOI.

### pudl.scripts.update_zenodo_dois.update_yaml_dois(yaml_file: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path), datasets: [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[str](https://docs.python.org/3/library/stdtypes.html#str), Ellipsis]) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [dict](https://docs.python.org/3/library/stdtypes.html#dict)]

Check all DOIs and update to latest record versions.

### pudl.scripts.update_zenodo_dois.main(ctx: click.Context, datasets: [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[str](https://docs.python.org/3/library/stdtypes.html#str), Ellipsis]) → [None](https://docs.python.org/3/library/constants.html#None)

Auto-update Zenodo DOIs to the latest value.
