# pudl.scripts.generate_ferc_provenance

CLI for generating FERC SQLite provenance requirements for caching.

This tool generates a JSON representation of the inputs that affect the
materialization of FERC SQLite assets. This representation can be hashed
in CI to detect when the cached SQLite databases need to be rebuilt.

## Functions

| [`get_provenance`](#pudl.scripts.generate_ferc_provenance.get_provenance)(→ dict)   | Return the current provenance requirements for a FERC SQLite asset.       |
|-------------------------------------------------------------------------------------|---------------------------------------------------------------------------|
| [`main`](#pudl.scripts.generate_ferc_provenance.main)(→ int)                        | Generate a JSON representation of the provenance for a FERC SQLite asset. |

## Module Contents

### pudl.scripts.generate_ferc_provenance.get_provenance(dataset: [str](https://docs.python.org/3/library/stdtypes.html#str), data_format: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)

Return the current provenance requirements for a FERC SQLite asset.

* **Parameters:**
  * **dataset** – The name of the dataset (e.g. ‘ferc1’, ‘ferc714’).
  * **data_format** – The data format (‘dbf’ or ‘xbrl’).
* **Returns:**
  A dictionary containing the provenance requirements.

### pudl.scripts.generate_ferc_provenance.main(dataset: [str](https://docs.python.org/3/library/stdtypes.html#str), data_format: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [int](https://docs.python.org/3/library/functions.html#int)

Generate a JSON representation of the provenance for a FERC SQLite asset.

This output can be hashed to detect changes in raw data versions (DOIs),
data configurations (years), or extractor versions.
