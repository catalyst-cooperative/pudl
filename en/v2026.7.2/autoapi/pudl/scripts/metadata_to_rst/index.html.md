# pudl.scripts.metadata_to_rst

Export PUDL table and field metadata to RST for use in documentation.

## Functions

| [`main`](#pudl.scripts.metadata_to_rst.main)(→ int)   | Export PUDL table and field metadata to RST for use in documentation.   |
|-------------------------------------------------------|-------------------------------------------------------------------------|

## Module Contents

### pudl.scripts.metadata_to_rst.main(skip: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], output: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path), docs_dir: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path), logfile: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path), loglevel: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [int](https://docs.python.org/3/library/functions.html#int)

Export PUDL table and field metadata to RST for use in documentation.

metadata_to_rst -s bad_table1 -s bad_table2 -d ./pudl/docs -o ./datadict.rst
