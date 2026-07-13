# pudl.extract.xbrl

Generic extractor for all FERC XBRL data.

## Attributes

| [`logger`](#pudl.extract.xbrl.logger)   |    |
|-----------------------------------------|----|

## Classes

| [`_FilteringStream`](#pudl.extract.xbrl._FilteringStream)   | Pass-through text stream that drops matching noisy lines.    |
|-------------------------------------------------------------|--------------------------------------------------------------|
| [`FercXbrlDatastore`](#pudl.extract.xbrl.FercXbrlDatastore) | Simple datastore wrapper for accessing ferc1 xbrl resources. |

## Functions

| [`_suppress_arelle_message_spam`](#pudl.extract.xbrl._suppress_arelle_message_spam)()   | Filter known Arelle console spam without suppressing normal logs.   |
|-----------------------------------------------------------------------------------------|---------------------------------------------------------------------|
| [`convert_form`](#pudl.extract.xbrl.convert_form)(→ None)                               | Clone a single FERC XBRL form to SQLite.                            |

## Module Contents

### pudl.extract.xbrl.logger

### *class* pudl.extract.xbrl.\_FilteringStream(wrapped, drop_patterns: [list](https://docs.python.org/3/library/stdtypes.html#list)[[re.Pattern](https://docs.python.org/3/library/re.html#re.Pattern)[[str](https://docs.python.org/3/library/stdtypes.html#str)]])

Pass-through text stream that drops matching noisy lines.

#### \_wrapped

#### \_drop_patterns

#### \_dropped_previous_line *= False*

#### write(text: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [int](https://docs.python.org/3/library/functions.html#int)

#### flush() → [None](https://docs.python.org/3/library/constants.html#None)

### pudl.extract.xbrl.\_suppress_arelle_message_spam()

Filter known Arelle console spam without suppressing normal logs.

### *class* pudl.extract.xbrl.FercXbrlDatastore(datastore: [pudl.workspace.datastore.Datastore](../../workspace/datastore/index.md#pudl.workspace.datastore.Datastore))

Simple datastore wrapper for accessing ferc1 xbrl resources.

#### datastore

#### get_taxonomy(form: [pudl.settings.FercForm](../../settings/index.md#pudl.settings.FercForm)) → [io.BytesIO](https://docs.python.org/3/library/io.html#io.BytesIO)

Returns the path to the taxonomy entry point within the an archive.

#### get_filings(year: [int](https://docs.python.org/3/library/functions.html#int), form: [pudl.settings.FercForm](../../settings/index.md#pudl.settings.FercForm)) → [io.BytesIO](https://docs.python.org/3/library/io.html#io.BytesIO)

Return the corresponding archive full of XBRL filings.

### pudl.extract.xbrl.convert_form(ferc_to_sqlite: [pudl.settings.FercToSqliteDataConfig](../../settings/index.md#pudl.settings.FercToSqliteDataConfig), form: [pudl.settings.FercForm](../../settings/index.md#pudl.settings.FercForm), datastore: [FercXbrlDatastore](#pudl.extract.xbrl.FercXbrlDatastore), pudl_paths: [pudl.workspace.setup.PudlPaths](../../workspace/setup/index.md#pudl.workspace.setup.PudlPaths), batch_size: [int](https://docs.python.org/3/library/functions.html#int) | [None](https://docs.python.org/3/library/constants.html#None) = None, workers: [int](https://docs.python.org/3/library/functions.html#int) | [None](https://docs.python.org/3/library/constants.html#None) = None, loglevel: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'INFO') → [None](https://docs.python.org/3/library/constants.html#None)

Clone a single FERC XBRL form to SQLite.

* **Parameters:**
  * **ferc_to_sqlite** – Validated data configuration for converting FERC data to SQLite.
  * **form** – FERC form number.
  * **datastore** – Instance of a FERC XBRL datastore for retrieving data.
  * **pudl_paths** – `PudlPaths` resource.
  * **batch_size** – Number of XBRL filings to process in a single CPU process.
  * **workers** – Number of CPU processes to create for processing XBRL filings.
  * **loglevel** – Log level to pass to `ferc_xbrl_extractor`.
