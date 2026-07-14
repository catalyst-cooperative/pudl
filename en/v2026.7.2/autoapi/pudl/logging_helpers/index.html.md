# pudl.logging_helpers

Configure logging for the PUDL package.

## Attributes

| [`DEFAULT_DEPENDENCY_LOGLEVELS`](#pudl.logging_helpers.DEFAULT_DEPENDENCY_LOGLEVELS)   |    |
|----------------------------------------------------------------------------------------|----|

## Functions

| [`get_logger`](#pudl.logging_helpers.get_logger)(name)                         | Helper function to append 'catalystcoop' to logger name and return logger.   |
|--------------------------------------------------------------------------------|------------------------------------------------------------------------------|
| [`configure_root_logger`](#pudl.logging_helpers.configure_root_logger)(→ None) | Configure the root catalystcoop logger.                                      |

## Module Contents

### pudl.logging_helpers.DEFAULT_DEPENDENCY_LOGLEVELS *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [int](https://docs.python.org/3/library/functions.html#int)]*

### pudl.logging_helpers.get_logger(name: [str](https://docs.python.org/3/library/stdtypes.html#str))

Helper function to append ‘catalystcoop’ to logger name and return logger.

### pudl.logging_helpers.configure_root_logger(logfile: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, loglevel: Literal['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'] = 'INFO', dependency_loglevels: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [int](https://docs.python.org/3/library/functions.html#int)] | [None](https://docs.python.org/3/library/constants.html#None) = None, color_logs: [bool](https://docs.python.org/3/library/functions.html#bool) = True, propagate: [bool](https://docs.python.org/3/library/functions.html#bool) = False) → [None](https://docs.python.org/3/library/constants.html#None)

Configure the root catalystcoop logger.

* **Parameters:**
  * **logfile** – Path to logfile or None.
  * **loglevel** – Level of detail at which to log. Defaults to `INFO`.
  * **dependency_loglevels** – Dictionary mapping dependency name to desired loglevel.
    This allows us to filter excessive logs from dependencies.
  * **color_logs** – Whether to emit ANSI color codes. Defaults to `True`.
  * **propagate** – Whether to propagate logs to ancestor loggers. Useful for ensuring
    that pytest has access to PUDL logs during testing.
