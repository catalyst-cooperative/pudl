# pudl.scripts.zenodo_data_release

Upload a prepared PUDL data release directory to Zenodo.

The PUDL data release process produces a directory of artifacts (zipped Parquet files,
SQLite databases, JSON metadata, logs, etc.) that are uploaded to CERN’s Zenodo data
repository for long-term archival access. Each new versioned release of PUDL is
associated with the same original PUDL concept DOI.

This module provides a CLI that handles the process of uploading a new PUDL data release
to Zenodo, given a prepared directory of artifacts typically produced by the PUDL builds.

It uses state objects to ensure that Zenodo API calls happen in a valid order. The files
to upload are read using `fsspec` and remote files are staged locally one at a time
so uploads can be retried, but without using excessive local disk space.

Retries are implemented for all upload requests to recover from transient network issues
and Zenodo server flakiness. Zero-byte uploads are prevented.

NOTE: PUDL nightly build outputs are NOT suitable for producing a Zenodo data release
unless the Parquet outputs are filtered out with an appropriate ignore_regex. Double
check what files should actually be distributed before running the script.

Run `zenodo_data_release --help` for CLI usage instructions.

## Attributes

| [`SANDBOX`](#pudl.scripts.zenodo_data_release.SANDBOX)                               |    |
|--------------------------------------------------------------------------------------|----|
| [`PRODUCTION`](#pudl.scripts.zenodo_data_release.PRODUCTION)                         |    |
| [`RETRYABLE_STATUS_CODES`](#pudl.scripts.zenodo_data_release.RETRYABLE_STATUS_CODES) |    |
| [`logger`](#pudl.scripts.zenodo_data_release.logger)                                 |    |

## Classes

| [`_LegacyLinks`](#pudl.scripts.zenodo_data_release._LegacyLinks)           | !!! abstract "Usage Documentation"                                |
|----------------------------------------------------------------------------|-------------------------------------------------------------------|
| [`_LegacyMetadata`](#pudl.scripts.zenodo_data_release._LegacyMetadata)     | !!! abstract "Usage Documentation"                                |
| [`_LegacyDeposition`](#pudl.scripts.zenodo_data_release._LegacyDeposition) | !!! abstract "Usage Documentation"                                |
| [`_NewFile`](#pudl.scripts.zenodo_data_release._NewFile)                   | !!! abstract "Usage Documentation"                                |
| [`_NewRecord`](#pudl.scripts.zenodo_data_release._NewRecord)               | !!! abstract "Usage Documentation"                                |
| [`ZenodoClient`](#pudl.scripts.zenodo_data_release.ZenodoClient)           | Thin wrapper over Zenodo REST API.                                |
| [`State`](#pudl.scripts.zenodo_data_release.State)                         | Parent class for dataset states.                                  |
| [`InitialDataset`](#pudl.scripts.zenodo_data_release.InitialDataset)       | Represent initial dataset state.                                  |
| [`EmptyDraft`](#pudl.scripts.zenodo_data_release.EmptyDraft)               | We can only sync the directory once we've gotten an empty draft.  |
| [`ContentComplete`](#pudl.scripts.zenodo_data_release.ContentComplete)     | Now that we've uploaded all the data, we need to update metadata. |
| [`CompleteDraft`](#pudl.scripts.zenodo_data_release.CompleteDraft)         | Now that we've uploaded all the data, we can publish.             |

## Functions

| [`build_zenodo_release_zulip_message`](#pudl.scripts.zenodo_data_release.build_zenodo_release_zulip_message)(→ str)   | Build a markdown Zulip message summarizing a Zenodo release attempt.   |
|-----------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------|
| [`main`](#pudl.scripts.zenodo_data_release.main)(→ int)                                                               | Publish a new PUDL data release to Zenodo.                             |

## Module Contents

### pudl.scripts.zenodo_data_release.SANDBOX *= 'sandbox'*

### pudl.scripts.zenodo_data_release.PRODUCTION *= 'production'*

### pudl.scripts.zenodo_data_release.RETRYABLE_STATUS_CODES

### pudl.scripts.zenodo_data_release.logger

### *class* pudl.scripts.zenodo_data_release.\_LegacyLinks(/, \*\*data: Any)

Bases: [`pydantic.BaseModel`](https://pydantic.dev/docs/validation/latest/api/pydantic/base_model/#pydantic.BaseModel)

!!! abstract “Usage Documentation”
: [Models](../concepts/models.md)

A base class for creating Pydantic models.

#### \_\_class_vars_\_

The names of the class variables defined on the model.

#### \_\_private_attributes_\_

Metadata about the private attributes of the model.

#### \_\_signature_\_

The synthesized \_\_init_\_ [Signature][inspect.Signature] of the model.

#### \_\_pydantic_complete_\_

Whether model building is completed, or if there are still undefined fields.

#### \_\_pydantic_core_schema_\_

The core schema of the model.

#### \_\_pydantic_custom_init_\_

Whether the model has a custom \_\_init_\_ function.

#### \_\_pydantic_decorators_\_

Metadata containing the decorators defined on the model.
This replaces Model._\_validators_\_ and Model._\_root_validators_\_ from Pydantic V1.

#### \_\_pydantic_generic_metadata_\_

A dictionary containing metadata about generic Pydantic models.
The origin and args items map to the [\_\_origin_\_][genericalias._\_origin_\_]
and [\_\_args_\_][genericalias._\_args_\_] attributes of [generic aliases][types-genericalias],
and the parameter item maps to the \_\_parameter_\_ attribute of generic classes.

#### \_\_pydantic_parent_namespace_\_

Parent namespace of the model, used for automatic rebuilding of models.

#### \_\_pydantic_post_init_\_

The name of the post-init method for the model, if defined.

#### \_\_pydantic_root_model_\_

Whether the model is a [RootModel][pydantic.root_model.RootModel].

#### \_\_pydantic_serializer_\_

The pydantic-core SchemaSerializer used to dump instances of the model.

#### \_\_pydantic_validator_\_

The pydantic-core SchemaValidator used to validate instances of the model.

#### \_\_pydantic_fields_\_

A dictionary of field names and their corresponding [FieldInfo][pydantic.fields.FieldInfo] objects.

#### \_\_pydantic_computed_fields_\_

A dictionary of computed field names and their corresponding [ComputedFieldInfo][pydantic.fields.ComputedFieldInfo] objects.

#### \_\_pydantic_extra_\_

A dictionary containing extra values, if [extra][pydantic.config.ConfigDict.extra]
is set to ‘allow’.

#### \_\_pydantic_fields_set_\_

The names of fields explicitly set during instantiation.

#### \_\_pydantic_private_\_

Values of private attributes set on the model instance.

#### html *: [pydantic.AnyHttpUrl](https://pydantic.dev/docs/validation/latest/api/pydantic/networks/#pydantic.networks.AnyHttpUrl)*

#### bucket *: [pydantic.AnyHttpUrl](https://pydantic.dev/docs/validation/latest/api/pydantic/networks/#pydantic.networks.AnyHttpUrl)*

### *class* pudl.scripts.zenodo_data_release.\_LegacyMetadata(/, \*\*data: Any)

Bases: [`pydantic.BaseModel`](https://pydantic.dev/docs/validation/latest/api/pydantic/base_model/#pydantic.BaseModel)

!!! abstract “Usage Documentation”
: [Models](../concepts/models.md)

A base class for creating Pydantic models.

#### \_\_class_vars_\_

The names of the class variables defined on the model.

#### \_\_private_attributes_\_

Metadata about the private attributes of the model.

#### \_\_signature_\_

The synthesized \_\_init_\_ [Signature][inspect.Signature] of the model.

#### \_\_pydantic_complete_\_

Whether model building is completed, or if there are still undefined fields.

#### \_\_pydantic_core_schema_\_

The core schema of the model.

#### \_\_pydantic_custom_init_\_

Whether the model has a custom \_\_init_\_ function.

#### \_\_pydantic_decorators_\_

Metadata containing the decorators defined on the model.
This replaces Model._\_validators_\_ and Model._\_root_validators_\_ from Pydantic V1.

#### \_\_pydantic_generic_metadata_\_

A dictionary containing metadata about generic Pydantic models.
The origin and args items map to the [\_\_origin_\_][genericalias._\_origin_\_]
and [\_\_args_\_][genericalias._\_args_\_] attributes of [generic aliases][types-genericalias],
and the parameter item maps to the \_\_parameter_\_ attribute of generic classes.

#### \_\_pydantic_parent_namespace_\_

Parent namespace of the model, used for automatic rebuilding of models.

#### \_\_pydantic_post_init_\_

The name of the post-init method for the model, if defined.

#### \_\_pydantic_root_model_\_

Whether the model is a [RootModel][pydantic.root_model.RootModel].

#### \_\_pydantic_serializer_\_

The pydantic-core SchemaSerializer used to dump instances of the model.

#### \_\_pydantic_validator_\_

The pydantic-core SchemaValidator used to validate instances of the model.

#### \_\_pydantic_fields_\_

A dictionary of field names and their corresponding [FieldInfo][pydantic.fields.FieldInfo] objects.

#### \_\_pydantic_computed_fields_\_

A dictionary of computed field names and their corresponding [ComputedFieldInfo][pydantic.fields.ComputedFieldInfo] objects.

#### \_\_pydantic_extra_\_

A dictionary containing extra values, if [extra][pydantic.config.ConfigDict.extra]
is set to ‘allow’.

#### \_\_pydantic_fields_set_\_

The names of fields explicitly set during instantiation.

#### \_\_pydantic_private_\_

Values of private attributes set on the model instance.

#### upload_type *: [str](https://docs.python.org/3/library/stdtypes.html#str)* *= 'dataset'*

#### title *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### access_right *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### creators *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[dict](https://docs.python.org/3/library/stdtypes.html#dict)]*

#### license *: [str](https://docs.python.org/3/library/stdtypes.html#str)* *= 'cc-by-4.0'*

#### publication_date *: [str](https://docs.python.org/3/library/stdtypes.html#str)* *= ''*

#### description *: [str](https://docs.python.org/3/library/stdtypes.html#str)* *= ''*

### *class* pudl.scripts.zenodo_data_release.\_LegacyDeposition(/, \*\*data: Any)

Bases: [`pydantic.BaseModel`](https://pydantic.dev/docs/validation/latest/api/pydantic/base_model/#pydantic.BaseModel)

!!! abstract “Usage Documentation”
: [Models](../concepts/models.md)

A base class for creating Pydantic models.

#### \_\_class_vars_\_

The names of the class variables defined on the model.

#### \_\_private_attributes_\_

Metadata about the private attributes of the model.

#### \_\_signature_\_

The synthesized \_\_init_\_ [Signature][inspect.Signature] of the model.

#### \_\_pydantic_complete_\_

Whether model building is completed, or if there are still undefined fields.

#### \_\_pydantic_core_schema_\_

The core schema of the model.

#### \_\_pydantic_custom_init_\_

Whether the model has a custom \_\_init_\_ function.

#### \_\_pydantic_decorators_\_

Metadata containing the decorators defined on the model.
This replaces Model._\_validators_\_ and Model._\_root_validators_\_ from Pydantic V1.

#### \_\_pydantic_generic_metadata_\_

A dictionary containing metadata about generic Pydantic models.
The origin and args items map to the [\_\_origin_\_][genericalias._\_origin_\_]
and [\_\_args_\_][genericalias._\_args_\_] attributes of [generic aliases][types-genericalias],
and the parameter item maps to the \_\_parameter_\_ attribute of generic classes.

#### \_\_pydantic_parent_namespace_\_

Parent namespace of the model, used for automatic rebuilding of models.

#### \_\_pydantic_post_init_\_

The name of the post-init method for the model, if defined.

#### \_\_pydantic_root_model_\_

Whether the model is a [RootModel][pydantic.root_model.RootModel].

#### \_\_pydantic_serializer_\_

The pydantic-core SchemaSerializer used to dump instances of the model.

#### \_\_pydantic_validator_\_

The pydantic-core SchemaValidator used to validate instances of the model.

#### \_\_pydantic_fields_\_

A dictionary of field names and their corresponding [FieldInfo][pydantic.fields.FieldInfo] objects.

#### \_\_pydantic_computed_fields_\_

A dictionary of computed field names and their corresponding [ComputedFieldInfo][pydantic.fields.ComputedFieldInfo] objects.

#### \_\_pydantic_extra_\_

A dictionary containing extra values, if [extra][pydantic.config.ConfigDict.extra]
is set to ‘allow’.

#### \_\_pydantic_fields_set_\_

The names of fields explicitly set during instantiation.

#### \_\_pydantic_private_\_

Values of private attributes set on the model instance.

#### id_ *: [int](https://docs.python.org/3/library/functions.html#int)* *= None*

#### conceptrecid *: [int](https://docs.python.org/3/library/functions.html#int)*

#### links *: [\_LegacyLinks](#pudl.scripts.zenodo_data_release._LegacyLinks)*

#### metadata *: [\_LegacyMetadata](#pudl.scripts.zenodo_data_release._LegacyMetadata)*

#### submitted *: [bool](https://docs.python.org/3/library/functions.html#bool)* *= False*

### *class* pudl.scripts.zenodo_data_release.\_NewFile(/, \*\*data: Any)

Bases: [`pydantic.BaseModel`](https://pydantic.dev/docs/validation/latest/api/pydantic/base_model/#pydantic.BaseModel)

!!! abstract “Usage Documentation”
: [Models](../concepts/models.md)

A base class for creating Pydantic models.

#### \_\_class_vars_\_

The names of the class variables defined on the model.

#### \_\_private_attributes_\_

Metadata about the private attributes of the model.

#### \_\_signature_\_

The synthesized \_\_init_\_ [Signature][inspect.Signature] of the model.

#### \_\_pydantic_complete_\_

Whether model building is completed, or if there are still undefined fields.

#### \_\_pydantic_core_schema_\_

The core schema of the model.

#### \_\_pydantic_custom_init_\_

Whether the model has a custom \_\_init_\_ function.

#### \_\_pydantic_decorators_\_

Metadata containing the decorators defined on the model.
This replaces Model._\_validators_\_ and Model._\_root_validators_\_ from Pydantic V1.

#### \_\_pydantic_generic_metadata_\_

A dictionary containing metadata about generic Pydantic models.
The origin and args items map to the [\_\_origin_\_][genericalias._\_origin_\_]
and [\_\_args_\_][genericalias._\_args_\_] attributes of [generic aliases][types-genericalias],
and the parameter item maps to the \_\_parameter_\_ attribute of generic classes.

#### \_\_pydantic_parent_namespace_\_

Parent namespace of the model, used for automatic rebuilding of models.

#### \_\_pydantic_post_init_\_

The name of the post-init method for the model, if defined.

#### \_\_pydantic_root_model_\_

Whether the model is a [RootModel][pydantic.root_model.RootModel].

#### \_\_pydantic_serializer_\_

The pydantic-core SchemaSerializer used to dump instances of the model.

#### \_\_pydantic_validator_\_

The pydantic-core SchemaValidator used to validate instances of the model.

#### \_\_pydantic_fields_\_

A dictionary of field names and their corresponding [FieldInfo][pydantic.fields.FieldInfo] objects.

#### \_\_pydantic_computed_fields_\_

A dictionary of computed field names and their corresponding [ComputedFieldInfo][pydantic.fields.ComputedFieldInfo] objects.

#### \_\_pydantic_extra_\_

A dictionary containing extra values, if [extra][pydantic.config.ConfigDict.extra]
is set to ‘allow’.

#### \_\_pydantic_fields_set_\_

The names of fields explicitly set during instantiation.

#### \_\_pydantic_private_\_

Values of private attributes set on the model instance.

#### id_ *: [str](https://docs.python.org/3/library/stdtypes.html#str)* *= None*

### *class* pudl.scripts.zenodo_data_release.\_NewRecord(/, \*\*data: Any)

Bases: [`pydantic.BaseModel`](https://pydantic.dev/docs/validation/latest/api/pydantic/base_model/#pydantic.BaseModel)

!!! abstract “Usage Documentation”
: [Models](../concepts/models.md)

A base class for creating Pydantic models.

#### \_\_class_vars_\_

The names of the class variables defined on the model.

#### \_\_private_attributes_\_

Metadata about the private attributes of the model.

#### \_\_signature_\_

The synthesized \_\_init_\_ [Signature][inspect.Signature] of the model.

#### \_\_pydantic_complete_\_

Whether model building is completed, or if there are still undefined fields.

#### \_\_pydantic_core_schema_\_

The core schema of the model.

#### \_\_pydantic_custom_init_\_

Whether the model has a custom \_\_init_\_ function.

#### \_\_pydantic_decorators_\_

Metadata containing the decorators defined on the model.
This replaces Model._\_validators_\_ and Model._\_root_validators_\_ from Pydantic V1.

#### \_\_pydantic_generic_metadata_\_

A dictionary containing metadata about generic Pydantic models.
The origin and args items map to the [\_\_origin_\_][genericalias._\_origin_\_]
and [\_\_args_\_][genericalias._\_args_\_] attributes of [generic aliases][types-genericalias],
and the parameter item maps to the \_\_parameter_\_ attribute of generic classes.

#### \_\_pydantic_parent_namespace_\_

Parent namespace of the model, used for automatic rebuilding of models.

#### \_\_pydantic_post_init_\_

The name of the post-init method for the model, if defined.

#### \_\_pydantic_root_model_\_

Whether the model is a [RootModel][pydantic.root_model.RootModel].

#### \_\_pydantic_serializer_\_

The pydantic-core SchemaSerializer used to dump instances of the model.

#### \_\_pydantic_validator_\_

The pydantic-core SchemaValidator used to validate instances of the model.

#### \_\_pydantic_fields_\_

A dictionary of field names and their corresponding [FieldInfo][pydantic.fields.FieldInfo] objects.

#### \_\_pydantic_computed_fields_\_

A dictionary of computed field names and their corresponding [ComputedFieldInfo][pydantic.fields.ComputedFieldInfo] objects.

#### \_\_pydantic_extra_\_

A dictionary containing extra values, if [extra][pydantic.config.ConfigDict.extra]
is set to ‘allow’.

#### \_\_pydantic_fields_set_\_

The names of fields explicitly set during instantiation.

#### \_\_pydantic_private_\_

Values of private attributes set on the model instance.

#### id_ *: [int](https://docs.python.org/3/library/functions.html#int)* *= None*

#### files *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[\_NewFile](#pudl.scripts.zenodo_data_release._NewFile)]*

### *class* pudl.scripts.zenodo_data_release.ZenodoClient(env: [str](https://docs.python.org/3/library/stdtypes.html#str))

Thin wrapper over Zenodo REST API.

Mostly legacy calls ([https://developers.zenodo.org/](https://developers.zenodo.org/)) (archive:
[https://web.archive.org/web/20231212025359/https://developers.zenodo.org/](https://web.archive.org/web/20231212025359/https://developers.zenodo.org/))
but due to inconsistent behavior of legacy API on sandbox environment, we
need some of the unreleased new API endpoints too:
[https://inveniordm.docs.cern.ch/reference/rest_api_drafts_records/](https://inveniordm.docs.cern.ch/reference/rest_api_drafts_records/)

#### auth_headers

#### retry_request(, method, url, max_tries: [int](https://docs.python.org/3/library/functions.html#int) = 6, request_timeout: [float](https://docs.python.org/3/library/functions.html#float) | [None](https://docs.python.org/3/library/constants.html#None) = None, data_factory: [collections.abc.Callable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[], IO[[bytes](https://docs.python.org/3/library/stdtypes.html#bytes)]] | [None](https://docs.python.org/3/library/constants.html#None) = None, \*\*kwargs) → requests.Response

Retry calls to `requests.request` with exponential backoff.

* **Parameters:**
  * **method** – HTTP method to use for the request (e.g. `GET`).
  * **url** – Fully-qualified URL to which the request is sent.
  * **max_tries** – Maximum number of attempts before surfacing an error.
  * **request_timeout** – Optional per-request timeout in seconds. When `None` the
    timeout grows exponentially (`2**attempt`).
  * **data_factory** – Optional callable that yields a fresh binary stream for each
    attempt. Useful for uploads that require reopening a file-like object.
  * **\*\*kwargs** – Additional keyword arguments passed through directly to
    `requests.request`.
* **Returns:**
  The `requests.Response` produced by the successful attempt.
* **Raises:**
  * **requests.RequestException** – If all attempts fail with a requests error.
  * [**OSError**](https://docs.python.org/3/library/exceptions.html#OSError) – If reading from disk fails when preparing a payload.
  * [**RuntimeError**](https://docs.python.org/3/library/exceptions.html#RuntimeError) – If no response object is produced (should be rare).

#### get_deposition(deposition_id: [int](https://docs.python.org/3/library/functions.html#int)) → [\_LegacyDeposition](#pudl.scripts.zenodo_data_release._LegacyDeposition)

LEGACY API: Get JSON describing a deposition.

Depositions can be published *or* unpublished.

#### get_record(record_id: [int](https://docs.python.org/3/library/functions.html#int)) → [\_NewRecord](#pudl.scripts.zenodo_data_release._NewRecord)

NEW API: Get JSON describing a record.

All records are published records.

#### new_record_version(record_id: [int](https://docs.python.org/3/library/functions.html#int)) → [\_NewRecord](#pudl.scripts.zenodo_data_release._NewRecord)

NEW API: get or create the draft associated with a record ID.

Finds the latest record in the concept that record_id points to, and
makes a new version unless one exists already.

#### update_deposition_metadata(deposition_id: [int](https://docs.python.org/3/library/functions.html#int), metadata: [\_LegacyMetadata](#pudl.scripts.zenodo_data_release._LegacyMetadata)) → [\_LegacyDeposition](#pudl.scripts.zenodo_data_release._LegacyDeposition)

LEGACY API: Update deposition metadata.

Replaces the existing metadata completely - so make sure to pass in complete
metadata. You cannot update metadata fields one at a time.

#### delete_deposition_file(deposition_id: [int](https://docs.python.org/3/library/functions.html#int), file_id) → requests.Response

LEGACY API: Delete file from deposition.

Note: file_id is not always the file name.

#### create_bucket_file(bucket_url: [pydantic.AnyHttpUrl](https://pydantic.dev/docs/validation/latest/api/pydantic/networks/#pydantic.networks.AnyHttpUrl), file_path: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path), max_tries: [int](https://docs.python.org/3/library/functions.html#int) = 6) → requests.Response

LEGACY API: Upload a file to a deposition’s file bucket.

We prefer this API this over the /deposit/depositions/{id}/files endpoint
because it allows for files >100MB.

* **Parameters:**
  * **bucket_url** – Upload destination returned by Zenodo for the draft.
  * **file_path** – Local path to the artifact being uploaded.
  * **max_tries** – Maximum number of upload attempts before failing.
* **Returns:**
  The `requests.Response` from the successful upload attempt.
* **Raises:**
  * [**ValueError**](https://docs.python.org/3/library/exceptions.html#ValueError) – If `file_path` is empty.
  * **requests.RequestException** – If all upload attempts fail.

#### publish_deposition(deposition_id: [int](https://docs.python.org/3/library/functions.html#int)) → [\_LegacyDeposition](#pudl.scripts.zenodo_data_release._LegacyDeposition)

LEGACY API: publish deposition.

The publish action isn’t safely retriable: if a request times out after
Zenodo already processed it server-side, a retried POST to the same
`actions/publish` URL 404s, since a deposition that’s already published no
longer has a pending publish action – even though the publish itself
succeeded. Rather than fail on that specific 404, check whether the
deposition is actually already published before giving up.

### *class* pudl.scripts.zenodo_data_release.State

Parent class for dataset states.

Provides an abstraction layer that hides Zenodo’s data model from the caller.

Subclasses + their limited method definitions provide a way to avoid calling the
operations in the wrong order.

#### record_id *: [int](https://docs.python.org/3/library/functions.html#int)*

#### zenodo_client *: [ZenodoClient](#pudl.scripts.zenodo_data_release.ZenodoClient)*

### *class* pudl.scripts.zenodo_data_release.InitialDataset

Bases: [`State`](#pudl.scripts.zenodo_data_release.State)

Represent initial dataset state.

At this point, we don’t know if there is an existing draft or not - the only thing
we can do is try to get a fresh draft.

#### get_empty_draft() → [EmptyDraft](#pudl.scripts.zenodo_data_release.EmptyDraft)

Get an empty draft for this dataset.

Use new API to get any draft, then use legacy API to delete any files
in the draft.

### *class* pudl.scripts.zenodo_data_release.EmptyDraft

Bases: [`State`](#pudl.scripts.zenodo_data_release.State)

We can only sync the directory once we’ve gotten an empty draft.

#### *static* \_sync_local_path(openable_file: fsspec.core.OpenFile, staging_dir: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)) → [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)

Ensure the given `fsspec` file exists on the local filesystem.

When `openable_file` already resides on the local filesystem we avoid
copying and return its existing path. Remote files are downloaded into
`staging_dir` (a shared temporary directory) so the rest of the upload
pipeline can treat every artifact as a simple `Path` without caring
where it came from.

* **Parameters:**
  * **openable_file** – `fsspec` handle pointing to the source artifact.
  * **staging_dir** – Directory used to cache remote files locally.
* **Returns:**
  A `Path` pointing to a readable local copy of `openable_file`.

#### sync_directory(source_dir: [str](https://docs.python.org/3/library/stdtypes.html#str), ignore: [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[str](https://docs.python.org/3/library/stdtypes.html#str)]) → [ContentComplete](#pudl.scripts.zenodo_data_release.ContentComplete)

Upload every file in `source_dir` to the draft bucket.

The method enumerates files (not subdirectories) via `fsspec` so the source
can live on local disk, GCS, S3, etc. Remote objects are first staged into a
temporary directory to ensure uploads always come from local `Path` objects
that can be rewound for retries. Regex patterns provided via `ignore` are
applied to the full path of each candidate file, allowing us to drop logs,
intermediate data, or other nightlies-only artifacts before hitting Zenodo.

* **Parameters:**
  * **source_dir** – Directory (local or remote) whose contents will be sent to
    Zenodo.
  * **ignore** – Tuple of regex patterns; any path matching one is skipped.
* **Returns:**
  A `ContentComplete` state ready for metadata updates.

### *class* pudl.scripts.zenodo_data_release.ContentComplete

Bases: [`State`](#pudl.scripts.zenodo_data_release.State)

Now that we’ve uploaded all the data, we need to update metadata.

#### update_metadata()

Copy over old metadata and update publication date.

We need to make sure there is complete metadata, including a publication date.

To do this, we:

1. use the *legacy* API to get the concept record ID associated with the draft
2. use the *new* API to get the latest record associated with the concept
3. use the *legacy* API to get the metadata from the latest record
4. use the *legacy* API to update the draft’s metadata

Since we are using the legacy API to publish, we need the legacy
metadata format. But the legacy concept DOI -> published record mapping
is broken, so we have to take a detour through the new API.

### *class* pudl.scripts.zenodo_data_release.CompleteDraft

Bases: [`State`](#pudl.scripts.zenodo_data_release.State)

Now that we’ve uploaded all the data, we can publish.

#### publish() → [None](https://docs.python.org/3/library/constants.html#None)

Publish the draft.

#### get_html_url()

A URL for viewing this draft.

### pudl.scripts.zenodo_data_release.build_zenodo_release_zulip_message(env: [str](https://docs.python.org/3/library/stdtypes.html#str), publish: [bool](https://docs.python.org/3/library/functions.html#bool), succeeded: [bool](https://docs.python.org/3/library/functions.html#bool), record_url: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)) → [str](https://docs.python.org/3/library/stdtypes.html#str)

Build a markdown Zulip message summarizing a Zenodo release attempt.

Makes the sandbox/production environment and publish/draft mode immediately
visible, so a misconfigured run is obvious at a glance, and links to the
resulting record when the release succeeded – the live record if `publish`
was requested, otherwise the draft awaiting manual review.

### pudl.scripts.zenodo_data_release.main(env: [str](https://docs.python.org/3/library/stdtypes.html#str), source_dir: [str](https://docs.python.org/3/library/stdtypes.html#str), publish: [bool](https://docs.python.org/3/library/functions.html#bool), ignore: [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[str](https://docs.python.org/3/library/stdtypes.html#str)]) → [int](https://docs.python.org/3/library/functions.html#int)

Publish a new PUDL data release to Zenodo.
