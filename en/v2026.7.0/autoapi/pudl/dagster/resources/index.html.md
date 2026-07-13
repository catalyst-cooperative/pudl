# pudl.dagster.resources

Dagster resources for PUDL.

This module defines the configurable resources that PUDL assets depend on at runtime,
such as data configuration, datastore access, and other run-scoped helpers, along with
the default resource mapping used by the assembled code location. Add
[`dagster.ConfigurableResource`](https://docs.dagster.io/api/dagster/resources/#dagster.ConfigurableResource) classes and configured singleton instances here
when they provide external services or shared runtime context to assets and jobs. Keep
asset logic out of this module; it should focus on dependency injection and default
resource wiring.

For the underlying Dagster concept, see
[https://docs.dagster.io/guides/build/external-resources](https://docs.dagster.io/guides/build/external-resources)

## Attributes

| [`global_data_config_resource`](#pudl.dagster.resources.global_data_config_resource)   |    |
|----------------------------------------------------------------------------------------|----|
| [`pudl_paths_resource`](#pudl.dagster.resources.pudl_paths_resource)                   |    |
| [`zenodo_doi_settings_resource`](#pudl.dagster.resources.zenodo_doi_settings_resource) |    |
| [`datastore_resource`](#pudl.dagster.resources.datastore_resource)                     |    |
| [`ferc_xbrl_runtime_settings`](#pudl.dagster.resources.ferc_xbrl_runtime_settings)     |    |
| [`ferceqr_archive`](#pudl.dagster.resources.ferceqr_archive)                           |    |
| [`ferceqr_deployment_targets`](#pudl.dagster.resources.ferceqr_deployment_targets)     |    |
| [`zulip_notification_resource`](#pudl.dagster.resources.zulip_notification_resource)   |    |
| [`default_resources`](#pudl.dagster.resources.default_resources)                       |    |

## Classes

| [`PudlPathsResource`](#pudl.dagster.resources.PudlPathsResource)                         | Load the input/output paths used by Dagster-managed PUDL runs.          |
|------------------------------------------------------------------------------------------|-------------------------------------------------------------------------|
| [`FercXbrlRuntimeSettings`](#pudl.dagster.resources.FercXbrlRuntimeSettings)             | Encodes runtime settings for the ferc_to_sqlite graphs.                 |
| [`GlobalDataConfigResource`](#pudl.dagster.resources.GlobalDataConfigResource)           | Load validated PUDL data configuration from a shared ETL YAML file.     |
| [`ZenodoDoiSettingsResource`](#pudl.dagster.resources.ZenodoDoiSettingsResource)         | Load the canonical Zenodo DOI settings for Dagster-managed runs.        |
| [`DatastoreResource`](#pudl.dagster.resources.DatastoreResource)                         | Dagster resource to interact with Zenodo archives.                      |
| [`FercEqrArchiveResource`](#pudl.dagster.resources.FercEqrArchiveResource)               | Configure which archived FERC EQR filings are available for extraction. |
| [`FercEqrDeploymentTargetConfig`](#pudl.dagster.resources.FercEqrDeploymentTargetConfig) | A single deployment destination for FERC EQR outputs.                   |
| [`FercEqrDeploymentResource`](#pudl.dagster.resources.FercEqrDeploymentResource)         | One or more deployment destinations for FERC EQR outputs.               |
| [`ZulipNotificationResource`](#pudl.dagster.resources.ZulipNotificationResource)         | Send notifications to Zulip streams via the Zulip API.                  |

## Module Contents

### *class* pudl.dagster.resources.PudlPathsResource

Bases: [`dagster.ConfigurableResource`](https://docs.dagster.io/api/dagster/resources/#dagster.ConfigurableResource)

Load the input/output paths used by Dagster-managed PUDL runs.

Explicit Dagster resource config takes precedence. Any unset field falls back to
the current process environment so dg runs, local .env files, test fixtures,
and container-provided environment variables all share a single typed entry point.

#### pudl_input *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### pudl_output *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### create_resource(context) → [pudl.workspace.setup.PudlPaths](../../workspace/setup/index.md#pudl.workspace.setup.PudlPaths)

Create validated runtime path settings for the current Dagster run.

### *class* pudl.dagster.resources.FercXbrlRuntimeSettings

Bases: [`dagster.ConfigurableResource`](https://docs.dagster.io/api/dagster/resources/#dagster.ConfigurableResource)

Encodes runtime settings for the ferc_to_sqlite graphs.

#### xbrl_num_workers *: [None](https://docs.python.org/3/library/constants.html#None) | [int](https://docs.python.org/3/library/functions.html#int)* *= None*

#### xbrl_batch_size *: [int](https://docs.python.org/3/library/functions.html#int)* *= 50*

#### xbrl_loglevel *: [str](https://docs.python.org/3/library/stdtypes.html#str)* *= 'INFO'*

### *class* pudl.dagster.resources.GlobalDataConfigResource

Bases: [`dagster.ConfigurableResource`](https://docs.dagster.io/api/dagster/resources/#dagster.ConfigurableResource)

Load validated PUDL data configuration from a shared ETL YAML file.

#### global_data_config_path *: [str](https://docs.python.org/3/library/stdtypes.html#str)* *= ''*

#### create_resource(context) → [pudl.settings.GlobalDataConfig](../../settings/index.md#pudl.settings.GlobalDataConfig)

Create runtime data configuration from the configured YAML file.

### *class* pudl.dagster.resources.ZenodoDoiSettingsResource

Bases: [`dagster.ConfigurableResource`](https://docs.dagster.io/api/dagster/resources/#dagster.ConfigurableResource)

Load the canonical Zenodo DOI settings for Dagster-managed runs.

Two configuration paths are supported:

* **Inline defaults** (`zenodo_dois_path=None`): uses the canonical Zenodo DOIs
  that are hardcoded as defaults in [`ZenodoDoiSettings`](../../workspace/datastore/index.md#pudl.workspace.datastore.ZenodoDoiSettings).
  This is the normal production path — no extra config file is needed.
* **Path override** (`zenodo_dois_path="..."`): loads DOIs from an external YAML
  file, allowing deployments or tests to substitute different DOIs without modifying
  the source code.

#### zenodo_dois_path *: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### create_resource(context) → [pudl.workspace.datastore.ZenodoDoiSettings](../../workspace/datastore/index.md#pudl.workspace.datastore.ZenodoDoiSettings)

Create runtime DOI settings, optionally from an override YAML file.

### *class* pudl.dagster.resources.DatastoreResource

Bases: [`dagster.ConfigurableResource`](https://docs.dagster.io/api/dagster/resources/#dagster.ConfigurableResource)

Dagster resource to interact with Zenodo archives.

#### zenodo_dois *: dagster.ResourceDependency[[ZenodoDoiSettingsResource](#pudl.dagster.resources.ZenodoDoiSettingsResource)]*

#### pudl_paths *: dagster.ResourceDependency[[PudlPathsResource](#pudl.dagster.resources.PudlPathsResource)]*

#### cloud_cache_path *: [str](https://docs.python.org/3/library/stdtypes.html#str)* *= 's3://pudl.catalyst.coop/zenodo'*

#### use_local_cache *: [bool](https://docs.python.org/3/library/functions.html#bool)* *= True*

#### create_resource(context) → [pudl.workspace.datastore.Datastore](../../workspace/datastore/index.md#pudl.workspace.datastore.Datastore)

Create a configured datastore runtime object.

### *class* pudl.dagster.resources.FercEqrArchiveResource

Bases: [`dagster.ConfigurableResource`](https://docs.dagster.io/api/dagster/resources/#dagster.ConfigurableResource)

Configure which archived FERC EQR filings are available for extraction.

The default value of `path` points to the published archive of FERC EQR filings on
GCS which is what we use in production. For testing or development, this can be
overridden to point to a local path with a subset of the archive.

#### path *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### *property* upath *: upath.UPath*

Return UPath pointing to archive base path.

### *class* pudl.dagster.resources.FercEqrDeploymentTargetConfig(\*\*config_dict)

Bases: [`dagster.Config`](https://docs.dagster.io/api/dagster/config/#dagster.Config)

A single deployment destination for FERC EQR outputs.

`path` is a UPath-compatible string: an absolute local directory path, `file://`
URI, `gs://` URI, or `s3://` URI.  `storage_options` is unpacked as
`**kwargs` when constructing the `UPath`, allowing per-target fsspec
settings such as `requester_pays=True` for requester-pays GCS buckets.

#### path *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### storage_options *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]*

#### append_build_id *: [bool](https://docs.python.org/3/library/functions.html#bool)* *= False*

#### *classmethod* validate_path(value: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [str](https://docs.python.org/3/library/stdtypes.html#str)

Validate deployment targets as remote URLs or local directories.

### *class* pudl.dagster.resources.FercEqrDeploymentResource

Bases: [`dagster.ConfigurableResource`](https://docs.dagster.io/api/dagster/resources/#dagster.ConfigurableResource)

One or more deployment destinations for FERC EQR outputs.

Deployment targets can be provided directly as structured config or loaded from a
YAML file. Direct `deployment_targets` take precedence. When neither explicit
targets nor a deployment config path are provided, deployment is skipped.

#### deployment_targets *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[FercEqrDeploymentTargetConfig](#pudl.dagster.resources.FercEqrDeploymentTargetConfig)]* *= []*

#### deployment_config_path *: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### *classmethod* from_yaml(deployment_config_path: [str](https://docs.python.org/3/library/stdtypes.html#str) | [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)) → [FercEqrDeploymentResource](#pudl.dagster.resources.FercEqrDeploymentResource)

Create a FERC EQR deployment resource from a YAML config file.

#### configured_targets() → [list](https://docs.python.org/3/library/stdtypes.html#list)[[FercEqrDeploymentTargetConfig](#pudl.dagster.resources.FercEqrDeploymentTargetConfig)]

Return deployment-target config with explicit overrides taking precedence.

#### resolved_targets() → [list](https://docs.python.org/3/library/stdtypes.html#list)[upath.UPath]

Return the list of `UPath` deployment destinations.

Each configured target is converted to a `UPath` using its
provided `storage_options`.

### *class* pudl.dagster.resources.ZulipNotificationResource

Bases: [`dagster.ConfigurableResource`](https://docs.dagster.io/api/dagster/resources/#dagster.ConfigurableResource)

Send notifications to Zulip streams via the Zulip API.

#### base_url *: [str](https://docs.python.org/3/library/stdtypes.html#str)* *= 'https://catalyst-cooperative.zulipchat.com'*

#### bot_email *: [str](https://docs.python.org/3/library/stdtypes.html#str)* *= 'build-status-bot@catalyst-cooperative.zulipchat.com'*

#### api_key *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### timeout_seconds *: [int](https://docs.python.org/3/library/functions.html#int)* *= 30*

#### send_stream_message(, stream: [str](https://docs.python.org/3/library/stdtypes.html#str), topic: [str](https://docs.python.org/3/library/stdtypes.html#str), content: [str](https://docs.python.org/3/library/stdtypes.html#str), file_path: [str](https://docs.python.org/3/library/stdtypes.html#str) | [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path) | [None](https://docs.python.org/3/library/constants.html#None) = None) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)

Send a message to a Zulip stream topic and return the API response.

Optionally upload a file and attach a download link to the message content.

Sends are best-effort: all failures are logged as warnings and returned in the
result dict so callers can inspect them, but no exception is raised. This
ensures a notification hiccup never crashes an asset.

### pudl.dagster.resources.global_data_config_resource

### pudl.dagster.resources.pudl_paths_resource

### pudl.dagster.resources.zenodo_doi_settings_resource

### pudl.dagster.resources.datastore_resource

### pudl.dagster.resources.ferc_xbrl_runtime_settings

### pudl.dagster.resources.ferceqr_archive

### pudl.dagster.resources.ferceqr_deployment_targets

### pudl.dagster.resources.zulip_notification_resource

### pudl.dagster.resources.default_resources *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]*
