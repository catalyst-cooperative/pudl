# pudl.workspace.datastore

Datastore manages file retrieval for PUDL datasets.

## Attributes

| [`logger`](#pudl.workspace.datastore.logger)       |    |
|----------------------------------------------------|----|
| [`ZenodoDoi`](#pudl.workspace.datastore.ZenodoDoi) |    |

## Exceptions

| [`ChecksumMismatchError`](#pudl.workspace.datastore.ChecksumMismatchError)   | Resource checksum (md5) does not match.   |
|------------------------------------------------------------------------------|-------------------------------------------|

## Classes

| [`DatapackageDescriptor`](#pudl.workspace.datastore.DatapackageDescriptor)   | A simple wrapper providing access to datapackage.json contents.             |
|------------------------------------------------------------------------------|-----------------------------------------------------------------------------|
| [`ZenodoDoiSettings`](#pudl.workspace.datastore.ZenodoDoiSettings)           | Digital Object Identifiers pointing to currently used Zenodo archives.      |
| [`ZenodoFetcher`](#pudl.workspace.datastore.ZenodoFetcher)                   | API for fetching datapackage descriptors and resource contents from zenodo. |
| [`Datastore`](#pudl.workspace.datastore.Datastore)                           | Handle connections and downloading of Zenodo Source archives.               |

## Functions

| [`get_zenodo_dois_path`](#pudl.workspace.datastore.get_zenodo_dois_path)(→ importlib.resources.abc.Traversable)   | Return the canonical packaged Zenodo DOI settings path.      |
|-------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------|
| [`validate_cache`](#pudl.workspace.datastore.validate_cache)(→ None)                                              | Validate elements in the datastore cache.                    |
| [`fetch_resources`](#pudl.workspace.datastore.fetch_resources)(→ None)                                            | Retrieve all matching resources and store them in the cache. |

## Module Contents

### pudl.workspace.datastore.logger

### pudl.workspace.datastore.ZenodoDoi

### pudl.workspace.datastore.get_zenodo_dois_path() → [importlib.resources.abc.Traversable](https://docs.python.org/3/library/importlib.resources.abc.html#importlib.resources.abc.Traversable)

Return the canonical packaged Zenodo DOI settings path.

### *exception* pudl.workspace.datastore.ChecksumMismatchError

Bases: [`ValueError`](https://docs.python.org/3/library/exceptions.html#ValueError)

Resource checksum (md5) does not match.

### *class* pudl.workspace.datastore.DatapackageDescriptor(datapackage_json: [dict](https://docs.python.org/3/library/stdtypes.html#dict), dataset: [str](https://docs.python.org/3/library/stdtypes.html#str), doi: [ZenodoDoi](#pudl.workspace.datastore.ZenodoDoi))

A simple wrapper providing access to datapackage.json contents.

#### datapackage_json

#### dataset

#### doi

#### \_get_resource_metadata(name: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)

#### get_resource_path(name: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [str](https://docs.python.org/3/library/stdtypes.html#str)

Returns zenodo url that holds contents of given named resource.

#### get_download_size() → [int](https://docs.python.org/3/library/functions.html#int)

Returns the total download size of all the resources in MB.

#### validate_checksum(name: [str](https://docs.python.org/3/library/stdtypes.html#str), content: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [bool](https://docs.python.org/3/library/functions.html#bool)

Returns True if content matches checksum for given named resource.

#### \_matches(res: [dict](https://docs.python.org/3/library/stdtypes.html#dict), \*\*filters: Any)

#### \_match_from_partition(parts: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)], k: [str](https://docs.python.org/3/library/stdtypes.html#str), v: [str](https://docs.python.org/3/library/stdtypes.html#str) | [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)])

#### get_resources(name: [str](https://docs.python.org/3/library/stdtypes.html#str) = None, \*\*filters: Any) → [collections.abc.Iterator](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterator)[[pudl.workspace.resource_cache.PudlResourceKey](../resource_cache/index.md#pudl.workspace.resource_cache.PudlResourceKey)]

Returns series of PudlResourceKey identifiers for matching resources.

* **Parameters:**
  * **name** – if specified, find resource(s) with this name.
  * **filters** ([*dict*](https://docs.python.org/3/library/stdtypes.html#dict)) – if specified, find resource(s) matching these key=value
    constraints. The constraints are matched against the ‘parts’ field of
    the resource entry in the datapackage.json.

#### get_partitions(name: [str](https://docs.python.org/3/library/stdtypes.html#str) = None) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [set](https://docs.python.org/3/library/stdtypes.html#set)[[str](https://docs.python.org/3/library/stdtypes.html#str)]]

Return mapping of known partition keys to their allowed known values.

#### get_partition_filters(\*\*filters: Any) → [collections.abc.Iterator](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterator)[[dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)]]

Returns list of all known partition mappings.

This can be used to iterate over all resources as the mappings can be directly
used as filters and should map to unique resource.

* **Parameters:**
  **filters** – additional constraints for selecting relevant partitions.

#### \_validate_datapackage(datapackage_json: [dict](https://docs.python.org/3/library/stdtypes.html#dict))

Checks the correctness of datapackage.json metadata.

Throws ValueError if invalid.

#### get_json_string() → [str](https://docs.python.org/3/library/stdtypes.html#str)

Exports the underlying json as normalized (sorted, indented) json string.

### *class* pudl.workspace.datastore.ZenodoDoiSettings(\*\*data: Any)

Bases: [`pydantic_settings.BaseSettings`](https://pydantic.dev/docs/validation/latest/api/pydantic_settings/#pydantic_settings.BaseSettings)

Digital Object Identifiers pointing to currently used Zenodo archives.

#### censusdp1tract *: [ZenodoDoi](#pudl.workspace.datastore.ZenodoDoi)*

#### censuspep *: [ZenodoDoi](#pudl.workspace.datastore.ZenodoDoi)*

#### eia176 *: [ZenodoDoi](#pudl.workspace.datastore.ZenodoDoi)*

#### eia191 *: [ZenodoDoi](#pudl.workspace.datastore.ZenodoDoi)*

#### eia757a *: [ZenodoDoi](#pudl.workspace.datastore.ZenodoDoi)*

#### eia860 *: [ZenodoDoi](#pudl.workspace.datastore.ZenodoDoi)*

#### eia860m *: [ZenodoDoi](#pudl.workspace.datastore.ZenodoDoi)*

#### eia861 *: [ZenodoDoi](#pudl.workspace.datastore.ZenodoDoi)*

#### eia923 *: [ZenodoDoi](#pudl.workspace.datastore.ZenodoDoi)*

#### eia930 *: [ZenodoDoi](#pudl.workspace.datastore.ZenodoDoi)*

#### eiaaeo *: [ZenodoDoi](#pudl.workspace.datastore.ZenodoDoi)*

#### eiaapi *: [ZenodoDoi](#pudl.workspace.datastore.ZenodoDoi)*

#### epacamd_eia *: [ZenodoDoi](#pudl.workspace.datastore.ZenodoDoi)*

#### epacems *: [ZenodoDoi](#pudl.workspace.datastore.ZenodoDoi)*

#### ferc1 *: [ZenodoDoi](#pudl.workspace.datastore.ZenodoDoi)*

#### ferc2 *: [ZenodoDoi](#pudl.workspace.datastore.ZenodoDoi)*

#### ferc6 *: [ZenodoDoi](#pudl.workspace.datastore.ZenodoDoi)*

#### ferc60 *: [ZenodoDoi](#pudl.workspace.datastore.ZenodoDoi)*

#### ferc714 *: [ZenodoDoi](#pudl.workspace.datastore.ZenodoDoi)*

#### ferceqr *: [ZenodoDoi](#pudl.workspace.datastore.ZenodoDoi)*

#### ferccid *: [ZenodoDoi](#pudl.workspace.datastore.ZenodoDoi)*

#### gridpathratoolkit *: [ZenodoDoi](#pudl.workspace.datastore.ZenodoDoi)*

#### nrelatb *: [ZenodoDoi](#pudl.workspace.datastore.ZenodoDoi)*

#### phmsagas *: [ZenodoDoi](#pudl.workspace.datastore.ZenodoDoi)*

#### rus7 *: [ZenodoDoi](#pudl.workspace.datastore.ZenodoDoi)*

#### rus12 *: [ZenodoDoi](#pudl.workspace.datastore.ZenodoDoi)*

#### sec10k *: [ZenodoDoi](#pudl.workspace.datastore.ZenodoDoi)*

#### vcerare *: [ZenodoDoi](#pudl.workspace.datastore.ZenodoDoi)*

#### model_config

Configuration for the model, should be a dictionary conforming to [ConfigDict][pydantic.config.ConfigDict].

#### get_doi(dataset: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [ZenodoDoi](#pudl.workspace.datastore.ZenodoDoi)

Look up configured DOI by dataset.

Throws a KeyError if dataset not configured.

#### *classmethod* from_yaml(path: [str](https://docs.python.org/3/library/stdtypes.html#str) | [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)) → [ZenodoDoiSettings](#pudl.workspace.datastore.ZenodoDoiSettings)

Create a ZenodoDoiSettings instance from a YAML file path.

* **Parameters:**
  **path** – Path to a YAML file.
* **Returns:**
  A ZenodoDoiSettings object with DOIs loaded from the YAML file.

### *class* pudl.workspace.datastore.ZenodoFetcher(zenodo_dois: [ZenodoDoiSettings](#pudl.workspace.datastore.ZenodoDoiSettings) | [None](https://docs.python.org/3/library/constants.html#None) = None, timeout: [float](https://docs.python.org/3/library/functions.html#float) = 100.0)

API for fetching datapackage descriptors and resource contents from zenodo.

#### \_descriptor_cache *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [DatapackageDescriptor](#pudl.workspace.datastore.DatapackageDescriptor)]*

#### zenodo_dois *: [ZenodoDoiSettings](#pudl.workspace.datastore.ZenodoDoiSettings)*

#### timeout *: [float](https://docs.python.org/3/library/functions.html#float)*

#### http

#### get_doi(dataset: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [ZenodoDoi](#pudl.workspace.datastore.ZenodoDoi)

Returns DOI for given dataset.

#### get_known_datasets() → [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]

Returns list of supported datasets.

#### \_get_url(doi: [ZenodoDoi](#pudl.workspace.datastore.ZenodoDoi)) → [pydantic.HttpUrl](https://pydantic.dev/docs/validation/latest/api/pydantic/networks/#pydantic.networks.HttpUrl)

Construct a Zenodo depsition URL based on its Zenodo DOI.

#### \_fetch_from_url(url: [pydantic.HttpUrl](https://pydantic.dev/docs/validation/latest/api/pydantic/networks/#pydantic.networks.HttpUrl)) → requests.Response

#### get_descriptor(dataset: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [DatapackageDescriptor](#pudl.workspace.datastore.DatapackageDescriptor)

Returns class:DatapackageDescriptor for given dataset.

#### get_resource(res: [pudl.workspace.resource_cache.PudlResourceKey](../resource_cache/index.md#pudl.workspace.resource_cache.PudlResourceKey)) → [bytes](https://docs.python.org/3/library/stdtypes.html#bytes)

Given resource key, retrieve contents of the file from zenodo.

### *class* pudl.workspace.datastore.Datastore(local_cache_path: [str](https://docs.python.org/3/library/stdtypes.html#str) | [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path) | upath.UPath | [None](https://docs.python.org/3/library/constants.html#None) = None, cloud_cache_path: [str](https://docs.python.org/3/library/stdtypes.html#str) | upath.UPath | [None](https://docs.python.org/3/library/constants.html#None) = 's3://pudl.catalyst.coop/zenodo', timeout: [float](https://docs.python.org/3/library/functions.html#float) = 15.0, zenodo_dois: [ZenodoDoiSettings](#pudl.workspace.datastore.ZenodoDoiSettings) | [None](https://docs.python.org/3/library/constants.html#None) = None)

Handle connections and downloading of Zenodo Source archives.

#### \_cache

#### \_datapackage_descriptors *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [DatapackageDescriptor](#pudl.workspace.datastore.DatapackageDescriptor)]*

#### temporary_extraction_dir

#### \_zenodo_fetcher

#### *property* zenodo_dois *: [ZenodoDoiSettings](#pudl.workspace.datastore.ZenodoDoiSettings)*

Expose the DOI settings used by this datastore instance.

#### get_doi(dataset: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [ZenodoDoi](#pudl.workspace.datastore.ZenodoDoi)

Return the configured DOI for a dataset.

#### get_known_datasets() → [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]

Returns list of supported datasets.

#### get_datapackage_descriptor(dataset: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [DatapackageDescriptor](#pudl.workspace.datastore.DatapackageDescriptor)

Fetch datapackage descriptor for dataset either from cache or Zenodo.

#### get_resources(dataset: [str](https://docs.python.org/3/library/stdtypes.html#str), cached_only: [bool](https://docs.python.org/3/library/functions.html#bool) = False, skip_optimally_cached: [bool](https://docs.python.org/3/library/functions.html#bool) = False, \*\*filters: Any) → [collections.abc.Iterator](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterator)[[tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[pudl.workspace.resource_cache.PudlResourceKey](../resource_cache/index.md#pudl.workspace.resource_cache.PudlResourceKey), [bytes](https://docs.python.org/3/library/stdtypes.html#bytes)]]

Return content of the matching resources.

* **Parameters:**
  * **dataset** – name of the dataset to query.
  * **cached_only** – if True, only retrieve resources that are present in the cache.
  * **skip_optimally_cached** – if True, only retrieve resources that are not optimally
    cached. This triggers attempt to optimally cache these resources.
  * **filters** (*key=val*) – only return resources that match the key-value mapping in their
  * **metadata****[****"parts"****]****.**
* **Yields:**
  (PudlResourceKey, io.BytesIO) holding content for each matching resource

#### remove_from_cache(res: [pudl.workspace.resource_cache.PudlResourceKey](../resource_cache/index.md#pudl.workspace.resource_cache.PudlResourceKey)) → [None](https://docs.python.org/3/library/constants.html#None)

Remove given resource from the associated cache.

#### get_unique_resource(dataset: [str](https://docs.python.org/3/library/stdtypes.html#str), \*\*filters: Any) → [bytes](https://docs.python.org/3/library/stdtypes.html#bytes)

Returns content of a resource assuming there is exactly one that matches.

#### get_zipfile_resource(dataset: [str](https://docs.python.org/3/library/stdtypes.html#str), \*\*filters: Any) → [zipfile.ZipFile](https://docs.python.org/3/library/zipfile.html#zipfile.ZipFile)

Retrieves unique resource and opens it as a ZipFile.

#### get_zipfile_resources(dataset: [str](https://docs.python.org/3/library/stdtypes.html#str), \*\*filters: Any) → [collections.abc.Iterator](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterator)[[tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[pudl.workspace.resource_cache.PudlResourceKey](../resource_cache/index.md#pudl.workspace.resource_cache.PudlResourceKey), [zipfile.ZipFile](https://docs.python.org/3/library/zipfile.html#zipfile.ZipFile)]]

Iterates over resources that match filters and opens each as ZipFile.

#### get_zipfile_file_names(zip_file: [zipfile.ZipFile](https://docs.python.org/3/library/zipfile.html#zipfile.ZipFile))

Given a zipfile, return a list of the file names in it.

### pudl.workspace.datastore.validate_cache(dstore: [Datastore](#pudl.workspace.datastore.Datastore), datasets: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], partition: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [int](https://docs.python.org/3/library/functions.html#int) | [str](https://docs.python.org/3/library/stdtypes.html#str)]) → [None](https://docs.python.org/3/library/constants.html#None)

Validate elements in the datastore cache.

Delete invalid entries from cache.

### pudl.workspace.datastore.fetch_resources(dstore: [Datastore](#pudl.workspace.datastore.Datastore), datasets: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], partition: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [int](https://docs.python.org/3/library/functions.html#int) | [str](https://docs.python.org/3/library/stdtypes.html#str)], cloud_cache_path: [str](https://docs.python.org/3/library/stdtypes.html#str), bypass_local_cache: [bool](https://docs.python.org/3/library/functions.html#bool)) → [None](https://docs.python.org/3/library/constants.html#None)

Retrieve all matching resources and store them in the cache.
