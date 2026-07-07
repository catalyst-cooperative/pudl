# pudl.workspace.resource_cache

Implementations of datastore resource caches.

## Attributes

| [`logger`](#pudl.workspace.resource_cache.logger)   |    |
|-----------------------------------------------------|----|

## Classes

| [`PudlResourceKey`](#pudl.workspace.resource_cache.PudlResourceKey)   | Uniquely identifies a specific resource.                                           |
|-----------------------------------------------------------------------|------------------------------------------------------------------------------------|
| [`AbstractCache`](#pudl.workspace.resource_cache.AbstractCache)       | Defines interaface for the generic resource caching layer.                         |
| [`UPathCache`](#pudl.workspace.resource_cache.UPathCache)             | Implements file cache using UPath for unified access to multiple storage backends. |
| [`LayeredCache`](#pudl.workspace.resource_cache.LayeredCache)         | Implements multi-layered system of caches.                                         |

## Module Contents

### pudl.workspace.resource_cache.logger

### *class* pudl.workspace.resource_cache.PudlResourceKey

Bases: `NamedTuple`

Uniquely identifies a specific resource.

#### dataset *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### doi *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### name *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### \_\_repr_\_() → [str](https://docs.python.org/3/library/stdtypes.html#str)

Returns string representation of PudlResourceKey.

#### get_local_path() → [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)

Returns (relative) path that should be used when caching this resource.

### *class* pudl.workspace.resource_cache.AbstractCache(read_only: [bool](https://docs.python.org/3/library/functions.html#bool) = False)

Bases: [`abc.ABC`](https://docs.python.org/3/library/abc.html#abc.ABC)

Defines interaface for the generic resource caching layer.

#### \_read_only *= False*

#### is_read_only() → [bool](https://docs.python.org/3/library/functions.html#bool)

Returns true if the cache is read-only and should not be modified.

#### *abstractmethod* get(resource: [PudlResourceKey](#pudl.workspace.resource_cache.PudlResourceKey)) → [bytes](https://docs.python.org/3/library/stdtypes.html#bytes)

Retrieves content of given resource or throws KeyError.

#### *abstractmethod* add(resource: [PudlResourceKey](#pudl.workspace.resource_cache.PudlResourceKey), content: [bytes](https://docs.python.org/3/library/stdtypes.html#bytes)) → [None](https://docs.python.org/3/library/constants.html#None)

Adds resource to the cache and sets the content.

#### *abstractmethod* delete(resource: [PudlResourceKey](#pudl.workspace.resource_cache.PudlResourceKey)) → [None](https://docs.python.org/3/library/constants.html#None)

Removes the resource from cache.

#### *abstractmethod* contains(resource: [PudlResourceKey](#pudl.workspace.resource_cache.PudlResourceKey)) → [bool](https://docs.python.org/3/library/functions.html#bool)

Returns True if the resource is present in the cache.

### *class* pudl.workspace.resource_cache.UPathCache(storage_upath: upath.UPath, \*\*kwargs: Any)

Bases: [`AbstractCache`](#pudl.workspace.resource_cache.AbstractCache)

Implements file cache using UPath for unified access to multiple storage backends.

This cache uses universal_pathlib’s UPath to provide a unified interface
for accessing data stored in S3, GCS, or local filesystems. It handles backend-specific
authentication and credential management internally.

Requires UPath objects with explicit protocols:
: - s3://bucket-name/path/prefix
  - gs://bucket-name/path/prefix
  - [file:///local/path](file:///local/path)

#### supported_protocols *: [set](https://docs.python.org/3/library/stdtypes.html#set)[[str](https://docs.python.org/3/library/stdtypes.html#str)]*

#### \_protocol

#### \_storage_options

#### \_base_path

#### \_\_repr_\_() → [str](https://docs.python.org/3/library/stdtypes.html#str)

Returns string representation of UPathCache.

#### \_setup_credentials() → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]

Set up backend-specific credentials and storage options.

This should be the only place where backend-specific logic is required.

* **Returns:**
  Dictionary of storage options to pass to UPath

#### \_resource_path(resource: [PudlResourceKey](#pudl.workspace.resource_cache.PudlResourceKey)) → upath.UPath

Get the UPath for a given resource.

* **Parameters:**
  **resource** – The resource to get the path for
* **Returns:**
  UPath object pointing to the resource location

#### is_anonymous() → [bool](https://docs.python.org/3/library/functions.html#bool)

Returns True if the cache is using anonymous access (no credentials).

#### get(resource: [PudlResourceKey](#pudl.workspace.resource_cache.PudlResourceKey)) → [bytes](https://docs.python.org/3/library/stdtypes.html#bytes)

Retrieves value associated with given resource.

* **Parameters:**
  **resource** – The resource to retrieve
* **Returns:**
  The content of the resource as bytes
* **Raises:**
  * [**KeyError**](https://docs.python.org/3/library/exceptions.html#KeyError) – if the resource doesn’t exist
  * [**Exception**](https://docs.python.org/3/library/exceptions.html#Exception) – for other storage backend errors

#### add(resource: [PudlResourceKey](#pudl.workspace.resource_cache.PudlResourceKey), content: [bytes](https://docs.python.org/3/library/stdtypes.html#bytes))

Adds (or updates) resource to the cache with given content.

* **Parameters:**
  * **resource** – The resource to add
  * **content** – The content to store
* **Raises:**
  [**RuntimeError**](https://docs.python.org/3/library/exceptions.html#RuntimeError) – if cache is read-only or credentials are insufficient

#### delete(resource: [PudlResourceKey](#pudl.workspace.resource_cache.PudlResourceKey))

Deletes resource from the cache.

* **Parameters:**
  **resource** – The resource to delete
* **Raises:**
  [**RuntimeError**](https://docs.python.org/3/library/exceptions.html#RuntimeError) – if cache is read-only or credentials are insufficient

#### contains(resource: [PudlResourceKey](#pudl.workspace.resource_cache.PudlResourceKey)) → [bool](https://docs.python.org/3/library/functions.html#bool)

Returns True if resource is present in the cache.

* **Parameters:**
  **resource** – The resource to check
* **Returns:**
  True if the resource exists, False otherwise

### *class* pudl.workspace.resource_cache.LayeredCache(\*caches: [AbstractCache](#pudl.workspace.resource_cache.AbstractCache), \*\*kwargs: Any)

Bases: [`AbstractCache`](#pudl.workspace.resource_cache.AbstractCache)

Implements multi-layered system of caches.

This allows building multi-layered system of caches. The idea is that you can have
faster local caches with fall-back to the more remote or expensive caches that can
be accessed in case of missing content.

Only the closest layer is being written to (set, delete), while all remaining layers
are read-only (get).

#### \_caches *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[AbstractCache](#pudl.workspace.resource_cache.AbstractCache)]* *= []*

#### add_cache_layer(cache: [AbstractCache](#pudl.workspace.resource_cache.AbstractCache))

Adds caching layer.

The priority is below all other.

#### num_layers()

Returns number of caching layers that are in this LayeredCache.

#### get(resource: [PudlResourceKey](#pudl.workspace.resource_cache.PudlResourceKey)) → [bytes](https://docs.python.org/3/library/stdtypes.html#bytes)

Returns content of a given resource.

When a resource is found in a distant cache layer, it is automatically populated
into all closer (higher-priority) cache layers that are writable.  This ensures
optimal cache performance for subsequent accesses.

#### add(resource: [PudlResourceKey](#pudl.workspace.resource_cache.PudlResourceKey), content)

Adds (or replaces) resource into the cache with given content.

#### delete(resource: [PudlResourceKey](#pudl.workspace.resource_cache.PudlResourceKey))

Removes resource from the cache if the cache is not in the read_only mode.

#### contains(resource: [PudlResourceKey](#pudl.workspace.resource_cache.PudlResourceKey)) → [bool](https://docs.python.org/3/library/functions.html#bool)

Returns True if resource is present in the cache.

#### is_optimally_cached(resource: [PudlResourceKey](#pudl.workspace.resource_cache.PudlResourceKey)) → [bool](https://docs.python.org/3/library/functions.html#bool)

Return True if resource is contained in the closest write-enabled layer.
