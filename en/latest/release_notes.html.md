# PUDL Release Notes

<a id="release-v2026-8-x"></a>

## v2026.8.x (2026-08-xx)

This is the upcoming quarterly PUDL release.

### New Data

#### EIA-860M

* Added Puerto Rico [EIA-860M](data_sources/eia860.md) data into EIA 860 tables. See
  issue [#4352](https://github.com/catalyst-cooperative/pudl/issues/4352) and PR [#5360](https://github.com/catalyst-cooperative/pudl/pull/5360). Shoutout to [@bsousa22](https://github.com/sponsors/bsousa22) for making his
  first PUDL contribution!

### Documentation

* Added LLM use guidelines and best practices to the
  [contributor guide](CONTRIBUTING.md) and [dev guide](dev/llm_best_practices.md).
* Set up the [sphinx_llm](https://github.com/NVIDIA/sphinx-llm) Sphinx extension to
  generate a Markdown version of the PUDL documentation, suitable for consumption by
  LLMs, based on the [llms.txt](https://llmstxt.org/) convention. Each page now
  advertises its Markdown counterpart via a `<link rel="alternate"
  type="text/markdown">` tag, and the site footer links directly to `llms.txt`, so
  that agents browsing the rendered HTML docs can discover and prefer the Markdown
  versions. See PRs [#5381](https://github.com/catalyst-cooperative/pudl/pull/5381), [#5393](https://github.com/catalyst-cooperative/pudl/pull/5393).

### Bug Fixes & Data Cleaning

* Fixed incorrectly mapped Western Area Power Authority BA codes in FERC 714
  data - previously, the Upper Great Plains West region FERC respondent was mapped
  to the Desert Southwest region EIA balancing authority information, and vice
  versa. See [#4644](https://github.com/catalyst-cooperative/pudl/issues/4644) and [#5408](https://github.com/catalyst-cooperative/pudl/pull/5408).

### Performance Improvements

* The fast ETL now processes only two representative
  [EIA-861](data_sources/eia861.md) years instead of the entire time series, bringing
  it in line with how every other dataset is already handled and speeding up both local
  development and CI. Processing all years was originally a workaround for discontinued
  columns and data validation tests that couldn’t tolerate partial coverage; those
  limitations have since been resolved. This change surfaced an implicit assumption in
  the [FERC-714](data_sources/ferc714.md) outputs that all EIA-861 years were always
  available, in the logic that repairs known-bad balancing authority/utility
  associations by copying data from a known-good year. That repair logic has been
  rewritten as an explicit, validated mapping of per-year fixes, so it degrades
  gracefully when only a subset of years is present, and is substantially easier to
  read, test, and extend than the compact form it replaces. See [#2628](https://github.com/catalyst-cooperative/pudl/issues/2628) and
  [#4568](https://github.com/catalyst-cooperative/pudl/pull/4568).

<a id="release-v2026-7-2"></a>

## v2026.7.2 (2026-07-14)

This is a monthly PUDL data release, primarily motivated by updating
the EIA-860M monthly data through May 2026. As usual, it also includes
all of the other changes that have accumulated on `main` since our
last release.

This month, we have new EIA-176 tables, the EIA-860 early release,
Parquet outputs for DBF assets, improved units handling, GeoParquet
bugfixes, and better signal:noise ratio in unit test logging outputs.

### Enhancements

* Added experimental Parquet outputs derived from the FERC DBF databases, and basic
  `datapackage.json` metadata describing their schemas to support querying and preview
  through the [PUDL Data Viewer](https://data.catalyst.coop). See PR [#5339](https://github.com/catalyst-cooperative/pudl/pull/5339).
* Standardized all unit strings in [`pudl.metadata.fields`](autoapi/pudl/metadata/fields/index.md#module-pudl.metadata.fields) to
  [Pint expression syntax](https://pint.readthedocs.io/), replacing ad-hoc
  abbreviations (`gpm`, `min`, `F`, `cfm`), underscore-separated
  compound units (`lb_per_MMBTU`, `USD_per_MWh`), and inconsistent
  capitalization. A new [`pudl.metadata.units`](autoapi/pudl/metadata/units/index.md#module-pudl.metadata.units) module defines
  `PUDL_UNIT_REGISTRY`, a `pint.UnitRegistry` extended with energy-industry
  units (`MMBtu`, `Mcf`, `MMcf`, `TBtu`, `VAr`, `USD`). Added extensive
  new per-column units annotations. These changes should facilitate programmatic unit
  parsing, display, and conversion, and they are now surfaced in the PUDL metadata and
  datapackage outputs as machine-readable Pint-compatible unit definitions. See
  [#5078](https://github.com/catalyst-cooperative/pudl/issues/5078) and [#5361](https://github.com/catalyst-cooperative/pudl/pull/5361).

### New Data

#### EIA-176

* Added detailed core [EIA-176](data_sources/eia176.md)
  continuation-line tables for natural gas imports, supplemental
  gaseous fuel supplies, gas exports, and other gas disposition. See
  [#5240](https://github.com/catalyst-cooperative/pudl/issues/5240) and [#5245](https://github.com/catalyst-cooperative/pudl/pull/5245).

### Expanded Data Coverage

#### EIA-923

* Added early release data for EIA-923 2025. See issue [#5372](https://github.com/catalyst-cooperative/pudl/issues/5372) and PR [#5391](https://github.com/catalyst-cooperative/pudl/pull/5391).
* Added 2026 data through April for EIA-923. See [#5391](https://github.com/catalyst-cooperative/pudl/pull/5391).

#### EIA-191

* Added [EIA-191](data_sources/eia191.md) data through end of
  March 2026. See PR [#5396](https://github.com/catalyst-cooperative/pudl/pull/5396).

#### EIA-930

* Added [EIA-930](data_sources/eia930.md) data through end of
  June 2026. See PR [#5396](https://github.com/catalyst-cooperative/pudl/pull/5396)

#### EIA Electricity API

* Updated the bulk [EIA Electricity API](data_sources/eiaapi.md) data
  used to fill in redacted fuel prices. See PR [#5396](https://github.com/catalyst-cooperative/pudl/pull/5396).

#### EPA CEMS

* Added [EPA CEMS](data_sources/epacems.md) data through end of
  March 2026. See PR [#5396](https://github.com/catalyst-cooperative/pudl/pull/5396)

#### EIA-860

* Added early release data for [EIA-860](data_sources/eia860.md) 2025. See issue [#5322](https://github.com/catalyst-cooperative/pudl/issues/5322) and PR
  [#5324](https://github.com/catalyst-cooperative/pudl/pull/5324).

#### EIA-860M

* Added [EIA-860M](data_sources/eia860.md) data through May 2026. See
  issue [#5369](https://github.com/catalyst-cooperative/pudl/issues/5369) and PR [#5371](https://github.com/catalyst-cooperative/pudl/pull/5371).

#### FERC Forms 2 and 6

* Updated the raw FERC Form 2 and 6 archives to include additional
  2025 data. This data is converted to SQLite, but not deeply
  integrated into PUDL. See PR [#5396](https://github.com/catalyst-cooperative/pudl/pull/5396).

#### FERC CID

* Updated the FERC company identifiers with data through end of
  June 2026. See PR [#5396](https://github.com/catalyst-cooperative/pudl/pull/5396).

### Documentation

* Expanded the developer docs around metadata naming, typing, and updates to explain
  how unit annotations, field namespaces, and namespace/table-specific metadata
  overrides should be defined and maintained. See [#5361](https://github.com/catalyst-cooperative/pudl/pull/5361).

### New Data Tests & Validations

* A new [`valid_datapackage_unit_strings_check()`](autoapi/pudl/dagster/asset_checks/index.md#pudl.dagster.asset_checks.valid_datapackage_unit_strings_check) asset
  check factory validates all unit strings in the PUDL datapackage descriptor against
  the registry after each ETL run. About a dozen fields in PHMSA gas and EIA-860 FGD
  data that were typed as `number` but contain integer counts have been corrected to
  `"type": "integer"`. A bug where `convert_cols_dtypes` and `get_parquet_table`
  overrides in `FIELD_METADATA_BY_RESOURCE`, were sometimes ignored has been fixed.
* Added `dbt` `expect_column_values_to_be_between` tests to codify range
  expectations for columns stated as percentages (0, 100) vs those that represent
  fractional values (0, 1). Column naming still needs to be standardized. FERC Form 1
  fraction tests use `error_if` thresholds to accommodate a known small number of
  out-of-range values. See [#5361](https://github.com/catalyst-cooperative/pudl/pull/5361).

### Bug Fixes & Data Cleaning

* Three EIA-860 columns that EIA reports as percentages but PUDL describes as
  fractions have been corrected. `standard_so2_percent_scrubbed` (boilers) was
  already stored as a fraction but misnamed; it is now renamed
  `standard_so2_fraction_scrubbed`. `max_oil_heat_input` (multi-fuel generators)
  and `dry_cooling_pct` (cooling equipment) were extracted as percentages; both are
  now divided by 100 in the transform step and the cooling column is renamed
  `dry_cooling_fraction`. Field descriptions for the FERC1 `*_fraction_cost`
  columns have been updated to say “fraction (0-1)” instead of “percentage”.
  All true `_pct` columns now carry an explicit `"unit": "percent"` annotation.
  See [#5361](https://github.com/catalyst-cooperative/pudl/pull/5361).
* Fixed several Click-based console scripts so shell callers now receive correct
  non-zero exit codes on failure. The script-entry conventions in
  [`pudl.scripts`](autoapi/pudl/scripts/index.md#module-pudl.scripts) now use Click-native exits and call `main()` directly in
  the module launcher, which fixes automation that branches on
  `pudl_check_for_build` success or failure. See PR [#5374](https://github.com/catalyst-cooperative/pudl/pull/5374).
* Fixed a DuckDB >= 1.5 incompatibility with PUDL’s GeoParquet outputs. DuckDB 1.5
  requires CRS metadata in PROJJSON format, but the old
  `PudlGeoParquetIOManager` wrote a WKT string,
  causing `"Geoparquet column 'geometry' has invalid CRS"` when the spatial extension
  was loaded. Switching to native `geopandas.GeoDataFrame.to_parquet()` produces
  spec-compliant GeoParquet 1.0.0 metadata. See issues [#4061](https://github.com/catalyst-cooperative/pudl/issues/4061), [#5074](https://github.com/catalyst-cooperative/pudl/issues/5074) and PR
  [#5347](https://github.com/catalyst-cooperative/pudl/pull/5347).
* Dropped the uninformative `is_total` column from
  [core_rus7_\_yearly_distribution_services](data_dictionaries/pudl_db.md#core-rus7-yearly-distribution-services) and
  [out_rus7_\_yearly_distribution_services](data_dictionaries/pudl_db.md#out-rus7-yearly-distribution-services), renamed the `service_status`
  values to the self-explanatory `connected_this_year`, `retired_this_year`,
  `total_in_place` and `idle_in_place`, and documented why RUS Form 7 Part B
  subcomponents do not sum to the reported total within a year. See issue
  [#5262](https://github.com/catalyst-cooperative/pudl/issues/5262) and PR [#5323](https://github.com/catalyst-cooperative/pudl/pull/5323).
* Fixed several nightly/stable/branch deployment flow-control bugs: nightly builds
  were silently landing in staging instead of production because
  `DEPLOYMENT_ENVIRONMENT` was never wired into the build container’s Batch job,
  PUDL Viewer redeploys and Zenodo releases fired for every deploy type instead of
  being restricted to nightly builds and non-branch builds respectively, stale
  objects from earlier deployments were never cleared from the public GCS/S3 paths
  before new outputs were written, and a staging copy of a stable release tag was
  incorrectly treated as an immutable path that could never be cleared. See issue
  [#5382](https://github.com/catalyst-cooperative/pudl/issues/5382) and PR [#5384](https://github.com/catalyst-cooperative/pudl/pull/5384).
* Added two new optional arguments to `get_pudl_dtypes` and `apply_pudl_dtypes`.
  `resource` (aka table) name will now return all of the authoritative
  resource-specific dtypes instead of the generic or source-specific types.
  `dtype_backend` will now return the types for the specific file type, which extends
  the previous behavior of returning pandas dtypes. The dtype management now lives in
  [`pudl.metadata.dtypes`](autoapi/pudl/metadata/dtypes/index.md#module-pudl.metadata.dtypes). See [#5361](https://github.com/catalyst-cooperative/pudl/pull/5361).
* Fixed `pudl_deploy` returning a plain integer exit code from its Click command,
  which Click’s standalone mode silently discards, so shell callers previously saw
  exit code 0 even when a deployment stage failed. It now uses `ctx.exit()` like
  the other scripts fixed in [#5374](https://github.com/catalyst-cooperative/pudl/pull/5374). See [#5382](https://github.com/catalyst-cooperative/pudl/issues/5382) and PR [#5384](https://github.com/catalyst-cooperative/pudl/pull/5384).
* Fixed the longstanding issue with `zenodo_data_release` sandbox release failures
  happening even when the publication actually succeeded, because a client-side
  timeout on the publish request triggered a retry that legitimately 404s once a
  deposit is already published, and that raw 404 body was then parsed as if it were
  a real deposition. Also fixed the build-ID provenance marker file being a
  zero-byte upload, which Zenodo rejects outright. See PR [#5384](https://github.com/catalyst-cooperative/pudl/pull/5384).

### Performance Improvements

* Sped up PUDL deploys by compressing SQLite databases concurrently in a thread
  pool and uploading to all four GCS/S3 targets concurrently instead of one at a time.
  Also reduced the SQLite `compresslevel` from 9 to 6, trading a little archive size
  for much faster compression step. See [#5382](https://github.com/catalyst-cooperative/pudl/issues/5382) and PR [#5384](https://github.com/catalyst-cooperative/pudl/pull/5384).

### Developer Experience

* Reduced spurious logging and error output from our unit tests. See PR [#5362](https://github.com/catalyst-cooperative/pudl/pull/5362).
* Added two new optional arguments to `get_pudl_dtypes` and `apply_pudl_dtypes`:
  `resource` (aka table) name will now return all of the authoritative
  resource-specific dtypes instead of the generic or source-specific types.
  `dtype_backend` will now return the types for the specific file type, which extends
  the previous behavior of returning pandas dtypes. The dtype management now lives in
  [`pudl.metadata.dtypes`](autoapi/pudl/metadata/dtypes/index.md#module-pudl.metadata.dtypes). See [#5361](https://github.com/catalyst-cooperative/pudl/pull/5361).
* Reworked the nightly PUDL build and deployment automation to send start and
  status notifications to the `pudl-deployments` Zulip stream directly from
  GitHub Actions and the batch build script, with per-stage timing summaries and
  direct links to build logs and outputs. The nightly build container also no
  longer installs or bootstraps a local PostgreSQL cluster solely to initialize
  Dagster, and the batch script now refuses to trigger deployment unless all
  required stages, including output upload, completed successfully. See PR
  [#5374](https://github.com/catalyst-cooperative/pudl/pull/5374).
* Merged `PudlGeoParquetIOManager` into
  [`PudlParquetIOManager`](autoapi/pudl/dagster/io_managers/index.md#pudl.dagster.io_managers.PudlParquetIOManager) and retired the
  `geoparquet_io_manager` Dagster resource key. The four geo assets
  (`out_censusdp1tract__states/counties/tracts` and
  `out_ferc714__georeferenced_respondents`) now use `parquet_io_manager`.
  Updated the DuckDB dependency to `>=1.5,<1.6`. See issues [#4061](https://github.com/catalyst-cooperative/pudl/issues/4061), [#5074](https://github.com/catalyst-cooperative/pudl/issues/5074) and
  PR [#5347](https://github.com/catalyst-cooperative/pudl/pull/5347).
* Extended the nightly build’s Zulip reporting and stage-tracking machinery (added
  in [#5374](https://github.com/catalyst-cooperative/pudl/pull/5374)) to `pudl_deploy`. Deployments now save logs to
  `builds.catalyst.coop` and send a per-stage duration + outcome report to Zulip.
  Centralized deployment logic and validation in a `DeploymentPlan` Pydantic model in
  [`pudl.deploy.pudl`](autoapi/pudl/deploy/pudl/index.md#module-pudl.deploy.pudl). Replaced some ad-hoc `curl`/JSON GitHub Actions dispatch
  with the `gh` CLI and a shared `dispatch_github_workflow()` helper. Added
  required-argument & duplicate-key validation to `devtools/generate_batch_config.py`
  Batch job config generator used by both the PUDL and FERC EQR build workflows. See
  issue [#5382](https://github.com/catalyst-cooperative/pudl/issues/5382) and PR [#5384](https://github.com/catalyst-cooperative/pudl/pull/5384).
* Added a guard so that deploying to a permanent, version-tagged stable-release
  path that already has content raises immediately instead of silently uploading
  over it, and updated the Zenodo data release Zulip notification to state plainly
  whether a release was sandbox or production and publish or draft, with a link to
  the resulting record. See issue [#5382](https://github.com/catalyst-cooperative/pudl/issues/5382) and PR [#5384](https://github.com/catalyst-cooperative/pudl/pull/5384).

<a id="release-v2026-6-1"></a>

## v2026.6.1 (2026-06-19)

This is a monthly PUDL data release, primarily motivated by updating the EIA-860M
monthly data through February 2026. As usual, it also includes all of the other changes
that have accumulated on `main` since our last release.

This month, we have the belated EPA CEMS update for 2026Q1, the annual update
for FERC 1, some great community contributions for RUS7 and EIA-176, and an
assortment of datapackage, Dagster, and deployment notification improvements.

### Enhancements

* Overhauled PUDL’s [Frictionless Data Package](https://datapackage.org/) output to
  conform to the v2 spec. The `pudl_datapackage` Dagster asset now generates
  `datapackage.json` directly during the ETL, including full column types,
  constraints, and foreign key relationships for every Parquet table.  The descriptor is
  distributed as `pudl_parquet_datapackage.json` at the top level of the S3 bucket and
  on Zenodo, allowing potential users to browse the PUDL schema without downloading any
  data. The `pudl_parquet.zip` archive also contains a `datapackage.json` descriptor
  so it can be used as a self-describing Frictionless package after extraction. A
  reusable [`valid_datapackage_check()`](autoapi/pudl/dagster/asset_checks/index.md#pudl.dagster.asset_checks.valid_datapackage_check) factory is now
  available in [`pudl.dagster.asset_checks`](autoapi/pudl/dagster/asset_checks/index.md#module-pudl.dagster.asset_checks) to add frictionless v2 validation as an
  asset check on any datapackage output. See issues [#5122](https://github.com/catalyst-cooperative/pudl/issues/5122), [#5237](https://github.com/catalyst-cooperative/pudl/issues/5237) and PR
  [#5270](https://github.com/catalyst-cooperative/pudl/pull/5270), [#5343](https://github.com/catalyst-cooperative/pudl/pull/5343). Also makes progress towards [catalyst-cooperative/agent-skills#14](https://github.com/catalyst-cooperative/agent-skills/issues/14)
* Added a bare-bones datapackage for DBF SQLite outputs. See issue [#5200](https://github.com/catalyst-cooperative/pudl/issues/5200)
  and PR [#5275](https://github.com/catalyst-cooperative/pudl/pull/5275).

### New Data

#### EIA-176

* Added [core_eia176_\_yearly_gas_supply](data_dictionaries/pudl_db.md#core-eia176-yearly-gas-supply), which contains cleaned
  company-level natural and supplemental gas supply data from Part 4 of the EIA-176
  survey. See [#4711](https://github.com/catalyst-cooperative/pudl/issues/4711) and [#5227](https://github.com/catalyst-cooperative/pudl/pull/5227).
* Added [core_eia176_\_yearly_liquefied_natural_gas_inventory](data_dictionaries/pudl_db.md#core-eia176-yearly-liquefied-natural-gas-inventory), a new table
  containing annual LNG storage volume and capacity reported by operators on EIA Form
  176 Part 5. Data covers 2002-2024 and includes LNG terminal and marine terminal
  records. See issue [#4695](https://github.com/catalyst-cooperative/pudl/issues/4695) and PR [#5219](https://github.com/catalyst-cooperative/pudl/pull/5219).

### Expanded Data Coverage

#### EIA-191

* Updated [EIA-191](data_sources/eia191.md) data to include additional 2026 data. See
  PR [#5292](https://github.com/catalyst-cooperative/pudl/pull/5292).

#### EIA-860M

* Added [EIA-860M](data_sources/eia860.md) data through April 2026. See
  issue [#5277](https://github.com/catalyst-cooperative/pudl/issues/5277) and PR [#5284](https://github.com/catalyst-cooperative/pudl/pull/5284).

#### FERC 1

* Added 2025 data from [FERC form 1](data_sources/ferc1.md). This update
  includes several new renewable and energy storage fields in several tables.
  See issue [#5214](https://github.com/catalyst-cooperative/pudl/issues/5214) and PRs [#5236](https://github.com/catalyst-cooperative/pudl/pull/5236), [#5325](https://github.com/catalyst-cooperative/pudl/pull/5325).

#### EIA Electricity API

* Updated the [bulk EIA Electricity API](data_sources/eiaapi.md) data used to
  fill in redacted fuel prices. See PR [#5292](https://github.com/catalyst-cooperative/pudl/pull/5292).

#### EPA CEMS

* Updated the [EPA CEMS](data_sources/epacems.md) data to include 2026Q1. See PR
  [#5292](https://github.com/catalyst-cooperative/pudl/pull/5292).

#### FERC Forms 2 & 6

* Updated the raw FERC Form 2 and 6 archives to include 2025 data. This data is
  converted to SQLite, but not deeply integrated into PUDL. See PR [#5292](https://github.com/catalyst-cooperative/pudl/pull/5292).

### Documentation

* Added a data source page for [EIA-191](data_sources/eia191.md). See PR
  [#5267](https://github.com/catalyst-cooperative/pudl/pull/5267) and issue [#4756](https://github.com/catalyst-cooperative/pudl/issues/4756).
* Updated the [EIA-930](data_sources/eia930.md) column descriptions to note that
  starting in 2024Q3 EIA began reporting more granular renewable energy source
  categories, differentiating wind and solar plants with and without energy storage,
  splitting pumped hydro from conventional hydro, and adding new battery storage and
  geothermal categories. See issue [#5335](https://github.com/catalyst-cooperative/pudl/issues/5335) and PR [#5336](https://github.com/catalyst-cooperative/pudl/pull/5336).

### New Data Tests & Validations

* Added validations to [RUS7](data_sources/rus7.md) service interruption
  tables to ensure subcomponents sum to the total for annual observation
  periods. See issue [#5285](https://github.com/catalyst-cooperative/pudl/issues/5285) and PR [#5286](https://github.com/catalyst-cooperative/pudl/pull/5286).
* Validate that sub-components in
  [core_rus7_\_yearly_transmission_and_distribution_mileage](data_dictionaries/pudl_db.md#core-rus7-yearly-transmission-and-distribution-mileage) and
  [out_rus7_\_yearly_transmission_and_distribution_mileage](data_dictionaries/pudl_db.md#out-rus7-yearly-transmission-and-distribution-mileage) sum to their reported
  totals. See issue [#5314](https://github.com/catalyst-cooperative/pudl/issues/5314) and PR [#5342](https://github.com/catalyst-cooperative/pudl/pull/5342).

### Bug Fixes & Data Cleaning

* Renamed the `fuel_consumed_mmbtu` column in the `out_eia923__fuel_receipts_costs`,
  `out_eia923__monthly_fuel_receipts_costs`, and
  `out_eia923__yearly_fuel_receipts_costs` tables. This column is the result of
  dividing `total_fuel_cost` by `fuel_received_mmbtu`. The name
  `fuel_consumed_mmbtu` was misleading because the fuel received in these tables is
  not necessarily consumed in the same month, and the fuel cost is not necessarily
  associated with fuel received in the same month. The new name,
  `fuel_received_mmbtu`, more accurately reflects what the column actually contains.
  See PR [#5294](https://github.com/catalyst-cooperative/pudl/pull/5294).
* Fixed a bug in the Zenodo Data Release script which was not actually skipping
  top-level directories when deciding what to upload to Zenodo, which caused release
  failures once we started leaving the `ferc*_xbrl` directories on the filesystem. See
  PR [#5254](https://github.com/catalyst-cooperative/pudl/pull/5254).

### Performance Improvements

### Quality of Life Improvements

* Refactored Dagster-managed path handling to use a dedicated `pudl_paths` resource
  instead of constructing [`pudl.workspace.setup.PudlPaths`](autoapi/pudl/workspace/setup/index.md#pudl.workspace.setup.PudlPaths) directly throughout
  assets, IO managers, and tests. This makes path resolution more explicit in Dagster
  contexts and allows interactive definitions to override `pudl_input` and
  `pudl_output` directly when calling
  [`pudl.dagster.build.build_interactive_defs()`](autoapi/pudl/dagster/build/index.md#pudl.dagster.build.build_interactive_defs). See PR [#5261](https://github.com/catalyst-cooperative/pudl/pull/5261), [#5288](https://github.com/catalyst-cooperative/pudl/pull/5288).
* Added a PUDL devcontainer configuration to make it easier for contributors to get up
  and running, and to enable the safe use of coding agents in YOLO mode. See PRs
  [#5260](https://github.com/catalyst-cooperative/pudl/pull/5260), [#5287](https://github.com/catalyst-cooperative/pudl/pull/5287).
* Cleaned up PUDL’s default Dagster wiring by separating default resources from
  IO managers, giving shared data-config resources clearer defaults, and
  simplifying the FERC SQLite IO manager and provenance stack. Consolidated the
  FERC EQR deployment helper assets with the rest of the Dagster package layout.
  Created a new Dagster definition builder for use in notebooks and other
  interactive environments outside of a `dg`-spawned environment:
  [`pudl.dagster.build.build_interactive_defs()`](autoapi/pudl/dagster/build/index.md#pudl.dagster.build.build_interactive_defs). See issue [#5118](https://github.com/catalyst-cooperative/pudl/issues/5118) and
  PR [#5242](https://github.com/catalyst-cooperative/pudl/pull/5242).
* Migrated build and deployment notifications from Slack to Zulip. All GitHub Actions
  workflows that previously posted to Slack now send notifications to the Catalyst
  Cooperative Zulip instance via the `zulip/github-actions-zulip` action. A new
  [`ZulipNotificationResource`](autoapi/pudl/dagster/resources/index.md#pudl.dagster.resources.ZulipNotificationResource) Dagster resource was added
  to send Zulip stream messages from within assets, with best-effort error handling. The
  FERC EQR deployment helpers in [`pudl.dagster.assets.deploy.ferceqr`](autoapi/pudl/dagster/assets/deploy/ferceqr/index.md#module-pudl.dagster.assets.deploy.ferceqr) were updated
  to use it. Notification coverage was also expanded to include community activity
  (issues, discussions, comments, and pull requests from non-Catalyst contributors).
  See PRs [#5298](https://github.com/catalyst-cooperative/pudl/pull/5298), [#5328](https://github.com/catalyst-cooperative/pudl/pull/5328), [#5331](https://github.com/catalyst-cooperative/pudl/pull/5331).
* FERC provenance metadata (Zenodo DOIs, data years, XBRL extractor version) is now
  stored in the FERC SQLite datapackage files rather than only in Dagster asset
  metadata. The `ferc_to_sqlite` asset can now optionally download and reuse pre-built
  FERC SQLite outputs from the most recent nightly build, skipping expensive
  re-extraction when the inputs haven’t changed. Set `PUDL_FERC_FORCE_EXTRACT=true` to
  force re-extraction regardless. See issue [#5220](https://github.com/catalyst-cooperative/pudl/issues/5220) and PR [#5264](https://github.com/catalyst-cooperative/pudl/pull/5264).
* Migrated hashtag-prefixed comments from soon-to-be-machine-generated dbt
  schema files into their corresponding human-editable schema input files
  (`dbt/schema_inputs/**/schema.human.yml`) to preserve their content, since
  any regenerated schemas will forcibly strip out hashtag comments. See PR [#5310](https://github.com/catalyst-cooperative/pudl/pull/5310).

<a id="release-v2026-5-0"></a>

## v2026.5.0 (2026-05-17)

This is a quarterly PUDL data release, updating datasets that are released on a monthly
or quarterly basis, including the EIA-860M, year-to-date EIA-923, EIA-930, and EIA-191.
It also includes an annual update for the EIA Annual Energy Outlook (AEO).

Normally this release would also update the EPA CEMS hourly emissions dataset.
Unfortunately, the bulk CEMS data product that we archive and process was not published
as usual. We are exploring other ways of integrating the updated data.

### Enhancements

* Started distributing the raw XBRL-derived data for FERC Forms 1, 2, 6, 60, and 714
  as collections of parquet files, alongside existing SQLite and DuckDB outputs. See PR
  [#5232](https://github.com/catalyst-cooperative/pudl/pull/5232). This change is primarily in support of making these data available through
  the [PUDL Data Viewer](https://data.catalyst.coop).

#### FERC 1

* Added new [out_ferc1_\_yearly_depreciation_factors_sched336](data_dictionaries/pudl_db.md#out-ferc1-yearly-depreciation-factors-sched336) table. See issue
  [#5103](https://github.com/catalyst-cooperative/pudl/issues/5103) and PR [#5112](https://github.com/catalyst-cooperative/pudl/pull/5112).
* Added FERC Form 1 respondents’ identification and certification information as
  [core_ferc1_\_yearly_identification_certification](data_dictionaries/pudl_db.md#core-ferc1-yearly-identification-certification). See [#5150](https://github.com/catalyst-cooperative/pudl/issues/5150) and
  [#5008](https://github.com/catalyst-cooperative/pudl/pull/5008).
* Added new [out_ferc1_\_yearly_other_regulatory_assets_sched232](data_dictionaries/pudl_db.md#out-ferc1-yearly-other-regulatory-assets-sched232) table. See issue
  [#5104](https://github.com/catalyst-cooperative/pudl/issues/5104) and PR [#5170](https://github.com/catalyst-cooperative/pudl/pull/5170).

### Expanded Data Coverage

#### EIA AEO

* Added 2026 Projections from EIA AEO. See issue [#5182](https://github.com/catalyst-cooperative/pudl/issues/5182) and PR [#5198](https://github.com/catalyst-cooperative/pudl/pull/5198).

#### EIA-860M

* Added EIA-860M data through March 2026. See issue [#5225](https://github.com/catalyst-cooperative/pudl/issues/5225) and PR [#5230](https://github.com/catalyst-cooperative/pudl/pull/5230).

#### EIA-923

* Added year-to-date updates for EIA-923 data through December 2025. See issue
  [#5226](https://github.com/catalyst-cooperative/pudl/issues/5226) and PR [#5230](https://github.com/catalyst-cooperative/pudl/pull/5230).

#### EIA-930

* Updated EIA-930 data through April 2026. See [#5209](https://github.com/catalyst-cooperative/pudl/issues/5209) and [#5216](https://github.com/catalyst-cooperative/pudl/pull/5216). In the
  process made accommodations for BA changes resulting from the [Southwest Power Pool
  RTO Expansion](https://www.spp.org/documents/75997/2026%20rtoe%20swpw%20transition%20plan%20%E2%80%93%20market%20participant.pdf)

#### EIA-191

* Added [core_eia191_\_monthly_gas_storage](data_dictionaries/pudl_db.md#core-eia191-monthly-gas-storage), a new table containing monthly
  underground natural gas storage activity reported by operators to EIA on Form 191.
  Data covers 2014-present, is updated through April 2026, and includes working gas,
  base gas, and total capacity by storage field. See issue [#5209](https://github.com/catalyst-cooperative/pudl/issues/5209) and PRs
  [#5058](https://github.com/catalyst-cooperative/pudl/pull/5058) and [#5216](https://github.com/catalyst-cooperative/pudl/pull/5216). Thanks to [@irubey](https://github.com/sponsors/irubey) for this contribution!

### Documentation

* Added new component to table descriptions showing the most recent data available. See
  issue [#4586](https://github.com/catalyst-cooperative/pudl/issues/4586) and PR [#4632](https://github.com/catalyst-cooperative/pudl/pull/4632).
* Added new `forensics` tables which can be used to see all input values before PUDL
  chooses canonical values/golden records in the [entity resolution process](methodology/entity_resolution.md). See issue [#4265](https://github.com/catalyst-cooperative/pudl/issues/4265) and PR [#5157](https://github.com/catalyst-cooperative/pudl/pull/5157).

### Bug Fixes & Data Cleaning

* Removed the already deprecated `pudl.extract.ferc1.extract_dbf`,
  `pudl.extract.ferc1.extract_xbrl`, `pudl.extract.ferc1.extract_xbrl_generic`, and
  `pudl.extract.ferc1.extract_dbf_generic` functions. The extraction logic is now
  covered by the [`pudl.dagster.io_managers.ferc1_xbrl_sqlite_io_manager`](autoapi/pudl/dagster/io_managers/index.md#pudl.dagster.io_managers.ferc1_xbrl_sqlite_io_manager) and
  [`pudl.dagster.io_managers.ferc1_dbf_sqlite_io_manager`](autoapi/pudl/dagster/io_managers/index.md#pudl.dagster.io_managers.ferc1_dbf_sqlite_io_manager) IO Managers.
* Fixed a [`TypeError`](https://docs.python.org/3/library/exceptions.html#TypeError) in MCOE asset checks where `sum(exc.null_rows)` iterated
  over a DataFrame’s column names as strings instead of counting rows. Replaced with
  `len(exc.null_rows)`. See PR [#5124](https://github.com/catalyst-cooperative/pudl/pull/5124).
* Fixed a data integrity bug in the FERC SQLite IO manager where SQLite silently
  auto-incremented `NULL` values in single-column `INTEGER PRIMARY KEY` columns
  (ROWID aliases) rather than raising an `IntegrityError`. An explicit null check now
  catches this case before writing. The bug affected 11 production entity and
  association tables (e.g. `core_eia__entity_plants`,
  `core_pudl__entity_utilities_pudl`); composite PKs and non-INTEGER single PKs are
  enforced normally by SQLite and were unaffected. See PR [#5124](https://github.com/catalyst-cooperative/pudl/pull/5124).
* Updated FERC XBRL extraction to handle a new upstream behavior in which empty
  instant or duration tables are omitted from published filings. See PR [#5239](https://github.com/catalyst-cooperative/pudl/pull/5239).

### Quality of Life Improvements

* **Reorganized the test suite from** `test/` **to** `tests/` with a three-tier
  layout that matches the existing Pixi tasks: `unit/` (fast, no data),
  `integration/` (software correctness against ETL outputs), and `validate/`
  (data quality on prebuilt outputs). The old `integration/etl_test.py` was
  dissolved into per-extractor files and a `dagster/pipeline_test.py`. New unit tests
  were added for MCOE asset checks, `no_null_rows`, `weighted_quantile`, and IO
  manager null-PK behavior. See PR [#5124](https://github.com/catalyst-cooperative/pudl/pull/5124).
* **Separated dbt row count checks into a distinct**
  `pytest-validate-row-counts-nightly` *Pixi stage.\**
  `check_row_counts_per_partition` is the most frequently failing dbt test; running it
  in its own stage produces a clearly labelled line in nightly Slack reports instead of
  failing the broader data validation stage, making failures easier to triage. The stage
  is automatically skipped outside of full ETL builds. See PR [#5124](https://github.com/catalyst-cooperative/pudl/pull/5124).
* **Renamed the** `docker/` **directory to** `builds/` to better reflect that it
  contains all production build scripts and infrastructure, not just Docker-related
  files. See PR [#5124](https://github.com/catalyst-cooperative/pudl/pull/5124).
* Updated `dbt_helper update-tables --schema` to ingest “human schema input
  files” (at `dbt/schema_inputs/**/schema.human.yml`) and generate the actual
  dbt-visible schema files automatically. This gives us clear separation between
  human and machine-generated schemas and allows us to add more machine-generated
  checks. See issue [#5208](https://github.com/catalyst-cooperative/pudl/issues/5208) and PRs [#5207](https://github.com/catalyst-cooperative/pudl/pull/5207) and [#5228](https://github.com/catalyst-cooperative/pudl/pull/5228).

### Major Dagster Project Refactor

We did a major overhaul of our Dagster configuration to bring it closer to the
framework’s current best-practice recommendations, and also to experiment with the
new `dg` CLI and [Dagster agent skills](https://github.com/dagster-io/skills).

See issue [#5066](https://github.com/catalyst-cooperative/pudl/issues/5066) for an overview of the issues involved, including issues
[#5120](https://github.com/catalyst-cooperative/pudl/issues/5120), [#5123](https://github.com/catalyst-cooperative/pudl/issues/5123) and PRs [#5071](https://github.com/catalyst-cooperative/pudl/pull/5071), [#5124](https://github.com/catalyst-cooperative/pudl/pull/5124), [#5153](https://github.com/catalyst-cooperative/pudl/pull/5153). This refactor includes the following
changes:

* **Replaced the custom \`\`pudl_etl\`\` and \`\`ferc_to_sqlite\`\` CLI entry points** with
  Dagster’s official `dg launch` tool. The old entry points assembled hand-crafted
  Dagster `run_config` dicts at runtime; `dg launch` reads YAML config files that
  are version-controlled alongside the code. Four packaged config files are provided:
  `dg_fast.yml`, `dg_full.yml`, `dg_pytest.yml`, and `dg_nightly.yml`.
  Pixi convenience tasks (`pudl-with-ferc-to-sqlite`,
  `pudl-with-ferc-to-sqlite-nightly`, `ferc-to-sqlite`) wrap the most common
  invocations. The integration test suite now runs the ETL via `dg launch` as a
  subprocess, so tests exercise exactly the same code path as production.
* **Consolidated the PUDL job graph.** The previous `etl_fast` and `etl_full`
  jobs were thin wrappers assembled at import time. These are replaced by three
  top-level jobs defined directly in `pudl.etl`: `ferc_to_sqlite` (raw FERC
  prerequisite databases only), `pudl` (the main PUDL ETL assuming those raw FERC
  databases already exist), and `pudl_with_ferc_to_sqlite` (end-to-end build in a
  single job). The FERC EQR pipeline is now the `ferceqr` job. Job selection and
  asset scoping is handled by `dg launch` config files rather than by code.
* **Switched to Dagster config YAML files** for all run configuration (what years to
  process, which datasets to include, resource settings). The settings flow is now:
  `dg launch --config some_dg.yml` → `pudl.resources.PudlEtlSettingsResource`
  loads a `pudl.settings.EtlSettings` object from a path declared in that YAML
  → individual assets and IO managers read from the injected
  `EtlSettings`. This replaces the old pattern of serializing
  Pydantic models to raw `run_config` dicts, which required keeping Dagster config
  schemas manually in sync with the Pydantic models.
* **Updated Dagster resources and IO managers to use Pydantic-native**
  [`dagster.ConfigurableResource`](https://docs.dagster.io/api/dagster/resources/#dagster.ConfigurableResource) **and** [`dagster.ConfigurableIOManager`](https://docs.dagster.io/api/dagster/io-managers/#dagster.ConfigurableIOManager)
  **base classes.**
  `pudl.workspace.datastore.DatastoreResource` and
  `pudl.workspace.datastore.ZenodoDoiSettingsResource` replace the legacy
  `@resource`-decorated functions;
  `pudl.io_managers.PudlMixedFormatIOManager`,
  `pudl.io_managers.FercDbfSqliteIOManager`, and
  `pudl.io_managers.FercXbrlSqliteIOManager` replace the legacy
  `@io_manager` wrappers. Resources now receive settings via Pydantic field
  injection rather than via [`dagster.build_init_resource_context()`](https://docs.dagster.io/api/dagster/resources/#dagster.build_init_resource_context) config dicts.
* **Added FERC SQLite provenance tracking** via the new
  `pudl.ferc_sqlite_provenance` module. Each time a FERC SQLite asset
  materializes, it records a fingerprint as [`dagster.MaterializeResult`](https://docs.dagster.io/api/dagster/assets/#dagster.MaterializeResult)
  metadata: the Zenodo DOI of the source archive, the years included, and a hash of
  the ETL settings. When a downstream PUDL asset subsequently loads from that SQLite
  file, the IO manager checks the stored fingerprint against the current run’s
  settings and raises a descriptive error if the DOIs, years, or settings are
  incompatible. This eliminates a class of silent correctness failures that occurred
  when stale FERC SQLite databases from a previous run were silently reused.
* **Replaced the \`\`disabled: true\`\` flag** in FERC-to-SQLite settings with
  `years: []` (empty list). An empty `years` list is unambiguous — “process zero
  years” — and eliminates the need for a separate boolean field that had to be
  checked in addition to the years list. The `disabled` flag has been removed from
  all settings classes and YAML config files; FERC 2, 6, and 60 DBF/XBRL configs
  that previously used `disabled: true` now use `years: []`.
* **Reorganized the integration test infrastructure** in `tests/conftest.py`. The
  old approach ran the PUDL ETL in-process using `execute_in_process`, which
  bypassed the standard `dg launch` entry point and required each test fixture to
  hand-assemble Dagster `run_config` dicts. All three FERC extraction fixtures and
  the `pudl_io_manager` fixture are replaced by a single `prebuilt_outputs`
  fixture that runs the full `pudl_with_ferc_to_sqlite` job via `dg launch` as a
  subprocess, with coverage collection appended to the existing test coverage report.
  A persistent [`dagster.DagsterInstance`](https://docs.dagster.io/api/dagster/internals/#dagster.DagsterInstance) fixture allows test code to read
  asset materialisation metadata written by that subprocess. Pytest CLI flags are
  renamed for clarity: `--live-dbs` → `--live-pudl-output`, `--tmp-data` →
  `--temp-pudl-input`, `--etl-settings` → `--dg-config`.
* Made [`pudl.dagster`](autoapi/pudl/dagster/index.md#module-pudl.dagster) the canonical Dagster orchestration package while keeping
  [`pudl.definitions`](autoapi/pudl/definitions/index.md#module-pudl.definitions) as the stable `dg` code location entrypoint. As part of
  this boundary cleanup, Dagster-specific resources (including the FERC EQR deployment
  sensor and the FERC EQR partition definition) were consolidated under
  [`pudl.dagster`](autoapi/pudl/dagster/index.md#module-pudl.dagster), older top-level Dagster compatibility exposure was removed, and
  internal imports and documentation were updated to use [`pudl.dagster`](autoapi/pudl/dagster/index.md#module-pudl.dagster). See issue
  [#5123](https://github.com/catalyst-cooperative/pudl/issues/5123) and PR [#5124](https://github.com/catalyst-cooperative/pudl/pull/5124).
* **Cleaned up several legacy package boundaries** that had accumulated over time.
  The `pudl.etl` package was removed after the Dagster refactor had already moved
  its substantive content elsewhere — what remained was foreign key validation and a
  continuity check helper that now live with the validation and asset-check code that
  actually uses them. The `pudl.convert` subpackage was an arbitrary grouping of two
  unrelated utilities; each was moved to the package that reflects what it actually
  does (extraction vs. documentation generation). The `pudl.validate` module grew
  into a subpackage to keep dbt orchestration, database integrity checks, and data
  quality utilities from being lumped together in a single file.  See [#5123](https://github.com/catalyst-cooperative/pudl/issues/5123)
  and PR [#5124](https://github.com/catalyst-cooperative/pudl/pull/5124).
* **Consolidated all CLI entry points under** `src/pudl/scripts/`. Previously,
  `pudl_datastore` lived inside the datastore module and `pudl_service_territories`
  lived inside the analysis module — logical homes for the underlying logic, but
  inconvenient for anyone trying to find all the command-line tools in one place.
  All scripts are now thin wrappers in `src/pudl/scripts/`, with heavy imports
  deferred so `--help` is fast (or… will be, once we thin out the monstrous
  top-level PUDL imports). `pudl_datastore` also gained a new `--all` flag
  to download every known dataset without having to enumerate them explicitly. A unit
  test enforces many of these CLI conventions going forward.  See [#5123](https://github.com/catalyst-cooperative/pudl/issues/5123) and PR
  [#5124](https://github.com/catalyst-cooperative/pudl/pull/5124).
* **Renamed the \`\`eia_bulk_elec\`\` module to \`\`eiaapi_electricity\`\`** to match the
  naming of the underlying source.  See [#5123](https://github.com/catalyst-cooperative/pudl/issues/5123) and PR [#5124](https://github.com/catalyst-cooperative/pudl/pull/5124).
* **Standardized acronym capitalization in compound class names.** Classes that
  combined two acronyms (e.g. `FERC` + `SQLite`) were inconsistently named.
  They now follow the Python convention of treating each acronym as a single
  title-cased word, so `SQLite` becomes `Sqlite` when it appears mid-name
  (e.g. `FercDbfSqliteIOManager`).  See [#5123](https://github.com/catalyst-cooperative/pudl/issues/5123) and PR [#5124](https://github.com/catalyst-cooperative/pudl/pull/5124).
* **Renamed Pydantic settings classes from** `*Settings` **to** `*DataConfig`
  **and tightened container field names.** The old names were too vague — these
  classes define *which data gets processed*, not general application settings.
  The new names make that explicit and align with Dagster’s own `Config`
  naming convention. The top-level `EtlSettings` is now `GlobalDataConfig`;
  `DatasetsSettings` (the PUDL job) is now `PudlDataConfig`; and field names
  on the container classes drop redundant suffixes (e.g. `ferc_to_sqlite_settings`
  → `ferc_to_sqlite`, `datasets` → `pudl`). The data config and Dagster
  config YAML files are updated to match. See PR [#5153](https://github.com/catalyst-cooperative/pudl/pull/5153).

<a id="release-v2026-4-0"></a>

## v2026.4.0 (2026-04-09)

This is a monthly PUDL data release, primarily motivated by updating the EIA-860M
monthly data through February 2026. As usual, it also includes all of the other changes
that have accumulated on `main` since our last release.

This month, that means a substantial expansion of our USDA Rural Utilities Service (RUS)
Forms 7 and 12 coverage, and additional validations and metadata cleanup as those tables
stabilized. We addressed a few data quality issues, including fixes for FERC EQR,
EIA-757A extraction, EIA-861 column naming, and duplicate utility ID mappings.

On the tooling and documentation side, PUDL now has a refreshed PyData-based docs theme,
a new entity-resolution methodology page with Mermaid
diagrams, an experimental standalone data deployment workflow, and several improvements
to developer tooling and automation, including automated Zenodo DOI updates, more
resilient docs checks, and new secret-scanning hooks. See below for all the details.

### New Data

#### RUS 7 & RUS 12

* Added de-normalized output tables for RUS 7 and RUS 12 as a follow up from
  [#5040](https://github.com/catalyst-cooperative/pudl/pull/5040). See [#5077](https://github.com/catalyst-cooperative/pudl/pull/5077).
* Added last rounds of core and output tables from RUS Form 7 and 12.
  See [#5087](https://github.com/catalyst-cooperative/pudl/pull/5087), [#5091](https://github.com/catalyst-cooperative/pudl/pull/5091) and [#5145](https://github.com/catalyst-cooperative/pudl/pull/5145).

### Expanded Data Coverage

#### EIA-860M

* Updated EIA-860M with monthly data through February 2026. See [#5148](https://github.com/catalyst-cooperative/pudl/issues/5148) and
  [#5161](https://github.com/catalyst-cooperative/pudl/pull/5161).

### Documentation

* We have a new look! As part of preparing to move our documentation from RTD to
  our own GitHub Pages site, we needed to switch our Sphinx theme from Furo to
  PyData, in order to take advantage of their version switcher feature. All
  pages are still there, no URLs have changed, but you may find familiar links
  in a different spot on the page than you are used to. The top nav bar has
  limited real estate so we have collected our docs into two groups:
  * Data Documentation now houses the data access, data dictionary, data source,
    and methodology pages
  * Development now houses the API reference, developer guide, contributing
    guide, and code of conduct

  See issue [#4822](https://github.com/catalyst-cooperative/pudl/issues/4822) and PR [#5057](https://github.com/catalyst-cooperative/pudl/pull/5057) for more details.
* Added a [methodology page](methodology/entity_resolution.md) explaining
  how EIA entity harvesting reconciles inconsistently reported plant, utility,
  boiler, and generator attributes into normalized entity and yearly SCD
  tables. The docs now also support
  [Mermaid diagrams](https://sphinxcontrib-mermaid-demo.readthedocs.io)
  for illustrating pipeline behavior. See [#5108](https://github.com/catalyst-cooperative/pudl/pull/5108).
* Fixed the data dictionary’s Polars examples for public AWS-hosted Parquet
  access so they work without AWS credentials. See [#5171](https://github.com/catalyst-cooperative/pudl/pull/5171).

### New Data Tests & Validations

* Validate that sub-components in [core_rus7_\_yearly_energy_efficiency](data_dictionaries/pudl_db.md#core-rus7-yearly-energy-efficiency),
  [core_rus7_\_yearly_patronage_capital](data_dictionaries/pudl_db.md#core-rus7-yearly-patronage-capital),
  [core_rus7_\_yearly_power_requirements_electric_customers](data_dictionaries/pudl_db.md#core-rus7-yearly-power-requirements-electric-customers),
  [core_rus7_\_yearly_power_requirements_electric_sales](data_dictionaries/pudl_db.md#core-rus7-yearly-power-requirements-electric-sales),
  [core_rus7_\_yearly_statement_of_operations](data_dictionaries/pudl_db.md#core-rus7-yearly-statement-of-operations) and
  [core_rus12_\_yearly_statement_of_operations](data_dictionaries/pudl_db.md#core-rus12-yearly-statement-of-operations) and their corresponding output
  tables sum to their reported totals. See [#5039](https://github.com/catalyst-cooperative/pudl/issues/5039) and [#5073](https://github.com/catalyst-cooperative/pudl/pull/5073).
* Expanded validation coverage for newly added RUS Form 7 and 12 tables as the
  tables and their metadata stabilized. See [#5125](https://github.com/catalyst-cooperative/pudl/pull/5125), [#5131](https://github.com/catalyst-cooperative/pudl/pull/5131), and
  [#5138](https://github.com/catalyst-cooperative/pudl/pull/5138).
* Modified schema checks so they can be applied to the largest tables, which have
  typically been excluded from these checks. See Issue [#5022](https://github.com/catalyst-cooperative/pudl/issues/5022) and PR [#5043](https://github.com/catalyst-cooperative/pudl/pull/5043).

### Bug Fixes & Data Cleaning

* Fixed a bug in [`pudl.analysis.allocate_gen_fuel`](autoapi/pudl/analysis/allocate_gen_fuel/index.md#module-pudl.analysis.allocate_gen_fuel) that caused
  [out_eia923_\_monthly_generation_fuel_by_generator_energy_source](data_dictionaries/pudl_db.md#out-eia923-monthly-generation-fuel-by-generator-energy-source) to incorrectly
  allocate generation and fuel consumption to retired generators. The previous logic
  identified “retiring” generators by checking whether any generation or fuel columns
  were non-null after the generation fuel table was merged in on prime mover and energy
  source code (not generator ID), so a retired generator sharing a PM/ESC combo with
  active generators at the same plant was incorrectly kept as active. The fix narrows
  the retiring-generator check to only the generator-level generation table column
  and also preserves retired generators whose PM/ESC combination is unique to them at
  the plant, enabling generator-level attribution of the reported fuel/generation.  See
  [#4789](https://github.com/catalyst-cooperative/pudl/pull/4789). Thanks to [@grgmiller](https://github.com/sponsors/grgmiller) for identifying this issue and making a PR!
* Fixed a FERC EQR transform bug that was incorrectly parsing non-date contract
  fields as datetimes, which caused several output columns to become entirely
  `NULL`. Also clarified and separated the `product_name` metadata
  descriptions and allowed values for
  [core_ferceqr_\_contracts](data_dictionaries/pudl_db.md#core-ferceqr-contracts) and [core_ferceqr_\_transactions](data_dictionaries/pudl_db.md#core-ferceqr-transactions) so their
  constraints match their distinct ENUM constraints as documented in
  [`v3.5 of the FERC EQR data dictionary`](data_sources/ferceqr/ferceqr_data_dictionary_v35_2020-11-23.pdf).
  See [#5085](https://github.com/catalyst-cooperative/pudl/pull/5085).
* Fixed EIA-757A extraction so raw columns are renamed correctly into PUDL’s
  standard naming conventions. See [#4722](https://github.com/catalyst-cooperative/pudl/issues/4722) and [#5107](https://github.com/catalyst-cooperative/pudl/pull/5107).
* Removed approximately 200 duplicate PUDL utility IDs from
  `src/pudl/package_data/glue/utility_id_pudl.csv`, where a FERC or EIA utility was
  mapped to more than one PUDL ID. See [#4988](https://github.com/catalyst-cooperative/pudl/issues/4988) and [#5117](https://github.com/catalyst-cooperative/pudl/pull/5117).
* Fixed some wonky column names in the EIA-861
  `core_eia861__yearly_demand_side_management_ee_dr` table. See issue [#5132](https://github.com/catalyst-cooperative/pudl/issues/5132)
  and PR [#5135](https://github.com/catalyst-cooperative/pudl/pull/5135).

### Quality of Life Improvements

* Added a new standalone data deployment workflow, `deploy-pudl.yml`. This is
  still in testing, but will allow us to separate deployment from builds, enabling
  deployment from an existing build and creating more modular and reusable
  infrastructure. See issue [#5003](https://github.com/catalyst-cooperative/pudl/issues/5003) and PR [#5016](https://github.com/catalyst-cooperative/pudl/pull/5016).
* Moved large FERC1 category dicts to .yaml files to reduce LOC. See [#4989](https://github.com/catalyst-cooperative/pudl/issues/4989) and
  PR [#5023](https://github.com/catalyst-cooperative/pudl/pull/5023). Thanks to [@andbusch](https://github.com/sponsors/andbusch) for getting this in!
* Added a script and GitHub Actions workflow to automatically update Zenodo DOIs
  in package data for straightforward data-source refreshes. See [#5051](https://github.com/catalyst-cooperative/pudl/pull/5051).
* Added environment variable controls for Sphinx docs builds:
  `PUDL_DOCS_KEEP_GENERATED_FILES` now preserves generated docs artifacts for
  debugging, and `PUDL_DOCS_DISABLE_INTERSPHINX` disables intersphinx lookups
  when needed (for example in CI docs checks to avoid external docs outages).
  See PR [#5095](https://github.com/catalyst-cooperative/pudl/pull/5095).
* Added a fast `docs-check` Pixi task for validation-only Sphinx runs and
  updated the `pytest` GitHub Actions docs check job to use it, while leaving
  Read the Docs and GitHub Pages HTML builds unchanged. See PR [#5128](https://github.com/catalyst-cooperative/pudl/pull/5128).
* Added a `docs-linkcheck` Pixi task and a separate manually triggered GitHub
  Actions workflow for experimenting with automated documentation link checking.
  See PR [#5128](https://github.com/catalyst-cooperative/pudl/pull/5128).
* Switched repository tooling from `pre-commit` to `prek` and added
  `trufflehog` and `detect-secrets` hooks to help prevent secrets from being
  committed to the repository. See [#5141](https://github.com/catalyst-cooperative/pudl/pull/5141).

<a id="release-v2026-3-0"></a>

## v2026.3.0 (2026-03-12)

This is a monthly PUDL data release, nominally aimed at updating the EIA-860M monthly
data, but this month there’s a lot of other brand new data along for the ride!

With the addition of the [\_core_eia923_\_yearly_emissions_control](data_dictionaries/pudl_db.md#i-core-eia923-yearly-emissions-control) which describes
installed emissions control equipment and its operation we’ve completed our initial
coverage of EIA-923 Schedule 8. We’ve continued expanding our coverage of USDA Rural
Utilities Service (RUS) Forms 7 and 12, and now have data source documentation pages for
both forms.

On the tooling side, PUDL is now compatible with Dagster’s official `dg` CLI, and a
new DuckDB helper script makes it easy to compare local, nightly, and stable data builds
during development.  Data quality improvements include standardizing emissions control
efficiency values from percentages to decimals, cleaning up missing measurement codes in
EPA CEMS data, and uniformly adopting `report_date` across several EIA environmental
equipment tables.

### Enhancements

* Renamed `core_eia923__monthly_fuel_receipts_costs` to
  [core_eia923_\_fuel_receipts_costs](data_dictionaries/pudl_db.md#core-eia923-fuel-receipts-costs) as it is not aggregated monthly and
  does not belong with our other timeseries tables. Updated table description
  details for this and related tables to explain why receipts are not aggregated
  in this table, and how aggregation in the associated monthly and yearly tables
  affects the columns available and missingness handling. See [#5029](https://github.com/catalyst-cooperative/pudl/pull/5029).

### New Data

#### EIA-923

* Added a new table derived from EIA-923 Schedule 8C describing installed emissions
  control equipment and its operation: [\_core_eia923_\_yearly_emissions_control](data_dictionaries/pudl_db.md#i-core-eia923-yearly-emissions-control).
  With this table, we now have preliminary versions of all of EIA-923 Schedule 8.
  See issue [#4081](https://github.com/catalyst-cooperative/pudl/issues/4081) and PRs [#4668](https://github.com/catalyst-cooperative/pudl/pull/4668), [#5048](https://github.com/catalyst-cooperative/pudl/pull/5048). Thanks to [@alexclippinger](https://github.com/sponsors/alexclippinger) for
  working on this!

#### RUS 7

* Extracted the remaining RUS Form 7 tables, completing initial extraction of all RUS
  Form 7 data. Also standardized the extraction method across RUS Forms 7 and 12. See
  [#5030](https://github.com/catalyst-cooperative/pudl/issues/5030) and [#5031](https://github.com/catalyst-cooperative/pudl/pull/5031).
* Transformed more RUS 7 tables. See PR [#5034](https://github.com/catalyst-cooperative/pudl/pull/5034).

#### RUS 12

* Extracted the remaining RUS Form 12 tables, completing initial extraction of all RUS
  Form 12 data. See [#4959](https://github.com/catalyst-cooperative/pudl/issues/4959) and [#5031](https://github.com/catalyst-cooperative/pudl/pull/5031).
* Transformed more RUS 12 tables. See [#4886](https://github.com/catalyst-cooperative/pudl/issues/4886), PR [#5018](https://github.com/catalyst-cooperative/pudl/pull/5018) and PR [#5034](https://github.com/catalyst-cooperative/pudl/pull/5034).

### Expanded Data Coverage

#### EIA-860M

* Updated EIA-860M with monthly data through January 2026. See [#5042](https://github.com/catalyst-cooperative/pudl/issues/5042) and
  [#5044](https://github.com/catalyst-cooperative/pudl/pull/5044).

### Documentation

* Fixed remaining tables with malformed summaries so they render starting with a
  complete sentence. Added checks to prevent future regressions. See [#5029](https://github.com/catalyst-cooperative/pudl/pull/5029).
* Added data source documentation pages for
  [RUS Form 7](data_sources/rus7.md) and [RUS Form 12](data_sources/rus12.md).
  See [#4889](https://github.com/catalyst-cooperative/pudl/issues/4889) and [#5028](https://github.com/catalyst-cooperative/pudl/pull/5028).
* Added direct links to table previews on the
  [PUDL Data Viewer](https://data.catalyst.coop) from PUDL data dictionary and data
  source documentation pages. See [#5047](https://github.com/catalyst-cooperative/pudl/pull/5047).
* Replaced stale references to our use of `make` with current `pixi run` task
  commands. See PR [#5075](https://github.com/catalyst-cooperative/pudl/pull/5075)

### New Data Tests & Validations

* Added an initial set of dbt data validations for the new RUS Form 7
  and Form 12 tables. See [#4887](https://github.com/catalyst-cooperative/pudl/issues/4887), [#4888](https://github.com/catalyst-cooperative/pudl/issues/4888) and [#5017](https://github.com/catalyst-cooperative/pudl/pull/5017).
* Add dbt data validations that will flag emissions removal efficiencies outside the
  valid range 0.0-1.0 and emissions control equipment test dates from before 1950 or
  after the current year. See PR [#5048](https://github.com/catalyst-cooperative/pudl/pull/5048).
* Normalized RUS-7 and RUS-12 borrower ID’s, names and state in
  [core_rus7_\_entity_borrowers](data_dictionaries/pudl_db.md#core-rus7-entity-borrowers) and [core_rus12_\_entity_borrowers](data_dictionaries/pudl_db.md#core-rus12-entity-borrowers).
  See [#5040](https://github.com/catalyst-cooperative/pudl/issues/5040) and PR [#5056](https://github.com/catalyst-cooperative/pudl/pull/5056).
* Added row count and data validation tests for the new RUS Form 12 tables introduced
  in [#5018](https://github.com/catalyst-cooperative/pudl/pull/5018). See [#5060](https://github.com/catalyst-cooperative/pudl/pull/5060).

### Bug Fixes & Data Cleaning

* Set unknown `mass_measurement_code` values to `NULL` in
  [core_epacems_\_hourly_emissions](data_dictionaries/pudl_db.md#core-epacems-hourly-emissions) so the data conforms to the expected ENUM
  constraint. See [#5041](https://github.com/catalyst-cooperative/pudl/pull/5041).
* Improved parsing of the poorly formatted `so2_test_date` column found in
  [\_core_eia923_\_yearly_fgd_operation_maintenance](data_dictionaries/pudl_db.md#i-core-eia923-yearly-fgd-operation-maintenance). See PR [#5048](https://github.com/catalyst-cooperative/pudl/pull/5048).
* Standardized emissions control equipment efficiencies to be stated as a decimal number
  between 0.0-1.0, rather than a percentage between 0-100. Removed misleading `_pct`
  column name suffixes on efficiency columns that had values between 0.0-1.0. See PR
  [#5048](https://github.com/catalyst-cooperative/pudl/pull/5048).
* Standardized a few new environmental equipment tables from EIA to use `report_date`
  rather than `report_year` as their time dimension in anticipation of more deeply
  integrating them into PUDL. See issue [#4741](https://github.com/catalyst-cooperative/pudl/issues/4741) and PR [#5063](https://github.com/catalyst-cooperative/pudl/pull/5063). Affected
  tables include:
  * [\_core_eia923_\_yearly_byproduct_disposition](data_dictionaries/pudl_db.md#i-core-eia923-yearly-byproduct-disposition)
  * [\_core_eia923_\_yearly_byproduct_expenses_and_revenues](data_dictionaries/pudl_db.md#i-core-eia923-yearly-byproduct-expenses-and-revenues)
  * [core_eia860_\_scd_emissions_control_equipment](data_dictionaries/pudl_db.md#core-eia860-scd-emissions-control-equipment)
  * [out_eia860_\_yearly_emissions_control_equipment](data_dictionaries/pudl_db.md#out-eia860-yearly-emissions-control-equipment)

### Quality of Life Improvements

* Added a DuckDB helper script (`devtools/duckdb/`) that generates a DuckDB file with
  views pointing at local, nightly, and stable PUDL Parquet outputs. This makes it easy
  to compare data versions during development and to use the
  [DuckDB UI](https://duckdb.org/docs/stable/core_extensions/ui) for column-level
  statistics and data inspection. See [#5015](https://github.com/catalyst-cooperative/pudl/pull/5015).
* Improved schema enforcement for tables created with DuckDB by updating
  `Resource.to_duckdb_dtypes` to handle ENUM types, enabling FERC EQR tables
  produced with DuckDB to properly conform to their defined schema. See [#5027](https://github.com/catalyst-cooperative/pudl/pull/5027).
* Made our raw spreadsheet extraction multi-assets and static table multi-assets
  subsettable for better ergonomics when selecting upstream asset dependencies using
  Dagster’s `dg` CLI. See issue [#5061](https://github.com/catalyst-cooperative/pudl/issues/5061) and PR [#5062](https://github.com/catalyst-cooperative/pudl/pull/5062).
* Adapted the PUDL project layout and configuration slightly in order to allow us to
  start using
  [dg: Dagster’s official CLI tool](https://docs.dagster.io/api/clis/dg-cli/dg-cli-reference).
  See PR [#5075](https://github.com/catalyst-cooperative/pudl/pull/5075).

<a id="release-v2026-2-0"></a>

## v2026.2.0 (2026-02-12)

This is a quarterly PUDL data release, and includes quarterly updates to data
sources that are released continuously, like EIA-930, bulk EIA electricity API
data, EPA CEMS hourly emissions and EIA-860M. This is also our first release of
the FERC EQR company identifiers table, and tables from USDA’s Rural Utility
Service (RUS) forms 7 and 12, which collect financial and operational
information about rural utilities in a manner similar to EIA Form 861 and FERC
Form 1. FERC EQR data is now available for download, though in a slightly
different location due to its scale. Along for the ride are improvements to
accuracy, memory performance, and Zenodo handling. See below for all the
details.

### New Data

#### RUS 7

* Extracted data for ten USDA RUS tables. See [#4897](https://github.com/catalyst-cooperative/pudl/issues/4897) and PR [#4906](https://github.com/catalyst-cooperative/pudl/pull/4906).
* Transformed and published USDA RUS tables. See [#4885](https://github.com/catalyst-cooperative/pudl/issues/4885), PR [#4939](https://github.com/catalyst-cooperative/pudl/pull/4939), PR
  [#4971](https://github.com/catalyst-cooperative/pudl/pull/4971) and PR [#4974](https://github.com/catalyst-cooperative/pudl/pull/4974).

#### RUS-12

* Extracted data for twelve USDA RUS tables. See [#4900](https://github.com/catalyst-cooperative/pudl/issues/4900) and PR [#4916](https://github.com/catalyst-cooperative/pudl/pull/4916).
* Transformed and published USDA RUS tables. See [#4901](https://github.com/catalyst-cooperative/pudl/issues/4901), PR [#4970](https://github.com/catalyst-cooperative/pudl/pull/4970) and PR
  [#4979](https://github.com/catalyst-cooperative/pudl/pull/4979).

#### FERC EQR

* Added the company identifiers (CID) table from EQR. See [#4851](https://github.com/catalyst-cooperative/pudl/issues/4851) and
  [#4967](https://github.com/catalyst-cooperative/pudl/pull/4967). Also, note that the actual FERC EQR data is available on [PUDL
  Viewer](https://data.catalyst.coop/search?q=ferceqr) as well as [on S3 for
  direct download](https://docs.catalyst.coop/pudl/en/nightly/data_access.html#ferc-eqr-experimental)

### Expanded Data Coverage

* Updated DOIs for the EIA-191 and EIA-757a (they pertain to natural gas) since we
  extract them, even though we don’t process the data yet. This added 2 more years to
  the EIA-191 data. See PR [#4879](https://github.com/catalyst-cooperative/pudl/pull/4879).

#### EPA CEMS

* Updated EPA CEMS hourly emissions data through December 2025. See [#4986](https://github.com/catalyst-cooperative/pudl/issues/4986)
  and [#4990](https://github.com/catalyst-cooperative/pudl/pull/4990).

#### EIA-860M

* Updated EIA-860M with monthly data through December 2025. See [#4983](https://github.com/catalyst-cooperative/pudl/issues/4983) and
  [#4993](https://github.com/catalyst-cooperative/pudl/pull/4993).

#### EIA-923

* Updated EIA-923 with monthly data through November 2025. See [#4984](https://github.com/catalyst-cooperative/pudl/issues/4984) and
  [#4993](https://github.com/catalyst-cooperative/pudl/pull/4993).

#### EIA-930

* Updated EIA-930 data through December 2025. See [#4985](https://github.com/catalyst-cooperative/pudl/issues/4985)
  and [#4995](https://github.com/catalyst-cooperative/pudl/pull/4995).

#### EIA Bulk Electricity API

* Updated the EIA Bulk Electricity data through November 2025.
  See [#4987](https://github.com/catalyst-cooperative/pudl/issues/4987) and PR [#5001](https://github.com/catalyst-cooperative/pudl/pull/5001).

#### EIA-176

* Updated EIA-176 data through 2024. See [#5000](https://github.com/catalyst-cooperative/pudl/issues/5000) and [#5005](https://github.com/catalyst-cooperative/pudl/pull/5005).

### Documentation

* Added a data source documentation page for the [FERC EQR](data_sources/ferceqr.md).
  See [#4852](https://github.com/catalyst-cooperative/pudl/issues/4852) and PR [#4879](https://github.com/catalyst-cooperative/pudl/pull/4879).
* Added data access instructions for the [FERC EQR](data_sources/ferceqr.md) and
  created examples specific to our larger (>1GB) and partitioned tables in the
  [PUDL Data Dictionary](data_dictionaries/pudl_db.md). See issues [#4869](https://github.com/catalyst-cooperative/pudl/issues/4869), [#4951](https://github.com/catalyst-cooperative/pudl/issues/4951) and PR [#4958](https://github.com/catalyst-cooperative/pudl/pull/4958).
  Affected tables include:
  * [core_epacems_\_hourly_emissions](data_dictionaries/pudl_db.md#core-epacems-hourly-emissions)
  * [core_ferceqr_\_contracts](data_dictionaries/pudl_db.md#core-ferceqr-contracts)
  * [core_ferceqr_\_quarterly_identity](data_dictionaries/pudl_db.md#core-ferceqr-quarterly-identity)
  * [core_ferceqr_\_quarterly_index_pub](data_dictionaries/pudl_db.md#core-ferceqr-quarterly-index-pub)
  * [core_ferceqr_\_transactions](data_dictionaries/pudl_db.md#core-ferceqr-transactions)
  * [out_vcerare_\_hourly_available_capacity_factor](data_dictionaries/pudl_db.md#out-vcerare-hourly-available-capacity-factor)

### Bug Fixes & Data Cleaning

* We added an automatic script to help match FERC and EIA utilities with near-identical
  utility names as part of our ongoing data updates. As a result, we have matched an
  additional 115 utilities and resolved a small handful of cases where a FERC utility
  was mapped to more than one PUDL ID. Through this process, we also identified a bug
  that was resulting in us assigning the least common utility name and prime mover code
  to records to harvested EIA records when there were inconsistent values reported.
  Fixing this resulted in overall improved accuracy of the data. 3,650 utilities were
  reassigned names, resulting in approximately 150 additional matches to SEC 10K
  filings. 86 generators were reassigned prime mover codes, resulting in re-allocated
  net generation. See [#1317](https://github.com/catalyst-cooperative/pudl/issues/1317), [#4934](https://github.com/catalyst-cooperative/pudl/issues/4934) and [#4913](https://github.com/catalyst-cooperative/pudl/issues/4913), as well as PR
  [#4975](https://github.com/catalyst-cooperative/pudl/pull/4975).

### Performance Improvements

* Improved memory performance of EIA-930 by translating transforms to use `duckdb`.

### Quality of Life Improvements

* Consolidated local and remote Zenodo cache management under a single API that uses the
  high-level abstraction of the `upath.UPath` class. See issue [#4860](https://github.com/catalyst-cooperative/pudl/issues/4860) and
  PR [#4870](https://github.com/catalyst-cooperative/pudl/pull/4870).
* Pulled the list of Zenodo DOIs that define the raw input data used by PUDL out into a
  stand-alone settings file, rather than hard-coding them in the PUDL Datastore module.
  This makes the DOIs more easily accessible for use in other contexts, such as when
  calculating the GitHub Actions cache hash. Also made the GitHub Actions cache more
  lenient, so that if it misses on an exact cache key, it will just download the most
  recent cache of inputs. This should reduce the amount of data we need to download to
  run the CI on GitHub and speed things up slightly. It also means we can be more
  selective about when the `zenodo-cache-sync` workflow is run. Now it is only
  triggered when the `zenodo_dois.yml` file is changed, not any time the Datastore
  module is changed. See issue [#4494](https://github.com/catalyst-cooperative/pudl/issues/4494) and PR [#4870](https://github.com/catalyst-cooperative/pudl/pull/4870).
* Modernized the `datapackage.json` metadata stored on Zenodo for the
  [Census DP1](data_sources/censusdp1tract.md) data source, enabling the removal of
  a special case in the Datastore that only existed to deal with very old archive
  metadata. See PR [#4879](https://github.com/catalyst-cooperative/pudl/pull/4879).
* Data source documentation pages now display the source data concept DOI with a link to
  the archive on Zenodo. See PR [#4879](https://github.com/catalyst-cooperative/pudl/pull/4879).
* Made a change to the Datastore that allows it to obtain metadata from a
  `datapackage.json` file stored on Zenodo, even if the data referenced by the data
  package is stored on GCS, as is the case with FERC EQR. See the
  [FERC EQR archive on Zenodo](https://doi.org/10.5281/zenodo.18251901) as an
  example. See PR [#4879](https://github.com/catalyst-cooperative/pudl/pull/4879).
* Added handling to [`pudl.transform.classes.StringCategories`](autoapi/pudl/transform/classes/index.md#pudl.transform.classes.StringCategories) so that the
  `categories` key of transform params can be specified as a Path to a YAML file.
  This will make it possible to include large categorization sets without the params
  Python files becoming unwieldy. See PR [#4978](https://github.com/catalyst-cooperative/pudl/pull/4978).

<a id="release-v2026-1-0"></a>

## v2026.1.0 (2026-01-14)

This is a regular monthly data release, primarily intended to ensure that PUDL has the
most up-to-date EIA-860M data. Along for the ride are the initial ETL for FERC EQR data,
changes to the build system, and nicer units on a few columns.

### Application, not Library

From now on we will treat PUDL like an application rather than a library that other
projects are expected to install as a package and depend on.  There’s no change in the
licensing or openness of the project – this is just a technical evolution.  Explicitly
treating PUDL like a standalone application will make it easier for us to do releases
and ensure that we have a reproducible environment. The packages which we’ve been
distributing on PyPI and `conda-forge` have also had major dependency resolution
issues for a while now.

It’s also been our experience that almost all users want the data, not the pipeline. The
backend is primarily of interest to Catalyst developers and open source contributors,
who will continue to work within the development environment. See [Development Setup](dev/dev_setup.md)
for instructions on how to set it up. See PR [#4871](https://github.com/catalyst-cooperative/pudl/pull/4871) for where many of these changes
were made.

* We are no longer publishing PUDL releases as packages on [PyPI](https://pypi.org/project/catalystcoop.pudl/)
  or [conda-forge](https://anaconda.org/channels/conda-forge/packages/catalystcoop.pudl/overview).
* Instead, PUDL will need to be installed from source, and is expected to be run in a
  locked environment, and not specified as a normal dependency in other projects.
* Tagged PUDL releases (corresponding to each of our data releases) will still be
  archived automatically on [Zenodo](https://doi.org/10.5281/zenodo.3404014) as well
  as on our [GitHub Releases page](https://github.com/catalyst-cooperative/pudl/releases).
* PUDL data releases will continue to be distributed through a variety of channels. See
  [Data Access](data_access.md) for the details.

### Enhancements

### New Data

* Added a new ETL for FERC EQR data, as well as associated infrastructure for running
  the job and publishing outputs, which can be found at
  `s3://pudl.catalyst.coop/ferceqr`. There are 4 new tables which are produced by
  this ETL including, [core_ferceqr_\_quarterly_identity](data_dictionaries/pudl_db.md#core-ferceqr-quarterly-identity),
  [core_ferceqr_\_contracts](data_dictionaries/pudl_db.md#core-ferceqr-contracts), [core_ferceqr_\_quarterly_index_pub](data_dictionaries/pudl_db.md#core-ferceqr-quarterly-index-pub), and
  [core_ferceqr_\_transactions](data_dictionaries/pudl_db.md#core-ferceqr-transactions). Due to the size of this data, the tables are split
  into a set of parquet files partitioned by year-quarter, and cannot be downloaded
  as a single file like other PUDL tables.

### Expanded Data Coverage

#### EIA-860M

* Updated EIA-860M with monthly data through November 2025. See [#4903](https://github.com/catalyst-cooperative/pudl/pull/4903).

### New Data Tests & Validations

### Bug Fixes & Data Cleaning

* Standardized `max_steam_flow_1000_lbs_per_hour` to `max_steam_flow_lbs_per_hour`.
  Units changed to “lbs_per_hour” and rounded to nearest 100 lbs in the
  [core_eia860_\_scd_boilers](data_dictionaries/pudl_db.md#core-eia860-scd-boilers) and [out_eia_\_yearly_boilers](data_dictionaries/pudl_db.md#out-eia-yearly-boilers) tables. See issue
  [#4301](https://github.com/catalyst-cooperative/pudl/issues/4301) and PR [#4810](https://github.com/catalyst-cooperative/pudl/pull/4810).
* Standardized `steam_load_1000_lbs` to `steam_load_lbs`. Units changed to “lbs” in
  the [core_epacems_\_hourly_emissions](data_dictionaries/pudl_db.md#core-epacems-hourly-emissions) table. See issue [#4301](https://github.com/catalyst-cooperative/pudl/issues/4301) and PR
  [#4810](https://github.com/catalyst-cooperative/pudl/pull/4810).
* Corrected incorrect column mappings in [core_eia861_\_yearly_reliability](data_dictionaries/pudl_db.md#core-eia861-yearly-reliability) and
  `raw_eia861__frame` that were introduced for 2024 data during the EIA-861 2024
  data update. See [#4907](https://github.com/catalyst-cooperative/pudl/issues/4907) and [#4908](https://github.com/catalyst-cooperative/pudl/pull/4908).

### Performance Improvements

### Quality of Life Improvements

* Switched from caching Zenodo archives in GCS to AWS S3, using our free and public AWS
  Open Data Registry bucket at `s3://pudl.catalyst.coop/zenodo`. This will make it
  easier for open source contributors to run continuous integration tests, since no
  cloud credentials are required to download the raw data from S3, and they will not be
  subject to the flakiness of the Zenodo API. It will also allow us to access the raw
  PUDL inputs and associated metadata in environments where we may not easily be able to
  authenticate to GCS, such as Read The Docs. This was partly an attempt to mitigate the
  Error 429 “too many requests” responses we have started getting from Zenodo, described
  in [#4856](https://github.com/catalyst-cooperative/pudl/issues/4856). See PR [#4857](https://github.com/catalyst-cooperative/pudl/pull/4857). This should also address the timeouts and
  new data download failures that came up in issue [#4675](https://github.com/catalyst-cooperative/pudl/issues/4675).
* We’ve overhauled some of our tooling:
  * Instead of using `conda` or `mamba` / `micromamba` to manage dependencies
    we’ve switched to [Pixi](https://pixi.prefix.dev/)
  * The venerable `setuptools` has been replaced with [Hatch](https://hatch.pypa.io/latest/)
  * `setuptools_scm` has been replaced with [hatch-vcs](https://github.com/ofek/hatch-vcs)
  * Our `make` targets have been converted into [Pixi tasks](https://pixi.prefix.dev/latest/workspace/advanced_tasks/)

  See issues [#4604](https://github.com/catalyst-cooperative/pudl/issues/4604), [#4872](https://github.com/catalyst-cooperative/pudl/issues/4872) and PR [#4871](https://github.com/catalyst-cooperative/pudl/pull/4871) for more details.

<a id="release-v2025-12-1"></a>

## v2025.12.1 (2025-12-13)

This is a monthly release primarily intended to update the generators reporting in
EIA-860M, with some other minor improvements coming along for the ride. These include
another new EIA Form 176 natural gas disposition table, and experimental access to the
FERC XBRL derived databases using DuckDB. Details below.

#### NOTE
There was a misconfiguration in the build for `v2025.12.0` that prevented it from
deploying.

### Enhancements

* We are experimenting with distributing the XBRL-derived databases for FERC Forms 1, 2,
  6, 60, and 714 using [DuckDB](https://duckdb.org/docs/stable/), which (unlike
  SQLite) can be queried remotely when stored in a cloud bucket. This will also let us
  provide access to this relatively raw but complete FERC data through the [PUDL Data
  Viewer](https://data.catalyst.coop). Note that the XBRL data only covers 2021 to
  the present. For links and an access example, see [Raw FERC XBRL data converted to DuckDB (EXPERIMENTAL)](data_access.md#access-raw-ferc-duckdb). See
  PR [#4782](https://github.com/catalyst-cooperative/pudl/pull/4782) for this change, which is mostly implemented in the
  1.7.x releases of our [FERC XBRL Extractor](https://github.com/catalyst-cooperative/ferc-xbrl-extractor/releases).

### New Data

#### EIA-176

Thanks to open source contributions from [SwitchBox](https://switch.box) and funding
from the [NSF POSE program](https://new.nsf.gov/funding/opportunities/pose-pathways-enable-open-source-ecosystems)
we continue to bring in more EIA natural gas data.

* Added [core_eia176_\_yearly_gas_disposition](data_dictionaries/pudl_db.md#core-eia176-yearly-gas-disposition), which contains cleaned
  company-wide natural gas disposition data from Part 6B of the EIA-176 survey. See
  [#4708](https://github.com/catalyst-cooperative/pudl/issues/4708) and [#4765](https://github.com/catalyst-cooperative/pudl/pull/4765). Thanks to [@MeadBarrel](https://github.com/sponsors/MeadBarrel)!

### Expanded Data Coverage

#### EIA-860M

* Updated EIA-860M with monthly data through October 2025. See [#4788](https://github.com/catalyst-cooperative/pudl/pull/4788).

#### EIA-861

* Added EIA-861 re-released final release data from 2024. See [#4826](https://github.com/catalyst-cooperative/pudl/issues/4826) and PR
  [#4827](https://github.com/catalyst-cooperative/pudl/pull/4827).

#### FERC Form 6

* Updated to using the [latest archive of FERC Form 6](https://zenodo.org/records/17119798) to capture a few late revisions. See PR
  [#4784](https://github.com/catalyst-cooperative/pudl/pull/4784).

### New Data Tests & Validations

* Added dbt data validations to check the uniqueness of natural primary keys in tables
  where some elements of the primary key contain `NULL` values, preventing them from
  being used explicitly as primary keys in SQLite. This only covers tables where we had
  already explicitly identified the natural primary key in our metadata notes. See PR
  [#4811](https://github.com/catalyst-cooperative/pudl/pull/4811).

### Bug Fixes

* Improve the retry logic we use when uploading a PUDL data release to Zenodo: Catch
  common transient error status codes and retry the upload instead of continuing as if
  nothing had gone wrong. When retrying, restart the upload from the beginning of the
  file rather than uploading a zero-length file. Previously both types of errors
  (missing files and zero-length files) were only caught through manual inspection of
  draft data releases. See issue [#4290](https://github.com/catalyst-cooperative/pudl/issues/4290) and PR [#4778](https://github.com/catalyst-cooperative/pudl/pull/4778).
* Remove row with plant ID 68815 and generator ID `GAPPV` that was erroneously
  included in the 2024 from the EIA-860 generators data. See [#4769](https://github.com/catalyst-cooperative/pudl/issues/4769) and PR
  [#4824](https://github.com/catalyst-cooperative/pudl/pull/4824).

### Performance Improvements

* Reduced peak memory usage for [core_eia860m_\_changelog_generators](data_dictionaries/pudl_db.md#core-eia860m-changelog-generators) from 22GB to
  16GB. See issue [#4686](https://github.com/catalyst-cooperative/pudl/issues/4686) and PR [#4707](https://github.com/catalyst-cooperative/pudl/pull/4707).

### Quality of Life Improvements

* Added `balancing_authority_code_eia` and `balancing_authority_name_eia` to the
  set of plant-level attributes that are merged into the denormalized
  [out_eia_\_monthly_generators](data_dictionaries/pudl_db.md#out-eia-monthly-generators) and [out_eia_\_yearly_generators](data_dictionaries/pudl_db.md#out-eia-yearly-generators) tables, as
  multiple users have requested them. Most recently [@sam-hostetter](https://github.com/sponsors/sam-hostetter) in issue
  [#4772](https://github.com/catalyst-cooperative/pudl/issues/4772). See [#4776](https://github.com/catalyst-cooperative/pudl/pull/4776).
* Decouple the publication of Zenodo data releases from the nightly and release builds
  by creating a `zenodo-data-release` GitHub Actions workflow that can create a new
  archive of a PUDL data release from nightly or stable build outputs. This should
  reduce the idle capacity and runtime of our nightly build VM significantly, and also
  allow us to retry Zenodo release uploads when Zenodo flakes out. The nightly and
  release builds will now trigger the `zenodo-data-release` workflow using `curl`
  and the GitHub API. See issue [#4775](https://github.com/catalyst-cooperative/pudl/issues/4775) and PR [#4778](https://github.com/catalyst-cooperative/pudl/pull/4778).
* Disabled the distribution of build outputs to S3/GCS during `workflow_dispatch`
  builds since these uploads are pretty robust, they slow down the build, we delete the
  outputs right after uploading them, and there are egress fees associated with sending
  the data to S3. Build artifacts are still uploaded to `gs://builds.catalyst.coop`.
  See PR [#4778](https://github.com/catalyst-cooperative/pudl/pull/4778).
* Reduced the size of our nightly build VM to 8 CPUs & 64GB RAM since that configuration
  works again after our performance improvements, and it’s cheaper and not that much
  slower than the bigger VM. See [#4778](https://github.com/catalyst-cooperative/pudl/pull/4778).
* Replaced `fgd_sorbent_consumption_1000_tons` with `fgd_sorbent_consumption_tons`
  and changed units, consumption tons, to be rounded to nearest 100 tons in the
  [\_core_eia923_\_yearly_fgd_operation_maintenance](data_dictionaries/pudl_db.md#i-core-eia923-yearly-fgd-operation-maintenance) table. See issue [#4301](https://github.com/catalyst-cooperative/pudl/issues/4301)
  and PR [#4426](https://github.com/catalyst-cooperative/pudl/pull/4426).

<a id="release-v2025-11-0"></a>

## v2025.11.0 (2025-11-13)

This is a quarterly PUDL data release, and includes final 2024 data for a number of
annually reported EIA forms, as well as quarterly updates to data sources that are
released more continuously, like EIA-930, bulk EIA electricity API data, EPA CEMS hourly
emissions and EIA-860M. We’re also beginning to integrate natural gas data, and have
made some performance improvements that will hopefully make it easier for contributors
run the full ETL locally. See below for all the details.

### New Data

#### EIA-176

Thanks to open source contributions from [SwitchBox](https://switch.box) and funding
from the [NSF POSE program](https://new.nsf.gov/funding/opportunities/pose-pathways-enable-open-source-ecosystems)
that helps us support outside contributors, we’re beginning to integrate natural gas
data into PUDL, starting with the [EIA Form 176](data_sources/eia176.md). Follow the
sub-issues listed in issue [#4693](https://github.com/catalyst-cooperative/pudl/issues/4693) to track our progress.

* Added [core_eia176_\_yearly_gas_disposition_by_consumer](data_dictionaries/pudl_db.md#core-eia176-yearly-gas-disposition-by-consumer), which contains cleaned
  natural gas disposition data from Part 6 of EIA-176. Thanks to [@MeadBarrel](https://github.com/sponsors/MeadBarrel) for
  all your work on this. See issues [#4694](https://github.com/catalyst-cooperative/pudl/issues/4694), [#4709](https://github.com/catalyst-cooperative/pudl/issues/4709) and PRs [#4737](https://github.com/catalyst-cooperative/pudl/pull/4737), [#4721](https://github.com/catalyst-cooperative/pudl/pull/4721), [#4728](https://github.com/catalyst-cooperative/pudl/pull/4728).

### Discontinued Data

#### NREL ATB for Electricity

Sadly, no update to [NREL’s Annual Technology Baseline for Electricity](https://atb.nrel.gov/electricity/2024/index) has been published for 2025.
Historically this dataset has been updated in the summer, and would be integrated into
PUDL’s Q3 release. It seems as if it may have been quietly discontinued or at least
deprioritized. We will continue to check for updates and integrate them if they become
available. If you know of alternative public sources for this kind of forward-looking
electricity sector cost projections, please let us know!

### Expanded Data Coverage

#### Census PEP

* Expanded geocodes to include vintages for each year from 2011-2024. See issue
  [#4637](https://github.com/catalyst-cooperative/pudl/issues/4637) and PR [#4665](https://github.com/catalyst-cooperative/pudl/pull/4665).

#### EIA AEO

* Added economic projections from the 2025 AEO. See issue [#4591](https://github.com/catalyst-cooperative/pudl/issues/4591) and PR
  [#4631](https://github.com/catalyst-cooperative/pudl/pull/4631).

#### EIA-860M

* Updated EIA-860M with monthly data through September 2025. See [#4698](https://github.com/catalyst-cooperative/pudl/issues/4698) and
  [#4706](https://github.com/catalyst-cooperative/pudl/pull/4706).

#### EIA-861

* Added EIA-861 final release data from 2024. See [#4648](https://github.com/catalyst-cooperative/pudl/issues/4648) and PR [#4672](https://github.com/catalyst-cooperative/pudl/pull/4672).

#### EIA-923

* Updated EIA-923 with final release data from 2024 and 2025 data through August.
  See PR [#4641](https://github.com/catalyst-cooperative/pudl/pull/4641), [#4699](https://github.com/catalyst-cooperative/pudl/issues/4699) and [#4706](https://github.com/catalyst-cooperative/pudl/pull/4706).

#### EIA-930

* Updated EIA-930 with data published through the end of October 2025. See
  [#4719](https://github.com/catalyst-cooperative/pudl/issues/4719) and PR [#4743](https://github.com/catalyst-cooperative/pudl/pull/4743).

#### EIA Bulk Electricity API

* Updated the EIA Bulk Electricity API data to include data published through
  the beginning of November 2025. See [#4724](https://github.com/catalyst-cooperative/pudl/issues/4724) and PR [#4725](https://github.com/catalyst-cooperative/pudl/pull/4725).

#### EPA/CAMD-EIA Crosswalk

* Updated EPA/CAMD-EIA crosswalk through 2024. See PR [#4749](https://github.com/catalyst-cooperative/pudl/pull/4749).

#### EPA CEMS

* Updated EPA CEMS hourly emissions data through September 2025. See [#4723](https://github.com/catalyst-cooperative/pudl/issues/4723)
  and [#4733](https://github.com/catalyst-cooperative/pudl/pull/4733).

#### FERC Form 1

* Updated FERC Form 1 2024 data to include late respondents. See [#4747](https://github.com/catalyst-cooperative/pudl/pull/4747).

### Documentation

* Added data source pages for:
  * [EIA Annual Energy Outlook (AEO)](data_sources/eiaaeo.md); see issue [#4371](https://github.com/catalyst-cooperative/pudl/issues/4371) and PR [#4660](https://github.com/catalyst-cooperative/pudl/pull/4660).
  * [EIA Form 176 – Annual Report of Natural and Supplemental Gas Supply and Disposition](data_sources/eia176.md); see issue [#4696](https://github.com/catalyst-cooperative/pudl/issues/4696) and PR [#4746](https://github.com/catalyst-cooperative/pudl/pull/4746).

### Bug Fixes

* Fixed a bug where the EIA-930 subregion data from 2018-07-01 to 2019-01-01 was
  being dropped. See PR [#4731](https://github.com/catalyst-cooperative/pudl/pull/4731).

### Dev Tooling

* As part of a performance push, we added some tools for quick memory profiling
  of asset materialization. See issue [#4619](https://github.com/catalyst-cooperative/pudl/issues/4619) and PR [#4655](https://github.com/catalyst-cooperative/pudl/pull/4655).
* We are no longer relying on Dask dataframes for processing data larger than memory.
  We’ve started using [Polars](https://pola.rs) and [DuckDB](https://duckdb.org)
  instead. For now this primarily affects the very large [EPA Hourly Continuous Emission Monitoring System (CEMS)](data_sources/epacems.md)
  dataset, but we anticipate using these tools in other contexts to address performance
  bottlenecks. See issue [#4663](https://github.com/catalyst-cooperative/pudl/issues/4663) and PR [#4676](https://github.com/catalyst-cooperative/pudl/pull/4676) for the conversion of EPA
  CEMS from Dask to Polars.
* We also added `devtools/check_against_nightly.py` to quickly compare local
  Parquet outputs with those from the nightly builds.

<a id="release-v2025-10-0"></a>

## v2025.10.0 (2025-10-14)

This is a regular monthly data release, primarily intended to ensure that PUDL has the
most up-to-date EIA-860M data. It also happens to include final EIA-860 data for 2024,
and some newly integrated EIA-923 financial data and PHMSA natural gas data. See below
for details.

### Expanded Data Coverage

#### EIA-860

* Updated EIA-860 with final release data from 2024. See issue [#4616](https://github.com/catalyst-cooperative/pudl/issues/4616) and
  PR [#4617](https://github.com/catalyst-cooperative/pudl/pull/4617).

#### EIA-860M

* Updated EIA-860M monthly generator report with newly published data for August
  of 2025. See issue [#4639](https://github.com/catalyst-cooperative/pudl/issues/4639) and PR [#4638](https://github.com/catalyst-cooperative/pudl/pull/4638).

#### Re-introduce 88888 and 99999 utility_id_eia

These values, representing redacted values and state aggregates, were
intentionally dropped from eia923 and eia861 due to primary key and
data inconsistency issues. We’re adding them back in! See [#808](https://github.com/catalyst-cooperative/pudl/issues/808)
and PR [#4291](https://github.com/catalyst-cooperative/pudl/pull/4291).

### New Data

#### PHMSA

* Added eight transformed table containing annual data from PHMSA natural gas
  distributors from 1970 to the present. Note that these containing mostly numeric
  values are named as `_core` - indicating that these tables have not been fully
  cleaned and validated. We’ve published these tables to make the 50+
  years of PHMSA data we’ve extracted and mapped available for others to use and for
  contributors to more easily improve incrementally. See [#3770](https://github.com/catalyst-cooperative/pudl/issues/3770) and [#4005](https://github.com/catalyst-cooperative/pudl/pull/4005).
* The first cleaned table, `core_phmsagas__distribution_operators` has been added
  to our PUDL database. Thanks to [@seeess1](https://github.com/sponsors/seeess1) for all of your work on this!

#### EIA-923

* Thanks to contributions from [@alexclippinger](https://github.com/sponsors/alexclippinger), we’ve added cleaned EIA923
  Schedule 8B Financial Information to the PUDL database as
  [\_core_eia923_\_yearly_byproduct_expenses_and_revenues](data_dictionaries/pudl_db.md#i-core-eia923-yearly-byproduct-expenses-and-revenues). Once harvested, this
  table will be replaced with a well-normalized version of the same data, but it is
  being published in this form until then. See [#4099](https://github.com/catalyst-cooperative/pudl/issues/4099) and [#2448](https://github.com/catalyst-cooperative/pudl/issues/2448), and
  [#4636](https://github.com/catalyst-cooperative/pudl/pull/4636).

### Documentation

* Added data source pages for:
  * [Population Estimates Program's (PEP) Federal Information Processing Series (FIPS) Codes](data_sources/censuspep.md); see issue [#4375](https://github.com/catalyst-cooperative/pudl/issues/4375) and PR [#4622](https://github.com/catalyst-cooperative/pudl/pull/4622).
  * [U.S. Securities and Exchange Commission (SEC) Form 10-K](data_sources/sec10k.md); see issue [#4329](https://github.com/catalyst-cooperative/pudl/issues/4329), [#4347](https://github.com/catalyst-cooperative/pudl/issues/4347) and PR [#4562](https://github.com/catalyst-cooperative/pudl/pull/4562).

### New Data Tests & Data Validations

* After investigating some modest discrepancies between our imputed hourly electricity
  demand and prior work by [@truggles](https://github.com/sponsors/truggles) & [@awongel](https://github.com/sponsors/awongel), we’re removing the
  “EXPERIMENTAL” warning label that we had on those tables. See [our discussion
  about the imputation results in the PUDL Examples repo](https://github.com/catalyst-cooperative/pudl-examples/pull/10). The [associated
  notebook is available on Kaggle](https://www.kaggle.com/code/catalystcooperative/06-pudl-imputed-electricity-demand)

  This relates to the PUDL imputed demand values in following tables:
  * [out_eia930_\_hourly_operations](data_dictionaries/pudl_db.md#out-eia930-hourly-operations)
  * [out_eia930_\_hourly_subregion_demand](data_dictionaries/pudl_db.md#out-eia930-hourly-subregion-demand)
  * [out_eia930_\_hourly_aggregated_demand](data_dictionaries/pudl_db.md#out-eia930-hourly-aggregated-demand)

### Deprecations

* We have finally shut down our long-suffering [Datasette](https://datasette.io)
  deployment, but are still working on achieiving feature parity in the new [PUDL Data
  Viewer](https://data.catalyst.coop). We have [an epic tracking our progress](https://github.com/catalyst-cooperative/eel-hole/issues/36). See issue
  [#4481](https://github.com/catalyst-cooperative/pudl/issues/4481) and PR [#4605](https://github.com/catalyst-cooperative/pudl/pull/4605) for the removal of Datasette references within the
  main PUDL repo.

<a id="release-v2025-9-1"></a>

## v2025.9.1 (2025-09-05)

#### NOTE
There was an issue with the `v2025.9.0` release process and that tag was deleted.

This is a monthly release primarily focused on updating the EIA-860M, with other
incremental changes coming along for the ride. A couple of things to be aware of:

* [@mfripp](https://github.com/sponsors/mfripp) identified a bug in how we were constructing detailed utility
  asset/liability and income/expense tables from FERC Form 1. This has been partially
  addressed, but the fix needs to be applied to a couple of additional tables. See
  [#4593](https://github.com/catalyst-cooperative/pudl/issues/4593) to track our progress.
* We are now producing GeoParquet outputs for tables that contain spatial data. This
  is a great new feature! But also potentially a breaking change, depending on what
  tools you’ve been using to read our Parquet outputs. [GeoPandas](https://geopandas.org/) and [DuckDB’s spatial extension](https://duckdb.org/docs/stable/core_extensions/spatial/overview.html) both work
  well.

### Enhancements

#### Geospatial outputs with GeoParquet

We’ve started producing [GeoParquet](https://geoparquet.org/) outputs that include
explicit geometries for use with [GeoPandas](https://geopandas.org/) and other
mapping and geospatial analysis packages. See [`geopandas.read_parquet()`](https://geopandas.org/en/stable/docs/reference/api/geopandas.read_parquet.html#geopandas.read_parquet) for
documentation on how to read them. We’ve also tested it with the [DuckDB Spatial
extension](https://duckdb.org/docs/stable/core_extensions/spatial/overview.html).
This is still experimental and there are only a handful of tables that currently include
geometries, but we hope to apply it more widely in the future for any tables with
geospatial information. See PR [#4546](https://github.com/catalyst-cooperative/pudl/pull/4546).

We’ve started by writing the [Census DP1 – Profile of General Demographic Characteristics](data_sources/censusdp1tract.md) state, county, and tract
level data out as GeoParquet files, so they can be used alongside the other Parquet data
without needing to read the Census DP1 SQLite DB.  This will allow us to point our
[Kaggle (and other) notebooks](https://www.kaggle.com/catalystcooperative/code) that
make maps directly at the Parquet files in S3 rather than depending on the (somewhat
chonky) [Kaggle PUDL dataset](https://www.kaggle.com/datasets/catalystcooperative/pudl-project). For now the only
tables with a valid `geometry` column are:

* [out_censusdp1tract_\_states](data_dictionaries/pudl_db.md#out-censusdp1tract-states)
* [out_censusdp1tract_\_counties](data_dictionaries/pudl_db.md#out-censusdp1tract-counties)
* [out_censusdp1tract_\_tracts](data_dictionaries/pudl_db.md#out-censusdp1tract-tracts)
* [out_ferc714_\_georeferenced_respondents](data_dictionaries/pudl_db.md#out-ferc714-georeferenced-respondents)

### Expanded Data Coverage

#### EIA-860M

* Updated EIA-860M monthly generator report with newly published data for July
  of 2025. See issue [#4590](https://github.com/catalyst-cooperative/pudl/issues/4590) and PR [#4594](https://github.com/catalyst-cooperative/pudl/pull/4594).

#### FERC Form 1

* Updated FERC Form 1 2024 data to include late respondents. See [#4630](https://github.com/catalyst-cooperative/pudl/pull/4630).

#### FERC Forms 2, 6 and 60

* Updated our extraction of FERC Forms 2, 6, and 60 to raw SQLite databases to include
  late respondents. See [#4630](https://github.com/catalyst-cooperative/pudl/pull/4630).

### Quality of Life Improvements

* We updated [our Kaggle notebooks](https://www.kaggle.com/catalystcooperative/code)
  to read PUDL data from our [AWS Open Data Registry](https://registry.opendata.aws/catalyst-cooperative-pudl/) S3 bucket instead of
  relying on the [PUDL Kaggle Dataset](https://www.kaggle.com/datasets/catalystcooperative/pudl-project), since copying
  all of the PUDL data into the notebook workspace was taking more than 5 minutes, which
  made it frustrating for users to get started working with the data. This also means it
  should be easier to run the notebooks locally (in an appropriate Python environment)
  since the data doesn’t need to be present locally. The notebooks are also pushed to
  our [PUDL Examples GitHub repo](https://github.com/catalyst-cooperative/pudl-examples/). See issue [#4381](https://github.com/catalyst-cooperative/pudl/issues/4381).
* When running `dbt_helper update-tables` without the `--clobber` flag, existing
  schema tests, descriptions and other metadata are now preserved. Furthermore, the
  `--update` flag has been removed, with the default schema update logic behaving
  as follows: if columns are added or removed, updates are allowed to pass. However, if
  any metadata is removed, such as tests or descriptions, the update fails unless
  `--clobber` is used. See issue [#4466](https://github.com/catalyst-cooperative/pudl/issues/4466) and PR [#4525](https://github.com/catalyst-cooperative/pudl/pull/4525).

### Bug Fixes

* Stopped nulling values in columns with ENUM constraints when the value was not found
  in the ENUM. Previously we logged a warning, and now it will raise an error. There
  were a couple of trivial cases in which we were losing values that violated the
  constraints, but nothing serious. See PR [#4548](https://github.com/catalyst-cooperative/pudl/pull/4548).
* Fixed a user identified bug within the
  [out_ferc1_\_yearly_detailed_income_statements](data_dictionaries/pudl_db.md#out-ferc1-yearly-detailed-income-statements) table unnecessarily dropping
  records. See PR [#4580](https://github.com/catalyst-cooperative/pudl/pull/4580).

### Documentation

* Added data source pages for:
  * [EIA Bulk API Data](data_sources/eiaapi.md); see issue [#4372](https://github.com/catalyst-cooperative/pudl/issues/4372) and PR [#4567](https://github.com/catalyst-cooperative/pudl/pull/4567).

<a id="release-v2025-8-0"></a>

## v2025.8.0 (2025-08-14)

This is a regular quarterly release of PUDL. It includes new 2024 annual updates for a
number of datasets (FERC Forms 2, 6, 60, & 714), and a minor update to the 2024 FERC
Form 1 data that includes late filings & revisions. It also includes year-to-date
updates for the monthly and quarterly datasets, including EIA-860M, EIA-923, EIA-930,
and the EPA CEMS hourly emissions. There were also a number of data processing bug fixes
and data usability improvements. See the full notes below for details.

### New Data

* Thanks to contributions from [@alexclippinger](https://github.com/sponsors/alexclippinger), we’ve added cleaned EIA923
  Schedule 8A Byproduct Disposition to the PUDL database as
  [\_core_eia923_\_yearly_byproduct_disposition](data_dictionaries/pudl_db.md#i-core-eia923-yearly-byproduct-disposition). Once harvested, this table will
  be replaced with a well-normalized version of the same data, but it is being published
  in this form until then. See [#4100](https://github.com/catalyst-cooperative/pudl/issues/4100) and [#2448](https://github.com/catalyst-cooperative/pudl/issues/2448), and [#4502](https://github.com/catalyst-cooperative/pudl/pull/4502).

### Expanded Data Coverage

#### EIA-860M

* Updated EIA-860M monthly generator report with newly published data for May and June
  of 2025. See issue [#4379](https://github.com/catalyst-cooperative/pudl/issues/4379) and PR [#4536](https://github.com/catalyst-cooperative/pudl/pull/4536).

#### EIA-923

* Added EIA-923 data through May 2025. See [#4516](https://github.com/catalyst-cooperative/pudl/issues/4516) and [#4538](https://github.com/catalyst-cooperative/pudl/pull/4538).

#### EIA-930

* Updated EIA-930 data published up through the beginning of August 2025. See
  [#4517](https://github.com/catalyst-cooperative/pudl/issues/4517) and PR [#4523](https://github.com/catalyst-cooperative/pudl/pull/4523).

#### EIA Bulk Electricity API

* Updated the EIA Bulk Electricity data to include data published up through
  the beginning of August 2025. See [#4519](https://github.com/catalyst-cooperative/pudl/issues/4519) and PR [#4523](https://github.com/catalyst-cooperative/pudl/pull/4523).

#### EPA CEMS

* Added EPA CEMS data through June 2025. See [#4518](https://github.com/catalyst-cooperative/pudl/issues/4518) and [#4531](https://github.com/catalyst-cooperative/pudl/pull/4531).

#### FERC Form 1

* Updated FERC Form 1 2024 data to include late respondents. See [#4493](https://github.com/catalyst-cooperative/pudl/issues/4493) and
  [#4507](https://github.com/catalyst-cooperative/pudl/pull/4507).

#### FERC Forms 2, 6 and 60

* Updated our extraction of FERC Forms 2, 6, and 60 to raw SQLite databases to include
  2024 data. See [#4418](https://github.com/catalyst-cooperative/pudl/issues/4418) and [#4433](https://github.com/catalyst-cooperative/pudl/pull/4433).

#### FERC Form 714

* Integrated 2024 data for FERC Form 714. See issue [#4409](https://github.com/catalyst-cooperative/pudl/issues/4409) and PR [#4530](https://github.com/catalyst-cooperative/pudl/pull/4530).

#### PHMSA Gas Data

* Extracted 2023 and 2024 PHMSA distribution and transmission data to raw assets. This
  data is not currently published to the PUDL database. See [#4449](https://github.com/catalyst-cooperative/pudl/issues/4449) and
  [#4470](https://github.com/catalyst-cooperative/pudl/pull/4470).
* Extracted 1970 through 1989 PHMSA transmission data to raw assets.  This data is not
  currently published to the PUDL database. See [#3290](https://github.com/catalyst-cooperative/pudl/issues/3290) and [#4500](https://github.com/catalyst-cooperative/pudl/pull/4500).

### Quality of Life Improvements

* The output of `dbt_helper update-tables` now conforms to the format that
  our pre-commit hooks expect, reducing annoying back-and-forth and diffs. See
  [#4119](https://github.com/catalyst-cooperative/pudl/issues/4119) and [#4401](https://github.com/catalyst-cooperative/pudl/pull/4401).
* Improved behavior of `dbt_helper` when interacting with row count test definitions
  as well as updating the row counts stored in dbt seed tables: the logic for writing
  a new table dbt schema no longer includes automatically adding a row count test. Also,
  the logic for updating row counts now depends on whether a test has been defined in
  the dbt schema, whether any existing row counts for that table are present in the seed
  table, as well as user provided settings such as `--clobber`.
* Stopped running code checks in CI when only the documentation has changed.
  See issue [#4410](https://github.com/catalyst-cooperative/pudl/issues/4410) and PR [#4429](https://github.com/catalyst-cooperative/pudl/pull/4429).
* Added `utility_id_ferc1_dbf` and `utility_id_ferc1_xbrl` columns into all ferc1
  output tables. See [#4365](https://github.com/catalyst-cooperative/pudl/issues/4365) and PR [#4528](https://github.com/catalyst-cooperative/pudl/pull/4528).

### Bug Fixes

* Fixed bug in how we were labeling the `data_maturity` of EIA-923. See issue
  [#4328](https://github.com/catalyst-cooperative/pudl/issues/4328) and PR [#4392](https://github.com/catalyst-cooperative/pudl/pull/4392).
* Fixed bug in how we were repairing a misfiled EIA code in
  [core_ferc714_\_respondent_id](data_dictionaries/pudl_db.md#core-ferc714-respondent-id). See issue [#4439](https://github.com/catalyst-cooperative/pudl/issues/4439) and PR [#4497](https://github.com/catalyst-cooperative/pudl/pull/4497).
* Fixed bug in how we were removing duplicates in [core_eia923_\_monthly_generation](data_dictionaries/pudl_db.md#core-eia923-monthly-generation)
  resulting in ~400 more records in this table over several years. See details in PR
  [#4538](https://github.com/catalyst-cooperative/pudl/pull/4538)

### Documentation

* Migrated table description metadata into new format; see epic [#4358](https://github.com/catalyst-cooperative/pudl/issues/4358) for
  issues & PRs for all source groups.
  * This included renaming two of the preliminarily published `_core` tables to better
    conform with our table naming conventions. Table
    `_core_eia923__cooling_system_information` is now
    [\_core_eia923_\_monthly_cooling_system_information](data_dictionaries/pudl_db.md#i-core-eia923-monthly-cooling-system-information) and
    `_core_eia923__fgd_operation_maintenance` is now
    [\_core_eia923_\_yearly_fgd_operation_maintenance](data_dictionaries/pudl_db.md#i-core-eia923-yearly-fgd-operation-maintenance). See [#4422](https://github.com/catalyst-cooperative/pudl/pull/4422).
* Added data source pages for:
  * [EPA CAMD to EIA Power Sector Data Crosswalk](data_sources/epacamd_eia.md); see issue [#4376](https://github.com/catalyst-cooperative/pudl/issues/4376) and PR [#4403](https://github.com/catalyst-cooperative/pudl/pull/4403)

### New Tests and Data Validations

#### EIA-930 and FERC-714 Hourly Imputed Demand

Added checks which ensure that *only* hourly electricity demand values which are flagged
for imputation change significantly from their reported values before and after the
imputation. Check that the missingness of various columns in the hourly reported demand
and imputed demand are within expected ranges. Explicitly flag years of which are
dropped due to insufficient data for meaningful imputation with `BAD_YEAR`. Affected
tables include [out_eia930_\_hourly_operations](data_dictionaries/pudl_db.md#out-eia930-hourly-operations),
[out_eia930_\_hourly_subregion_demand](data_dictionaries/pudl_db.md#out-eia930-hourly-subregion-demand), and
[out_ferc714_\_hourly_planning_area_demand](data_dictionaries/pudl_db.md#out-ferc714-hourly-planning-area-demand). See PR [#4334](https://github.com/catalyst-cooperative/pudl/pull/4334).

#### Check for entirely null column-years

Previously we had a data validation check that ensured there were no entirely null
columns applied to a handful of tables. Such columns were typically the result of typos
or failures to update column names, or application of an incompatible dtype, e.g.
casting an uncleaned column containing Y or N to `boolean`. A similar check has been
implemented in our dbt data validation checks and is now applied to all tables. See
issue [#4105](https://github.com/catalyst-cooperative/pudl/issues/4105) and PR [#4382](https://github.com/catalyst-cooperative/pudl/pull/4382). As a result of more broadly applying this check,
we found and fixed a few data quality and column naming issues resulting in minor
changes to the database schema:

* `id_dc_coupled_tightly` was renamed to `is_dc_coupled_tightly` (typo).
* `switch_operating` was consolidated with the existing
  `can_switch_when_operating` column found in the multi-fuel generator tables.
* The `model_tax_credit_case_nrelatb` column had its allowable enumerated values
  corrected, resulting in real non-null contents. See PR [#4384](https://github.com/catalyst-cooperative/pudl/pull/4384).
* Three previously entirely null `boolean` columns in the multifuel generator table
  now contain real values, they are: `can_fuel_switch`, `has_regulatory_limits`,
  and `can_cofire_oil_and_gas`.

Unusual patterns of null values were identified and investigated in issue [#4407](https://github.com/catalyst-cooperative/pudl/issues/4407)
with some additional explanations added in PR [#4442](https://github.com/catalyst-cooperative/pudl/pull/4442).

<a id="release-v2025-7-0"></a>

## v2025.7.0 (2025-07-03)

This release integrates early release annual 2024 data for the EIA Forms 860 and 923,
as well as fresh EIA-860M monthly data. It also includes a few small bug-fixes, some of
which result in minor changes to the database schema. It also removes the deprecated
`PudlTabl` output management class.

We are experimenting a new **monthly** release schedule for PUDL, to keep the EIA-860M
data as fresh as possible. This is the first of those monthly releases.

### New Data

#### EIA AEO

* Extracted table 2 from the EIA Annual Energy Outlook 2023, which includes future
  projections for energy use through the year 2050 across a variety of scenarios.
  Integrated a subset of available table 2 series as a new core table:
  * `core_eiaaeo__yearly_projected_energy_use_by_sector_and_type` contains
    projected energy use for the commercial, electric power, industrial,
    residential, and transportation sectors across different fuels and electricity
    modes. See [#4228](https://github.com/catalyst-cooperative/pudl/issues/4228) and [#4273](https://github.com/catalyst-cooperative/pudl/pull/4273).

### Expanded Data Coverage

#### EIA-860

* Added EIA-860 early release data from 2024. See [#4323](https://github.com/catalyst-cooperative/pudl/issues/4323) and PR [#4332](https://github.com/catalyst-cooperative/pudl/pull/4332).

#### EIA-860M

* Added EIA-860M data from April 2025. See [#4324](https://github.com/catalyst-cooperative/pudl/issues/4324) and PR [#4332](https://github.com/catalyst-cooperative/pudl/pull/4332).

#### EIA-923

* Added EIA-923 early release data from 2024 and monthly data from March 2025. See
  [#4325](https://github.com/catalyst-cooperative/pudl/issues/4325) and PR [#4332](https://github.com/catalyst-cooperative/pudl/pull/4332).

### Bug Fixes

* Fixed a number of typos in our documentation and codebase, which resulted in
  renaming `synchronized_transmission_grid` in [core_eia860_\_scd_generators](data_dictionaries/pudl_db.md#core-eia860-scd-generators),
  [out_eia_\_monthly_generators](data_dictionaries/pudl_db.md#out-eia-monthly-generators), and [out_eia_\_yearly_generators](data_dictionaries/pudl_db.md#out-eia-yearly-generators).
  See issue [#3783](https://github.com/catalyst-cooperative/pudl/issues/3783) and [#4355](https://github.com/catalyst-cooperative/pudl/pull/4355).

#### VCE RARE

* Standardized `place_name` using data from the latest Census PEP vintage,
  found in `_core_censuspep__yearly_geocodes`. See issue [#3914](https://github.com/catalyst-cooperative/pudl/issues/3914) and PR
  [#4319](https://github.com/catalyst-cooperative/pudl/pull/4319).

### Deprecations

* After more than a year of deprecation warnings, we’ve removed the `PudlTabl`
  output management class, and have stopped distributing a handful of tables that were
  only around to allow the behavior of that class to be maintained. See issues
  [#3215](https://github.com/catalyst-cooperative/pudl/issues/3215), [#2911](https://github.com/catalyst-cooperative/pudl/issues/2911) and PR [#4316](https://github.com/catalyst-cooperative/pudl/pull/4316).
* Undeploy superset, given that we are going with Marimo for our usage metrics
  dashboards, and the Eel Hole for publicly facing data access. See PR [#4353](https://github.com/catalyst-cooperative/pudl/pull/4353).

### Quality of Life Improvements

* We’ve added a new sub-command to `dbt_helper` - `dbt_helper validate`.
  This lets you run validation tests for a selection of DBT models and also
  see what the failing outputs are, instead of doing a bunch of digging after
  the fact.
* We’ve added a new devtool in `devtools/materialize_to_parquet.py` - this
  lets you export and share assets that were previously not persisted to Parquet,
  such as `raw` assets that have been extracted but not cleaned. Run
  `./materialize_to_parquet --help` from within the `devtools` directory for
  details. See [#4320](https://github.com/catalyst-cooperative/pudl/pull/4320).

### New Tests

* Added a validation pipeline for our EIA-930 hourly demand imputation. This
  pipeline will perform imputation on a set of values which did not require imputation,
  so there is ground truth data to compare against. It will then compute the percent
  error for all of these imputed values against the reported data. This metric is
  checked during nightly builds and will result in an error if it ever drifts too high.

<a id="release-v2025-5-0"></a>

## v2025.5.0 (2025-05-20)

This is our regular quarterly PUDL data release for 2025Q2. It includes sub-annual
updates to the EIA-860M, EIA-923, EIA-930, EIA bulk electricity API, and EPA CEMS
datasets. It also includes preliminary 2024 data for FERC Form 1 (integrated into PUDL)
and FERC Forms 2, 6, and 60 (as stand-alone SQLite databases). The VCE RARE hourly
county-level renewable energy generation curves have been extended back to cover
2014-2018.

This release also includes new imputed versions of the FERC-714 and EIA-930 hourly
demand curves with missing values filled in and a better organized version of the SEC
10-K company ownership data. Note that work on the demand imputations and SEC 10-K data
is ongoing.

All federal data was archived from the publishing agencies on May 1st, 2025.

### Upcoming Deprecations

* Due to the growing size of PUDL database, we are no longer updating our [Datasette
  deployment](https://data.catalyst.coop) and that URL will soon begin redirecting
  users to the [PUDL Data Viewer](https://data.catalyst.coop). You can track our
  progress toward feature parity with the old Datasette deployment in
  [this issue](https://github.com/catalyst-cooperative/eel-hole/issues/36).
* When we complete the migration of our data validation tests to the `dbt` framework,
  we will remove the deprecated `pudl.output.pudltabl.PudlTabl` output class.
  This will also happen before our next quarterly release.

### New Data

#### FERC 714

* We refactored our timseries imputation functions to be more generalized and reusable,
  so they can be applied to electricity demand curves from both FERC-714 and EIA-930,
  as well as other time series data in the future. This resulted in some minor changes
  to the imputation results. See issue [#4112](https://github.com/catalyst-cooperative/pudl/issues/4112) and PR [#4113](https://github.com/catalyst-cooperative/pudl/pull/4113).
* Added the table [out_ferc714_\_hourly_planning_area_demand](data_dictionaries/pudl_db.md#out-ferc714-hourly-planning-area-demand), which contains an
  imputed version of demand. Previously these imputed values were not being distributed
  directly, and fed into the [out_ferc714_\_hourly_estimated_state_demand](data_dictionaries/pudl_db.md#out-ferc714-hourly-estimated-state-demand) table.

#### EIA-930

Work on producing EIA-930 demand curves suitable for use in electricity system modeling
is being done in collaboration with [@awongel](https://github.com/sponsors/awongel) at
[Carnegie Science](https://carnegiescience.edu), with support from [GridLab](https://gridlab.org). See issue [#4083](https://github.com/catalyst-cooperative/pudl/issues/4083) for a list of related issues.

* Added the table [out_eia930_\_hourly_subregion_demand](data_dictionaries/pudl_db.md#out-eia930-hourly-subregion-demand), which
  contains an imputed version of subregion demand. See issues [#4124](https://github.com/catalyst-cooperative/pudl/issues/4124), [#4136](https://github.com/catalyst-cooperative/pudl/issues/4136) and PR
  [#4149](https://github.com/catalyst-cooperative/pudl/pull/4149)
* Added the table [out_eia930_\_hourly_operations](data_dictionaries/pudl_db.md#out-eia930-hourly-operations), which
  contains an imputed version of BA level demand. See issue [#4138](https://github.com/catalyst-cooperative/pudl/issues/4138) and PR
  [#4162](https://github.com/catalyst-cooperative/pudl/pull/4162)

#### SEC 10-K

* Reorganized the preliminary SEC 10-K data that was integrated into our last release.
  See issue [#4078](https://github.com/catalyst-cooperative/pudl/issues/4078) and PR [#4134](https://github.com/catalyst-cooperative/pudl/pull/4134). The SEC 10-K tables are now more fully
  normalized and better conform to existing PUDL naming conventions. Overall revision of
  the SEC 10-K data is being tracked in issue [#4085](https://github.com/catalyst-cooperative/pudl/issues/4085).

  Note that the SEC 10-K data is still a work in progress, and there are known issues
  that remain to be resolved in the [upstream repository](https://github.com/catalyst-cooperative/mozilla-sec-eia) that generates this data.

  The new tables include:
  * [core_sec10k_\_quarterly_filings](data_dictionaries/pudl_db.md#core-sec10k-quarterly-filings)
  * [core_sec10k_\_quarterly_company_information](data_dictionaries/pudl_db.md#core-sec10k-quarterly-company-information)
  * [core_sec10k_\_changelog_company_name](data_dictionaries/pudl_db.md#core-sec10k-changelog-company-name)
  * [core_sec10k_\_quarterly_exhibit_21_company_ownership](data_dictionaries/pudl_db.md#core-sec10k-quarterly-exhibit-21-company-ownership)
  * [core_sec10k_\_assn_sec10k_filers_and_eia_utilities](data_dictionaries/pudl_db.md#core-sec10k-assn-sec10k-filers-and-eia-utilities)
  * [out_sec10k_\_quarterly_filings](data_dictionaries/pudl_db.md#out-sec10k-quarterly-filings)
  * [out_sec10k_\_changelog_company_name](data_dictionaries/pudl_db.md#out-sec10k-changelog-company-name)

### Expanded Data Coverage

#### FERC Form 1

* Integrated FERC Form 1 data from 2024 into the main PUDL SQLite DB. See issue
  [#4207](https://github.com/catalyst-cooperative/pudl/issues/4207) and PR [#4215](https://github.com/catalyst-cooperative/pudl/pull/4215). FERC Form 1 has a filing deadline of
  [April 18th](https://www.ferc.gov/general-information-0/electric-industry-forms)
  for utility respondents, but late filings may come throughout the year. This update
  includes ~95% of the expected utility responses for 2024.

#### FERC Forms 2, 6, & 60

* Updated the FERC archive DOIs and `ferc_to_sqlite` settings to extract 2024 XBRL
  data for FERC Forms 2, 6, and 60 and add them to their respective SQLite databases.
  Note that this data is not yet being processed beyond the conversion from XBRL to
  SQLite. See PR [#4250](https://github.com/catalyst-cooperative/pudl/pull/4250). The reporting deadline for these forms was April 18th, 2025
  so they should include the vast bulk of the expected data, however there may be some
  late filings which will be added in the next quarterly release.

#### EIA Bulk Electricity

* Updated the EIA Bulk Electricity data to include data published up through
  2025-05-01. Also adapted the extractor to handle changes in formatting for the
  EIA Bulk API archive. See [#4237](https://github.com/catalyst-cooperative/pudl/issues/4237) and PR [#4246](https://github.com/catalyst-cooperative/pudl/pull/4246).

#### EPA CEMS

* Added 2025 Q1 of CEMS data. See [#4236](https://github.com/catalyst-cooperative/pudl/issues/4236) and [#4238](https://github.com/catalyst-cooperative/pudl/pull/4238).

#### EIA-930

* Updated EIA-930 to include data published up through the beginning of May 2025.
  See [#4235](https://github.com/catalyst-cooperative/pudl/issues/4235) and [#4242](https://github.com/catalyst-cooperative/pudl/pull/4242). Raw data now includes adjusted and imputed
  values for the `unknown` fuel source, making it behave like other fuel sources;
  see [Changes in energy source granularity over time](data_sources/eia930.md#data-sources-eia930-changes-in-energy-source-granularity-over-time) for
  more information.

#### EIA-860M

* Added EIA-860M data from January, February, and March 2025. See [#4233](https://github.com/catalyst-cooperative/pudl/issues/4233) and
  PR [#4242](https://github.com/catalyst-cooperative/pudl/pull/4242).

#### EIA-923

* Added EIA-923 from January and February 2025. See [#4234](https://github.com/catalyst-cooperative/pudl/issues/4234) and PR [#4242](https://github.com/catalyst-cooperative/pudl/pull/4242).

#### VCE RARE

* Integrated 2014-2018 RARE data into PUDL. Also fixed misleading latitude and longitude
  field descriptions, and renamed the field `county_or_lake_name` to `place_name`.
  See issue [#4226](https://github.com/catalyst-cooperative/pudl/issues/4226) and PR [#4239](https://github.com/catalyst-cooperative/pudl/pull/4239).

### Bug Fixes

* Fixed a bug in FERC XBRL extraction that led to quietly skipping tables with names
  that didn’t conform to expected format. The only known table affected was in the FERC
  Form 6. See issue [#4203](https://github.com/catalyst-cooperative/pudl/issues/4203) and PRs [#4224](https://github.com/catalyst-cooperative/pudl/pull/4224) and
  [catalyst-cooperative/ferc-xbrl-extractor #320](https://github.com/catalyst-cooperative/ferc-xbrl-extractor/pull/320).
* As part of [#4215](https://github.com/catalyst-cooperative/pudl/pull/4215) we fixed a bug introduced in the last release that was causing
  most values in the `out_ferc1__yearly_rate_base` table to be dropped. See
  [this commit](https://github.com/catalyst-cooperative/pudl/pull/4215/commits/65b36e3121bdfb792ae59c0b94b0ed473307bd78).

### Quality of Life Improvements

* We now publish a [Frictionless data package](https://datapackage.org/standard/data-package/) describing our Parquet
  outputs, with the name `pudl_datapackage.json`. See [#4069](https://github.com/catalyst-cooperative/pudl/issues/4069) and [#4070](https://github.com/catalyst-cooperative/pudl/pull/4070).
* We renamed `eia_bulk_elec` to `eiaapi` to conform to our dataset naming protocols
  and reflect the expansion of the EIA Bulk API archive to include all datasets
  published through the EIA API, not just the bulk electricity data. See [this PUDL
  archiver issue](https://github.com/catalyst-cooperative/pudl-archiver/issues/628)
  and PR [#4212](https://github.com/catalyst-cooperative/pudl/pull/4212).
* To improve human readability, we added `utility_id_pudl` and `utility_name_ferc1`
  columns to a number of derived FERC 1 output tables including:
  * [out_ferc1_\_yearly_rate_base](data_dictionaries/pudl_db.md#out-ferc1-yearly-rate-base)
  * [out_ferc1_\_yearly_detailed_income_statements](data_dictionaries/pudl_db.md#out-ferc1-yearly-detailed-income-statements)
  * [out_ferc1_\_yearly_detailed_balance_sheet_assets](data_dictionaries/pudl_db.md#out-ferc1-yearly-detailed-balance-sheet-assets)
  * [out_ferc1_\_yearly_detailed_balance_sheet_liabilities](data_dictionaries/pudl_db.md#out-ferc1-yearly-detailed-balance-sheet-liabilities)

  See PR [#4260](https://github.com/catalyst-cooperative/pudl/pull/4260).

### New Tests

We’re in the process of migrating hundrds of data validation tests to use the [dbt](https://docs.getdbt.com/docs/introduction) framework. We have converted at least the
following classes of tests:

* `check_column_correlation` – a more generic replacement for the old
  `test_fbp_ferc1_mmbtu_cost_correlation` pytest.
  See [#4094](https://github.com/catalyst-cooperative/pudl/issues/4094), [#4117](https://github.com/catalyst-cooperative/pudl/pull/4117). You can find the implementation in the
  [check_column_correlation.sql](../../dbt/tests/data_tests/generic_tests/check_column_correlation.sql) file.
* `expect_includes_all_value_combinations_from` - a more generic replacement for the
  old `ensure_all_ppe_ids_are_in_assn` pytest. See [#4096](https://github.com/catalyst-cooperative/pudl/issues/4096), [#9123](https://github.com/catalyst-cooperative/pudl/pull/9123). You
  can find the implementation in the [expect_includes_all_value_combinations_from.sql](../../dbt/tests/data_tests/generic_tests/expect_includes_all_value_combinations_from.sql)
  file.
* `expect_quantile_constraints` - a more generic replacement for the old
  `vs_bounds` pytest. See [#4106](https://github.com/catalyst-cooperative/pudl/issues/4106), [#4090](https://github.com/catalyst-cooperative/pudl/pull/4090), and [#4171](https://github.com/catalyst-cooperative/pudl/pull/4171). You can find the
  implementation in the [expect_quantile_constraints.sql](../../dbt/tests/data_tests/generic_tests/expect_quantile_constraints.sql) file.
* 19 tests which required special handling; see [#4093](https://github.com/catalyst-cooperative/pudl/issues/4093), [#4114](https://github.com/catalyst-cooperative/pudl/pull/4114), [#4151](https://github.com/catalyst-cooperative/pudl/pull/4151).

<a id="release-v2025-2-0"></a>

## v2025.2.0 (2025-02-13)

This is our regular quarterly release for 2025Q1. It includes updates to all the
datasets that are published with quarterly or higher frequency, plus initial versions
of a few new data sources that have been in the works for a while.

One major change this quarter is that we are now publishing all processed PUDL data as
Apache Parquet files, alongside our existing SQLite databases. See [Data Access](data_access.md)
for more on how to access these outputs.

Some potentially breaking changes to be aware of:

* In the [EIA Form 930 – Hourly and Daily Balancing Authority Operations Report](data_sources/eia930.md) a number of new energy sources have been added, and
  some old energy sources have been split into more granular categories. See
  [Changes in energy source granularity over time](data_sources/eia930.md#data-sources-eia930-changes-in-energy-source-granularity-over-time).
* We are now running the EPA’s CAMD to EIA unit crosswalk code for each individual year
  starting from 2018, rather than just 2018 and 2021, resulting in more connections
  between these two datasets and changes to some sub-plant IDs. See the note below for
  more details.

Many thanks to the organizations who make these regular updates possible! Especially
[GridLab](https://gridlab.org), and [RMI](https://rmi.org). If you rely on PUDL
and would like to help ensure that the data keeps flowing, please consider joining them
as a [PUDL Sustainer](https://opencollective.com/pudl), as we are still fundraising
for 2025.

### New Data

#### EIA-176

* Add a couple of semi-transformed interim EIA-176 (natural gas sources and
  dispositions) tables. They aren’t yet being written to the database, but are one step
  closer. See [#3555](https://github.com/catalyst-cooperative/pudl/issues/3555) and PRs [#3590](https://github.com/catalyst-cooperative/pudl/pull/3590), [#3978](https://github.com/catalyst-cooperative/pudl/pull/3978). Thanks to [@davidmudrauskas](https://github.com/sponsors/davidmudrauskas)
  for moving this dataset forward.
* Extracted these interim tables up through the latest 2023 data release. See
  [#4002](https://github.com/catalyst-cooperative/pudl/issues/4002) and [#4004](https://github.com/catalyst-cooperative/pudl/pull/4004).

#### EIA-860

* Added EIA-860 Multifuel table. See [#3438](https://github.com/catalyst-cooperative/pudl/issues/3438) and [#3988](https://github.com/catalyst-cooperative/pudl/pull/3988). Thanks to
  [@jmelot](https://github.com/sponsors/jmelot) for working on adding this new table.

#### FERC 1

* Added three new output tables containing granular utility accounting data.
  See [#4057](https://github.com/catalyst-cooperative/pudl/pull/4057), [#3642](https://github.com/catalyst-cooperative/pudl/issues/3642) and the table descriptions in the data dictionary:
  * [out_ferc1_\_yearly_detailed_income_statements](data_dictionaries/pudl_db.md#out-ferc1-yearly-detailed-income-statements)
  * [out_ferc1_\_yearly_detailed_balance_sheet_assets](data_dictionaries/pudl_db.md#out-ferc1-yearly-detailed-balance-sheet-assets)
  * [out_ferc1_\_yearly_detailed_balance_sheet_liabilities](data_dictionaries/pudl_db.md#out-ferc1-yearly-detailed-balance-sheet-liabilities)

#### SEC Form 10-K Parent-Subsidiary Ownership

* We have added some new tables describing the parent-subsidiary company ownership
  relationships reported in the
  [SEC’s Form 10-K](https://en.wikipedia.org/wiki/Form_10-K), Exhibit 21
  “Subsidiaries of the Registrant”. Where possible these tables link the SEC filers or
  their subsidiary companies to the corresponding EIA utilities. This work was funded
  by
  [a grant from the Mozilla Foundation](https://catalyst.coop/2024/02/15/beating-utility-ownership-shell-game/).
  Most of the ML models and data preparation took place in the [mozilla-sec-eia
  repository](https://github.com/catalyst-cooperative/mozilla-sec-eia) separate from
  the main PUDL ETL, as it requires processing hundreds of thousands of PDFs and the
  deployment of some ML experiment tracking infrastructure. The new tables are handed
  off as nearly finished products to the PUDL ETL pipeline. **Note that these are
  preliminary, experimental data products and are known to be incomplete and to contain
  errors.** Extracting data tables from unstructured PDFs and the SEC to EIA record
  linkage are necessarily probabilistic processes.
* See PRs [#4026](https://github.com/catalyst-cooperative/pudl/pull/4026), [#4031](https://github.com/catalyst-cooperative/pudl/pull/4031), [#4035](https://github.com/catalyst-cooperative/pudl/pull/4035), [#4046](https://github.com/catalyst-cooperative/pudl/pull/4046), [#4048](https://github.com/catalyst-cooperative/pudl/pull/4048), [#4050](https://github.com/catalyst-cooperative/pudl/pull/4050), [#4079](https://github.com/catalyst-cooperative/pudl/pull/4079) and check out the table descriptions
  in the PUDL data dictionary:
  * [core_sec10k_\_quarterly_filings](data_dictionaries/pudl_db.md#core-sec10k-quarterly-filings)
  * [core_sec10k_\_quarterly_exhibit_21_company_ownership](data_dictionaries/pudl_db.md#core-sec10k-quarterly-exhibit-21-company-ownership)
  * [core_sec10k_\_quarterly_company_information](data_dictionaries/pudl_db.md#core-sec10k-quarterly-company-information)
  * [core_sec10k_\_changelog_company_name](data_dictionaries/pudl_db.md#core-sec10k-changelog-company-name)

### Expanded Data Coverage

#### EPA CEMS

* Added 2024 Q4 of CEMS data. See [#4041](https://github.com/catalyst-cooperative/pudl/issues/4041) and [#4052](https://github.com/catalyst-cooperative/pudl/pull/4052).

#### EPA CAMD EIA Crosswalk

* In the past, the crosswalk in PUDL has used the EPA’s published crosswalk (run with
  2018 data), and an additional crosswalk we ran with 2021 EIA-860 data. To ensure that
  the crosswalk reflects updates in both EIA and EPA data, we re-ran the EPA R code
  which generates the EPA CAMD EIA crosswalk with 4 new years of data: 2019, 2020, 2022
  and 2023. Re-running the crosswalk pulls the latest data from the CAMD FACT API, which
  results in some changes to the generator and unit IDs reported on the EPA side of the
  crosswalk, which feeds into the creation of [core_epa_\_assn_eia_epacamd](data_dictionaries/pudl_db.md#core-epa-assn-eia-epacamd).
* The changes only result in the addition of new units and generators in the EPA data,
  with no changes to matches at the plant level. However, the updates to generator and
  unit IDs have resulted in changes to the subplant IDs - some EIA boilers and
  generators which previously had no matches to EPA data have now been matched to EPA
  unit data, resulting in an overall **reduction** in the number of rows in the
  [core_epa_\_assn_eia_epacamd_subplant_ids](data_dictionaries/pudl_db.md#core-epa-assn-eia-epacamd-subplant-ids) table. See issues [#4039](https://github.com/catalyst-cooperative/pudl/issues/4039)
  and PR [#4056](https://github.com/catalyst-cooperative/pudl/pull/4056) for a discussion of the changes observed in the course of this
  update.

#### EIA-860M

* Added EIA-860m through December 2024. See [#4038](https://github.com/catalyst-cooperative/pudl/issues/4038) and [#4047](https://github.com/catalyst-cooperative/pudl/pull/4047).

#### EIA-923

* Added EIA-923 monthly data through September 2024. See [#4038](https://github.com/catalyst-cooperative/pudl/issues/4038) and [#4047](https://github.com/catalyst-cooperative/pudl/pull/4047).

#### EIA Bulk Electricity Data

* Updated the EIA Bulk Electricity data to include data published up through
  2024-11-01. See [#4042](https://github.com/catalyst-cooperative/pudl/issues/4042) and PR [#4051](https://github.com/catalyst-cooperative/pudl/pull/4051).

#### EIA-930

* Updated the EIA-930 data to include data published up through the beginning of
  February 2025. See [#4040](https://github.com/catalyst-cooperative/pudl/issues/4040) and PR [#4054](https://github.com/catalyst-cooperative/pudl/pull/4054). 10 new energy sources
  were added and 3 were retired; see
  [Changes in energy source granularity over time](data_sources/eia930.md#data-sources-eia930-changes-in-energy-source-granularity-over-time) for
  more information.

### Bug Fixes

* Fix an accidentally swapped set of starting balance / ending balance column rename
  parameters in the pre-2021 DBF derived data that feeds into
  [core_ferc1_\_yearly_other_regulatory_liabilities_sched278](data_dictionaries/pudl_db.md#core-ferc1-yearly-other-regulatory-liabilities-sched278). See issue
  [#3952](https://github.com/catalyst-cooperative/pudl/issues/3952) and PRs [#3969](https://github.com/catalyst-cooperative/pudl/pull/3969), [#3979](https://github.com/catalyst-cooperative/pudl/pull/3979). Thanks to [@yolandazzz13](https://github.com/sponsors/yolandazzz13) for making
  this fix.
* Added preliminary data validation checks for several FERC 1 tables that were
  missing it [#3860](https://github.com/catalyst-cooperative/pudl/pull/3860).
* Fix spelling of Lake Huron and Lake Saint Clair in
  [out_vcerare_\_hourly_available_capacity_factor](data_dictionaries/pudl_db.md#out-vcerare-hourly-available-capacity-factor) and related tables. See issue
  [#4007](https://github.com/catalyst-cooperative/pudl/issues/4007) and PR [#4029](https://github.com/catalyst-cooperative/pudl/pull/4029).

### Quality of Life Improvements

* We added a `sources` parameter to `pudl.metadata.classes.DataSource.from_id()`
  in order to make it possible to use the [pudl-archiver](https://www.github.com/catalyst-cooperative/pudl-archiver) repository to
  archive datasets that won’t necessarily be ingested into PUDL. See [this PUDL archiver
  issue](https://github.com/catalyst-cooperative/pudl-archiver/pull/506) and PRs
  [#4003](https://github.com/catalyst-cooperative/pudl/pull/4003) and [#4013](https://github.com/catalyst-cooperative/pudl/pull/4013).

<a id="release-v2024-11-0"></a>

## v2024.11.0 (2024-11-14)

PUDL v2024.11.0 is a regularly scheduled quarterly release, incorporating a few updates
to the following datasets that have come out since the special release we did in
October.

### New Data Coverage

#### EIA-930

* Added EIA-930 hourly data through the end of October as part of the Q3 quarterly
  release. See [#3942](https://github.com/catalyst-cooperative/pudl/issues/3942) and [#3946](https://github.com/catalyst-cooperative/pudl/pull/3946).

#### EIA-923

* Added EIA-923 data from August 2024 as part of the Q3 quarterly release.
  See [#3941](https://github.com/catalyst-cooperative/pudl/issues/3941) and PR [#3950](https://github.com/catalyst-cooperative/pudl/pull/3950).

#### EIA-860M

* Added 2024 EIA-860m data from August, September, and October as part of the Q3
  quarterly release. See [#3940](https://github.com/catalyst-cooperative/pudl/issues/3940) and PR [#3949](https://github.com/catalyst-cooperative/pudl/pull/3949).

#### EIA-861

* Added final release EIA-861 data. See [#3905](https://github.com/catalyst-cooperative/pudl/issues/3905) and PR [#3911](https://github.com/catalyst-cooperative/pudl/pull/3911).

#### EIA Bulk Electricity Data

* Updated the EIA Bulk Electricity data to include data published up through
  2024-08-01. See [#3944](https://github.com/catalyst-cooperative/pudl/issues/3944) and PR [#3951](https://github.com/catalyst-cooperative/pudl/pull/3951).

#### EPA CEMS

* Added 2024 Q3 of CEMS data. See [#3943](https://github.com/catalyst-cooperative/pudl/issues/3943) and [#3948](https://github.com/catalyst-cooperative/pudl/pull/3948).

### Record Linkage

* Updated the `splink` FERC to EIA development notebook to be compatible with
  the latest version of `splink`. This notebook is not run in production but
  is helpful for visualizing model weights and what is happening under the hood.
* Updated `pudl.analysis.record_linkage.name_cleaner` company name cleaning
  module to be more efficient by removing all `.apply` and instead use
  `pd.Series.replace` to make regex replacement rules vectorized. Also removed
  some of the allowed replacement rules to make the cleaner simpler and more
  effective. This module runs approximately 3x faster now when cleaning a
  string Series.

<a id="release-v2024-10-0"></a>

## v2024.10.0 (2024-10-20)

This is a special early release to publish the new VCE Resource Adequacy Renewable
Energy (RARE) dataset. It also includes final releases of EIA-860 and 923 data for 2023
and the FERC Form 714 data for 2021-2023, which had previously been integrated from
the XBRL data published by FERC. See details below

### New Data

#### Vibrant Clean Energy Resource Adequacy Renewable Energy (RARE) Power Dataset

* Integrate the VCE hourly capacity factor data for solar PV, onshore wind, and
  offshore wind from 2019 through 2023. The data in this table were produced by
  Vibrant Clean Energy, and are licensed to the public under the Creative Commons
  Attribution 4.0 International license (CC-BY-4.0). This data complements the
  WECC-wide GridPath RA Toolkit data currently incorporated into PUDL, providing
  capacity factor data nation-wide with a different set of modeling assumptions and
  a different granularity for the aggregation of outputs.
  See [GridPath Resource Adequacy Toolkit Data](data_sources/gridpathratoolkit.md) and [Vibrant Clean Energy Resource Adequacy Renewable Energy (RARE) Power Dataset](data_sources/vcerare.md) for
  more information.  See [#3872](https://github.com/catalyst-cooperative/pudl/issues/#3872).

### New Data Coverage

#### EIA-860

* Added EIA-860 final release data from 2023. See [#3684](https://github.com/catalyst-cooperative/pudl/issues/3684) and PR [#3871](https://github.com/catalyst-cooperative/pudl/pull/3871).

#### EIA-861

* Added EIA-861 final release data from 2023. See [#3905](https://github.com/catalyst-cooperative/pudl/issues/3905) and PR [#3911](https://github.com/catalyst-cooperative/pudl/pull/3911). This
  includes a new `energy_capacity_mwh` field for battery storage in
  [core_eia861_\_yearly_net_metering_customer_fuel_class](data_dictionaries/pudl_db.md#core-eia861-yearly-net-metering-customer-fuel-class) and
  [core_eia861_\_yearly_non_net_metering_customer_fuel_class](data_dictionaries/pudl_db.md#core-eia861-yearly-non-net-metering-customer-fuel-class).

#### EIA-923

* Added EIA-923 final release data from 2023 and revised data from 2022. See
  [#3902](https://github.com/catalyst-cooperative/pudl/issues/3902) and PR [#3903](https://github.com/catalyst-cooperative/pudl/pull/3903).

#### FERC Form 714

* Integrated 2021-2023 years of the FERC Form 714 data. FERC updated its reporting
  format for 2021 from a CSV files to XBRL files. This update integrates the two
  raw data sources and extends the data coverage through 2023. See [#3809](https://github.com/catalyst-cooperative/pudl/issues/3809)
  and [#3842](https://github.com/catalyst-cooperative/pudl/pull/3842).

### Schema Changes

* Added [out_eia_\_yearly_assn_plant_parts_plant_gen](data_dictionaries/pudl_db.md#out-eia-yearly-assn-plant-parts-plant-gen) table. This table associates
  records from the [out_eia_\_yearly_plant_parts](data_dictionaries/pudl_db.md#out-eia-yearly-plant-parts) with `plant_gen` records from
  that same plant parts table. See issue [#3773](https://github.com/catalyst-cooperative/pudl/issues/3773) and PR [#3774](https://github.com/catalyst-cooperative/pudl/pull/3774).

### Bug Fixes

* Included more retiring generators in the net generation and fuel consumption
  allocation. Thanks to [@grgmiller](https://github.com/sponsors/grgmiller) for this contribution [#3690](https://github.com/catalyst-cooperative/pudl/pull/3690).
* Fixed a bug found in the rolling averages used to impute missing values in
  `fuel_cost_per_mmbtu` and to calculate `capex_annual_addition_rolling`. Thanks
  to RMI for identifying this bug! See issue [#3889](https://github.com/catalyst-cooperative/pudl/issues/3889) and PR [#3892](https://github.com/catalyst-cooperative/pudl/pull/3892).

### Major Dependency Updates

* Updated to use [Numpy v2.0](https://numpy.org/doc/stable/release/2.0.0-notes.html)
  and [Splink v4.0](https://moj-analytical-services.github.io/splink/blog/2024/07/24/splink-400-released.html).
  See issues [#3736](https://github.com/catalyst-cooperative/pudl/issues/3736), [#3735](https://github.com/catalyst-cooperative/pudl/issues/3735) and PRs [#3547](https://github.com/catalyst-cooperative/pudl/pull/3547), [#3834](https://github.com/catalyst-cooperative/pudl/pull/3834).

### Quality of Life Improvements

* We now use an asset factory to generate Dagster assets for near-identical FERC1 output
  tables. See [#3147](https://github.com/catalyst-cooperative/pudl/issues/3147) and [#3883](https://github.com/catalyst-cooperative/pudl/pull/3883). Thanks to [@hfireborn](https://github.com/sponsors/hfireborn) and
  [@denimalpaca](https://github.com/sponsors/denimalpaca) for their work on this one!

<a id="release-v2024-8-0"></a>

## v2024.8.0 (2024-08-19)

This is our regular quarterly release for 2024Q3. It includes quarterly updates to all
datasets that are updated with quarterly or higher frequency by their publishers,
including EIA-860M, EIA-923 (YTD data), EIA-930, the EIA’s bulk electricity API data
(used to fill in missing fuel prices), and the EPA CEMS hourly emissions data.

Annual datasets which have been published since our last quarterly release have also
been integrated. These include FERC Forms 1, 2, 6, 60, and 714, and the NREL ATB.

This release also includes provisional versions of the annual 2023 EIA-860 and EIA-923
datasets, whose final release will not happen until the fall.

### New Data Coverage

#### FERC Form 1

* Integrated FERC Form 1 data from 2023 into the main PUDL SQLite DB. See issue
  [#3700](https://github.com/catalyst-cooperative/pudl/issues/3700) and PR [#3701](https://github.com/catalyst-cooperative/pudl/pull/3701). This required updating to a new version of the
  `catalystcoop.ferc_xbrl_extractor` package because there are now multiple XBRL
  taxonomies in use by FERC in different years, or even within the same year. See [this
  PR](https://github.com/catalyst-cooperative/ferc-xbrl-extractor/pull/242) for more
  details, as well as issue [#3544](https://github.com/catalyst-cooperative/pudl/issues/3544) and PR [#3710](https://github.com/catalyst-cooperative/pudl/pull/3710).

#### FERC Forms 2, 6, 60, & 714

* Updated the `ferc_to_sqlite` settings to extract 2023 XBRL data for FERC Forms 2, 6
  60, and 714 and add them to their respective SQLite databases. Note that this data
  is not yet being processed beyond the conversion from XBRL to SQLite. See PR
  [#3710](https://github.com/catalyst-cooperative/pudl/pull/3710)

#### EIA AEO

* Added new tables from EIA AEO table 54:
  * [core_eiaaeo_\_yearly_projected_fuel_cost_in_electric_sector_by_type](data_dictionaries/pudl_db.md#core-eiaaeo-yearly-projected-fuel-cost-in-electric-sector-by-type)
    contains fuel costs for the electric power sector. These are broken out by
    fuel type, and include both nominal USD per MMBtu as well as real 2022 USD
    per MMBtu. See issue [#3649](https://github.com/catalyst-cooperative/pudl/issues/3649) and PR [#3656](https://github.com/catalyst-cooperative/pudl/pull/3656).

#### EIA-860

* Added EIA-860 early release data from 2023. This included adding a new tab with
  proposed energy storage generators as well as adding a number of new columns
  regarding energy storage and solar generators. See issue [#3676](https://github.com/catalyst-cooperative/pudl/issues/3676) and PR
  [#3681](https://github.com/catalyst-cooperative/pudl/pull/3681).
* Added EIA-860m data through June 2024. See issue [#3759](https://github.com/catalyst-cooperative/pudl/issues/3759) and PR [#3767](https://github.com/catalyst-cooperative/pudl/pull/3767).

#### EIA-923

* Added EIA-923 early release data from 2023. See [#3719](https://github.com/catalyst-cooperative/pudl/issues/3719) and PR [#3721](https://github.com/catalyst-cooperative/pudl/pull/3721).
* Added EIA-923 monthly data through May as part of the Q2 quarterly release. See
  [#3760](https://github.com/catalyst-cooperative/pudl/issues/3760) and [#3768](https://github.com/catalyst-cooperative/pudl/pull/3768).

#### EIA-930

* Added EIA-930 hourly data through the end of July as part of the Q2 quarterly release.
  See [#3761](https://github.com/catalyst-cooperative/pudl/issues/3761) and [#3789](https://github.com/catalyst-cooperative/pudl/pull/3789).

#### EPA CEMS

* Added 2024 Q2 of CEMS data. See [#3762](https://github.com/catalyst-cooperative/pudl/issues/3762) and [#3769](https://github.com/catalyst-cooperative/pudl/pull/3769).

#### EIA Bulk Electricity Data

* Updated the EIA Bulk Electricity data archive to include data that was available as of
  2024-08-01, which covers up through 2024-05-01 (3 months more than the previously
  used archive). See [#3763](https://github.com/catalyst-cooperative/pudl/issues/3763) and PR [#3785](https://github.com/catalyst-cooperative/pudl/pull/3785).

#### FERC 714

* Added [core_ferc714_\_yearly_planning_area_demand_forecast](data_dictionaries/pudl_db.md#core-ferc714-yearly-planning-area-demand-forecast) based on FERC
  Form 714, Part III, Schedule 2b. Data includes forecasted demand and net energy load.
  See issue [#3519](https://github.com/catalyst-cooperative/pudl/issues/3519) and PR [#3670](https://github.com/catalyst-cooperative/pudl/pull/3670).
* WIP: Adding XBRL(2021+) data for FERC 714 tables. Track progress in [#3822](https://github.com/catalyst-cooperative/pudl/issues/3822).

#### NREL ATB

* Added 2024 NREL ATB data. This includes adding a new tax credit case,
  `model_tax_credit_case_nrelatb`, a breakout of `capex_grid_connection_per_kw` for
  all technologies, and more detailed nuclear breakdowns of `fuel_cost_per_mwh`.
  Simultaneously, updated the `docs.dev.existing_data_updates` documentation to
  make it easier to add future years of data. See [#3706](https://github.com/catalyst-cooperative/pudl/issues/3706) and [#3719](https://github.com/catalyst-cooperative/pudl/pull/3719).
* Updated NREL ATB data to include [error corrections in the 2024 data](https://atb.nrel.gov/electricity/2024/errata).
  See [#3777](https://github.com/catalyst-cooperative/pudl/issues/3777) and PR [#3778](https://github.com/catalyst-cooperative/pudl/pull/3778).

### Data Cleaning

* When `generator_operating_date` values are too inconsistent to be harvested
  successfully, we now take the last reported date in EIA-860 and 860M. See [#423](https://github.com/catalyst-cooperative/pudl/issues/423)
  and PR [#3967](https://github.com/catalyst-cooperative/pudl/pull/3967).
* Added the `generator_operating_date` field into
  [core_eia860m_\_changelog_generators](data_dictionaries/pudl_db.md#core-eia860m-changelog-generators), adding 860M reported generator operating
  dates into the changelog table. This table is not harvested, and thus does not affect
  the `generator_operating_date` values reported in other core EIA tables. See
  [#3722](https://github.com/catalyst-cooperative/pudl/issues/3722) and PR [#3751.](https://github.com/catalyst-cooperative/pudl/pull/3751.)

### Bug Fixes

* Disabled filling of missing values using rolling averages for the
  `fuel_cost_per_mmbtu` column in the [out_eia923_\_fuel_receipts_costs](data_dictionaries/pudl_db.md#out-eia923-fuel-receipts-costs) table, as
  it was resulting in some anomlously high fuel prices. See [#3716](https://github.com/catalyst-cooperative/pudl/pull/3716). This results in
  about 2% more records in the table being left `NA` after filling with the average
  prices for that fuel type for the state and month found in the bulk EIA API data.

### Quality of Life Improvements

* The full ETL settings are now read directly from `etl_full.yml` instead of using
  default values defined in the settings classes.  This also results in the settings
  showing up in the Dagster UI Launchpad, which previously they didn’t, leading to
  confusion when trying to re-run the FERC to SQLite conversions. See [#3710](https://github.com/catalyst-cooperative/pudl/pull/3710).
* `mlflow` experiment tracking has been disabled by default when running the DAG,
  since it is only really helpful during development of new record linkage or other ML
  workflows. See [#3710](https://github.com/catalyst-cooperative/pudl/pull/3710).

<a id="release-v2024-5-0"></a>

## v2024.5.0 (2024-05-24)

We’ve just completed our quarterly integration of EIA data sources for 2024Q2
(in support of RMI’s Utility Transition Hub) and have also added a bunch of new
tables over the last few months in an effort to better support energy system
modelers (with support from GridLab). Details below.

### New Data Coverage

#### EIA-860 & EIA-923

* Added cleaned EIA860 Schedule 8E FGD Equipment and EIA923 Schedule 8C FGD Operation
  and Maintenance data to the PUDL database as
  [\_core_eia923_\_yearly_fgd_operation_maintenance](data_dictionaries/pudl_db.md#i-core-eia923-yearly-fgd-operation-maintenance) and
  [\_core_eia860_\_fgd_equipment](data_dictionaries/pudl_db.md#i-core-eia860-fgd-equipment). Once harvested, these tables will eventually be
  removed from the database, but they are being published until then. See [#3394](https://github.com/catalyst-cooperative/pudl/issues/3394)
  and [#3392](https://github.com/catalyst-cooperative/pudl/issues/3392), and [#3403](https://github.com/catalyst-cooperative/pudl/pull/3403).
* Added new [core_eia860_\_scd_generators_wind](data_dictionaries/pudl_db.md#core-eia860-scd-generators-wind) table from EIA860 Schedule 3.2
  which contains wind generator attributes. See [#3522](https://github.com/catalyst-cooperative/pudl/pull/3522) and [#3494](https://github.com/catalyst-cooperative/pudl/pull/3494).
* Added new [core_eia860_\_scd_generators_solar](data_dictionaries/pudl_db.md#core-eia860-scd-generators-solar) table from EIA860 Schedule 3.3
  which contains solar generator attributes. See [#3524](https://github.com/catalyst-cooperative/pudl/pull/3524) and [#3482](https://github.com/catalyst-cooperative/pudl/pull/3482).
* Added new [core_eia860_\_scd_generators_energy_storage](data_dictionaries/pudl_db.md#core-eia860-scd-generators-energy-storage) table from EIA860 Schedule
  3.4 which contains energy storage generator attributes. See [#3488](https://github.com/catalyst-cooperative/pudl/pull/3488) and [#3526](https://github.com/catalyst-cooperative/pudl/pull/3526).
  which contains solar generator attributes. See [#3524](https://github.com/catalyst-cooperative/pudl/pull/3524) and [#3482](https://github.com/catalyst-cooperative/pudl/pull/3482)
* Added new [core_eia923_\_monthly_energy_storage](data_dictionaries/pudl_db.md#core-eia923-monthly-energy-storage) table from EIA923 which contains
  monthly energy and fuel consumption metrics. See [#3516](https://github.com/catalyst-cooperative/pudl/pull/3516) and [#3546](https://github.com/catalyst-cooperative/pudl/pull/3546).
* Added 2024 Q1 EIA923 and EIA860m data. See issues [#3617](https://github.com/catalyst-cooperative/pudl/issues/3617), [#3618](https://github.com/catalyst-cooperative/pudl/issues/3618), and PR
  [#3625](https://github.com/catalyst-cooperative/pudl/pull/3625).

#### GridPath RA Toolkit

* Added a new `gridpathratoolkit` data source containing hourly wind and solar
  generation profiles from the [GridPath Resource Adequacy Toolkit](https://gridlab.org/gridpathratoolkit). See [GridPath Resource Adequacy Toolkit Data](data_sources/gridpathratoolkit.md)
  and the [new Zenodo archive](https://zenodo.org/records/10844662), PR [#3489](https://github.com/catalyst-cooperative/pudl/pull/3489)
  and [this PUDL archiver issue](https://github.com/catalyst-cooperative/pudl-archiver/issues/296).
* Integrated the most processed version of the GridPath RA Toolkit wind and solar
  generation profiles, as well as the tables describing how individual generators were
  aggregated together to create the profiles. See issues [#3509](https://github.com/catalyst-cooperative/pudl/issues/3509), [#3510](https://github.com/catalyst-cooperative/pudl/issues/3510), [#3511](https://github.com/catalyst-cooperative/pudl/issues/3511), [#3515](https://github.com/catalyst-cooperative/pudl/issues/3515)
  and PR [#3514](https://github.com/catalyst-cooperative/pudl/pull/3514). The new tables include:
  [out_gridpathratoolkit_\_hourly_available_capacity_factor](data_dictionaries/pudl_db.md#out-gridpathratoolkit-hourly-available-capacity-factor) and
  [core_gridpathratoolkit_\_assn_generator_aggregation_group](data_dictionaries/pudl_db.md#core-gridpathratoolkit-assn-generator-aggregation-group).

#### EIA AEO

* Extracted tables 13, 15, 20, and 54 from the [EIA Annual Energy Outlook 2023](https://www.eia.gov/outlooks/aeo/tables_ref.php), which include future
  projections related to electric power and renewable energy through the year
  2050, across a variety of scenarios. See [#3368](https://github.com/catalyst-cooperative/pudl/issues/3368) and [#3538](https://github.com/catalyst-cooperative/pudl/pull/3538).
* Added new [core_eia861_\_yearly_short_form](data_dictionaries/pudl_db.md#core-eia861-yearly-short-form) table from EIA861 which contains
  the shorter version of EIA861. See issues [#3540](https://github.com/catalyst-cooperative/pudl/issues/3540) and PR [#3565](https://github.com/catalyst-cooperative/pudl/pull/3565).
* Added new tables from EIA AEO table 54:
  * [core_eiaaeo_\_yearly_projected_generation_in_electric_sector_by_technology](data_dictionaries/pudl_db.md#core-eiaaeo-yearly-projected-generation-in-electric-sector-by-technology)
    contains generation capacity & generation projections for the electric
    sector, broken out by technology type. See [#3581](https://github.com/catalyst-cooperative/pudl/issues/3581) and [#3582](https://github.com/catalyst-cooperative/pudl/pull/3582).
  * [core_eiaaeo_\_yearly_projected_generation_in_end_use_sectors_by_fuel_type](data_dictionaries/pudl_db.md#core-eiaaeo-yearly-projected-generation-in-end-use-sectors-by-fuel-type)
    contains generation capacity & generation projections for the electric
    sector, broken out by technology type. See [#3581](https://github.com/catalyst-cooperative/pudl/issues/3581) and [#3598](https://github.com/catalyst-cooperative/pudl/pull/3598).
  * [core_eiaaeo_\_yearly_projected_electric_sales](data_dictionaries/pudl_db.md#core-eiaaeo-yearly-projected-electric-sales) contains electric sales
    projections until 2050, broken out by customer type. See [#3581](https://github.com/catalyst-cooperative/pudl/issues/3581) and
    [#3617](https://github.com/catalyst-cooperative/pudl/pull/3617).

#### NREL ATB

* Added new NREL ATB tables with annual technology cost and performance projections. See
  issue [#3465](https://github.com/catalyst-cooperative/pudl/issues/3465) and PRs [#3498](https://github.com/catalyst-cooperative/pudl/pull/3498), [#3570](https://github.com/catalyst-cooperative/pudl/pull/3570).

#### EIA-930

* Added hourly generation, demand, and interchange tables from the EIA-930. See issues
  [#3486](https://github.com/catalyst-cooperative/pudl/issues/3486), [#3505](https://github.com/catalyst-cooperative/pudl/issues/3505) PR [#3584](https://github.com/catalyst-cooperative/pudl/pull/3584) and [this issue in the PUDL archiver repo](https://github.com/catalyst-cooperative/pudl-archiver/issues/295). See the
  data source documentation [EIA Form 930 – Hourly and Daily Balancing Authority Operations Report](data_sources/eia930.md) for more information.

#### EPA CEMS

* Added 2024 Q1 of CEMS data. See [#3620](https://github.com/catalyst-cooperative/pudl/issues/3620) and [#3624](https://github.com/catalyst-cooperative/pudl/pull/3624)

#### EIA Bulk Electricity Data

* Updated the EIA Bulk Electricity data archive to include data that was available as of
  2024-05-01, which covers up through 2024-02-01 (3 months more than the previously
  used archive). See PR [#3615](https://github.com/catalyst-cooperative/pudl/pull/3615).

#### FERC Form 1

* Added new [out_ferc1_\_yearly_rate_base](data_dictionaries/pudl_db.md#out-ferc1-yearly-rate-base) table which includes granular financial
  data regarding what utilities include in their rate bases. See epic [#2016](https://github.com/catalyst-cooperative/pudl/issues/2016).

### Data Cleaning

* When `generator_operating_date` values are too inconsistent to be harvested
  successfully, we now take the max date within a year and attempt to harvest again, to
  rescue records lost because of inconsistent month reporting in EIA-860 and 860M. See
  [#3340](https://github.com/catalyst-cooperative/pudl/issues/3340) and PR [#3419](https://github.com/catalyst-cooperative/pudl/pull/3419). This change also fixed a bug that was preventing
  other columns harvested with a special process from being saved.
* When ingesting FERC 1 XBRL filings, we now take the most recent non-null
  value instead of the value from the latest filing that applies for a specific
  row. This means that we no longer lose data if a utility posts a FERC filing
  with only a small number of updated values.

### EIA - FERC1 Record Linkage Model Update

We merged in a refactor of the EIA plant parts to FERC1 plants record linkage
model, which was generously supported by a [CCAI Innovation Grant](https://www.climatechange.ai/calls/innovation_grants). This replaced the linear
regression model with a model built with the Python package [Splink](https://moj-analytical-services.github.io/splink/index.html). Splink provides helpful
visualizations to understand model performance and parameter tuning, which can be
generated with `devtools/splink-ferc1-eia-match.ipynb`. We measured model
performance with precision - a measure of accuracy when the model makes a prediction,
recall - a measure of coverage of FERC records model predicted a match for, and
accuracy - a measure of overall correctness of the predictions. Model performance
improved and now has a precision of .94, recall of .9, and overall accuracy of .85.

### Schema Changes

* Added `balancing_authority_code_eia` and `sector_id_eia` into the
  [core_eia860m_\_changelog_generators](data_dictionaries/pudl_db.md#core-eia860m-changelog-generators) table. The BA codes reported in the raw data
  contained a lot of non-standard values, which have now been standardized. See issue
  [#3437](https://github.com/catalyst-cooperative/pudl/issues/3437) and PR [#3442](https://github.com/catalyst-cooperative/pudl/pull/3442).
* Renamed the `utc_datetime` column found in the FERC-714 tables to `datetime_utc`
  in order to be consistent with `operating_datetime_utc` in the EPA CEMS data, and
  the new hourly renewable generation profiles in the GridPath RA Toolkit. See PR
  [#3514](https://github.com/catalyst-cooperative/pudl/pull/3514).
* Renamed the utility and balancing authority service territory tables to better conform
  to our naming conventions: `out_eia861__compiled_geometry_utilities` is now
  [out_eia861_\_yearly_utility_service_territory](data_dictionaries/pudl_db.md#out-eia861-yearly-utility-service-territory) and
  `out_eia861__compiled_geometry_balancing_authorities` is now
  [out_eia861_\_yearly_balancing_authority_service_territory](data_dictionaries/pudl_db.md#out-eia861-yearly-balancing-authority-service-territory). See PR [#3552](https://github.com/catalyst-cooperative/pudl/pull/3552).
* All hourly tables are now published only as Apache Parquet files, rather than being
  written to the main PUDL SQLite database. This reduces the size of the PUDL DB, and
  also makes accessing these large table much faster both during data processing and for
  end users. See PR [#3584](https://github.com/catalyst-cooperative/pudl/pull/3584).  Affected tables include:
  * [core_eia930_\_hourly_interchange](data_dictionaries/pudl_db.md#core-eia930-hourly-interchange)
  * [core_eia930_\_hourly_net_generation_by_energy_source](data_dictionaries/pudl_db.md#core-eia930-hourly-net-generation-by-energy-source)
  * [core_eia930_\_hourly_operations](data_dictionaries/pudl_db.md#core-eia930-hourly-operations)
  * [core_eia930_\_hourly_subregion_demand](data_dictionaries/pudl_db.md#core-eia930-hourly-subregion-demand)
  * [core_epacems_\_hourly_emissions](data_dictionaries/pudl_db.md#core-epacems-hourly-emissions)
  * [out_ferc714_\_hourly_estimated_state_demand](data_dictionaries/pudl_db.md#out-ferc714-hourly-estimated-state-demand)
  * [out_ferc714_\_hourly_planning_area_demand](data_dictionaries/pudl_db.md#out-ferc714-hourly-planning-area-demand)
  * [out_gridpathratoolkit_\_hourly_available_capacity_factor](data_dictionaries/pudl_db.md#out-gridpathratoolkit-hourly-available-capacity-factor)

  The FERC-714 hourly demand tables have been removed from the
  `pudl.output.pudltabl.PudlTabl` class, which has been deprecated.
* The long derelict `core_ferc__codes_accounts` table has been removed from the PUDL
  database. This table contained descriptions of the FERC accounts that were found in
  the Electric Plant in Service table, but only pertained to a single year, and was not
  being referenced or maintained elsewhere. See PR [#3584](https://github.com/catalyst-cooperative/pudl/pull/3584).
* Additional columns were added to the [core_eia_\_codes_balancing_authorities](data_dictionaries/pudl_db.md#core-eia-codes-balancing-authorities)
  table, indicating the timezone associated with each BA’s reporting, whether it is a
  generation only BA, and its date of retirement, and what region it is part of. See PR
  [#3584](https://github.com/catalyst-cooperative/pudl/pull/3584).
* A new [core_eia_\_codes_balancing_authority_subregions](data_dictionaries/pudl_db.md#core-eia-codes-balancing-authority-subregions) table was added to
  describe the relationships between BAs and their subregions. See PR [#3584](https://github.com/catalyst-cooperative/pudl/pull/3584).

### Bug Fixes

* Ensure that all columns fed into the harvesting / reconciliation process are encoded
  before harvesting takes place, improving the consistency of harvested fields. See
  issue [#3542](https://github.com/catalyst-cooperative/pudl/issues/3542) and PR [#3558](https://github.com/catalyst-cooperative/pudl/pull/3558). This change also simplifies the encoding
  process in the vast majority of cases, since the same global set of encoders can be
  used on any dataframe, with every column encoded based on the field definitions and
  FK constraints associated with the column name.

### CLI Changes

* Removed the `--clobber` option from the `ferc_to_sqlite` command and associated
  assets. We rebuild these databases infrequently, and needing to either edit the
  runtime parameters in Dagster’s Launchpad or remove the existing databases from the
  filesystem manually are brittle. Partly in response to issue [#3612](https://github.com/catalyst-cooperative/pudl/issues/3612); see PR
  [#3622](https://github.com/catalyst-cooperative/pudl/pull/3622).

<a id="release-v2024-2-6"></a>

## v2024.2.6 (2024-02-25)

The main impetus behind this release is the quarterly update of some of our
core datasets with preliminary data for 2023Q4. The [EIA Form 860 – Annual Electric Generator Report](data_sources/eia860.md),
[EPA Hourly Continuous Emission Monitoring System (CEMS)](data_sources/epacems.md), and bulk EIA API data are all up to date through the end of
2023, while the [EIA Form 923 – Power Plant Operations Report](data_sources/eia923.md) lags a month behind and is currently only
available through November, 2023. We also addressed several issues we found in our
initial release automation process that will make it easier for us to do more frequent
releases, like this one!

We’re also for the first time publishing the full historical time series of of generator
data available in the EIA860M, rather than just using the most recent release to update
the EIA860 outputs. This enables tracking of how planned fossil plant retirement dates
have evolved over time.

There are also updates to our data validation system, a new version of Pandas, and
experimental Parquet outputs. See below for the details.

### New Data Coverage

* Add EIA860M data through December 2023 [#3313](https://github.com/catalyst-cooperative/pudl/issues/3313), [#3367](https://github.com/catalyst-cooperative/pudl/pull/3367).
* Add 2023 Q4 of CEMS data. See [#3315](https://github.com/catalyst-cooperative/pudl/issues/3315), [#3379](https://github.com/catalyst-cooperative/pudl/pull/3379).
* Add EIA923 monthly data through November 2023 [#3314](https://github.com/catalyst-cooperative/pudl/issues/3314), [#3398](https://github.com/catalyst-cooperative/pudl/pull/3398), [#3422](https://github.com/catalyst-cooperative/pudl/pull/3422).
* Create a new table [core_eia860m_\_changelog_generators](data_dictionaries/pudl_db.md#core-eia860m-changelog-generators) which tracks the
  evolution of all generator data reported in the EIA860M, in particular the stated
  retirement dates. see issue [#3330](https://github.com/catalyst-cooperative/pudl/issues/3330) and PR [#3331](https://github.com/catalyst-cooperative/pudl/pull/3331). Previously only the most
  recent month of reported EIA860M data was available within the PUDL DB.

### Release Infrastructure

* Use the same logic to merge version tags into the `stable` branch as we are using
  to merge the nightly build tags into the `nightly` branch. See PR [#3347](https://github.com/catalyst-cooperative/pudl/pull/3347)
* Automatically place a [temporary object hold](https://cloud.google.com/storage/docs/holding-objects#use-object-holds)
  on all versioned data releases that we publish to GCS, to ensure that they can’t be
  accidentally deleted. See issue [#3400](https://github.com/catalyst-cooperative/pudl/issues/3400) and PR [#3421](https://github.com/catalyst-cooperative/pudl/pull/3421).

### Schema Changes

* Restored the individual FERC Form 1 plant output tables, providing direct access to
  denormalized versions of the specific plant types via:
  * [out_ferc1_\_yearly_steam_plants_sched402](data_dictionaries/pudl_db.md#out-ferc1-yearly-steam-plants-sched402)
  * [out_ferc1_\_yearly_small_plants_sched410](data_dictionaries/pudl_db.md#out-ferc1-yearly-small-plants-sched410)
  * [out_ferc1_\_yearly_hydroelectric_plants_sched406](data_dictionaries/pudl_db.md#out-ferc1-yearly-hydroelectric-plants-sched406)
  * [out_ferc1_\_yearly_pumped_storage_plants_sched408](data_dictionaries/pudl_db.md#out-ferc1-yearly-pumped-storage-plants-sched408)

  See issue [#3416](https://github.com/catalyst-cooperative/pudl/issues/3416) & PR [#3417](https://github.com/catalyst-cooperative/pudl/pull/3417)

### Data Validation with Pandera

We’ve started integrating `pandera` dataframe schemas and checks with
`dagster` [asset checks](https://docs.dagster.io/concepts/assets/asset-checks)
to validate data while our ETL pipeline is running instead of only after all the data
has been produced. Initially we are using the various database schema checks that are
generated by our metadata, but the goal is to migrate all of our data validation tests
into this framework over time, and to start using it to encode any new data validations
immediately. See issues [#941](https://github.com/catalyst-cooperative/pudl/issues/941), [#1572](https://github.com/catalyst-cooperative/pudl/issues/1572), [#3318](https://github.com/catalyst-cooperative/pudl/issues/3318), [#3412](https://github.com/catalyst-cooperative/pudl/issues/3412) and PR [#3282](https://github.com/catalyst-cooperative/pudl/pull/3282).

### Pandas 2.2

We’ve updated to Pandas 2.2, which has a number of changes and deprecations.  See PRs
[#3272](https://github.com/catalyst-cooperative/pudl/pull/3272), [#3410](https://github.com/catalyst-cooperative/pudl/pull/3410).

* Changes in
  [how merge results are sorted](https://pandas.pydata.org/pandas-docs/stable/whatsnew/v2.2.0.html#merge-and-dataframe-join-now-consistently-follow-documented-sort-behavior)
  impacted the assignment of `unit_id_pudl` values, so any hard-coded values that
  dependent on the previous assignments will likely be incorrect now. We had to update a
  number of tests and FERC1-EIA record linkage training data to account for this change.
* Pandas is also deprecating the use of the `AS` frequency alias, in favor of `YS`,
  so many references to the old alias have been updated.
* We’ve switched to using the `calamine` engine for reading Excel files, which is
  much faster than the old `openpyxl` library.

### Parquet Outputs

The ETL now outputs PyArrow Parquet files for all tables that are written to the PUDL
DB. The Parquet outputs are used as the interim storage for the ETL, rather than reading
all tables out of the SQLite DB. We aren’t publicly distributing the Parquet outputs
yet, but are giving them a test run with some existing users. See [#3102](https://github.com/catalyst-cooperative/pudl/issues/3102)
[#3296](https://github.com/catalyst-cooperative/pudl/pull/3296), [#3399](https://github.com/catalyst-cooperative/pudl/pull/3399).

### Dependencies

* Update PUDL to use Python 3.12. See issue [#3327](https://github.com/catalyst-cooperative/pudl/issues/3327) and PR [#3413](https://github.com/catalyst-cooperative/pudl/pull/3413).

<a id="release-v2024-02-05"></a>

## v2024.02.05

This release contains only minor data updates compared to what we put out in December,
however the database naming conventions and release process has changed pretty
dramatically. We are confident these changes will make the data we publish more
accessible, and allow us to push out updates much more frequently going forward.

We also finally merged in improvements and generalizations to our record linkage
processes, which were generously supported by a [CCAI Innovation Grant](https://www.climatechange.ai/calls/innovation_grants). Connecting disparate public
datasets that describe the same physical infrastructure and corporate entities is one
of the most valuable improvements we make to the data, and we are excited to be able to
be able to do it in a more general, reproducible way so we can easily apply it to other
datasets. We’ve already started work on a Mozilla Foundation grant to link SEC data to
the FERC and EIA data we already have, allowing us to track ownership relationships
between utility holding companies and their many subsidiaries. We expect the same kind
of process will be useful for linking the PHMSA gas pipeline data to natural gas
utilities that report to EIA and FERC.

### Database Naming Conventions

Our main focus with this release was to overhaul the naming system for our nearly 200
database tables. This will hopefully make it easier to find what you’re looking for,
especially if you are a new PUDL user. We think it will also make it easier for us to
keep the database organized as we continue to expand its scope.  For an explanation of
the new naming conventions, see [Naming Conventions](dev/naming_conventions.md), and to see the full list
of all available tables, see the [PUDL Data Dictionary](data_dictionaries/pudl_db.md).

This is a major breaking change for anybody is accessing the database directly. Stick
with the [v2023.12.01](#release-v2023-12-01) release until you’re ready to update your references
to the old database table names. For the time being we have patched the old
`pudl.output.pudltabl.PudlTabl` class so that it behaves as similarly as possible
to before. However, we plan to remove this output class in the near future, and no new
database tables will be made accessible through it. Going forward we expect users to use
the database directly, freeing them from the need to install all of the software and
dependencies which we use to produce it, hopefully improving the data’s technical
accessibility and platform independence.

For more development details see [#2765](https://github.com/catalyst-cooperative/pudl/issues/2765) which was the main epic tracking this
process (with many sub-issues: [#2777](https://github.com/catalyst-cooperative/pudl/issues/2777), [#2788](https://github.com/catalyst-cooperative/pudl/issues/2788), [#2812](https://github.com/catalyst-cooperative/pudl/issues/2812), [#2868](https://github.com/catalyst-cooperative/pudl/issues/2868), [#2992](https://github.com/catalyst-cooperative/pudl/issues/2992), [#3030](https://github.com/catalyst-cooperative/pudl/issues/3030), [#3173](https://github.com/catalyst-cooperative/pudl/issues/3173), [#3174](https://github.com/catalyst-cooperative/pudl/issues/3174), [#3223](https://github.com/catalyst-cooperative/pudl/issues/3223))
and PR [#2818](https://github.com/catalyst-cooperative/pudl/pull/2818).

### Changes to CLI Tools

* The `epacems_to_parquet` and `state_demand` scripts have been retired in favor of
  using the Dagster UI. See [#3107](https://github.com/catalyst-cooperative/pudl/issues/3107) and [#3086](https://github.com/catalyst-cooperative/pudl/pull/3086). Visualizations of hourly
  state-level electricity demand have been moved into our example notebooks which can
  be found both [on Kaggle](https://www.kaggle.com/code/catalystcooperative/02-state-hourly-electricity-demand)
  and [on GitHub](https://github.com/catalyst-cooperative/pudl-examples/)
* The `pudl_setup` script has been retired. All input/output locations are now set
  using the `$PUDL_INPUT` and `$PUDL_OUTPUT` environment variables.  See
  [#3107](https://github.com/catalyst-cooperative/pudl/issues/3107) and [#3086](https://github.com/catalyst-cooperative/pudl/pull/3086).
* The `pudl.analysis.service_territory.pudl_service_territories()` script has been
  fixed, and can be used to generate [GeoParquet](https://geoparquet.org/)
  outputs describing historical utility and balancing authority service territories. See
  [#1174](https://github.com/catalyst-cooperative/pudl/issues/1174) and [#3086](https://github.com/catalyst-cooperative/pudl/pull/3086).

### Development Infrastructure

* Automate the process of doing software and data releases when a new version tag is
  pushed to facilitate continuous deployment. See [#3127](https://github.com/catalyst-cooperative/pudl/pull/3127), [#3158](https://github.com/catalyst-cooperative/pudl/pull/3158)
* To make development more convenient given our long-running integration tests, the PUDL
  repository now uses a [merge queue](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/configuring-pull-request-merges/managing-a-merge-queue).
* Switch to using Google Batch for our data builds. See [#3211](https://github.com/catalyst-cooperative/pudl/pull/3211).
* Deprecated the `dev` branch and updated our nightly builds and GitHub workflow to
  use three persistent branches: `main` for bleeding edge changes, `nightly` for the
  most recent commit to have a successful nightly build output, and `stable` for the
  most recently released version of PUDL. The `nightly` and `stable` branches are
  protected and automatically updated. Build outputs are now written to
  `gs://builds.catalyst.coop` and retained for 30 days. See issues [#3140](https://github.com/catalyst-cooperative/pudl/issues/3140), [#3179](https://github.com/catalyst-cooperative/pudl/issues/3179)
  and PRs [#3195](https://github.com/catalyst-cooperative/pudl/pull/3195), [#3206](https://github.com/catalyst-cooperative/pudl/pull/3206), [#3212](https://github.com/catalyst-cooperative/pudl/pull/3212), [#3188](https://github.com/catalyst-cooperative/pudl/pull/3188), [#3164](https://github.com/catalyst-cooperative/pudl/pull/3164)

### Record Linkage Improvements

* The [`pudl.analysis.record_linkage.eia_ferc1_record_linkage`](autoapi/pudl/analysis/record_linkage/eia_ferc1_record_linkage/index.md#module-pudl.analysis.record_linkage.eia_ferc1_record_linkage) module has been
  refactored substantially to make use of more generic PUDL record linkage
  infrastructure and include extra cleaning steps. This resulted in around 500 or 2% of
  matches changing. See [catalyst-cooperative/ccai-entity-matching#108](http://github.com/catalyst-cooperative/ccai-entity-matching/issues/108)
  and [#3184](https://github.com/catalyst-cooperative/pudl/pull/3184).
* Update the FERC Form 1 plant ID assignment (Identifying related plant records from
  different years within the FERC Form 1 data) to use the new record linkage
  infrastructure. See [#3007](https://github.com/catalyst-cooperative/pudl/pull/3007), [#3137](https://github.com/catalyst-cooperative/pudl/pull/3137)

### New Data Coverage

* Updated [EPA Hourly Continuous Emission Monitoring System (CEMS)](data_sources/epacems.md) to switch to pulling the quarterly updates of
  CEMS instead of the annual files. Integrates CEMS through 2023Q3. See issue
  [#2973](https://github.com/catalyst-cooperative/pudl/issues/2973) & PR [#3096](https://github.com/catalyst-cooperative/pudl/pull/3096), [#3139](https://github.com/catalyst-cooperative/pudl/pull/3139).
* Began integration of PHMSA gas distribution and transmission tables into PUDL,
  extracting raw data from 1990-present. Note that these tables are not yet being
  written to the database as they are still raw. See epic [#2848](https://github.com/catalyst-cooperative/pudl/issues/2848), and constituent
  PRs: [#2932](https://github.com/catalyst-cooperative/pudl/pull/2932), [#3242](https://github.com/catalyst-cooperative/pudl/pull/3242), [#3254](https://github.com/catalyst-cooperative/pudl/pull/3254), [#3260](https://github.com/catalyst-cooperative/pudl/pull/3260), [#3262](https://github.com/catalyst-cooperative/pudl/pull/3262), [#3266](https://github.com/catalyst-cooperative/pudl/pull/3266), [#3267](https://github.com/catalyst-cooperative/pudl/pull/3267), [#3269](https://github.com/catalyst-cooperative/pudl/pull/3269), [#3270](https://github.com/catalyst-cooperative/pudl/pull/3270), [#3279](https://github.com/catalyst-cooperative/pudl/pull/3279), [#3280](https://github.com/catalyst-cooperative/pudl/pull/3280).
* We began integration of data from EIA Forms 176, 191, and 757, describing natural gas
  sources, storage, transportation, and disposition. Note this data is still in its raw
  extracted form and is not yet being written to the PUDL DB. See [#3304](https://github.com/catalyst-cooperative/pudl/pull/3304), [#3227](https://github.com/catalyst-cooperative/pudl/pull/3227)
* Updated the EIA Bulk Electricity data archive so that the available data now to runs
  through 2023-10-01. See [#3252](https://github.com/catalyst-cooperative/pudl/pull/3252).  Also added this dataset to the set of data that
  will automatically generate archives each month. See [This PUDL Archiver PR](https://github.com/catalyst-cooperative/pudl-archiver/pull/257) and [this Zenodo
  archive](https://doi.org/10.5281/zenodo.10525348)

### Data Cleaning

* Filled in null annual balances with fourth-quarter quarterly balances in
  [core_ferc1_\_yearly_balance_sheet_liabilities_sched110](data_dictionaries/pudl_db.md#core-ferc1-yearly-balance-sheet-liabilities-sched110). [#3233](https://github.com/catalyst-cooperative/pudl/issues/3233) and
  [#3234](https://github.com/catalyst-cooperative/pudl/pull/3234).
* Added a notebook `devtools/debug-column-mapping.ipynb` to make debugging manual
  column maps for new datasets simpler and faster.

### Metadata Cleaning

* Fix metadata structures and pyarrow schema generation process so that all tables can
  now be output as Parquet files. See issue [#3102](https://github.com/catalyst-cooperative/pudl/issues/3102) and PR [#3222](https://github.com/catalyst-cooperative/pudl/pull/3222).
* Made a description field mandatory for all instances of `Field` and `Resource`.
  Updated the `pudl.metadata.fields.FIELD_METADATA`` and
  `pudl.metadata.resources.RESOURCE_METADATA`` so that all of them have a
  description. This primarily affected [EIA Form 861 – Annual Electric Power Industry Report](data_sources/eia861.md) tables. See
  [#3224](https://github.com/catalyst-cooperative/pudl/issues/3224), [#3283](https://github.com/catalyst-cooperative/pudl/pull/3283).
* Removed fields that are not used in any tables and removed the xfail from the
  `test_defined_fields_are_used` test. [#3224](https://github.com/catalyst-cooperative/pudl/issues/3224), [#3283](https://github.com/catalyst-cooperative/pudl/pull/3283).

<a id="release-v2023-12-01"></a>

## v2023.12.01

### Dagster Adoption

* After comparing comparing python orchestration tools [#1487](https://github.com/catalyst-cooperative/pudl/issues/1487), we decided to
  adopt [Dagster](https://dagster.io/). Dagster will allow us to parallelize the ETL,
  persist datafarmes at any step in the data cleaning process, visualize data
  dependencies and run subsets of the ETL from upstream caches.
* We are converting PUDL code to use dagster concepts in two phases. The first phase
  converts the ETL portion of the code base to use
  [software defined assets](https://docs.dagster.io/concepts/assets/software-defined-assets)
  [#1570](https://github.com/catalyst-cooperative/pudl/issues/1570). The second phase converts the output and analysis tables in the
  `pudl.output.pudltabl.PudlTabl` class to use software defined assets, replacing
  the existing `pudl_out` output functions.
* General changes:
  * `pudl.etl` is now a subpackage that collects all pudl assets into a dagster
    [Definition](https://docs.dagster.io/concepts/code-locations).
  * The `pudl_settings`, `Datastore` and `DatasetSettings` are now dagster
    resources. See `pudl.resources`.
  * The `pudl_etl`  and `ferc_to_sqlite` commands no longer support loading
    specific tables. The commands run all of the tables. Use dagster assets to
    run subsets of the tables.
  * The `--clobber` argument has been removed from the `pudl_etl` command.
  * New static method [`pudl.metadata.classes.Package.get_etl_group_tables`](autoapi/pudl/metadata/classes/index.md#pudl.metadata.classes.Package.get_etl_group_tables)
    returns the resources ids for a given etl group.
  * `pudl.settings.FercToSqliteSettings` class now loads all FERC
    datasources if no datasets are specified.
  * The Excel extractor in `pudl.extract.excel` has been updated to parallelize
    Excel spreadsheet extraction using Dagster `@multi_asset` functionality, thanks to
    [@dstansby](https://github.com/sponsors/dstansby). This is currently being used for EIA-860, 861 and 923 data. See
    [#2385](https://github.com/catalyst-cooperative/pudl/issues/2385) and PRs [#2644](https://github.com/catalyst-cooperative/pudl/pull/2644), [#2943](https://github.com/catalyst-cooperative/pudl/pull/2943).
* EIA ETL changes:
  * The EIA table level cleaning functions are now
    dagster assets. The table level cleaning assets now have a “clean_” prefix
    and a “_{datasource}” suffix to distinguish them from the final harvested tables.
  * `pudl.transform.eia.transform()` is now a `@multi_asset` that depends
    on all of the EIA table level cleaning functions / assets.
* EPA CEMS ETL changes:
  * `pudl.transform.epacems.transform()` now loads the `epacamd_eia` and
    `plants_entity_eia` tables as dataframes using the
    `pudl.io_manager.pudl_sqlite_io_manager` instead of reading the tables
    using a `pudl_engine`.
  * Adds a Ohio plant that is in 2021 CEMS but missing from EIA since 2018 to
    the `additional_epacems_plants.csv` sheet.
* FERC ETL changes:
  * `pudl.extract.ferc1.dbf2sqlite()` and `pudl.extract.xbrl.xbrl2sqlite()`
    are now configurable dagster ops. These ops make up the
    `ferc_to_sqlite` dagster graph in `pudl.ferc_to_sqlite.defs`.
  * FERC 714 extraction methods are now subsettable by year, with 2019 and 2020 data
    included in the `etl_fast.yml` by default. See [#2628](https://github.com/catalyst-cooperative/pudl/issues/2628) and PR [#2649](https://github.com/catalyst-cooperative/pudl/pull/2649).
* Census DP1 ETL changes:
  * `pudl.convert.censusdp1tract_to_sqlite` and [`pudl.output.censusdp1tract`](autoapi/pudl/output/censusdp1tract/index.md#module-pudl.output.censusdp1tract)
    are now integrated into dagster. See [#1973](https://github.com/catalyst-cooperative/pudl/issues/1973) and [#2621](https://github.com/catalyst-cooperative/pudl/pull/2621).

### New Asset Naming Convention

There are hundreds of new tables in `pudl.sqlite` now that the methods in `PudlTabl`
have been converted to Dagster assets. This significant increase in tables and diversity
of table types prompted us to create a new naming convention to make the table names
more descriptive and organized. You can read about the new naming convention in the
[docs](dev/naming_conventions.md#asset-naming).

To help users migrate away from using `PudlTabl` and our temporary table names,
we’ve created a [google sheet](https://docs.google.com/spreadsheets/d/1RBuKl_xKzRSLgRM7GIZbc5zUYieWFE20cXumWuv5njo/edit?usp=sharing)
that maps the old table names and `PudlTabl` methods to the new table names.

We’ve added deprecation warnings to the `PudlTabl` class. We plan to remove
`PudlTabl` from the `pudl` package once our known users have
successfully migrated to pulling data directly from `pudl.sqlite`.

### Data Coverage

* Updated [EIA Form 860 – Annual Electric Generator Report](data_sources/eia860.md) to include final release data from 2022, see
  [#3008](https://github.com/catalyst-cooperative/pudl/issues/3008) & PR [#3040](https://github.com/catalyst-cooperative/pudl/pull/3040).
* Updated [EIA Form 861 – Annual Electric Power Industry Report](data_sources/eia861.md) to include final release data from 2022, see
  [#3034](https://github.com/catalyst-cooperative/pudl/issues/3034) & PR [#3048](https://github.com/catalyst-cooperative/pudl/pull/3048).
* Updated [EIA Form 923 – Power Plant Operations Report](data_sources/eia923.md) to include final release data from 2022 and
  monthly YTD data as of October 2023, see [#3009](https://github.com/catalyst-cooperative/pudl/issues/3009) & PR [#3073](https://github.com/catalyst-cooperative/pudl/pull/#3073).
* Extracted the raw `raw_eia923__emissions_control` table, see PR [#3100](https://github.com/catalyst-cooperative/pudl/pull/3100).
* Updated [EPA Hourly Continuous Emission Monitoring System (CEMS)](data_sources/epacems.md) to switch from the old FTP server to the new
  CAMPD API, and to include 2022 data. Due to changes in the ETL, Alaska, Puerto Rico
  and Hawaii are now included in CEMS processing. See issue [#1264](https://github.com/catalyst-cooperative/pudl/issues/1264) & PRs
  [#2779](https://github.com/catalyst-cooperative/pudl/pull/2779), :pr:\` 2816\`.
* New [core_epa_\_assn_eia_epacamd](data_dictionaries/pudl_db.md#core-epa-assn-eia-epacamd) crosswalk version v0.3, see issue [#2317](https://github.com/catalyst-cooperative/pudl/issues/2317)
  and PR [#2316](https://github.com/catalyst-cooperative/pudl/pull/2316). EPA’s updates add manual matches and exclusions focusing on
  operating units with a generator ID as of 2018.
* New PUDL tables from [FERC Form 1 – Annual Report of Major Electric Utilities](data_sources/ferc1.md), integrating older DBF and newer XBRL
  data. See [#1574](https://github.com/catalyst-cooperative/pudl/issues/1574) for an overview of our progress integrating FERC’s XBRL data.
  To see which DBF and XBRL tables the following PUDL tables are derived from, refer to
  `pudl.extract.ferc1.TABLE_NAME_MAP`
  * [core_ferc1_\_yearly_energy_sources_sched401](data_dictionaries/pudl_db.md#core-ferc1-yearly-energy-sources-sched401), see issue [#1819](https://github.com/catalyst-cooperative/pudl/issues/1819) & PR
    [#2094](https://github.com/catalyst-cooperative/pudl/pull/2094).
  * [core_ferc1_\_yearly_energy_dispositions_sched401](data_dictionaries/pudl_db.md#core-ferc1-yearly-energy-dispositions-sched401), see issue [#1819](https://github.com/catalyst-cooperative/pudl/issues/1819) &
    PR [#2100](https://github.com/catalyst-cooperative/pudl/pull/2100).
  * [core_ferc1_\_yearly_transmission_lines_sched422](data_dictionaries/pudl_db.md#core-ferc1-yearly-transmission-lines-sched422), see issue [#1822](https://github.com/catalyst-cooperative/pudl/issues/1822) & PR
    [#2103](https://github.com/catalyst-cooperative/pudl/pull/2103)
  * [core_ferc1_\_yearly_utility_plant_summary_sched200](data_dictionaries/pudl_db.md#core-ferc1-yearly-utility-plant-summary-sched200), see issue
    [#1806](https://github.com/catalyst-cooperative/pudl/issues/1806) & PR [#2105](https://github.com/catalyst-cooperative/pudl/pull/2105).
  * [core_ferc1_\_yearly_balance_sheet_assets_sched110](data_dictionaries/pudl_db.md#core-ferc1-yearly-balance-sheet-assets-sched110), see issue [#1805](https://github.com/catalyst-cooperative/pudl/issues/1805) &
    PRs [#2112](https://github.com/catalyst-cooperative/pudl/pull/2112), [#2127](https://github.com/catalyst-cooperative/pudl/pull/2127).
  * [core_ferc1_\_yearly_balance_sheet_liabilities_sched110](data_dictionaries/pudl_db.md#core-ferc1-yearly-balance-sheet-liabilities-sched110), see issue
    [#1810](https://github.com/catalyst-cooperative/pudl/issues/1810) & PR [#2134](https://github.com/catalyst-cooperative/pudl/pull/2134).
  * [core_ferc1_\_yearly_depreciation_summary_sched336](data_dictionaries/pudl_db.md#core-ferc1-yearly-depreciation-summary-sched336), see issue [#1816](https://github.com/catalyst-cooperative/pudl/issues/1816)
    & PR [#2143](https://github.com/catalyst-cooperative/pudl/pull/2143).
  * [core_ferc1_\_yearly_income_statements_sched114](data_dictionaries/pudl_db.md#core-ferc1-yearly-income-statements-sched114), see issue [#1813](https://github.com/catalyst-cooperative/pudl/issues/1813) & PR
    [#2147](https://github.com/catalyst-cooperative/pudl/pull/2147).
  * [core_ferc1_\_yearly_depreciation_changes_sched219](data_dictionaries/pudl_db.md#core-ferc1-yearly-depreciation-changes-sched219) see issue
    [#1808](https://github.com/catalyst-cooperative/pudl/issues/1808) & [#2119](https://github.com/catalyst-cooperative/pudl/pull/2119).
  * [core_ferc1_\_yearly_depreciation_by_function_sched219](data_dictionaries/pudl_db.md#core-ferc1-yearly-depreciation-by-function-sched219) see issue
    [#1808](https://github.com/catalyst-cooperative/pudl/issues/1808) & PR [#2183](https://github.com/catalyst-cooperative/pudl/pull/2183).
  * [core_ferc1_\_yearly_operating_expenses_sched320](data_dictionaries/pudl_db.md#core-ferc1-yearly-operating-expenses-sched320), see issue [#1817](https://github.com/catalyst-cooperative/pudl/issues/1817) & PR
    [#2162](https://github.com/catalyst-cooperative/pudl/pull/2162).
  * [core_ferc1_\_yearly_retained_earnings_sched118](data_dictionaries/pudl_db.md#core-ferc1-yearly-retained-earnings-sched118), see issue [#1811](https://github.com/catalyst-cooperative/pudl/issues/1811) & PR
    [#2155](https://github.com/catalyst-cooperative/pudl/pull/2155).
  * [core_ferc1_\_yearly_cash_flows_sched120](data_dictionaries/pudl_db.md#core-ferc1-yearly-cash-flows-sched120), see issue [#1821](https://github.com/catalyst-cooperative/pudl/issues/1821) & PR
    [#2184](https://github.com/catalyst-cooperative/pudl/pull/2184).
  * [core_ferc1_\_yearly_sales_by_rate_schedules_sched304](data_dictionaries/pudl_db.md#core-ferc1-yearly-sales-by-rate-schedules-sched304), see issue
    [#1823](https://github.com/catalyst-cooperative/pudl/issues/1823) & PR [#2205](https://github.com/catalyst-cooperative/pudl/pull/2205).
* Harvested owner utilities from the EIA-860 ownership table which are now included in
  the [core_eia_\_entity_utilities](data_dictionaries/pudl_db.md#core-eia-entity-utilities) and [core_pudl_\_assn_eia_pudl_utilities](data_dictionaries/pudl_db.md#core-pudl-assn-eia-pudl-utilities)
  tables. See [#2714](https://github.com/catalyst-cooperative/pudl/pull/2714). Renamed columns with owner or operator suffix to differentiate
  between owner and operator utility columns in [core_eia860_\_scd_ownership](data_dictionaries/pudl_db.md#core-eia860-scd-ownership) and
  [out_eia860_\_yearly_ownership](data_dictionaries/pudl_db.md#out-eia860-yearly-ownership). See [#2903](https://github.com/catalyst-cooperative/pudl/pull/2903).
* New PUDL tables from [EIA Form 860 – Annual Electric Generator Report](data_sources/eia860.md):
  * [core_eia860_\_scd_emissions_control_equipment](data_dictionaries/pudl_db.md#core-eia860-scd-emissions-control-equipment), see issue [#2338](https://github.com/catalyst-cooperative/pudl/issues/2338) & PR
    [#2561](https://github.com/catalyst-cooperative/pudl/pull/2561).
  * [out_eia860_\_yearly_emissions_control_equipment](data_dictionaries/pudl_db.md#out-eia860-yearly-emissions-control-equipment), see issue [#2338](https://github.com/catalyst-cooperative/pudl/issues/2338) & PR
    [#2561](https://github.com/catalyst-cooperative/pudl/pull/2561).
  * [core_eia860_\_assn_yearly_boiler_emissions_control_equipment](data_dictionaries/pudl_db.md#core-eia860-assn-yearly-boiler-emissions-control-equipment), see
    [#2338](https://github.com/catalyst-cooperative/pudl/issues/2338) & PR [#2561](https://github.com/catalyst-cooperative/pudl/pull/2561).
  * [core_eia860_\_assn_boiler_cooling](data_dictionaries/pudl_db.md#core-eia860-assn-boiler-cooling), see [#2586](https://github.com/catalyst-cooperative/pudl/issues/2586) & PR [#2587](https://github.com/catalyst-cooperative/pudl/pull/2587)
  * [core_eia860_\_assn_boiler_stack_flue](data_dictionaries/pudl_db.md#core-eia860-assn-boiler-stack-flue), see [#2586](https://github.com/catalyst-cooperative/pudl/issues/2586) & PR [#2587](https://github.com/catalyst-cooperative/pudl/pull/2587)
* The [core_eia860_\_scd_boilers](data_dictionaries/pudl_db.md#core-eia860-scd-boilers) table now includes annual boiler attributes from
  [EIA Form 860 – Annual Electric Generator Report](data_sources/eia860.md) Schedule 6.2 Environmental Equipment data, and the new
  [core_eia_\_entity_boilers](data_dictionaries/pudl_db.md#core-eia-entity-boilers) table now includes static boiler attributes. See issue
  [#1162](https://github.com/catalyst-cooperative/pudl/issues/1162) & PR [#2319](https://github.com/catalyst-cooperative/pudl/pull/2319).
* All [EIA Form 861 – Annual Electric Power Industry Report](data_sources/eia861.md) tables are now being loaded into the PUDL DB, rather
  than only being available via an ad-hoc ETL process that was only accessible through
  the `pudl.output.pudltabl.PudlTabl` class. Note that most of these tables have
  not been normalized, and the `utility_id_eia` and `balancing_authority_id_eia`
  values in them haven’t been harvested, so these tables have very few valid foreign key
  relationships with the rest of the database right now – but at least the data is
  available in the database! Existing methods for accessing these tables have been
  preserved. The `PudlTabl` methods just read directly from the DB and apply uniform
  data types, rather than actually doing the ETL. See [#2265](https://github.com/catalyst-cooperative/pudl/issues/2265) & [#2403](https://github.com/catalyst-cooperative/pudl/pull/2403). The
  newly accessible tables contain data from 2001-2021 and include:
  * [core_eia861_\_yearly_advanced_metering_infrastructure](data_dictionaries/pudl_db.md#core-eia861-yearly-advanced-metering-infrastructure)
  * [core_eia861_\_yearly_balancing_authority](data_dictionaries/pudl_db.md#core-eia861-yearly-balancing-authority)
  * [core_eia861_\_assn_balancing_authority](data_dictionaries/pudl_db.md#core-eia861-assn-balancing-authority)
  * [core_eia861_\_yearly_demand_response](data_dictionaries/pudl_db.md#core-eia861-yearly-demand-response)
  * [core_eia861_\_yearly_demand_response_water_heater](data_dictionaries/pudl_db.md#core-eia861-yearly-demand-response-water-heater)
  * [core_eia861_\_yearly_demand_side_management_sales](data_dictionaries/pudl_db.md#core-eia861-yearly-demand-side-management-sales)
  * [core_eia861_\_yearly_demand_side_management_ee_dr](data_dictionaries/pudl_db.md#core-eia861-yearly-demand-side-management-ee-dr)
  * [core_eia861_\_yearly_demand_side_management_misc](data_dictionaries/pudl_db.md#core-eia861-yearly-demand-side-management-misc)
  * [core_eia861_\_yearly_distributed_generation_tech](data_dictionaries/pudl_db.md#core-eia861-yearly-distributed-generation-tech)
  * [core_eia861_\_yearly_distributed_generation_fuel](data_dictionaries/pudl_db.md#core-eia861-yearly-distributed-generation-fuel)
  * [core_eia861_\_yearly_distributed_generation_misc](data_dictionaries/pudl_db.md#core-eia861-yearly-distributed-generation-misc)
  * [core_eia861_\_yearly_distribution_systems](data_dictionaries/pudl_db.md#core-eia861-yearly-distribution-systems)
  * [core_eia861_\_yearly_dynamic_pricing](data_dictionaries/pudl_db.md#core-eia861-yearly-dynamic-pricing)
  * [core_eia861_\_yearly_energy_efficiency](data_dictionaries/pudl_db.md#core-eia861-yearly-energy-efficiency)
  * [core_eia861_\_yearly_green_pricing](data_dictionaries/pudl_db.md#core-eia861-yearly-green-pricing)
  * [core_eia861_\_yearly_mergers](data_dictionaries/pudl_db.md#core-eia861-yearly-mergers)
  * [core_eia861_\_yearly_net_metering_customer_fuel_class](data_dictionaries/pudl_db.md#core-eia861-yearly-net-metering-customer-fuel-class)
  * [core_eia861_\_yearly_net_metering_misc](data_dictionaries/pudl_db.md#core-eia861-yearly-net-metering-misc)
  * [core_eia861_\_yearly_non_net_metering_customer_fuel_class](data_dictionaries/pudl_db.md#core-eia861-yearly-non-net-metering-customer-fuel-class)
  * [core_eia861_\_yearly_non_net_metering_misc](data_dictionaries/pudl_db.md#core-eia861-yearly-non-net-metering-misc)
  * [core_eia861_\_yearly_operational_data_revenue](data_dictionaries/pudl_db.md#core-eia861-yearly-operational-data-revenue)
  * [core_eia861_\_yearly_operational_data_misc](data_dictionaries/pudl_db.md#core-eia861-yearly-operational-data-misc)
  * [core_eia861_\_yearly_reliability](data_dictionaries/pudl_db.md#core-eia861-yearly-reliability)
  * [core_eia861_\_yearly_sales](data_dictionaries/pudl_db.md#core-eia861-yearly-sales)
  * [core_eia861_\_yearly_service_territory](data_dictionaries/pudl_db.md#core-eia861-yearly-service-territory)
  * [core_eia861_\_assn_utility](data_dictionaries/pudl_db.md#core-eia861-assn-utility)
  * [core_eia861_\_yearly_utility_data_nerc](data_dictionaries/pudl_db.md#core-eia861-yearly-utility-data-nerc)
  * [core_eia861_\_yearly_utility_data_rto](data_dictionaries/pudl_db.md#core-eia861-yearly-utility-data-rto)
  * [core_eia861_\_yearly_utility_data_misc](data_dictionaries/pudl_db.md#core-eia861-yearly-utility-data-misc)
* A couple of tables from [FERC Form 714 – Annual Electric Balancing Authority Area and Planning Area Report](data_sources/ferc714.md) have been added to the PUDL DB.
  These tables contain data from 2006-2020 (2021 is distributed by FERC in XBRL format
  and we have not yet integrated it). See [#2266](https://github.com/catalyst-cooperative/pudl/issues/2266), [#2421](https://github.com/catalyst-cooperative/pudl/pull/2421) and [#2550](https://github.com/catalyst-cooperative/pudl/pull/2550).
  The newly accessible tables include:
  * [core_ferc714_\_respondent_id](data_dictionaries/pudl_db.md#core-ferc714-respondent-id) (linking FERC-714 respondents to EIA utilities)
  * [out_ferc714_\_hourly_planning_area_demand](data_dictionaries/pudl_db.md#out-ferc714-hourly-planning-area-demand) (hourly electricity demand by
    planning area)
  * [out_ferc714_\_respondents_with_fips](data_dictionaries/pudl_db.md#out-ferc714-respondents-with-fips) (annual respondents with county FIPS IDs)
  * [out_ferc714_\_summarized_demand](data_dictionaries/pudl_db.md#out-ferc714-summarized-demand) (annual demand for FERC-714 respondents)
* Added new table [core_epa_\_assn_eia_epacamd_subplant_ids](data_dictionaries/pudl_db.md#core-epa-assn-eia-epacamd-subplant-ids), which arguments the
  [core_epa_\_assn_eia_epacamd](data_dictionaries/pudl_db.md#core-epa-assn-eia-epacamd) glue table. This table incorporates all
  [core_eia_\_entity_generators](data_dictionaries/pudl_db.md#core-eia-entity-generators) and all [core_epacems_\_hourly_emissions](data_dictionaries/pudl_db.md#core-epacems-hourly-emissions) ID’s
  and uses these complete IDs to develop a full-coverage `subplant_id` column which
  granularly connects EPA CAMD with EIA. Thanks to [@grgmiller](https://github.com/sponsors/grgmiller) for his
  contribution to this process. See [#2456](https://github.com/catalyst-cooperative/pudl/issues/2456) & [#2491](https://github.com/catalyst-cooperative/pudl/pull/2491).
* Added new table [out_pudl_\_yearly_assn_eia_ferc1_plant_parts](data_dictionaries/pudl_db.md#out-pudl-yearly-assn-eia-ferc1-plant-parts) which links FERC1
  records from [out_ferc1_\_yearly_all_plants](data_dictionaries/pudl_db.md#out-ferc1-yearly-all-plants) and
  [out_eia_\_yearly_plant_parts](data_dictionaries/pudl_db.md#out-eia-yearly-plant-parts).
* Thanks to contributions from [@rousik](https://github.com/sponsors/rousik) we’ve generalized the code we use to
  convert FERC’s old annual Visual FoxPro databases into multi-year SQLite databases.
  * We have started extracting the FERC Form 2 (natural gas utility financial reports).
    See issues [#1984](https://github.com/catalyst-cooperative/pudl/issues/1984), [#2642](https://github.com/catalyst-cooperative/pudl/issues/2642) and PRs [#2536](https://github.com/catalyst-cooperative/pudl/pull/2536), [#2564](https://github.com/catalyst-cooperative/pudl/pull/2564), [#2652](https://github.com/catalyst-cooperative/pudl/pull/2652). We haven’t yet done any
    integration of the Form 2 into the cleaned and normalized PUDL DB, but the converted
    [FERC Form 2 is available on Datasette](https://data.catalyst.coop/ferc2)
    covering 1996-2020. Earlier years (1991-1995) were distributed using a different
    binary format and we don’t currently have plans to extract them. From 2021 onward we
    are extracting the [FERC 2 from XBRL](https://data.catalyst.coop/ferc2_xbrl).
  * Similarly [#2595](https://github.com/catalyst-cooperative/pudl/pull/2595) converts the earlier years of FERC Form 6 (2000-2020) from DBF
    to SQLite, describing the finances of oil pipeline companies. When the nightly
    builds succeed, [FERC Form 6 will be available on Datasette](https://data.catalyst.coop/ferc6)
    as well.
  * [#2734](https://github.com/catalyst-cooperative/pudl/pull/2734) converts the earlier years of FERC Form 60 (2006-2020) from DBF to
    SQLite. Form 60 is a comprehensive financial and operating report submitted for
    centralized service companies. [FERC Form 60 will also be available on Datasette](https://data.catalyst.coop/ferc6).

### Data Cleaning

* Removed inconsistently reported leading zeroes from numeric `boiler_id` values. This
  affected a small number of records in any table referring to boilers, including
  [core_eia_\_entity_boilers](data_dictionaries/pudl_db.md#core-eia-entity-boilers), [core_eia860_\_scd_boilers](data_dictionaries/pudl_db.md#core-eia860-scd-boilers),
  [core_eia923_\_monthly_boiler_fuel](data_dictionaries/pudl_db.md#core-eia923-monthly-boiler-fuel), [core_eia860_\_assn_boiler_generator](data_dictionaries/pudl_db.md#core-eia860-assn-boiler-generator)
  and the [core_epa_\_assn_eia_epacamd](data_dictionaries/pudl_db.md#core-epa-assn-eia-epacamd) crosswalk. It also had some minor downstream
  effects on the MCOE outputs. See [#2366](https://github.com/catalyst-cooperative/pudl/issues/2366) and [#2367](https://github.com/catalyst-cooperative/pudl/pull/2367).
* The [core_eia923_\_monthly_boiler_fuel](data_dictionaries/pudl_db.md#core-eia923-monthly-boiler-fuel) table now includes the
  `prime_mover_code` column. This column was previously incorrectly being associated
  with boilers in the [core_eia_\_entity_boilers](data_dictionaries/pudl_db.md#core-eia-entity-boilers) table. See issue [#2349](https://github.com/catalyst-cooperative/pudl/issues/2349) &
  PR [#2362](https://github.com/catalyst-cooperative/pudl/pull/2362).
* Fixed column naming issues in the
  [core_ferc1_\_yearly_operating_revenues_sched300](data_dictionaries/pudl_db.md#core-ferc1-yearly-operating-revenues-sched300) table.
* Made minor calculation fixes in the metadata for
  [core_ferc1_\_yearly_income_statements_sched114](data_dictionaries/pudl_db.md#core-ferc1-yearly-income-statements-sched114),
  [core_ferc1_\_yearly_utility_plant_summary_sched200](data_dictionaries/pudl_db.md#core-ferc1-yearly-utility-plant-summary-sched200),
  [core_ferc1_\_yearly_operating_revenues_sched300](data_dictionaries/pudl_db.md#core-ferc1-yearly-operating-revenues-sched300),
  [core_ferc1_\_yearly_balance_sheet_assets_sched110](data_dictionaries/pudl_db.md#core-ferc1-yearly-balance-sheet-assets-sched110),
  [core_ferc1_\_yearly_balance_sheet_liabilities_sched110](data_dictionaries/pudl_db.md#core-ferc1-yearly-balance-sheet-liabilities-sched110), and
  [core_ferc1_\_yearly_operating_expenses_sched320](data_dictionaries/pudl_db.md#core-ferc1-yearly-operating-expenses-sched320),
  [core_ferc1_\_yearly_depreciation_changes_sched219](data_dictionaries/pudl_db.md#core-ferc1-yearly-depreciation-changes-sched219) and
  [core_ferc1_\_yearly_depreciation_by_function_sched219](data_dictionaries/pudl_db.md#core-ferc1-yearly-depreciation-by-function-sched219). See [#2016](https://github.com/catalyst-cooperative/pudl/issues/2016),
  [#2563](https://github.com/catalyst-cooperative/pudl/pull/2563), [#2662](https://github.com/catalyst-cooperative/pudl/pull/2662) and [#2687](https://github.com/catalyst-cooperative/pudl/pull/2687).
* Changed the [core_ferc1_\_yearly_retained_earnings_sched118](data_dictionaries/pudl_db.md#core-ferc1-yearly-retained-earnings-sched118) table transform to
  restore factoids for previous year balances, and added calculation metadata. See
  [#1811](https://github.com/catalyst-cooperative/pudl/issues/1811), [#2016](https://github.com/catalyst-cooperative/pudl/issues/2016), and [#2645](https://github.com/catalyst-cooperative/pudl/pull/2645).
* Added “correction” records to many FERC Form 1 tables where the reported totals do not
  match the outcomes of calculations specified in XBRL metadata (even after cleaning up
  the often incorrect calculation specifications!). See [#2957](https://github.com/catalyst-cooperative/pudl/issues/2957) and [#2620](https://github.com/catalyst-cooperative/pudl/pull/2620).
* Flip the sign of some erroneous negative values in the
  [core_ferc1_\_yearly_plant_in_service_sched204](data_dictionaries/pudl_db.md#core-ferc1-yearly-plant-in-service-sched204) and
  [core_ferc1_\_yearly_utility_plant_summary_sched200](data_dictionaries/pudl_db.md#core-ferc1-yearly-utility-plant-summary-sched200) tables. See
  [#2599](https://github.com/catalyst-cooperative/pudl/issues/2599), and [#2647](https://github.com/catalyst-cooperative/pudl/pull/2647).

### Analysis

* Added a method for attributing fuel consumption reported on the basis of boiler ID and
  fuel to individual generators, analogous to the existing method for attributing net
  generation reported on the basis of prime mover & fuel. This should allow much more
  complete estimates of generator heat rates and thus fuel costs and emissions. Thanks
  to [@grgmiller](https://github.com/sponsors/grgmiller) for his contribution, which was integrated by [@cmgosnell](https://github.com/sponsors/cmgosnell)!
  See PRs [#1096](https://github.com/catalyst-cooperative/pudl/pull/1096), [#1608](https://github.com/catalyst-cooperative/pudl/pull/1608) and issues [#1468](https://github.com/catalyst-cooperative/pudl/issues/1468), [#1478](https://github.com/catalyst-cooperative/pudl/issues/1478).
* Integrated `pudl.analysis.eia_ferc1_record_linkage` from our RMI collaboration
  repo, which uses logistic regression to match FERC1 plants data to EIA-860 records.
  While far from perfect, this baseline model utilizes the manually created training
  data and plant IDs to perform record linkage on the FERC1 data and EIA plant parts
  list created in [`pudl.analysis.plant_parts_eia`](autoapi/pudl/analysis/plant_parts_eia/index.md#module-pudl.analysis.plant_parts_eia). See issue [#1064](https://github.com/catalyst-cooperative/pudl/issues/1064) & PR
  [#2224](https://github.com/catalyst-cooperative/pudl/pull/2224). To account for 1:m matches in the manual data, we added
  `plant_match_ferc1` as a plant part in [`pudl.analysis.plant_parts_eia`](autoapi/pudl/analysis/plant_parts_eia/index.md#module-pudl.analysis.plant_parts_eia).
* Refined how we are associating generation and fuel data in
  [`pudl.analysis.allocate_gen_fuel`](autoapi/pudl/analysis/allocate_gen_fuel/index.md#module-pudl.analysis.allocate_gen_fuel), which was renamed from `allocate_net_gen`.
  Energy source codes that show up in the [core_eia923_\_monthly_generation_fuel](data_dictionaries/pudl_db.md#core-eia923-monthly-generation-fuel) or
  the [core_eia923_\_monthly_boiler_fuel](data_dictionaries/pudl_db.md#core-eia923-monthly-boiler-fuel) are now added into the
  [core_eia860_\_scd_generators](data_dictionaries/pudl_db.md#core-eia860-scd-generators) table so associating those gf and bf records are
  more cleanly associated with generators. Thanks to [@grgmiller](https://github.com/sponsors/grgmiller) for his
  contribution, which was integrated by [@cmgosnell](https://github.com/sponsors/cmgosnell)! See PRs [#2235](https://github.com/catalyst-cooperative/pudl/pull/2235), [#2446](https://github.com/catalyst-cooperative/pudl/pull/2446).
* The [`pudl.analysis.mcoe`](autoapi/pudl/analysis/mcoe/index.md#module-pudl.analysis.mcoe) table now uses the allocated estimates for per-generator
  net generation and fuel consumption. See PR [#2553](https://github.com/catalyst-cooperative/pudl/pull/2553).
* Additionally, the [`pudl.analysis.mcoe`](autoapi/pudl/analysis/mcoe/index.md#module-pudl.analysis.mcoe) table now only includes attributes
  pertaining to the generator capacity, heat rate, and fuel cost. No additional
  generator attributes are included in this table. The full table with generator
  attributes merged on is now provided by `pudl.analysis.mcoe_generators`. See PR
  [#2553](https://github.com/catalyst-cooperative/pudl/pull/2553).
* Added outputs from [`pudl.analysis.service_territory`](autoapi/pudl/analysis/service_territory/index.md#module-pudl.analysis.service_territory) and
  [`pudl.analysis.state_demand`](autoapi/pudl/analysis/state_demand/index.md#module-pudl.analysis.state_demand) into PUDL. These outputs include the US Census
  geometries associated with balancing authority and utility data from EIA-861
  ([out_eia861_\_yearly_balancing_authority_service_territory](data_dictionaries/pudl_db.md#out-eia861-yearly-balancing-authority-service-territory) and
  [out_eia861_\_yearly_utility_service_territory](data_dictionaries/pudl_db.md#out-eia861-yearly-utility-service-territory)), and the estimated total hourly
  electricity demand for each US state in
  [out_ferc714_\_hourly_estimated_state_demand](data_dictionaries/pudl_db.md#out-ferc714-hourly-estimated-state-demand). See [#1973](https://github.com/catalyst-cooperative/pudl/issues/1973)
  and [#2550](https://github.com/catalyst-cooperative/pudl/pull/2550).

### Deprecations

* Replace references to deprecated `pudl-scrapers` and
  `pudl-zenodo-datastore` repositories with references to [pudl-archiver](https://www.github.com/catalyst-cooperative/pudl-archiver) repository in
  [Working with the Datastore](dev/datastore.md), and [Existing Data Updates](dev/existing_data_updates.md). See
  [#2190](https://github.com/catalyst-cooperative/pudl/pull/2190).
* `pudl.etl` is now a subpackage that collects all pudl assets into a dagster
  [Definition](https://docs.dagster.io/concepts/code-locations). All
  `pudl.etl._etl_{datasource}` functions have been deprecated. The coordination
  of ETL steps is being handled by dagster.
* The `pudl.load` module has been removed in favor of using the
  `pudl.io_managers.pudl_sqlite_io_manager`.
* The `pudl_etl`  and `ferc_to_sqlite` commands no longer support loading
  specific tables. The commands run all of the tables. Use dagster assets to
  run subsets of the tables.
* The `--clobber` argument has been removed from the `pudl_etl` command.
* `pudl.transform.eia860.transform()` and `pudl.transform.eia923.transform()`
  functions have been deprecated. The table level EIA cleaning functions are now
  coordinated using dagster.
* `pudl.transform.ferc1.transform()` has been removed. The ferc1 table
  : transformations are now being orchestrated with Dagster.
* `pudl.transform.ferc1.transform` can no longer be executed as a script.
  Use dagster-webserver to execute just the FERC Form 1 pipeline.
* `pudl.extract.ferc1.extract_dbf`, `pudl.extract.ferc1.extract_xbrl`
  `pudl.extract.ferc1.extract_xbrl_single`,
  `pudl.extract.ferc1.extract_dbf_single`,
  `pudl.extract.ferc1.extract_xbrl_generic`,
  `pudl.extract.ferc1.extract_dbf_generic` have all been deprecated. The extraction
  logic is now covered by the
  `pudl.io_managers.ferc1_xbrl_sqlite_io_manager` and
  `pudl.io_managers.ferc1_dbf_sqlite_io_manager` IO Managers.
* `pudl.ferc1.extract_xbrl_metadata` has been replaced by the
  `pudl.extract.ferc1.xbrl_metadata_json()` asset.
* All sub classes of `pudl.settings.GenericDatasetSettings()` in
  [`pudl.settings`](autoapi/pudl/settings/index.md#module-pudl.settings) no longer have table attributes because the ETL no longer
  supports loading specific tables via settings. Use dagster to select subsets of
  tables to process.

### Miscellaneous

* Updated PUDL to use Python 3.11. See [#2408](https://github.com/catalyst-cooperative/pudl/pull/2408) & [#2383](https://github.com/catalyst-cooperative/pudl/issues/2383)
* Apply start and end dates to ferc1 data in `pudl.output.pudltabl.PudlTabl`.
  See [#2238](https://github.com/catalyst-cooperative/pudl/pull/2238) & [#274](https://github.com/catalyst-cooperative/pudl/issues/274).
* Add generic spot fix method to transform process, to manually rescue FERC1 records.
  See [#2254](https://github.com/catalyst-cooperative/pudl/pull/2254) & [#1980](https://github.com/catalyst-cooperative/pudl/issues/1980).
* Reverted a fix made in [#1909](https://github.com/catalyst-cooperative/pudl/pull/1909), which mapped all plants located in NY state that
  reported a balancing authority code of “ISONE” to “NYISO”. These plants now retain
  their original EIA codes. Plants with manual re-mapping of BA codes have also been
  fixed to have correctly updated BA names. See [#2312](https://github.com/catalyst-cooperative/pudl/pull/2312) and [#2255](https://github.com/catalyst-cooperative/pudl/issues/2255).
* Fixed a column naming bug that was causing EIA860 monthly retirement dates to get
  nulled out. See [#2834](https://github.com/catalyst-cooperative/pudl/issues/2834) and [#2835](https://github.com/catalyst-cooperative/pudl/pull/2835)
* Switched to using `conda-lock` and `Makefile` to manage testing and python
  environment. Moved away from packaging PUDL for distribution via PyPI and
  `conda-forge` and toward treating it as an application.  See [#2968](https://github.com/catalyst-cooperative/pudl/pull/2968)
* The two-point-ohening: We now require Pandas v2 (see [#2320](https://github.com/catalyst-cooperative/pudl/pull/2320)), SQLAlchemy v2 (see
  [#2267](https://github.com/catalyst-cooperative/pudl/pull/2267)) and Pydantic v2 (see [#3051](https://github.com/catalyst-cooperative/pudl/pull/3051)).
* Update the names of our FERC SQLite DBs to indicate what source data they come from.
  See issue [#3079](https://github.com/catalyst-cooperative/pudl/issues/3079) and\` [#3094](https://github.com/catalyst-cooperative/pudl/pull/3094).

<a id="release-v2022-11-30"></a>

## v2022.11.30

### Data Coverage

* Added archives of the bulk EIA electricity API data to our datastore, since the API
  itself is too unreliable for production use. This is part of [#1763](https://github.com/catalyst-cooperative/pudl/issues/1763). The code
  for this new data is `eia_bulk_elec` and the data comes as a single 200MB zipped
  JSON file. [#1922](https://github.com/catalyst-cooperative/pudl/pull/1922) updates the datastore to include
  [this archive on Zenodo](https://zenodo.org/record/7067367) but most of the work
  happened in the
  [pudl-scrapers](https://github.com/catalyst-cooperative/pudl-scrapers) and
  [pudl-zenodo-storage](https://github.com/catalyst-cooperative/pudl-zenodo-storage)
  repositories. See issue [catalyst-cooperative/pudl-zenodo-storage#29](https://github.com/catalyst-cooperative/pudl-zenodo-storage/issues/29).
* Incorporated 2021 data from the [EPA Hourly Continuous Emission Monitoring System (CEMS)](data_sources/epacems.md) dataset. See [#1778](https://github.com/catalyst-cooperative/pudl/pull/1778)
* Incorporated Final Release 2021 data from the [EIA Form 860 – Annual Electric Generator Report](data_sources/eia860.md),
  [EIA Form 861 – Annual Electric Power Industry Report](data_sources/eia861.md), and [EIA Form 923 – Power Plant Operations Report](data_sources/eia923.md). We also integrated a
  `data_maturity` column and related `data_maturities` table into most of the EIA
  data tables in order to alter users to the level of finality of the data. See
  [#1834](https://github.com/catalyst-cooperative/pudl/pull/1834), [#1855](https://github.com/catalyst-cooperative/pudl/pull/1855), [#1915](https://github.com/catalyst-cooperative/pudl/pull/1915), [#1921](https://github.com/catalyst-cooperative/pudl/pull/1921).
* Incorporated 2022 data from the [EIA Form 860 – Annual Electric Generator Report](data_sources/eia860.md) monthly update from
  September 2022. See [#2079](https://github.com/catalyst-cooperative/pudl/pull/2079). A June 2022 eia860m update included adding new
  `energy_storage_capacity_mwh` (for batteries) and `net_capacity_mwdc` (for
  behind-the-meter solar PV) attributes to the `generators_eia860` table, as they
  appear in the [EIA Form 860 – Annual Electric Generator Report](data_sources/eia860.md) monthly updates for 2022.  See [#1834](https://github.com/catalyst-cooperative/pudl/pull/1834).
* Added new `datasources` table, which includes partitions used to generate the
  database. See [#2079](https://github.com/catalyst-cooperative/pudl/pull/2079).
* Integrated several new columns into the EIA-860 and EIA-923 including several
  codes with coding tables (See [PUDL Code Metadata](data_dictionaries/codes_and_labels.md)). [#1836](https://github.com/catalyst-cooperative/pudl/pull/1836)
* Added the [EPACAMD-EIA Crosswalk](https://github.com/USEPA/camd-eia-crosswalk) to
  the database. Previously, the crosswalk was a csv stored in `package_data/glue`,
  but now it has its own scraper
  [#https://github.com/catalyst-cooperative/pudl-scrapers/pull/20](https://github.com/catalyst-cooperative/pudl/pull/https://github.com/catalyst-cooperative/pudl-scrapers/pull/20), archiver,
  [#https://github.com/catalyst-cooperative/pudl-zenodo-storage/pull/20](https://github.com/catalyst-cooperative/pudl/pull/https://github.com/catalyst-cooperative/pudl-zenodo-storage/pull/20)
  and place in the PUDL db. For now there’s a `epacamd_eia` output table you can use
  to merge CEMS and EIA data yourself [#1692](https://github.com/catalyst-cooperative/pudl/pull/1692). Eventually we’ll work these crosswalk
  values into an output table combining CEMS and EIA.
* Integrated 2021 from the [FERC Form 1 – Annual Report of Major Electric Utilities](data_sources/ferc1.md) data. FERC updated its reporting
  format for 2021 from a DBF file to a XBRL files. This required a major overhaul of
  the extract and transform step. The updates were accumulated in [#1665](https://github.com/catalyst-cooperative/pudl/pull/1665). The raw
  XBRL data is being extracted through a
  [FERC XBRL Extractor](https://github.com/catalyst-cooperative/ferc-xbrl-extractor).
  This work is ongoing with additional tasks being tracked in [#1574](https://github.com/catalyst-cooperative/pudl/issues/1574). Specific
  updates in this release include:
  * Convert XBRL into raw sqlite database [#1831](https://github.com/catalyst-cooperative/pudl/pull/1831)
  * Build transformer infrastructure & Add `fuel_ferc1` table [#1721](https://github.com/catalyst-cooperative/pudl/pull/1721)
  * Map utility XBRL and DBF utility IDs [#1931](https://github.com/catalyst-cooperative/pudl/pull/1931)
  * Add `plants_steam_ferc1` table [#1881](https://github.com/catalyst-cooperative/pudl/pull/1881)
  * Add `plants_hydro_ferc1` [#1992](https://github.com/catalyst-cooperative/pudl/pull/1992)
  * Add `plants_pumped_storage_ferc1` [#2005](https://github.com/catalyst-cooperative/pudl/pull/2005)
  * Add `purchased_power_ferc1` [#2011](https://github.com/catalyst-cooperative/pudl/pull/2011)
  * Add `plants_small_ferc1` table [#2035](https://github.com/catalyst-cooperative/pudl/pull/2035)
  * Add `plant_in_service_ferc1` table [#2025](https://github.com/catalyst-cooperative/pudl/pull/2025) & [#2058](https://github.com/catalyst-cooperative/pudl/pull/2058)
* Added all of the SQLite databases which we build from FERC’s raw XBRL filings to our
  Datasette deployment. See [#2095](https://github.com/catalyst-cooperative/pudl/pull/2095) & [#2080](https://github.com/catalyst-cooperative/pudl/issues/2080). Browse the published data here:
  * [FERC Form 1](https://data.catalyst.coop/ferc1_xbrl)
  * [FERC Form 2](https://data.catalyst.coop/ferc2_xbrl)
  * [FERC Form 6](https://data.catalyst.coop/ferc6_xbrl)
  * [FERC Form 60](https://data.catalyst.coop/ferc60_xbrl)
  * [FERC Form 714](https://data.catalyst.coop/ferc714_xbrl)

### Data Analysis

* Instead of relying on the EIA API to fill in redacted fuel prices with aggregate
  values for individual states and plants, use the archived `eia_bulk_elec` data. This
  means we no longer have any reliance on the API, which should make the fuel price
  filling faster and more reliable. Coverage is still only about 90%. See [#1764](https://github.com/catalyst-cooperative/pudl/issues/1764)
  and [#1998](https://github.com/catalyst-cooperative/pudl/pull/1998). Additional filling with aggregate and/or imputed values is still on
  the workplan. You can follow the progress in [#1708](https://github.com/catalyst-cooperative/pudl/issues/1708).

### Nightly Data Builds

* We added infrastructure to run the entire ETL and all tests nightly
  so we can catch data errors when they are merged into `dev`. This allows us
  to automatically update the [PUDL Intake data catalogs](https://github.com/catalyst-cooperative/pudl-catalog)
  when there are new code releases. See [#1177](https://github.com/catalyst-cooperative/pudl/issues/1177) for more details.
* Created a [docker image](https://hub.docker.com/r/catalystcoop/pudl-etl)
  that installs PUDL and its dependencies. The `build-deploy-pudl.yaml` GitHub
  Action builds and pushes the image to Docker Hub and deploys the image on
  a Google Compute Engine instance. The ETL outputs are then loaded to Google
  Cloud buckets for the data catalogs to access.
* Added `GoogleCloudStorageCache` support to `ferc1_to_sqlite` and
  `censusdp1tract_to_sqlite` commands and pytest.
* Allow users to create monolithic and partitioned EPA CEMS outputs without having
  to clobber or move any existing CEMS outputs.
* `GoogleCloudStorageCache` now supports accessing requester pays buckets.
* Added a `--loglevel` arg to the package entrypoint commands.

### Database Schema Changes

* After learning that generators’ prime movers do very occasionally change over
  time, we recategorized the `prime_mover_code` column in our entity resolution
  process to enable the rare but real variability over time. We moved the
  `prime_mover_code` column from the statically harvested/normalized data
  column to an annually harvested data column (i.e. from `generators_entity_eia`
  to `generators_eia860`) [#1600](https://github.com/catalyst-cooperative/pudl/pull/1600). See [#1585](https://github.com/catalyst-cooperative/pudl/issues/1585) for more details.
* Created `operational_status_eia` into our static metadata tables (See
  [PUDL Code Metadata](data_dictionaries/codes_and_labels.md)). Used these standard codes and code
  fixes to clean `operational_status_code` in the `generators_entity_eia`
  table. [#1624](https://github.com/catalyst-cooperative/pudl/pull/1624)
* Moved a number of slowly changing plant attributes from the `plants_entity_eia`
  table to the annual `plants_eia860` table. See [#1748](https://github.com/catalyst-cooperative/pudl/issues/1748) and [#1749](https://github.com/catalyst-cooperative/pudl/pull/1749).
  This was initially inspired by the desire to more accurately reproduce the aggregated
  fuel prices which are available in the EIA’s API. Along with state, census region,
  month, year, and fuel type, those prices are broken down by industrial sector.
  Previously `sector_id_eia` (an aggregation of several `primary_purpose_naics_id`
  values) had been assumed to be static over a plant’s lifetime, when in fact it can
  change if e.g. a plant is sold to an IPP by a regulated utility. Other plant
  attributes which are now allowed to vary annually include:
  * `balancing_authority_code_eia`
  * `balancing_authority_name_eia`
  * `ferc_cogen_status`
  * `ferc_exempt_wholesale_generator`
  * `ferc_small_power_producer`
  * `grid_voltage_1_kv`
  * `grid_voltage_2_kv`
  * `grid_voltage_3_kv`
  * `iso_rto_code`
  * `primary_purpose_id_naics`
* Renamed `grid_voltage_kv` to `grid_voltage_1_kv` in the `plants_eia860`
  table, to follow the pattern of many other multiply reported values.
* Added a `balancing_authorities_eia` coding table mapping BA codes found in the
  [EIA Form 860 – Annual Electric Generator Report](data_sources/eia860.md) and [EIA Form 923 – Power Plant Operations Report](data_sources/eia923.md) to their names, cleaning up
  non-standard codes, and fixing some reporting errors for `PACW` vs. `PACE`
  (PacifiCorp West vs. East) based on the state associated with the plant reporting the
  code. Also added backfilling for codes in years before 2013 when BA Codes first
  started being reported, but only in the output tables. See: [#1906](https://github.com/catalyst-cooperative/pudl/pull/1906), [#1911](https://github.com/catalyst-cooperative/pudl/pull/1911)
* Renamed and removed some columns in the [EPA Hourly Continuous Emission Monitoring System (CEMS)](data_sources/epacems.md) dataset.
  `unitid` was changed to `emissions_unit_id_epa` to clarify the type of unit it
  represents. `unit_id_epa` was removed because it is a unique identifier for
  `emissions_unit_id_epa` and not otherwise useful or transferable to other datasets.
  `facility_id` was removed because it is specific to EPA’s internal database and does
  not aid in connection with other data. [#1692](https://github.com/catalyst-cooperative/pudl/pull/1692)
* Added a new table `political_subdivisions` which consolidated various bits of
  information about states, territories, provinces etc. that had previously been
  scattered across constants stored in the codebase. The `ownership_eia860` table
  had a mix of state and country information stored in the same column, and to retain
  all of it we added a new `owner_country_code` column. [#1966](https://github.com/catalyst-cooperative/pudl/pull/1966)

### Data Accuracy

* Retain NA values for [EPA Hourly Continuous Emission Monitoring System (CEMS)](data_sources/epacems.md) fields `gross_load_mw` and
  `heat_content_mmbtu`. Previously, these fields converted NA to 0, but this is not
  accurate, so we removed this step.
* Update the `plant_id_eia` field from [EPA Hourly Continuous Emission Monitoring System (CEMS)](data_sources/epacems.md) with values from
  the newly integrated `epacamd_eia` crosswalk as not all EPA’s ORISPL codes are
  correct.

### Helper Function Updates

* Replaced the PUDL helper function `clean_merge_asof` that merged two dataframes
  reported on different temporal granularities, for example monthly vs yearly data.
  The reworked function, [`pudl.helpers.date_merge`](autoapi/pudl/helpers/index.md#pudl.helpers.date_merge), is more encapsulating and
  faster and replaces `clean_merge_asof` in the MCOE table and EIA-923 tables. See
  [#1103](https://github.com/catalyst-cooperative/pudl/pull/1103), [#1550](https://github.com/catalyst-cooperative/pudl/pull/1550)
* The helper function [`pudl.helpers.expand_timeseries`](autoapi/pudl/helpers/index.md#pudl.helpers.expand_timeseries) was also added, which
  expands a dataframe to include a full timeseries of data at a certain frequency.
  The coordinating function [`pudl.helpers.full_timeseries_date_merge`](autoapi/pudl/helpers/index.md#pudl.helpers.full_timeseries_date_merge) first calls
  [`pudl.helpers.date_merge`](autoapi/pudl/helpers/index.md#pudl.helpers.date_merge) to merge two dataframes of different temporal
  granularities, and then calls [`pudl.helpers.expand_timeseries`](autoapi/pudl/helpers/index.md#pudl.helpers.expand_timeseries) to expand the
  merged dataframe to a full timeseries. The added `timeseries_filling` argument,
  makes this function optionally used to generate the MCOE table that includes a full
  monthly timeseries even in years when annually reported generators don’t have
  matching monthly data. See [#1550](https://github.com/catalyst-cooperative/pudl/pull/1550)
* Updated the `fix_leading_zero_gen_ids` function by changing the name to
  `remove_leading_zeros_from_numeric_strings` because it’s used to fix more than just
  the `generator_id` column. Included a new argument to specify which column you’d
  like to fix.

### Plant Parts List Module Changes

* We refactored a couple components of the Plant Parts List module in preparation
  for the next round of entity matching of EIA and FERC Form 1 records with the
  Panda model developed by the
  [Chu Data Lab at Georgia Tech](https://chu-data-lab.cc.gatech.edu/), through work
  funded by a
  [CCAI Innovation Grant](https://www.climatechange.ai/calls/innovation_grants).
  The labeling of different aggregations of EIA generators as the true granularity was
  sped up, resulting in faster generation of the final plant parts list. In addition,
  the generation of the `installation_year` column in the plant parts list was fixed
  and a `construction_year` column was also added. Finally, `operating_year` was
  added as a level that the EIA generators are now aggregated to.
* The mega generators table and in turn the plant parts list requires the MCOE table
  to generate. The MCOE table is now created with the new [`pudl.helpers.date_merge`](autoapi/pudl/helpers/index.md#pudl.helpers.date_merge)
  helper function (described above). As a result, now by default only columns from the
  EIA-860 generators table that are necessary for the creation of the plant parts list
  will be included in the MCOE table. This list of columns is defined by the global
  [`pudl.analysis.mcoe.DEFAULT_GENS_COLS`](autoapi/pudl/analysis/mcoe/index.md#pudl.analysis.mcoe.DEFAULT_GENS_COLS). If additional columns that are not part
  of the default list are needed from the EIA-860 generators table, these columns can be
  passed in with the `gens_cols` argument.  See [#1550](https://github.com/catalyst-cooperative/pudl/pull/1550)
* For memory efficiency, appropriate columns are now cast to string and
  categorical types when the full plant parts list is created. The resource and field
  metadata is now included in the PUDL metadata. See [#1865](https://github.com/catalyst-cooperative/pudl/pull/1865)
* For clarity and specificity, the `plant_name_new` column was renamed
  `plant_name_ppe` and the `ownership` column was renamed `ownership_record_type`.
  See [#1865](https://github.com/catalyst-cooperative/pudl/pull/1865)
* The `PLANT_PARTS_ORDERED` list was removed and `PLANT_PARTS` is now an
  `OrderedDict` that establishes the plant parts hierarchy in its keys. All references
  to `PLANT_PARTS_ORDERED` were replaced with the `PLANT_PARTS` keys. See [#1865](https://github.com/catalyst-cooperative/pudl/pull/1865)

### Metadata

* Used the data source metadata class added in release 0.6.0 to dynamically generate
  the data source documentation (See [Data Sources](data_sources/index.md)). [#1532](https://github.com/catalyst-cooperative/pudl/pull/1532)
* The EIA plant parts list was added to the resource and field metadata. This is the
  first output table to be included in the metadata. See [#1865](https://github.com/catalyst-cooperative/pudl/pull/1865)

### Documentation

* Fixed broken links in the documentation since the Air Markets Program Data (AMPD)
  changed to Clean Air Markets Data (CAMD).
* Added graphics and clearer descriptions of EPA data and reporting requirements to the
  [EPA Hourly Continuous Emission Monitoring System (CEMS)](data_sources/epacems.md) page. Also included information about the `epacamd_eia`
  crosswalk.

### Bug Fixes

* [Dask v2022.4.2](https://docs.dask.org/en/stable/changelog.html#v2022-04-2)
  introduced breaking changes into `dask.dataframe.read_parquet()`.  However, we
  didn’t catch this when it happened because it’s only a problem when there’s more than
  one row-group. Now we’re processing 2019-2020 data for both ID and ME (two of the
  smallest states) in the tests. Also restricted the allowed Dask versions in our
  `setup.py` so that we get notified by the dependabot any time even a minor update.
  happens to any of the packages we depend on that use calendar versioning. See
  [#1618](https://github.com/catalyst-cooperative/pudl/pull/1618).
* Fixed a testing bug where the partitioned EPA CEMS outputs generated using parallel
  processing were getting output in the same output directory as the real ETL, which
  should never happen. See [#1618](https://github.com/catalyst-cooperative/pudl/pull/1618).
* Changed the way fixes to the EIA-861 balancing authority names and IDs are applied,
  so that they still work when only some years of data are being processed. See
  [#1671](https://github.com/catalyst-cooperative/pudl/pull/1671) and [#828](https://github.com/catalyst-cooperative/pudl/issues/828).

### Dependencies / Environment

* In conjunction with getting the [@dependabot](https://github.com/sponsors/dependabot) set up to merge its own PRs if CI
  passes, we tightened the version constraints on a lot of our dependencies. This should
  reduce the frequency with which we get surprised by changes breaking things after
  release. See [#1655](https://github.com/catalyst-cooperative/pudl/pull/1655)
* We’ve switched to using [mambaforge](https://github.com/conda-forge/miniforge) to
  manage our environments internally, and are recommending that users use it as well.
* We’re moving toward treating PUDL like an application rather than a library, and part
  of that is no longer trying to be compatible with a wide range of versions of our
  dependencies, instead focusing on a single reproducible environment that is associated
  with each release, using lockfiles, etc. See [#1669](https://github.com/catalyst-cooperative/pudl/issues/1669)
* As an “application” PUDL is now only supporting the most recent major version of
  Python (currently 3.10). We used
  [pyupgrade](https://github.com/asottile/pyupgrade) and
  [pep585-upgrade](https://github.com/snok/pep585-upgrade) to update the syntax of
  to use Python 3.10 norms, and are now using those packages as pre-commit hooks as
  well. See [#1685](https://github.com/catalyst-cooperative/pudl/pull/1685)

<a id="release-v0-6-0"></a>

## 0.6.0 (2022-03-11)

### Data Coverage

* [EIA Form 860 – Annual Electric Generator Report](data_sources/eia860.md) monthly updates (`eia860m`) up to the end of 2021.
  [#1510](https://github.com/catalyst-cooperative/pudl/pull/1510)

### New Analyses

* For the purposes of linking EIA and FERC Form 1 records, we (mostly [@cmgosnell](https://github.com/sponsors/cmgosnell))
  have created a new output called the Plant Parts List in
  [`pudl.analysis.plant_parts_eia`](autoapi/pudl/analysis/plant_parts_eia/index.md#module-pudl.analysis.plant_parts_eia) which combines many different sub-parts of the
  EIA generators based on their fuel type, prime movers, ownership, etc. This allows a
  huge range of hypothiecally possible FERC Form 1 plant records to be synthesized, so
  that we can identify exactly what data in EIA should be associated with what data in
  FERC using a variety of record linkage & entity matching techniques. This is still a
  work in progress, both with our partners at RMI, and in collaboration with the
  [Chu Data Lab at Georgia Tech](https://chu-data-lab.cc.gatech.edu/), through work
  funded by a
  [CCAI Innovation Grant](https://www.climatechange.ai/calls/innovation_grants).
  [#1157](https://github.com/catalyst-cooperative/pudl/pull/1157)

### Metadata

* Column data types for our database and Apache Parquet outputs, as well as pandas
  dataframes are all based on the same underlying schemas, and should be much more
  consistent. [#1370](https://github.com/catalyst-cooperative/pudl/pull/1370), [#1377](https://github.com/catalyst-cooperative/pudl/pull/1377), [#1408](https://github.com/catalyst-cooperative/pudl/pull/1408)
* Defined a data source metadata class [`pudl.metadata.classes.DataSource`](autoapi/pudl/metadata/classes/index.md#pudl.metadata.classes.DataSource) using
  Pydantic to store information and procedures specific to each data source (e.g.
  [FERC Form 1 – Annual Report of Major Electric Utilities](data_sources/ferc1.md), [EIA Form 923 – Power Plant Operations Report](data_sources/eia923.md)). [#1446](https://github.com/catalyst-cooperative/pudl/pull/1446)
* Use the data source metadata classes to automatically export rich metadata for use
  with our Datasette deployment. [#1479](https://github.com/catalyst-cooperative/pudl/pull/1479)
* Use the data source metadata classes to store rich metadata for use with our
  [Zenodo raw data archives](https://github.com/catalyst-cooperative/pudl-zenodo-storage/)
  so that information is no longer duplicated and liable to get out of sync.
  [#1475](https://github.com/catalyst-cooperative/pudl/pull/1475)
* Added static tables and metadata structures that store definitions and additional
  information related to the many coded categorical columns in the database. These
  tables are exported directly into the documentation (See
  [PUDL Code Metadata](data_dictionaries/codes_and_labels.md)). The metadata structures also document all
  of the non-standard values that we’ve identified in the raw data, and the standard
  codes that they are mapped to. [#1388](https://github.com/catalyst-cooperative/pudl/pull/1388)
* As a result of all these metadata improvements we were finally able to close
  [#52](https://github.com/catalyst-cooperative/pudl/issues/52) and delete the `pudl.constants` junk-drawer module… after 5 years.

### Data Cleaning

* Fixed a few inaccurately hand-mapped PUDL Plant & Utility IDs. [#1458](https://github.com/catalyst-cooperative/pudl/pull/1458), [#1480](https://github.com/catalyst-cooperative/pudl/pull/1480)
* We are now using the coding table metadata mentioned above and the foreign key
  relationships that are part of the database schema to automatically recode any column
  that refers to the codes defined in the coding table. This results in much more
  uniformity across the whole database, especially in the EIA `energy_source_code`
  columns. [#1416](https://github.com/catalyst-cooperative/pudl/pull/1416)
* In the raw input data, often NULL values will be represented by the empty string or
  other not really NULL values. We went through and cleaned these up in all of the
  categorical / coded columns so that their values can be validated based on either an
  ENUM constraint in the database, or a foreign key constraint linking them to the
  static coding tables. Now they should primarily use the pandas NA value, or numpy.nan
  in the case of floats. [#1376](https://github.com/catalyst-cooperative/pudl/pull/1376)
* Many FIPS and ZIP codes that appear in the raw data are stored as integers rather than
  strings, meaning that they lose their leading zeros, rendering them invalid in many
  contexts. We use the same method to clean them all up now, and enforce a uniform
  field width with leading zero padding. This also allows us to enforce a regex pattern
  constraint on these fields in the database outputs. [#1405](https://github.com/catalyst-cooperative/pudl/pull/1405), [#1476](https://github.com/catalyst-cooperative/pudl/pull/1476)
* We’re now able to fill in missing values in the very useful `generators_eia860`
  `technology_description` field. Currently this is optionally available in the output
  layer, but we want to put more of this kind of data repair into the core database
  gong forward. [#1075](https://github.com/catalyst-cooperative/pudl/pull/1075)

### Miscellaneous

* Created a simple script that allows our SQLite DB to be loaded into Google’s CloudSQL
  hosted PostgreSQL service [pgloader](https://pgloader.io/) and
  [pg_dump](https://www.postgresql.org/docs/14/app-pgdump.html). [#1361](https://github.com/catalyst-cooperative/pudl/pull/1361)
* Made better use of our
  [Pydantic settings classes](https://pydantic-docs.helpmanual.io/usage/settings/) to
  validate and manage the ETL settings that are read in from YAML files and passed
  around throughout the functions that orchestrate the ETL process. [#1506](https://github.com/catalyst-cooperative/pudl/pull/1506)
* PUDL now works with pandas 1.4 ([#1421](https://github.com/catalyst-cooperative/pudl/pull/1421)) and Python 3.10 ([#1373](https://github.com/catalyst-cooperative/pudl/pull/1373)).
* Addressed a bunch of deprecation warnings being raised by `geopandas`. [#1444](https://github.com/catalyst-cooperative/pudl/pull/1444)
* Integrated the [pre-commit.ci](https://pre-commit.ci) service into our GitHub CI
  in order to automatically apply a variety of code formatting & checks to all commits.
  [#1482](https://github.com/catalyst-cooperative/pudl/pull/1482)
* Fixed random seeds to avoid stochastic test coverage changes in the
  [`pudl.analysis.timeseries_cleaning`](autoapi/pudl/analysis/timeseries_cleaning/index.md#module-pudl.analysis.timeseries_cleaning) module. [#1483](https://github.com/catalyst-cooperative/pudl/pull/1483)
* Silenced a bunch of 3rd party module warnings in the tests. See [#1476](https://github.com/catalyst-cooperative/pudl/pull/1476)

### Bug Fixes

* In addressing [#851](https://github.com/catalyst-cooperative/pudl/issues/851), [#1296](https://github.com/catalyst-cooperative/pudl/issues/1296), [#1325](https://github.com/catalyst-cooperative/pudl/issues/1325) the `generation_fuel_eia923` table was split
  to create a `generation_fuel_nuclear_eia923` table since they have different
  primary keys. This meant that the `pudl.output.pudltabl.PudlTabl.gf_eia923()`
  method no longer included nuclear generation. This impacted the net generation
  allocation process and MCOE calculations downstream, which were expecting to have all
  the reported nuclear generation. This has now been fixed, and the generation fuel
  output includes both the nuclear and non-nuclear generation, with nuclear generation
  aggregated across nuclear unit IDs so that it has the same primary key as the rest
  of the generation fuel table. [#1518](https://github.com/catalyst-cooperative/pudl/pull/1518)
* EIA changed the URL of their API to only accept connections over HTTPS, but we had
  a hard-coded HTTP URL, meaning the historical fuel price filling that uses the API
  broke. This has been fixed.

### Known Issues

* Everything is fiiiiiine.

<a id="release-v0-5-0"></a>

## 0.5.0 (2021-11-11)

### Data Coverage Changes

* Integration of 2020 data for all our core datasets (See [#1255](https://github.com/catalyst-cooperative/pudl/issues/1255)):
  * [EIA Form 860 – Annual Electric Generator Report](data_sources/eia860.md) for 2020 as well as 2001-2003 (see [#1122](https://github.com/catalyst-cooperative/pudl/issues/1122)).
  * EIA Form 860m through 2021-08.
  * [EIA Form 923 – Power Plant Operations Report](data_sources/eia923.md) for 2020.
  * [FERC Form 1 – Annual Report of Major Electric Utilities](data_sources/ferc1.md) for 2020.
  * [EIA Form 861 – Annual Electric Power Industry Report](data_sources/eia861.md) data for 2020.
  * [FERC Form 714 – Annual Electric Balancing Authority Area and Planning Area Report](data_sources/ferc714.md) for 2020.
  * Note: the 2020 [EPA Hourly Continuous Emission Monitoring System (CEMS)](data_sources/epacems.md) data was already available in v0.4.0.
* **EPA IPM / NEEDS** data has been removed from PUDL as we didn’t have the internal
  resources to maintain it, and it was no longer working. Apologies to
  [@gschivley](https://github.com/sponsors/gschivley)!

### SQLite and Parquet Outputs

* The ETL pipeline now outputs SQLite databases and Apache Parquet datasets
  directly, rather than generating tabular data packages. This is much faster
  and simpler, and also takes up less space on disk. Running the full ETL
  including all EPA CEMS data should now take around 2 hours if you have all the
  data downloaded.
* The new `pudl.load.sqlite` and `pudl.load.parquet` modules contain
  this logic. The `pudl.load.csv` and `pudl.load.metadata` modules have been
  removed along with other remaining datapackage infrastructure. See [#1211](https://github.com/catalyst-cooperative/pudl/issues/1211)
* Many more tables now have natural primary keys explicitly specified within the
  database schema.
* The `datapkg_to_sqlite` script has been removed and the `epacems_to_parquet`
  script can now be used to process the original EPA CEMS CSV data directly to
  Parquet using an existing PUDL database to source plant timezones.  See
  [#1176](https://github.com/catalyst-cooperative/pudl/issues/1176), [#806](https://github.com/catalyst-cooperative/pudl/issues/806).
* Data types, specified value constraints, and the uniqueness / non-null
  constraints on primary keys are validated during insertion into the SQLite DB.
* The PUDL ETL CLI `pudl.etl.cli` now has flags to toggle various constraint
  checks including `--ignore-foreign-key-constraints`
  `--ignore-type-constraints` and `--ignore-value-constraints`.

### New Metadata System

With the deprecation of tabular data package outputs, we’ve adopted a more
modular metadata management system that uses [Pydantic](https://pydantic-docs.helpmanual.io/).  This setup will allow us to easily
validate the metadata schema and export to a variety of formats to support data
distribution via [Datasette](https://datasette.io) and [Intake catalogs](https://intake.readthedocs.io), and automatic generation of data
dictionaries and documentation. See [#806](https://github.com/catalyst-cooperative/pudl/issues/806), [#1271](https://github.com/catalyst-cooperative/pudl/issues/1271), [#1272](https://github.com/catalyst-cooperative/pudl/issues/1272) and the [`pudl.metadata`](autoapi/pudl/metadata/index.md#module-pudl.metadata)
subpackage. Many thanks to [@ezwelty](https://github.com/sponsors/ezwelty) for most of this work.

### ETL Settings File Format Changed

We are also using [Pydantic](https://pydantic-docs.helpmanual.io/) to parse and
validate the YAML settings files that tell PUDL what data to include in an ETL run. If
you have any old settings files of your own lying around they’ll need to be updated.
Examples of the new format will be deployed to your system if you re-run the
`pudl_setup` script. Or you can make a copy of the `etl_full.yml` or
`etl_fast.yml` files that are stored under `src/pudl/package_data/settings` and
edit them to reflect your needs.

### Database Schema Changes

With the direct database output and the new metadata system, it’s much easier for us
to create foreign key relationships automatically. Updates that are in progress to
the database normalization and entity resolution process also benefit from using
natural primary keys when possible. As a result we’ve made some changes to the PUDL
database schema, which will probably affect some users.

* We have split out a new `generation_fuel_nuclear_eia923` table from the existing
  `generation_fuel_eia923` table, as nuclear generation and fuel consumption are
  reported at the generation unit level, rather than the plant level, requiring a
  different natural primary key. See [#851](https://github.com/catalyst-cooperative/pudl/issues/851), [#1296](https://github.com/catalyst-cooperative/pudl/issues/1296), [#1325](https://github.com/catalyst-cooperative/pudl/issues/1325).
* Implementing a natural primary key for the `boiler_fuel_eia923` table required
  the aggregation of a small number of records that didn’t have well-defined
  `prime_mover_code` values. See [#852](https://github.com/catalyst-cooperative/pudl/issues/852), [#1306](https://github.com/catalyst-cooperative/pudl/issues/1306), [#1311](https://github.com/catalyst-cooperative/pudl/issues/1311).
* We repaired, aggregated, or dropped a small number of records in the
  `generation_eia923` (See [#1208](https://github.com/catalyst-cooperative/pudl/issues/1208), [#1248](https://github.com/catalyst-cooperative/pudl/issues/1248)) and
  `ownership_eia860` (See [#1207](https://github.com/catalyst-cooperative/pudl/issues/1207), [#1258](https://github.com/catalyst-cooperative/pudl/issues/1258)) tables due to null values in their
  primary key columns.
* Many new foreign key constraints are being enforced between the EIA data tables,
  entity tables, and coding tables. See [#1196](https://github.com/catalyst-cooperative/pudl/issues/1196).
* Fuel types and energy sources reported to EIA are now defined in / constrained by
  the static `energy_sources_eia` table.
* The columns that indicate the mode of transport for various fuels now contain short
  codes rather than longer labels, and are defined in / constrained by the static
  `fuel_transportation_modes_eia` table.
* In the simplified FERC 1 fuel type categories, we’re now using `other` instead of
  `unknown`.
* Several columns have been renamed to harmonize meanings between different tables and
  datasets, including:
  * In `generation_fuel_eia923` and `boiler_fuel_eia923` the `fuel_type` and
    `fuel_type_code` columns have been replaced with `energy_source_code`, which
    appears in various forms in `generators_eia860` and
    `fuel_receipts_costs_eia923`.
  * `fuel_qty_burned` is now `fuel_consumed_units`
  * `fuel_qty_units` is now `fuel_received_units`
  * `heat_content_mmbtu_per_unit` is now `fuel_mmbtu_per_unit`
  * `sector_name` and `sector_id` are now `sector_name_eia` and `sector_id_eia`
  * `primary_purpose_naics_id` is now `primary_purpose_id_naics`
  * `mine_type_code` is now `mine_type` (a human readable label, not a code).

### New Analyses

* Added a deployed console script for running the state-level hourly electricity
  demand allocation, using FERC 714 and EIA-861 data, simply called
  `state_demand` and implemented in [`pudl.analysis.state_demand`](autoapi/pudl/analysis/state_demand/index.md#module-pudl.analysis.state_demand). This
  script existed in the v0.4.0 release, but was not deployed on the user’s
  system.

### Known Issues

* The `pudl_territories` script has been disabled temporarily due to a memory
  issue. See [#1174](https://github.com/catalyst-cooperative/pudl/issues/1174)
* Utility and Balancing Authority service territories for 2020 have not been vetted,
  and may contain errors or omissions. In particular there seems to be some missing
  demand in ND, SD, NE, KS, and OK. See [#1310](https://github.com/catalyst-cooperative/pudl/issues/1310)

### Updated Dependencies

* **SQLAlchemy 1.4.x:** Addressed all deprecation warnings associated with API changes
  coming in SQLAlchemy 2.0, and bumped current requirement to 1.4.x
* **Pandas 1.3.x:** Addressed many data type issues resulting from changes in how Pandas
  preserves and propagates ExtensionArray / nullable data types.
* **PyArrow v5.0.0** Updated to the most recent version
* **PyGEOS v0.10.x** Updated to the most recent version
* **contextily** has been removed, since we only used it optionally for making a single
  visualization and it has substantial dependencies itself.
* **goodtables-pandas-py** has been removed since we’re no longer producing or
  validating datapackages.
* **SQLite 3.32.0** The type checks that we’ve implemented currently only work with
  SQLite version 3.32.0 or later, as we discovered in debugging build failures on PR
  [#1228](https://github.com/catalyst-cooperative/pudl/issues/1228). Unfortunately Ubuntu 20.04 LTS shipped with SQLite 3.31.1. Using
  `conda` to manage your Python environment avoids this issue.

<a id="release-v0-4-0"></a>

## 0.4.0 (2021-08-16)

This is a ridiculously large update including more than a year and a half’s
worth of work.

### New Data Coverage

* [EIA Form 860 – Annual Electric Generator Report](data_sources/eia860.md) for 2004-2008 + 2019, plus eia860m through 2020
* [EIA Form 923 – Power Plant Operations Report](data_sources/eia923.md) for 2001-2008 + 2019
* [EPA Hourly Continuous Emission Monitoring System (CEMS)](data_sources/epacems.md) for 2019-2020
* [FERC Form 1 – Annual Report of Major Electric Utilities](data_sources/ferc1.md) for 2019
* [Census DP1 – Profile of General Demographic Characteristics](data_sources/censusdp1tract.md) for 2010
* [FERC Form 714 – Annual Electric Balancing Authority Area and Planning Area Report](data_sources/ferc714.md) for 2006-2019 (experimental)
* [EIA Form 861 – Annual Electric Power Industry Report](data_sources/eia861.md) for 2001-2019 (experimental)

### Documentation & Data Accessibility

We’ve updated and (hopefully) clarified the documentation, and no longer expect
most users to perform the data processing on their own. Instead, we are offering
several methods of directly accessing already processed data:

* Processed data archives on Zenodo that include a Docker container preserving
  the required software environment for working with the data.
* [A repository of PUDL example notebooks](https://github.com/catalyst-cooperative/pudl-examples)
* [A JupyterHub instance](https://catalyst-cooperative.pilot.2i2c.cloud/)
  hosted in collaboration with [2i2c](https://2i2c.org)
* Browsable database access via [Datasette](https://datasette.io) at
  [https://data.catalyst.coop](https://data.catalyst.coop)

Users who still want to run the ETL themselves will need to set up the
[set up the PUDL development environment](dev/dev_setup.md)

### Data Cleaning & Integration

* We now inject placeholder utilities in the cloned FERC Form 1 database when
  respondent IDs appear in the data tables, but not in the respondent table.
  This addresses a bunch of unsatisfied foreign key constraints in the original
  databases published by FERC.
* We’re doing much more software testing and data validation, and so hopefully
  we’re catching more issues early on.

### Hourly Electricity Demand and Historical Utility Territories

With support from [GridLab](https://gridlab.org) and in collaboration with
researchers at Berkeley’s [Center for Environmental Public Policy](https://gspp.berkeley.edu/faculty-and-impact/centers/cepp), we did a bunch
of work on spatially attributing hourly historical electricity demand. This work
was largely done by [@ezwelty](https://github.com/sponsors/ezwelty) and [@yashkumar1803](https://github.com/sponsors/yashkumar1803) and included:

* Semi-programmatic compilation of historical utility and balancing authority
  service territory geometries based on the counties associated with utilities,
  and the utilities associated with balancing authorities in the EIA-861
  (2001-2019). See e.g. [#670](https://github.com/catalyst-cooperative/pudl/pull/670) but also many others.
* A method for spatially allocating hourly electricity demand from FERC 714 to
  US states based on the overlapping historical utility service territories
  described above. See [#741](https://github.com/catalyst-cooperative/pudl/pull/741)
* A fast timeseries outlier detection routine for cleaning up the FERC 714
  hourly data using correlations between the time series reported by all of the
  different entities. See [#871](https://github.com/catalyst-cooperative/pudl/pull/871)

### Net Generation and Fuel Consumption for All Generators

We have developed an experimental methodology to produce net generation and
fuel consumption for all generators. The process has known issues and is being
actively developed. See [#989](https://github.com/catalyst-cooperative/pudl/pull/989)

Net electricity generation and fuel consumption are reported in multiple ways in
the EIA-923. The `generation_fuel_eia923` table reports both generation and
fuel consumption, and breaks them down by plant, prime mover, and fuel. In
parallel, the `generation_eia923` table reports generation by generator,
and the `boiler_fuel_eia923` table reports fuel consumption by boiler.

The `generation_fuel_eia923` table is more complete, but the
`generation_eia923` + `boiler_fuel_eia923` tables are more granular.
The `generation_eia923` table includes only ~55% of the total MWhs reported
in the `generation_fuel_eia923` table.

The [`pudl.analysis.allocate_gen_fuel`](autoapi/pudl/analysis/allocate_gen_fuel/index.md#module-pudl.analysis.allocate_gen_fuel) module estimates the net electricity
generation and fuel consumption attributable to individual generators based on
the more expansive reporting of the data in the `generation_fuel_eia923`
table.

### Data Management and Archiving

* We now use a series of web scrapers to collect snapshots of the raw input data
  that is processed by PUDL. These original data are archived as
  [Frictionless Data Packages](https://specs.frictionlessdata.io/data-package/)
  on [Zenodo](https://zenodo.org), so that they can be accessed reproducibly
  and programmatically via a REST API. This addresses the problems we were
  having with the v0.3.x releases, in which the original data on the agency
  websites was liable to be modified long after its “final” release, rendering
  it incompatible with our software. These scrapers and the Zenodo archiving
  scripts can be found in our
  [pudl-scrapers](https://github.com/catalyst-cooperative/pudl-scrapers) and
  [pudl-zenodo-storage](https://github.com/catalyst-cooperative/pudl-zenodo-storage)
  repositories. The archives themselves can be found within the
  [Catalyst Cooperative community on Zenodo](https://zenodo.org/communities/catalyst-cooperative/)
* There’s an experimental caching system that allows these Zenodo archives to
  work as long-term “cold storage” for citation and reproducibility, with
  cloud object storage acting as a much faster way to access the same data for
  day to day non-local use, implemented by [@rousik](https://github.com/sponsors/rousik)
* We’ve decided to shift to producing a combination of relational databases
  (SQLite files) and columnar data stores (Apache Parquet files) as the primary
  outputs of PUDL. [Tabular Data Packages](https://specs.frictionlessdata.io/tabular-data-package/)
  didn’t end up serving either database or spreadsheet users very well. The CSV
  file were often too large to access via spreadsheets, and users missed out on
  the relationships between data tables. Needing to separately load the data
  packages into SQLite and Parquet was a hassle and generated a lot of overly
  complicated and fragile code.

### Known Issues

* The EIA-861 and FERC 714 data are not yet integrated into the SQLite database
  outputs, because we need to overhaul our entity resolution process to
  accommodate them in the database structure. That work is ongoing, see
  [#639](https://github.com/catalyst-cooperative/pudl/issues/639)
* The EIA-860 and EIA-923 data don’t cover exactly the same rage of years. EIA
  860 only goes back to 2004, while EIA-923 goes back to 2001. This is because
  the pre-2004 EIA-860 data is stored in the DBF file format, and we need to
  update our extraction code to deal with the different format. This means some
  analyses that require both EIA-860 and EIA-923 data (like the calculation of
  heat rates) can only be performed as far back as 2004 at the moment. See
  [#848](https://github.com/catalyst-cooperative/pudl/issues/848)
* There are 387 EIA utilities and 228 EIA palnts which appear in the EIA-923,
  but which haven’t yet been assigned PUDL IDs and associated with the
  corresponding utilities and plants reported in the FERC Form 1. These entities
  show up in the 2001-2008 EIA-923 data that was just integrated. These older
  plants and utilities can’t yet be used in conjunction with FERC data. When the
  EIA-860 data for 2001-2003 has been integrated, we will finish this manual
  ID assignment process. See [#848](https://github.com/catalyst-cooperative/pudl/issues/848), [#1069](https://github.com/catalyst-cooperative/pudl/issues/1069)
* 52 of the algorithmically assigned `plant_id_ferc1` values found in the
  `plants_steam_ferc1` table are currently associated with more than one
  `plant_id_pudl` value (99 PUDL plant IDs are involved), indicating either
  that the algorithm is making poor assignments, or that the manually assigned
  `plant_id_pudl` values are incorrect. This is out of several thousand
  distinct `plant_id_ferc1` values. See [#954](https://github.com/catalyst-cooperative/pudl/issues/954)
* The county FIPS codes associated with coal mines reported in the Fuel Receipts and
  Costs table are being treated inconsistently in terms of their data types, especially
  in the output functions, so they are currently being output as floating point numbers
  that have been cast to strings, rather than zero-padded integers that are strings. See
  [#1119](https://github.com/catalyst-cooperative/pudl/issues/1119)

<a id="release-v0-3-2"></a>

## 0.3.2 (2020-02-17)

The primary changes in this release:

* The 2009-2010 data for EIA-860 have been integrated, including updates
  to the data validation test cases.
* Output tables are more uniform and less restrictive in what they
  include, no longer requiring PUDL Plant & Utility IDs in some tables.  This
  release was used to compile v1.1.0 of the PUDL Data Release, which is archived
  at Zenodo under this DOI: [https://doi.org/10.5281/zenodo.3672068](https://doi.org/10.5281/zenodo.3672068)

  With this release, the EIA-860 & 923 data now (finally!) cover the same span
  of time. We do not anticipate integrating any older EIA-860 or 923 data at
  this time.

<a id="release-v0-3-1"></a>

## 0.3.1 (2020-02-05)

A couple of minor bugs were found in the preparation of the first PUDL data
release:

* No maximum version of Python was being specified in setup.py. PUDL currently
  only works on Python 3.7, not 3.8.
* `epacems_to_parquet` conversion script was erroneously attempting to
  verify the availability of raw input data files, despite the fact that it now
  relies on the packaged post-ETL epacems data. Didn’t catch this before since
  it was always being run in a context where the original data was lying
  around… but that’s not the case when someone just downloads the released
  data packages and tries to load them.

<a id="release-v0-3-0"></a>

## 0.3.0 (2020-01-30)

This release is mostly about getting the infrastructure in place to do regular
data releases via Zenodo, and updating ETL with 2018 data.

Added lots of data validation / quality assurance test cases in anticipation of
archiving data. See the pudl.validate module for more details.

New data since v0.2.0 of PUDL:

* EIA Form 860 for 2018
* EIA Form 923 for 2018
* FERC Form 1 for 1994-2003 and 2018 (select tables)

We removed the FERC Form 1 accumulated depreciation table from PUDL because it
requires detailed row-mapping in order to be accurate across all the years. It
and many other FERC tables will be integrated soon, using new row-mapping
methods.

Lots of new plants and utilities integrated into the PUDL ID mapping process,
for the earlier years (1994-2003).  All years of FERC 1 data should be
integrated for all future ferc1 tables.

Command line interfaces of some of the ETL scripts have changed, see their help
messages for details.

<a id="release-v0-2-0"></a>

## 0.2.0 (2019-09-17)

This is the first release of PUDL to generate data packages as the canonical
output, rather than loading data into a local PostgreSQL database. The data
packages can then be used to generate a local SQLite database, without relying
on any software being installed outside of the Python requirements specified for
the catalyst.coop package.

This change will enable easier installation of PUDL, as well as archiving and
bulk distribution of the data products in a platform independent format.

<a id="release-v0-1-0"></a>

## 0.1.0 (2019-09-12)

This is the only release of PUDL that will be made that makes use of
PostgreSQL as the primary data product. It is provided for reference, in case
there are users relying on this setup who need access to a well defined release.
