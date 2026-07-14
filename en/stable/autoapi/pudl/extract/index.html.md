# pudl.extract

Modules implementing the “Extract” step of the PUDL ETL pipeline.

Each module in this subpackage implements data extraction for a single data source from
the PUDL [Data Sources](../../../data_sources/index.md#data-sources). This process begins with the original data as retrieved by
the [`pudl.workspace`](../workspace/index.md#module-pudl.workspace) subpackage, and ends with a dictionary of “raw”
`pandas.DataFrame`s, that have been minimally altered from the original data, and
are ready for normalization and data cleaning by the data source specific modules in the
:mod:`pudl.transform` subpackage.

## Submodules

* [pudl.extract.censusdp1tract](censusdp1tract/index.md)
* [pudl.extract.censuspep](censuspep/index.md)
* [pudl.extract.csv](csv/index.md)
* [pudl.extract.dbf](dbf/index.md)
* [pudl.extract.eia176](eia176/index.md)
* [pudl.extract.eia191](eia191/index.md)
* [pudl.extract.eia757a](eia757a/index.md)
* [pudl.extract.eia860](eia860/index.md)
* [pudl.extract.eia860m](eia860m/index.md)
* [pudl.extract.eia861](eia861/index.md)
* [pudl.extract.eia923](eia923/index.md)
* [pudl.extract.eia930](eia930/index.md)
* [pudl.extract.eiaaeo](eiaaeo/index.md)
* [pudl.extract.eiaapi](eiaapi/index.md)
* [pudl.extract.epacems](epacems/index.md)
* [pudl.extract.excel](excel/index.md)
* [pudl.extract.extractor](extractor/index.md)
* [pudl.extract.ferc](ferc/index.md)
* [pudl.extract.ferc1](ferc1/index.md)
* [pudl.extract.ferc2](ferc2/index.md)
* [pudl.extract.ferc6](ferc6/index.md)
* [pudl.extract.ferc60](ferc60/index.md)
* [pudl.extract.ferc714](ferc714/index.md)
* [pudl.extract.ferccid](ferccid/index.md)
* [pudl.extract.ferceqr](ferceqr/index.md)
* [pudl.extract.gridpathratoolkit](gridpathratoolkit/index.md)
* [pudl.extract.nrelatb](nrelatb/index.md)
* [pudl.extract.parquet](parquet/index.md)
* [pudl.extract.phmsagas](phmsagas/index.md)
* [pudl.extract.rus12](rus12/index.md)
* [pudl.extract.rus7](rus7/index.md)
* [pudl.extract.sec10k](sec10k/index.md)
* [pudl.extract.vcerare](vcerare/index.md)
* [pudl.extract.xbrl](xbrl/index.md)
