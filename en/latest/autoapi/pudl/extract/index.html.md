# pudl.extract

Modules implementing the “Extract” step of the PUDL ETL pipeline.

Each module in this subpackage implements data extraction for a single data source from
the PUDL [Data Sources](../../../data_sources/index.html.md#data-sources). This process begins with the original data as retrieved by
the [`pudl.workspace`](../workspace/index.html.md#module-pudl.workspace) subpackage, and ends with a dictionary of “raw”
`pandas.DataFrame`s, that have been minimally altered from the original data, and
are ready for normalization and data cleaning by the data source specific modules in the
:mod:`pudl.transform` subpackage.

## Submodules

* [pudl.extract.censusdp1tract](censusdp1tract/index.html.md)
* [pudl.extract.censuspep](censuspep/index.html.md)
* [pudl.extract.csv](csv/index.html.md)
* [pudl.extract.dbf](dbf/index.html.md)
* [pudl.extract.eia176](eia176/index.html.md)
* [pudl.extract.eia191](eia191/index.html.md)
* [pudl.extract.eia757a](eia757a/index.html.md)
* [pudl.extract.eia860](eia860/index.html.md)
* [pudl.extract.eia860m](eia860m/index.html.md)
* [pudl.extract.eia861](eia861/index.html.md)
* [pudl.extract.eia923](eia923/index.html.md)
* [pudl.extract.eia930](eia930/index.html.md)
* [pudl.extract.eiaaeo](eiaaeo/index.html.md)
* [pudl.extract.eiaapi](eiaapi/index.html.md)
* [pudl.extract.epacems](epacems/index.html.md)
* [pudl.extract.epamats](epamats/index.html.md)
* [pudl.extract.excel](excel/index.html.md)
* [pudl.extract.extractor](extractor/index.html.md)
* [pudl.extract.ferc](ferc/index.html.md)
* [pudl.extract.ferc1](ferc1/index.html.md)
* [pudl.extract.ferc2](ferc2/index.html.md)
* [pudl.extract.ferc6](ferc6/index.html.md)
* [pudl.extract.ferc60](ferc60/index.html.md)
* [pudl.extract.ferc714](ferc714/index.html.md)
* [pudl.extract.ferccid](ferccid/index.html.md)
* [pudl.extract.ferceqr](ferceqr/index.html.md)
* [pudl.extract.gridpathratoolkit](gridpathratoolkit/index.html.md)
* [pudl.extract.nrelatb](nrelatb/index.html.md)
* [pudl.extract.parquet](parquet/index.html.md)
* [pudl.extract.phmsagas](phmsagas/index.html.md)
* [pudl.extract.rus12](rus12/index.html.md)
* [pudl.extract.rus7](rus7/index.html.md)
* [pudl.extract.sec10k](sec10k/index.html.md)
* [pudl.extract.vcerare](vcerare/index.html.md)
* [pudl.extract.xbrl](xbrl/index.html.md)
