The data associated with PUDL is too large for easy storage within the
Git repository. Many data sources have scripts associated with them that
will pull the entire dataset down from e.g. FERC or EIA automatically.

e.g.:

./ferc/form1/get_ferc1.sh
./eia/form860/get_eia860.sh
./eia/form923/get_eia923.sh

The (uncompressed) data store can also be downloaded from:

https://spideroak.com/browse/share/CatalystCoop/pudl
