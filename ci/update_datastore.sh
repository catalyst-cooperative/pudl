#!/bin/bash
# Download some federal utility data to test with.

echo
echo "[updating PUDL datastore]"
cd scripts
which python
source activate pudl
which python
python update_datastore.py --source ferc1 eia860 --years 2012 2016
python update_datastore.py --source eia923 --years 2016
python update_datastore.py --source epacems --states CO --years 2016
cd ..
