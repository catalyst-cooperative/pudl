#!/bin/bash
# A convenience function that invokes all of the data downloading scripts.
# For bootstrapping new projects.

./ferc/form1/get_ferc1.sh
./eia/form860/get_eia860.sh
./eia/form923/get_eia923.sh
