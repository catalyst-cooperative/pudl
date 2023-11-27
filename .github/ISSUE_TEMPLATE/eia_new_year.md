---
name: Integrate New Year of EIA data
about: Check-list for integrating a new year of EIA data
title: ''
labels: new-data
assignees: ''

---

### EIA new year of data integration check-list:

Review Annual Updates Docs: https://catalystcoop-pudl.readthedocs.io/en/dev/dev/annual_updates.html

Replace references to `EIA_DATASET_NAME` with either `eia_860`, `eia923`, or `eia861` 
depending on which EIA dataset you are updating.

- [ ] Add the new Zenodo archive DOI to `pudl/workspace/datastore.py`.
- [ ] Run the datastore script to download the new raw data: `pudl_datastore --dataset 
EIA_DATASET_NAME`. The new raw data will appear in `pudl_input/eia923/<ZENODO_DOI>/...`
- [ ] Update the mapping information in `pudl/package_data/EIA_DATASET_NAME` if 
necessary:
     - [ ] file maps (may need to add or remove _Early_Release suffix!).
     - [ ] column maps (if there are new or chaned columns).
     - [ ] page maps (if the tab order changes).
     - [ ] skip footer (if there is new, non-data content at the end of the file).
     - [ ] skip rows (the early release data tends to have an extra, skippable row at 
     the top of the file. Either add or subtract that row depending on whether you are
     adding early release or final data).
- [ ] Launch dagit and refresh the code location (run in your terminal 
`dagster-webserver -m pudl.etl` and then open 
`http://127.0.0.1:3000/locations/pudl.etl/jobs/etl_full` in a browser)
- [ ] Materialize the `raw_EIA_DATASET_NAME` asset group. Look out for warnings in the 
logs about missing or extra columns. If they appear, check and update the `package_data` 
accordingly.
- [ ] Materialize the `_core_EIA_DATASET_NAME` asset group. Look out for warnings and 
fix accordingly.
- [ ] Materialize the `norm_eia` and then `denorm_eia` asset groups. You may see errors 
related to encoding. Take a look at which column it's talking about and look at 
`metadata/resources/eia.py` to see which encoder in `CODE_METADATA` to tweak.
- [ ] Test table outputs in a Jupyter notebook to make sure expected dates appear.
- [ ] Update the validation test `test_minmax_rows` in `test/validate/eia_test.py` and 
`test/validate/mcoe.py`. Sometimes it helps to run the tests 
(`pytest test/validate/eia_test.py::test_minmax_rows --live-dbs`) in the terminal 
because it will print out how many rows it found vs. how many it expected and you can 
put the found rows into the code so they become expected rows. Make sure none of the 
rows have _less_ rows than before. Also make sure none of the row changes are 
unexpectedly large.
- [ ] Run `tox` and troubleshoot what else might be broken! Might include things like:
     - Foreign key errors.
     - The need to map new plants or utilities (https://catalystcoop-pudl.readthedocs.io/en/dev/dev/annual_updates.html#connect-datasets).
     - All sorts of other whacky stuff. 