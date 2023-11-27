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
- Update the mapping information in `pudl/package_data/EIA_DATASET_NAME` if 
necessary:
     - [ ] file maps (may need to add or remove _Early_Release suffix!).
     - [ ] column maps (if there are new or chaned columns).
     - [ ] page maps (if the tab order changes).
     - [ ] skip footer (if there is new, non-data content at the end of the file).
     - [ ] skip rows (the early release data tends to have an extra, skippable row at 
     the top of the file. Either add or subtract that row depending on whether you are
     adding early release or final data).
- [ ] Launch dagit and refresh the code location (run `dagster-webserver -m pudl.etl`
in your terminal and open the link it generates in a browser).
- [ ] Materialize the `raw_EIA_DATASET_NAME` asset group. Look out for warnings in the 
logs about missing or extra columns. If they appear, check and update the `package_data` 
accordingly.
- [ ] Materialize the `_core_EIA_DATASET_NAME` asset group. Look out for warnings and 
fix accordingly.
- [ ] Materialize the `norm_eia` and then `denorm_eia` asset groups. You may see errors 
related to encoding. Take a look at which column it's talking about and look at 
`metadata/resources/eia.py` to see which encoder in `CODE_METADATA` to tweak.
- [ ] Test table outputs in a Jupyter notebook to make sure expected dates appear.
- [ ] Map new plant and utility IDs that appear in the new data: https://catalystcoop-pudl.readthedocs.io/en/dev/dev/annual_updates.html#connect-datasets.
- [ ] Run the validation test `test_minmax_rows` in `test/validate/eia_test.py` and 
`test/validate/mcoe.py` (in the terminal: `pytest test/validate/eia_test.py::test_minmax_rows --live-dbs` or `pytest test/validate/mcoe_test.py::test_minmax_rows_mcoe --live-dbs`).
Because you've added new data (more rows), these tests will fail. You can use the
failure logs to see how many rows were "found" compared to how many rows were 
"expected". Make sure that the differene between "found" and "expected" rows is
reasonable given the change in data. I.e., a new year of data should not change the 
overall row count by more than 1%. If any of the tables have _fewer_ rows than before, investigate why (paying mind to the overall impact on the data--if it's only affecting 
0.001% of the data, don't spend that much time on it).
- Once you've updated the min and max row count (known errors for the validation 
test suite), run the rest of the tests. Read about the testing process: https://catalystcoop-pudl.readthedocs.io/en/latest/dev/testing.html.
    - [ ] Validation tests: `make pytest-validate`
    - [ ] Integration tests: `make pytest-integration`
    - [ ] Unit tests: `make pytest-unit`