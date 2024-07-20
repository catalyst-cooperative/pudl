---
name: Quarterly Update {{ date | date('[Q]Q YYYY') }}
about: Check-list for integrating the quarterly updates into PUDL
title: ""
labels: data-update
assignees: ""
---

### Quarterly Update Check-list

Once the new archives have been vetted and published, you can begin the process of integrating the new quarterly update data into PUDL.

Follow the steps in [Existing Data Updates Docs](https://catalystcoop-pudl.readthedocs.io/en/latest/dev/existing_data_updates.html)

```[tasklist]
- [ ] EIA 860m {{ date | date('[Q]Q YYYY') }} Update
- [ ] EIA 923 {{ date | date('[Q]Q YYYY') }} Update
- [ ] EIA 930 {{ date | date('[Q]Q YYYY') }} Update
- [ ] CEMS {{ date | date('[Q]Q YYYY') }} Update
- [ ] EIA Bulk API {{ date | date('[Q]Q YYYY') }} Update
```