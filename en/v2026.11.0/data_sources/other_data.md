<a id="other-data"></a>

# Other Data in PUDL

This page describes minor datasets that are included in PUDL, datasets from which we’ve
only integrated a small portion of the available data, or datasets that are included
with little to no processing, and thus don’t yet have their own dedicated page under
[Data Sources](index.md). Or just data sources for which we haven’t yet compiled a complete
description.

## FERC DBF & XBRL Data

FERC publishes Forms 1, 2, 6, and 60 data as VisualFoxPro DBF files (2020 and earlier)
and XBRL documents (2021 and later). We distribute these data as standalone SQLite
database files which contain all the data from the original FERC filings, but converted
to a more easily accessible format. Only a few dozen of the highest priority FERC Form 1
tables have been integrated into the main PUDL database. See the [Data Access](../data_access.md)
page for detailed instructions.

<a id="data-ferc2"></a>

### FERC Form 2

[FERC Form 2](https://www.ferc.gov/industries-data/natural-gas/overview/general-information/natural-gas-industry-forms/form-22a-data)
is analogous to FERC Form 1, but reports on the finances of gas, rather than electric
utilities. Unfortunately because FERC’s jurisdiction over gas utilities is more limited
than for electricity, Form 2 mostly describes interstate gas transmission pipeline
companies, and not local gas distribution utilities.

<a id="data-ferc6"></a>

### FERC Form 6

[FERC Form 6](https://www.ferc.gov/industries-data/electric/general-information/electric-industry-forms/form-66-q-overview-orders)
(Annual Report of Oil Pipeline Companies) is a comprehensive financial and operating
report submitted for oil pipelines rate regulation and financial audits.

<a id="data-ferc60"></a>

### FERC Form 60

[FERC Form 60](https://www.ferc.gov/ferc-online/ferc-online/filing-forms/service-companies-filing-forms/form-60-annual-report)
(Annual Report of Centralized Service Companies) is a comprehensive financial and
operating report submitted for centralized service companies. These are utility
subsidaries that provide services to more than one type of utility (electric, gas, or
oil pipeline) such that they don’t fit into any of the above Forms 1, 2, or 6.
