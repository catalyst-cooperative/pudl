This Public Utility Data Liberation (PUDL) toolkit depends on a variety of
other open source tools and public data. We use the
[Anaconda](https://www.anaconda.com/download/) Python 3 distribution, and
manage package dependencies using conda environments. See the PUDL
[`environment.yml`](../environment.yml) file at the top level of the repository
for the most up to date list of required Python packages and their versions.

## Postgresql
PUDL is currently designed to populate a local Postgresql relational database,
so you'll need to install the database server software:
 - [Postgres](https://www.postgresql.org/), version 9.6 or later.

## Public Data:
All that software isn't any good without some data! The raw data comes from the
US government. The [update_datastore.py](../scripts/update_datastore.py)
script efficiently downloads and organizes this data for you, so that PUDL
knows where to find it. Currently the data PUDL can ingest includes:
 - [EIA Form 860](https://www.eia.gov/electricity/data/eia860/)
 - [EIA Form 923](https://www.eia.gov/electricity/data/eia923/)
 - [EPA CEMS Hourly](https://ampd.epa.gov/ampd/)
 - [FERC Form 1](https://www.ferc.gov/docs-filing/forms/form-1/data.asp)
