"""Transform Census PEP FIPS codes data."""

import importlib

import pandas as pd
from dagster import asset

from pudl.metadata import dfs


@asset
def _core_censuspep__yearly_geocodes(raw_censuspep__geocodes):
    """Create a cleaned up table of FIPS codes.

    There are two stages of amendments done to the raw table:

    * we add in a ``state`` column with the state abbreviation
    * we squish the raw two digit ``state_id_fips`` with the raw three digit
      ``county_id_fips`` to get the full 5 digit code (these codes are all
      codes of integers but there are meaningful leading zeros so they have
      nullable string dtypes).

    Note: There are state fips codes in this pudl-compiled POLITICAL_SUBDIVISIONS
    going with using the census based codes instead of our compiled codes even though
    I checked they are all the same. Still feels better to use a primary source.

    """
    territories_and_fixes = pd.read_csv(
        importlib.resources.files("pudl.package_data.censuspep") / "territories.csv",
        dtype=pd.StringDtype(),
    ).astype({"report_date": pd.Int64Dtype()})
    geocodes = pd.concat([raw_censuspep__geocodes, territories_and_fixes])

    state_abbr = (
        dfs.POLITICAL_SUBDIVISIONS.loc[:, ["subdivision_code", "subdivision_name"]]
        .rename(columns={"subdivision_code": "state", "subdivision_name": "area_name"})
        .assign(area_name_tmp=lambda x: x["area_name"].str.lower())
    )
    states = (
        geocodes.loc[geocodes["fips_level"] == "040", ["area_name", "state_id_fips"]]
        .drop_duplicates()
        .assign(area_name_tmp=lambda x: x["area_name"].str.lower())
        .merge(state_abbr, on=["area_name_tmp"], how="left", validate="1:1")[
            ["state", "state_id_fips"]
        ]
    )
    geocodes = geocodes.merge(
        states,
        on=["state_id_fips"],
        how="left",
        validate="m:1",
    ).assign(  # squish the state and county fips together to get the full code
        county_id_fips=lambda x: x.state_id_fips + x.county_id_fips
    )
    return geocodes
