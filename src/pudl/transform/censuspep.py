"""Transform Census PEP FIPS codes data."""

import importlib

import pandas as pd
from dagster import asset

from pudl.metadata import dfs


@asset
def _core_censuspep__yearly_geocodes(raw_censuspep__geocodes):
    """Create a cleaned up table of FIPS codes.

    There are three stages of amendments done to the raw table:

    * we add in a ``state`` column with the state abbreviation
    * we squish the raw two digit ``state_id_fips`` with the raw three digit
      ``county_id_fips`` to get the full 5 digit code (these codes are all
      codes of integers but there are meaningful leading zeros so they have
      nullable string dtypes).
    * we add in territories that are missing from Census PEP FIPS codes.

    There are state fips codes in the pudl-compiled POLITICAL_SUBDIVISIONS, but we use
    the census based codes here so that everything is coming from the same primary
    source (though I checked and they are all the same).

    Sources of territories:

    * Main page: https://www.census.gov/library/reference/code-lists/ansi.html
    * txt file: https://www2.census.gov/geo/docs/reference/codes2020/national_county2020.txt

    There are only a small number of territory counties that were not in PEP. For 2020,
    all of the territories are at the bottom of the txt file. PEP had Puerto Rico so
    everything besides PR was migrated over. A fips_level of 050 was added to all of
    the counties to match the fips_level from PEP.
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
