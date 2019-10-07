#!/usr/bin/env python3
# coding: utf-8

import pandas as pd
import pudl

# Files read in:
plants_entity_csv = (
    "../pudl/datapackage/pudl-eia/eia-860-923/data/plants_entity_eia.csv"
)
fips_data_url = "https://www2.census.gov/programs-surveys/popest/geographies/2018/all-geocodes-v2018.xlsx"
county_correction_csv = "county_corrected_spelling.csv"


def download_fips_codes(fips_data_url):
    fips_col_renames = {
        "State Code (FIPS)": "fips_state",
        "County Code (FIPS)": "fips_county",
        "Area Name (including legal/statistical area description)": "name",
        "Summary Level": "summary_level",
    }

    fips_codes = pd.read_excel(
        fips_data_url,
        header=4,
        usecols=[
            "Summary Level",
            "State Code (FIPS)",
            "County Code (FIPS)",
            "Area Name (including legal/statistical area description)",
        ],
    ).rename(columns=fips_col_renames)

    return fips_codes


def load_state_fips(fips_codes):
    state_abbrevs = (
        pd.Series(pudl.constants.us_states)
        .reset_index()
        .rename(columns={"index": "state", 0: "name"})
    )
    state_fips = fips_codes.query("summary_level == 40").merge(
        state_abbrevs, on="name"
    )[["fips_state", "state"]]

    if state_fips.shape != (52, 2):
        raise AssertionError("State abbrevs dict is wrong.")

    return state_fips


def load_county_fips(fips_codes):

    # Pull the counties out of the full set of FIPS codes.
    # Virginia is trouble because there are cities outside of counties.
    county_fips = fips_codes.query(
        "(summary_level == 50) | (fips_state == 51 & name == 'Alexandria city')"
    )

    # Make the names easier to merge on.
    county_fips.loc[:, "county"] = (
        county_fips.loc[:, "name"]
        .replace(
            "\s*(County|Municipio|Borough|Census Area|Parish|Municipality)$",
            "",
            regex=True,
        )
        .replace("\s*City and$", "", regex=True)
        .replace("['\.]", "", regex=True)
        .replace("ñ", "n", regex=True)
        .replace("-", " ", regex=True)
    ).str.lower()
    del county_fips["name"]

    return county_fips


def merge_fips(plants, fips_codes, county_correction_csv):

    state_fips = load_state_fips(fips_codes)
    county_fips = load_county_fips(fips_codes)

    merged_state_fips = pd.merge(
        plants, state_fips, on="state", how="left", indicator="match"
    )

    if merged_state_fips.query("match != 'both' & state.notna()").shape[0] > 0:
        raise AssertionError("Failed to match state abbreviations")
    del merged_state_fips["match"]

    # Load a few hand corrections.
    # These corrections are all lower case because that's the normalization I've
    # applied ot the other parts.
    county_corrections = pd.read_csv(county_correction_csv)
    # Make the names easier to merge on.
    merged_state_fips.loc[:, "county"] = merged_state_fips.county.str.lower().replace(
        "'", "", regex=True
    )
    merged_state_fips = merged_state_fips.merge(
        county_corrections,
        left_on=["state", "county"],
        right_on=["state", "eia_spelling"],
        how="left",
    )

    merged_state_fips.loc[
        :, "county"
    ] = merged_state_fips.standard_spelling.combine_first(merged_state_fips.county)
    del merged_state_fips["eia_spelling"]
    del merged_state_fips["standard_spelling"]

    merged_fips = merged_state_fips.merge(
        county_fips, on=["fips_state", "county"], how="left", indicator="merge"
    )

    missing_county_match = merged_fips.query(
        "merge == 'left_only' & state.notna() & county.notna() & county != 'not in file'"
    )
    if missing_county_match.shape[0] > 0:
        print(missing_county_match)
        raise AssertionError("Failed to match some counties")

    mapping = merged_fips[["plant_id_eia", "fips_state", "fips_county"]].astype("Int32")

    return mapping


if __name__ == "__main__":
    plants = pd.read_csv(plants_entity_csv, usecols=["plant_id_eia", "county", "state"])
    fips_codes = download_fips_codes(fips_data_url)
    mapping = merge_fips(plants, fips_codes, county_correction_csv)
    print(mapping)
