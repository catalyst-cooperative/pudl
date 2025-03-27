"""Script to assist with metadata improvement project."""

import json

import pudl.metadata.resources as rs

sample_table_names = [
    "_core_eia860__fgd_equipment",
    "_core_eia923__cooling_system_information",
    "_out_eia__monthly_derived_generator_attributes",
    "_out_eia__plants_utilities",
    "_out_eia__yearly_fuel_cost_by_generator",
    "_out_ferc1__yearly_plants_utilities",
    "core_eia861__yearly_utility_data_nerc",
    "core_eia923__monthly_boiler_fuel",
    "core_eia923__monthly_fuel_receipts_costs",
    "core_ferc1__yearly_plant_in_service_sched204",
    "core_ferc1__yearly_transmission_lines_sched422",
    "core_ferc714__respondent_id",
    "core_nrelatb__yearly_projected_financial_cases",
    "out_eia923__monthly_generation_fuel_by_generator",
    "out_eia923__yearly_generation_fuel_by_generator_energy_source",
    "out_ferc1__yearly_balance_sheet_assets_sched110",
    "out_ferc1__yearly_sales_by_rate_schedules_sched304",
    "out_ferc714__hourly_planning_area_demand",
    "out_ferc714__respondents_with_fips",
]


def prefix_lines(text, prefix):
    """Add prefix to each line of given text."""
    return "\n".join(prefix + line for line in text.split("\n")).replace(prefix, "", 1)


FRONT_MATTER = """
[if timeseries] Time series of {short_description}.
[if assn] Links {source1} to {source2}.
[if codes] Explains codes for {purpose}.
[if entity/scd] Entity/SCD table for {short_description}.
[else] {Concise verb/noun indicating type} {short_description}.""".strip()

NO_PRIMARY_KEY = """
This table has no primary key. Each row represents {short_description}."""

STANDARD_SOURCE = """
Derived from {source}."""

NONSTANDARD_SOURCE = """
Sourced from {short_description}."""


def header(text):
    """Return text as an RST header."""
    return "\n" + text + "\n" + "^" * len(text)


USAGE_WARNINGS_HEADER = "Usage Warnings"

ADDL_DETAILS_HEADER = "Additional Details"


def print_template():
    """Print sample of pudl.metadata.resources.RESOURCE_METADATA as a metadata dict using the proposed description template."""
    tab = " " * 4
    print("metadata = {")
    for t in sample_table_names:
        print(f'{tab}"{t}": {{')
        tr = rs.RESOURCE_METADATA[t]
        for k, v in tr.items():
            if k == "description":
                print(f'{tab * 2}"original_description": """')
                print(v)
                print('""",')

                print(f'{tab * 2}"{k}": """')
                print(FRONT_MATTER)
                if (
                    "primary_key" not in tr["schema"]
                    or len(tr["schema"]["primary_key"]) == 0
                ):
                    print(NO_PRIMARY_KEY)
                if len(sources := tr["sources"]) > 0:
                    print(STANDARD_SOURCE.format(source=" and ".join(sources)))
                else:
                    print(NONSTANDARD_SOURCE)
                print(header(USAGE_WARNINGS_HEADER))
                print(header(ADDL_DETAILS_HEADER))
                print(v)
                print('""",')
            else:
                print(
                    f'{tab * 2}"{k}": {prefix_lines(json.dumps(v, sort_keys=True, indent=4), tab * 2)},'
                )
        print(f"{tab}}},")
    #         break
    print("}")


"""
[if timeseries] Time series of {short_description}.
[if assn] Links {source1} to {source2}.
[if codes] Explains codes for {purpose}.
[if entity/scd] Entity/SCD table for {short_description}.

[if no primary key] Each row represents {short_description}.

[if nonstandard source] Sourced from {short_description}.

## Usage warnings

[if missingness differs from source] This table is missing {data} when {condition}. [if necessary] {long_description}

[if harvesting] Some {columns} are null when {condition}

[...]
"""

if __name__ == "__main__":
    print_template()
