# EIA Annual Energy Outlook (AEO)

| Source URL                      | [https://www.eia.gov/outlooks/aeo/](https://www.eia.gov/outlooks/aeo/)                                                                                      |
|---------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Source Description              | The EIA Annual Energy Outlook provides projections of future fuel prices, energy supply and consumption, and carbon dioxide emissions by sector and region. |
| Source Format                   | JSON                                                                                                                                                        |
| Download Size                   | 476 MB                                                                                                                                                      |
| Temporal Coverage               | 2023-2026                                                                                                                                                   |
| PUDL Code                       | `eiaaeo`                                                                                                                                                    |
| Unprocessed Source Data Archive | [10.5281/zenodo.10838487](https://doi.org/10.5281/zenodo.10838487)                                                                                          |
| Issues                          | [Open EIA Annual Energy Outlook (AEO) issues](https://github.com/catalyst-cooperative/pudl/issues?utf8=%E2%9C%93&q=is%3Aissue+is%3Aopen+label%3Aeiaaeo)     |

## PUDL Database Tables

We’ve segmented the processed data into the following normalized data tables.
Clicking on the links will show you a description of the table as well as
the names and descriptions of each of its fields.

| Data Dictionary                                                                                                                                                                                    | Browse Online                                                                                                                                                                                                                           |
|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [core_eiaaeo_\_yearly_projected_electric_sales](../data_dictionaries/pudl_db.html.md#core-eiaaeo-yearly-projected-electric-sales)                                                           | [https://data.catalyst.coop/preview/pudl/core_eiaaeo_\_yearly_projected_electric_sales](https://data.catalyst.coop/preview/pudl/core_eiaaeo__yearly_projected_electric_sales)                                                           |
| [core_eiaaeo_\_yearly_projected_energy_use_by_sector_and_type](../data_dictionaries/pudl_db.html.md#core-eiaaeo-yearly-projected-energy-use-by-sector-and-type)                             | [https://data.catalyst.coop/preview/pudl/core_eiaaeo_\_yearly_projected_energy_use_by_sector_and_type](https://data.catalyst.coop/preview/pudl/core_eiaaeo__yearly_projected_energy_use_by_sector_and_type)                             |
| [core_eiaaeo_\_yearly_projected_fuel_cost_in_electric_sector_by_type](../data_dictionaries/pudl_db.html.md#core-eiaaeo-yearly-projected-fuel-cost-in-electric-sector-by-type)               | [https://data.catalyst.coop/preview/pudl/core_eiaaeo_\_yearly_projected_fuel_cost_in_electric_sector_by_type](https://data.catalyst.coop/preview/pudl/core_eiaaeo__yearly_projected_fuel_cost_in_electric_sector_by_type)               |
| [core_eiaaeo_\_yearly_projected_generation_in_electric_sector_by_technology](../data_dictionaries/pudl_db.html.md#core-eiaaeo-yearly-projected-generation-in-electric-sector-by-technology) | [https://data.catalyst.coop/preview/pudl/core_eiaaeo_\_yearly_projected_generation_in_electric_sector_by_technology](https://data.catalyst.coop/preview/pudl/core_eiaaeo__yearly_projected_generation_in_electric_sector_by_technology) |
| [core_eiaaeo_\_yearly_projected_generation_in_end_use_sectors_by_fuel_type](../data_dictionaries/pudl_db.html.md#core-eiaaeo-yearly-projected-generation-in-end-use-sectors-by-fuel-type)   | [https://data.catalyst.coop/preview/pudl/core_eiaaeo_\_yearly_projected_generation_in_end_use_sectors_by_fuel_type](https://data.catalyst.coop/preview/pudl/core_eiaaeo__yearly_projected_generation_in_end_use_sectors_by_fuel_type)   |

## Background

The Annual Energy Outlook (AEO) is a report published by EIA detailing projections and
trends of energy consumption and supply in the US stretching out to 2050. The EIA
generates the data by running a suite of input data through their [National Energy
Modeling System (NEMS)](https://www.eia.gov/outlooks/aeo/nems/documentation/)
([github](https://github.com/EIAgov/NEMS/tree/main)), actively updating the
model to accommodate new scenarios and legislation. The resulting data represent
a wide range of potential outcomes for the US energy sector based on a set of key
assumptions.

Projections are published annually with even years featuring full documentation
and extensive supplemental scenarios and odd years offering simpler, more concise
information. Typical scenarios include:

* Reference case
* Low and high economic growth
* Low and high oil and gas supply
* Low and high oil prices
* Low and high renewable costs

Alternative scenarios may explore potential policy impacts or other relevant changes.
However, it’s important to note that the energy system and accompanying policy
landscape are changing rapidly, and neither NEMS or AEO can capture all the
complexities. For instance, when designing model stipulations for a new year of AEO,
only laws implemented before that year are taken into consideration.

For more information about scenarios, policy assumptions, and more, visit [EIA’s AEO webpage](https://www.eia.gov/outlooks/aeo/).

### Download additional documentation

* [`Eiaaeo 2025 Carbon Capture Allocation Transport And Sequestration Assumptions (PDF)`](_downloads/c90c4e72fd54731ab93abc8be38b65a1/eiaaeo_2025_carbon_capture_allocation_transport_and_sequestration_assumptions.pdf)
* [`Eiaaeo 2025 Case Descriptions (PDF)`](_downloads/5d7431fba21e3f5ae6a9e8c4ff4869db/eiaaeo_2025_case_descriptions.pdf)
* [`Eiaaeo 2025 Coal Market Assumptions (PDF)`](_downloads/73e00bc5b5d9a849704cc6bb9be54c1a/eiaaeo_2025_coal_market_assumptions.pdf)
* [`Eiaaeo 2025 Commercial Demand Assumptions (PDF)`](_downloads/d92ddc3c3255d593915755d5e33d31c9/eiaaeo_2025_commercial_demand_assumptions.pdf)
* [`Eiaaeo 2025 Electricity Market Assumptions (PDF)`](_downloads/fcef77222b0e504440dfa03fa3984d08/eiaaeo_2025_electricity_market_assumptions.pdf)
* [`Eiaaeo 2025 Emissions Policy Assumptions (PDF)`](_downloads/fe375b9ff43bfe98d41823af35c3d491/eiaaeo_2025_emissions_policy_assumptions.pdf)
* [`Eiaaeo 2025 Hydrocarbon Supply Assumptions (PDF)`](_downloads/252d2d500400b47b70c5692166a96d35/eiaaeo_2025_hydrocarbon_supply_assumptions.pdf)
* [`Eiaaeo 2025 Hydrogen Market Assumptions (PDF)`](_downloads/bdb8d5b44c0bda5cdb57ae6bf8265309/eiaaeo_2025_hydrogen_market_assumptions.pdf)
* [`Eiaaeo 2025 Industrial Demand Assumptions (PDF)`](_downloads/ccea91e4be7d818fa8cc23002ccfd8c9/eiaaeo_2025_industrial_demand_assumptions.pdf)
* [`Eiaaeo 2025 International Energy Assumptions (PDF)`](_downloads/cfe9068d3598e04a84ba021f5a41c96c/eiaaeo_2025_international_energy_assumptions.pdf)
* [`Eiaaeo 2025 Liquid Fuels Market Assumptions (PDF)`](_downloads/9c3ccd30529f6af4037ad2a477e94651/eiaaeo_2025_liquid_fuels_market_assumptions.pdf)
* [`Eiaaeo 2025 Macroeconomic Activity Assumptions (PDF)`](_downloads/1785caec55593937c49d501665002ee6/eiaaeo_2025_macroeconomic_activity_assumptions.pdf)
* [`Eiaaeo 2025 Narrative (PDF)`](_downloads/737d5a9cbcaf293b3d74c1089ea96c94/eiaaeo_2025_narrative.pdf)
* [`Eiaaeo 2025 Natural Gas Market Assumptions (PDF)`](_downloads/a42e8e719e504619540e56982c78e626/eiaaeo_2025_natural_gas_market_assumptions.pdf)
* [`Eiaaeo 2025 Renewable Fuels Assumptions (PDF)`](_downloads/ba8ae1e8264be8d75dc26d2043a42b85/eiaaeo_2025_renewable_fuels_assumptions.pdf)
* [`Eiaaeo 2025 Residential Demand Assumptions (PDF)`](_downloads/4f6778d7575d1f108d94747a4522c324/eiaaeo_2025_residential_demand_assumptions.pdf)
* [`Eiaaeo 2025 Summary Of Legislation And Regulations (PDF)`](_downloads/d142fc1b8d5f135033cec12c568b62c7/eiaaeo_2025_summary_of_legislation_and_regulations.pdf)
* [`Eiaaeo 2025 Transportation Demand Assumptions (PDF)`](_downloads/ed1d40a6540e5ea526a5884f1dcaf0cd/eiaaeo_2025_transportation_demand_assumptions.pdf)
* [`Nems Overview (PDF)`](_downloads/b5c53c053a6c984c898cac6b86302be0/nems_overview.pdf)

### Data available through PUDL

PUDL has incorporated data from a small subset of AEO table based on input from the energy
modeling community. If there’s a table you don’t see but would like to, let us know or
consider contributing! You can [open an issue](https://github.com/catalyst-cooperative/pudl/issues/new?template=new_dataset.md)
or email us at [hello@catalyst.coop](mailto:hello@catalyst.coop).

PUDL hosts AEO data from 2023 onward. Each AEO release presents a fresh set of forward
looking projections so we haven’t prioritized integrating the older data. For a look
back at historic projections, poke around the [AEO Retrospective Review](https://www.eia.gov/outlooks/aeo/retrospective/)
published after each even year.

### Who submits this data?

AEO data is created by EIA staff using outputs from NEMS. It does not originate from utility or
plant operator respondents like EIA or FERC form data.

### What does the original data look like?

EIA publishes AEO data in several ways:

* Excel files by case and scenario ([reference case](https://www.eia.gov/outlooks/aeo/tables_ref_xls.php) and [other scenarios](https://www.eia.gov/outlooks/aeo/tables_side_xls.php))
* An [interactive table viewer](https://www.eia.gov/outlooks/aeo/data/browser/#/?id=1-AEO2025&region=0-0&cases=ref2025~hm2025~lm2025~highprice~lowprice~highogs~lowogs~highZTC~lowZTC~nocaa111~alttrnp~aeo2023ref&start=2023&end=2050&f=A&sourcekey=0)
* As [bulk JSON files](https://www.eia.gov/opendata/bulk/manifest.txt)
  (**this is the data integrated into PUDL**)

## Notable Irregularities

There are lots of subtotals embedded within the AEO data! Watch out when aggregating the data.
We have checks in place to make sure the totals are accurate sums of their stated
components [`pudl.transform.eiaaeo.subtotals_match_reported_totals_ratio()`](../autoapi/pudl/transform/eiaaeo/index.html.md#pudl.transform.eiaaeo.subtotals_match_reported_totals_ratio)

## PUDL Data Transformations

To see the transformations applied to the data in each table, you can read the
docstrings for [`pudl.transform.eiaaeo`](../autoapi/pudl/transform/eiaaeo/index.html.md#module-pudl.transform.eiaaeo) created for each table’s
respective transform function.
