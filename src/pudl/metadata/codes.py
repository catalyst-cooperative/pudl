"""Metadata for cleaning, re-encoding, and documenting coded data columns.

These dictionaries are used to create Encoder instances. They contain the following keys:
'df': A dataframe associating short codes with long descriptions and other information.
'code_fixes': A dictionary mapping non-standard codes to canonical, standardized codes.
'ignored_codes': A list of non-standard codes which appear in the data, and will be set to NA.
"""
from typing import Any, Dict

import numpy as np
import pandas as pd

CODE_METADATA: Dict[str, Dict[str, Any]] = {
    "coalmine_types_eia": {
        "df": pd.DataFrame(
            columns=['code', 'label', 'description'],
            data=[
                ('P', 'preparation_plant', 'A coal preparation plant.'),
                ('S', 'surface', 'A surface mine.'),
                ('U', 'underground', 'An underground mine.'),
                ('US', 'underground_and_surface',
                 'Both an underground and surface mine with most coal extracted from underground'),
                ('SU', 'surface_and_underground',
                 'Both an underground and surface mine with most coal extracted from surface'),
            ]
        ).convert_dtypes(),
        "code_fixes": {
            'p': 'P',
            'U/S': 'US',
            'S/U': 'SU',
            'Su': 'S',
        },
        "ignored_codes": [],
    },

    "power_purchase_types_ferc1": {
        "df": pd.DataFrame(
            columns=['code', 'label', 'description'],
            data=[
                ('AD', 'adjustment', 'Out-of-period adjustment. Use this code for any accounting adjustments or "true-ups" for service provided in prior reporting years. Provide an explanation in a footnote for each adjustment.'),
                ('EX', 'electricity_exchange', 'Exchanges of electricity. Use this category for transactions involving a balancing of debits and credits for energy, capacity, etc.  and any settlements for imbalanced exchanges.'),
                ('IF', 'intermediate_firm', 'Intermediate-term firm service. The same as LF service expect that "intermediate-term" means longer than one year but less than five years.'),
                ('IU', 'intermediate_unit', 'Intermediate-term service from a designated generating unit. The same as LU service expect that "intermediate-term" means longer than one year but less than five years.'),
                ('LF', 'long_firm', 'Long-term firm service. "Long-term" means five years or longer and "firm" means that service cannot be interrupted for economic reasons and is intended to remain reliable even under adverse conditions (e.g., the supplier must attempt to buy emergency energy from third parties to maintain deliveries of LF service). This category should not be used for long-term firm service firm service which meets the definition of RQ service. For all transaction identified as LF, provide in a footnote the termination date of the contract defined as the earliest date that either buyer or seller can unilaterally get out of the contract.'),
                ('LU', 'long_unit', 'Long-term service from a designated generating unit. "Long-term" means five years or longer. The availability and reliability of service, aside from transmission constraints, must match the availability and reliability of the designated unit.'),
                ('OS', 'other_service', 'Other service. Use this category only for those services which cannot be placed in the above-defined categories, such as all non-firm service regardless of the Length of the contract and service from designated units of Less than one year. Describe the nature of the service in a footnote for each adjustment.'),
                ('RQ', 'requirement', 'Requirements service. Requirements service is service which the supplier plans to provide on an ongoing basis (i.e., the supplier includes projects load for this service in its system resource planning). In addition, the reliability of requirement service must be the same as, or second only to, the supplier’s service to its own ultimate consumers.'),
                ('SF', 'short_firm', 'Short-term service. Use this category for all firm services, where the duration of each period of commitment for service is one year or less.'),
            ]
        ).convert_dtypes(),
        "code_fixes": {},
        "ignored_codes": [
            '', 'To', 'A"', 'B"', 'C"', 'ÿ\x16', 'NA', ' -', '-', 'OC', 'N/', 'Pa', '0',
        ],
    },

    "momentary_interruptions_eia": {
        "df": pd.DataFrame(
            columns=['code', 'label', 'description'],
            data=[
                ('L', 'less_than_1_minute',
                 'Respondent defines a momentary interruption as less than 1 minute.'),
                ('F', 'less_than_5_minutes',
                 'Respondent defines a momentary interruption as less than 5 minutes.'),
                ('O', 'other', 'Respondent defines a momentary interruption using some other criteria.'),
            ]
        ).convert_dtypes(),
        "code_fixes": {},
        "ignored_codes": [],
    },

    "entity_types_eia": {
        "df": pd.DataFrame(
            columns=[
                'code',
                'label',
                'description',
            ],
            data=[
                ('A', 'municipal_marketing_authority', 'Municipal Marketing Authority. Voted into existence by the residents of a municipality and given authority for creation by the state government. They are nonprofit organizations'),
                ('B', 'behind_the_meter', 'Behind the Meter. Entities that install, own, and/or operate a system (usually photovoltaic), and sell, under a long term power purchase agreement (PPA) or lease, all the production from the system to the homeowner or business with which there is a net metering agreement. Third Party Owners (TPOs) of PV solar installations use this ownership code.'),
                ('C', 'cooperative', 'Cooperative. Member-owned organizations.'),
                ('COM', 'commercial', 'Commercial facility.'),
                ('D', 'nonutility_dsm_administrator',
                 'Non-utility DSM Administrator. Only involved with Demand-Side Management activities.'),
                ('F', 'federal', 'Federal. Government agencies with the authority to deliver energy to end-use customers.'),
                ('G', 'community_choice_aggregator', 'Community Choice Aggregator.'),
                ('I', 'investor_owned',
                 'Investor-owned Utilities. Entities that are privately owned and provide a public service.'),
                ('IND', 'industrial', 'Industrial facility.'),
                ('M', 'municipal', 'Municipal: Entities that are organized under authority of state statute to provide a public service to residents of that area.'),
                ('O', 'other', 'Other entity type.'),
                ('P', 'political_subdivision', 'Political Subdivision. (also called "public utility district"): Independent of city or county government and voted into existence by a majority of the residents of any given area for the specific purpose of providing utility service to the voters. State laws provide for the formation of such districts.'),
                ('PO', 'power_marketer', 'Power marketer.'),
                ('PR', 'private', 'Private entity.'),
                ('Q', 'independent_power_producer',
                 'Independent Power Producer or Qualifying Facility. Entities that own power plants and sell their power into the wholesale market.'),
                ('R', 'retail_power_marketer',
                 'Retail Power Marketer or Energy Service Provider: Entities that market power to customers in restructured markets.'),
                ('S', 'state', 'State entities that own or operate facilities or provide a public service.'),
                ('T', 'transmission', 'Transmission: Entities that operate or own high voltage transmission wires that provide bulk power services.'),
                ('U', 'unknown', 'Unknown entity type.'),
                ('W', 'wholesale_power_marketer',
                 'Wholesale Power Marketer: Entities that buy and sell power in the wholesale market.'),
            ]
        ).convert_dtypes(),

        "code_fixes": {
            'Behind the Meter': 'B',
            'Community Choice Aggregator': 'G',
            'Cooperative': 'C',
            'Facility': 'Q',
            'Federal': 'F',
            'Investor Owned': 'I',
            'Municipal': 'M',
            'Political Subdivision': 'P',
            'Power Marketer': 'PO',
            'Retail Power Marketer': 'R',
            'State': 'S',
            'Unregulated': 'Q',
            'Wholesale Power Marketer': 'W',
        },
        "ignored_codes": [],
    },

    "energy_sources_eia": {
        "df": pd.DataFrame(
            columns=[
                "code",
                "label",
                "fuel_units",
                "min_fuel_mmbtu_per_unit",
                "max_fuel_mmbtu_per_unit",
                "fuel_group_eia",
                "fuel_derived_from",
                "fuel_phase",
                "fuel_type_code_pudl",
                "description"
            ],
            data=[
                ('AB', 'agricultural_byproducts', 'short_tons', 7.0, 18.0, 'renewable', 'biomass', 'solid', 'waste', 'Agricultural by-products'),  # nopep8
                ('ANT', 'anthracite', 'short_tons', 22.0, 28.0, 'fossil', 'coal', 'solid', 'coal', 'Anthracite coal'),  # nopep8
                ('BFG', 'blast_furnace_gas', 'mcf', 0.07, 0.12, 'fossil', 'gas', 'gas', 'gas', 'Blast furnace gas'),  # nopep8
                ('BIT', 'bituminous_coal', 'short_tons', 20.0, 29.0, 'fossil', 'coal', 'solid', 'coal', 'Bituminous coal'),  # nopep8
                ('BLQ', 'black_liquor', 'short_tons', 10.0, 14.0, 'renewable', 'biomass', 'liquid', 'waste', 'Black liquor'),  # nopep8
                ('DFO', 'distillate_fuel_oil', 'barrels', 5.5, 6.2, 'fossil', 'petroleum', 'liquid', 'oil', 'Distillate fuel oil, including diesel, No. 1, No. 2, and No. 4 fuel oils'),  # nopep8
                ('GEO', 'geothermal', pd.NA, np.nan, np.nan, 'renewable', 'other', pd.NA, 'other', 'Geothermal'),  # nopep8
                ('JF', 'jet_fuel', 'barrels', 5.0, 6.0, 'fossil', 'petroleum', 'liquid', 'oil', 'Jet fuel'),  # nopep8
                ('KER', 'kerosene', 'barrels', 5.6, 6.1, 'fossil', 'petroleum', 'liquid', 'oil', 'Kerosene'),  # nopep8
                ('LFG', 'landfill_gas', 'mcf', 0.3, 0.6, 'renewable', 'biomass', 'gas', 'waste', 'Landfill gas'),  # nopep8
                ('LIG', 'lignite', 'short_tons', 10.0, 14.5, 'fossil', 'coal', 'solid', 'coal', 'Lignite coal'),  # nopep8
                ('MSB', 'municipal_solid_waste_biogenic', 'short_tons', 9.0, 12.0, 'renewable', 'biomass', 'solid', 'waste', 'Municipal solid waste (biogenic)'),  # nopep8
                ('MSN', 'municipal_solid_nonbiogenic', 'short_tons', 9.0, 12.0, 'fossil', 'petroleum', 'solid', 'waste', 'Municipal solid waste (non-biogenic)'),  # nopep8
                ('MSW', 'municipal_solid_waste', 'short_tons', 9.0, 12.0, 'renewable', 'biomass', 'solid', 'waste', 'Municipal solid waste (all types)'),  # nopep8
                ('MWH', 'electricity_storage', 'mwh', np.nan, np.nan, 'other', 'other', pd.NA, 'other', 'Electricity used for electricity storage'),  # nopep8
                ('NG', 'natural_gas', 'mcf', 0.8, 1.1, 'fossil', 'gas', 'gas', 'gas', 'Natural gas'),  # nopep8
                ('NUC', 'nuclear', pd.NA, np.nan, np.nan, 'other', 'other', pd.NA, 'nuclear', 'Nuclear, including uranium, plutonium, and thorium'),  # nopep8
                ('OBG', 'other_biomass_gas', 'mcf', 0.36, 1.6, 'renewable', 'biomass', 'gas', 'waste', 'Other biomass gas, including digester gas, methane, and other biomass gasses'),  # nopep8
                ('OBL', 'other_biomass_liquid', 'barrels', 3.5, 4.0, 'renewable', 'biomass', 'liquid', 'waste', 'Other biomass liquids'),  # nopep8
                ('OBS', 'other_biomass_solid', 'short_tons', 8.0, 25.0, 'renewable', 'biomass', 'solid', 'waste', 'Other biomass solids'),  # nopep8
                ('OG', 'other_gas', 'mcf', 0.32, 3.3, 'fossil', 'other', 'gas', 'gas', 'Other gas'),  # nopep8
                ('OTH', 'other', pd.NA, np.nan, np.nan, 'other', 'other', pd.NA, 'other', 'Other'),  # nopep8
                ('PC', 'petroleum_coke', 'short_tons', 24.0, 30.0, 'fossil', 'petroleum', 'solid', 'coal', 'Petroleum coke'),  # nopep8
                ('PG', 'propane_gas', 'mcf', 2.5, 2.75, 'fossil', 'petroleum', 'gas', 'gas', 'Gaseous propane'),  # nopep8
                ('PUR', 'purchased_steam', pd.NA, np.nan, np.nan, 'other', 'other', pd.NA, 'other', 'Purchased steam'),  # nopep8
                ('RC', 'refined_coal', 'short_tons', 20.0, 29.0, 'fossil', 'coal', 'solid', 'coal', 'Refined coal'),  # nopep8
                ('RFO', 'residual_fuel_oil', 'barrels', 5.7, 6.9, 'fossil', 'petroleum', 'liquid', 'oil', 'Residual fuel oil, including Nos. 5 & 6 fuel oils and bunker C fuel oil'),  # nopep8
                ('SC', 'coal_synfuel', 'short_tons', np.nan, np.nan, 'fossil', 'coal', 'solid', 'coal', 'Coal synfuel. Coal-based solid fuel that has been processed by a coal synfuel plant, and coal-based fuels such as briquettes, pellets, or extrusions, which are formed from fresh or recycled coal and binding materials.'),  # nopep8
                ('SG', 'syngas_other', 'mcf', np.nan, np.nan, 'fossil', 'other', 'gas', 'gas', 'Synthetic gas, other than coal-derived'),  # nopep8
                ('SGC', 'syngas_coal', 'mcf', 0.2, 0.3, 'fossil', 'coal', 'gas', 'gas', 'Coal-derived synthesis gas'),  # nopep8
                ('SGP', 'syngas_petroleum_coke', 'mcf', 0.2, 1.1, 'fossil', 'petroleum', 'gas', 'gas', 'Synthesis gas from petroleum coke'),  # nopep8
                ('SLW', 'sludge_waste', 'short_tons', 10.0, 16.0, 'renewable', 'biomass', 'liquid', 'waste', 'Sludge waste'),  # nopep8
                ('SUB', 'subbituminous_coal', 'short_tons', 15.0, 20.0, 'fossil', 'coal', 'solid', 'coal', 'Sub-bituminous coal'),  # nopep8
                ('SUN', 'solar', pd.NA, np.nan, np.nan, 'renewable', 'other', pd.NA, 'solar', 'Solar'),  # nopep8
                ('TDF', 'tire_derived_fuels', 'short_tons', 16.0, 32.0, 'other', 'other', 'solid', 'waste', 'Tire-derived fuels'),  # nopep8
                ('WAT', 'water', pd.NA, np.nan, np.nan, 'renewable', 'other', pd.NA, 'hydro', 'Water at a conventional hydroelectric turbine, and water used in wave buoy hydrokinetic technology, current hydrokinetic technology, and tidal hydrokinetic technology, or pumping energy for reversible (pumped storage) hydroelectric turbine'),  # nopep8
                ('WC', 'waste_coal', 'short_tons', 6.5, 16.0, 'fossil', 'coal', 'solid', 'coal', 'Waste/Other coal, including anthracite culm, bituminous gob, fine coal, lignite waste, waste coal.'),  # nopep8
                ('WDL', 'wood_liquids', 'barrels', 8.0, 14.0, 'renewable', 'biomass', 'liquid', 'waste', 'Wood waste liquids excluding black liquor, including red liquor, sludge wood, spent sulfite liquor, and other wood-based liquids'),  # nopep8
                ('WDS', 'wood_solids', 'short_tons', 7.0, 18.0, 'renewable', 'biomass', 'solid', 'waste', 'Wood/Wood waste solids, including paper pellets, railroad ties, utility poles, wood chips, park, and wood waste solids'),  # nopep8
                ('WH', 'waste_heat', pd.NA, np.nan, np.nan, 'other', 'other', pd.NA, 'other', 'Waste heat not directly attributed to a fuel source. WH should only be reported when the fuel source is undetermined, and for combined cycle steam turbines that do not have supplemental firing.'),  # nopep8
                ('WND', 'wind', pd.NA, np.nan, np.nan, 'renewable', 'other', pd.NA, 'wind', 'Wind'),  # nopep8
                ('WO', 'waste_oil', 'barrels', 3.0, 5.8, 'fossil', 'petroleum', 'liquid', 'oil', 'Waste/Other oil, including crude oil, liquid butane, liquid propane, naptha, oil waste, re-refined motor oil, sludge oil, tar oil, or other petroleum-based liquid wastes'),  # nopep8
            ],
        ).convert_dtypes(),
        "code_fixes": {
            "BL": "BLQ",
            "HPS": "WAT",
            "ng": "NG",
            "WOC": "WC",
            "OW": "WO",
            "WT": "WND",
            "H2": "OG",
            "OOG": "OG",
        },
        "ignored_codes": [
            0, '0', 'OO', 'BM', 'CBL', 'COL', 'N', 'no', 'PL', 'ST',
        ]
    },

    "fuel_transportation_modes_eia": {
        "df": pd.DataFrame(
            columns=["code", "label", "description"],
            data=[
                ("GL", "great_lakes", "Shipments of coal moved to consumers via the Great Lakes. These shipments are moved via the Great Lakes coal loading docks."),  # nopep8
                ("OP", "onsite_production", "Fuel is produced on-site, making fuel shipment unnecessary."),  # nopep8
                ("RR", "rail", "Shipments of fuel moved to consumers by rail (private or public/commercial). Included is coal hauled to or away from a railroad siding by truck if the truck did not use public roads."),  # nopep8
                ("RV", "river", "Shipments of fuel moved to consumers via river by barge.  Not included are shipments to Great Lakes coal loading docks, tidewater piers, or coastal ports."),  # nopep8
                ("PL", "pipeline", "Shipments of fuel moved to consumers by pipeline"),
                ("SP", "slurry_pipeline", "Shipments of coal moved to consumers by slurry pipeline."),  # nopep8
                ("TC", "tramway_conveyor", "Shipments of fuel moved to consumers by tramway or conveyor."),  # nopep8
                ("TP", "tidewater_port", "Shipments of coal moved to Tidewater Piers and Coastal Ports for further shipments to consumers via coastal water or ocean."),  # nopep8
                ("TR", "truck", "Shipments of fuel moved to consumers by truck.  Not included is fuel hauled to or away from a railroad siding by truck on non-public roads."),  # nopep8
                ("WT", "other_waterway", "Shipments of fuel moved to consumers by other waterways."),  # nopep8
            ],
        ).convert_dtypes(),
        "code_fixes": {
            "TK": "TR",
            "tk": "TR",
            "tr": "TR",
            "WA": "WT",
            "wa": "WT",
            "CV": "TC",
            "cv": "TC",
            "rr": "RR",
            "pl": "PL",
            "rv": "RV",
        },
        "ignored_codes": ["UN"]
    },

    "fuel_types_aer_eia": {
        "df": pd.DataFrame(
            columns=["code", "description"],
            data=[
                ('SUN', 'Solar PV and thermal'),
                ('COL', 'Coal'),
                ('DFO', 'Distillate Petroleum'),
                ('GEO', 'Geothermal'),
                ('HPS', 'Hydroelectric Pumped Storage'),
                ('HYC', 'Hydroelectric Conventional'),
                ('MLG', 'Biogenic Municipal Solid Waste and Landfill Gas'),
                ('NG', 'Natural Gas'),
                ('NUC', 'Nuclear'),
                ('OOG', 'Other Gases'),
                ('ORW', 'Other Renewables'),
                ('OTH', 'Other (including Nonbiogenic Municipal Solid Waste)'),
                ('PC', 'Petroleum Coke'),
                ('RFO', 'Residual Petroleum'),
                ('WND', 'Wind'),
                ('WOC', 'Waste Coal'),
                ('WOO', 'Waste Oil'),
                ('WWW', 'Wood and Wood Waste'),
            ],
        ).convert_dtypes(),
        "code_fixes": {},
        "ignored_codes": []
    },

    "contract_types_eia": {
        "df": pd.DataFrame(
            columns=["code", "label", "description"],
            data=[
                ('C', 'contract', 'Fuel received under a purchase order or contract with a term of one year or longer.  Contracts with a shorter term are considered spot purchases '),  # nopep8
                ('NC', 'new_contract', 'Fuel received under a purchase order or contract with duration of one year or longer, under which deliveries were first made during the reporting month'),  # nopep8
                ('S', 'spot_purchase', 'Fuel obtained through a spot market purchase'),
                ('T', 'tolling_agreement', 'Fuel received under a tolling agreement (bartering arrangement of fuel for generation)'),  # nopep8
            ]
        ).convert_dtypes(),
        "code_fixes": {"N": "NC"},
        "ignored_codes": []
    },

    "prime_movers_eia": {
        "df": pd.DataFrame(
            columns=["code", "label", "description"],
            data=[
                ('BA', 'battery_storage', 'Energy Storage, Battery'),
                ('BT', 'binary_cycle_turbine', 'Turbines Used in a Binary Cycle. Including those used for geothermal applications'),  # nopep8
                ('CA', 'combined_cycle_steam_turbine',
                 'Combined-Cycle, Steam Turbine Part'),
                ('CC', 'combined_cycle_total', 'Combined-Cycle, Total Unit'),
                ('CE', 'compressed_air_storage', 'Energy Storage, Compressed Air'),
                ('CP', 'concentrated_solar_storage', 'Energy Storage, Concentrated Solar Power'),  # nopep8
                ('CS', 'combined_cycle_single_shaft', 'Combined-Cycle Single-Shaft Combustion Turbine and Steam Turbine share of single'),  # nopep8
                ('CT', 'combined_cycle_combustion_turbine', 'Combined-Cycle Combustion Turbine Part'),  # nopep8
                ('ES', 'other_storage', 'Energy Storage, Other (Specify on Schedule 9, Comments)'),  # nopep8
                ('FC', 'fuel_cell', 'Fuel Cell'),
                ('FW', 'flywheel_storage', 'Energy Storage, Flywheel'),
                ('GT', 'gas_combustion_turbine', 'Combustion (Gas) Turbine. Including Jet Engine design'),  # nopep8
                ('HA', 'hydrokinetic_axial_flow', 'Hydrokinetic, Axial Flow Turbine'),
                ('HB', 'hydrokinetic_wave_buoy', 'Hydrokinetic, Wave Buoy'),
                ('HK', 'hydrokinetic_other', 'Hydrokinetic, Other'),
                ('HY', 'hydraulic_turbine', 'Hydraulic Turbine. Including turbines associated with delivery of water by pipeline.'),  # nopep8
                ('IC', 'internal_combustion', 'Internal Combustion (diesel, piston, reciprocating) Engine'),  # nopep8
                ('OT', 'other', 'Other'),
                ('PS', 'pumped_storage', 'Energy Storage, Reversible Hydraulic Turbine (Pumped Storage)'),  # nopep8
                ('PV', 'solar_pv', 'Solar Photovoltaic'),
                ('ST', 'steam_turbine', 'Steam Turbine. Including Nuclear, Geothermal, and Solar Steam (does not include Combined Cycle).'),  # nopep8
                ('UNK', 'unknown', 'Unknown prime mover.'),
                ('WS', 'wind_offshore', 'Wind Turbine, Offshore'),
                ('WT', 'wind_onshore', 'Wind Turbine, Onshore'),
            ],
        ).convert_dtypes(),
        "code_fixes": {},
        "ignored_codes": []
    },

    "sector_consolidated_eia": {
        "df": pd.DataFrame(
            columns=["code", "label", "description"],
            data=[
                (1, "electric_utility", "Traditional regulated electric utilities."),
                (2, "ipp_non_cogen", "Independent power producers which are not cogenerators."),
                (3, "ipp_cogen", "Independent power producers which are cogenerators, but whose primary business purpose is the same of electricity to the public."),
                (4, "commercial_non_cogen", "Commercial non-cogeneration facilities that produce electric power, are connected to the grid, and can sell power to the public."),
                (5, "commercial_cogen", "Commercial cogeneration facilities that produce electric power, are connected to the grid, and can sell power to the public."),
                (6, "industrial_non_cogen", "Industrial non-cogeneration facilities that produce electric power, are connected to the grid, and can sell power to the public."),
                (7, "industrial_cogen", "Industrial cogeneration facilities that produce electric power, are connected to the grid, and can sell power to the public"),
            ]
        ).convert_dtypes(),
        "code_fixes": {},
        "ignored_codes": []
    }
}
