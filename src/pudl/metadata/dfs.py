"""Static database tables."""
import numpy as np
import pandas as pd

FUEL_TYPES_AER_EIA: pd.DataFrame = pd.DataFrame(
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
        ('OTH', 'Other (including nonbiogenic MSW)'),
        ('PC', 'Petroleum Coke'),
        ('RFO', 'Residual Petroleum'),
        ('WND', 'Wind'),
        ('WOC', 'Waste Coal'),
        ('WOO', 'Waste Oil'),
        ('WWW', 'Wood and Wood Waste'),
    ],
).astype({
    "code": pd.StringDtype(),
    "description": pd.StringDtype(),
})
"""
Descriptive labels for aggregated fuel types used in the Annual Energy Review.

See the EIA 923 Fuel Code table (Table 5) for additional information.
"""

CONTRACT_TYPES_EIA: pd.DataFrame = pd.DataFrame(
    columns=["code", "label", "description"],
    data=[
        ('C', 'contract', 'Fuel received under a purchase order or contract with a term of one year or longer.  Contracts with a shorter term are considered spot purchases '),
        ('NC', 'new_contract', 'Fuel received under a purchase order or contract with duration of one year or longer, under which deliveries were first made during the reporting month'),
        ('N', 'new_contract', 'Fuel received under a purchase order or contract with duration of one year or longer, under which deliveries were first made during the reporting month'),
        ('S', 'spot_purchase', pd.NA),
        ('T', 'tolling_agreement',
         'Fuel received under a tolling agreement (bartering arrangement of fuel for generation)'),
    ]
).astype({
    "code": pd.StringDtype(),
    "label": pd.StringDtype(),
    "description": pd.StringDtype(),
})
"""
Descriptive labels for fuel supply contract type codes reported in EIA 923.

The purchase type under which fuel receipts occurred in the reporting month.
"""

NAICS_SECTOR_CONSOLIDATED_EIA: pd.DataFrame = pd.DataFrame(
    columns=["code", "label", "description"],
    data=[
        ("1", "electric_utility", "Traditional regulated electric utilities."),
        ("2", "ipp_non_cogen", "Independent power producers which are not cogenerators."),
        ("3", "ipp_cogen", "Independent power producers which are cogenerators, but whose primary business purpose is the same of electricity to the public."),
        ("4", "commercial_non_cogen", "Commercial non-cogeneration facilities that produce electric power, are connected to the grid, and can sell power to the public."),
        ("5", "commercial_cogen", "Commercial cogeneration facilities that produce electric power, are connected to the grid, and can sell power to the public."),
        ("6", "industrial_non_cogen", "Industrial non-cogeneration facilities that produce electric power, are connected to the grid, and can sell power to the public."),
        ("7", "industrial_cogen", "Industrial cogeneration facilities that produce electric power, are connected to the grid, and can sell power to the public"),
    ]
).astype({
    "code": pd.StringDtype(),
    "label": pd.StringDtype(),
    "description": pd.StringDtype(),
})
"""
Descriptive labels for EIA consolidated NAICS sector codes.

For internal purposes, EIA consolidates NAICS categories into seven groups.
These codes and descriptions are listed on Page 7 of EIA Form 923 EIA’s internal
consolidated NAICS sectors.
"""

PRIME_MOVERS_EIA: pd.DataFrame = pd.DataFrame(
    columns=["code", "label", "description"],
    data=[
        ('BA', 'battery_storage', 'Energy Storage, Battery'),
        ('BT', 'binary_cycle_turbine', 'Turbines Used in a Binary Cycle. Including those used for geothermal applications'),  # nopep8
        ('CA', 'combined_cycle_steam_turbine', 'Combined-Cycle, Steam Turbine Part'),
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
        ('WS', 'wind_offshore', 'Wind Turbine, Offshore'),
        ('WT', 'wind_onshore', 'Wind Turbine, Onshore'),
    ],
).convert_dtypes()
"""Descriptive labels for EIA prime mover codes."""

FUEL_TRANSPORTATION_MODES_EIA: pd.DataFrame = pd.DataFrame(
    columns=["code", "label", "description"],
    data=[
        ("GL", "great_lakes", "Shipments of coal moved to consumers via the Great Lakes. These shipments are moved via the Great Lakes coal loading docks."),  # nopep8
        ("OP", "onsite_production", "Fuel is produced on-site, making fuel shipment unnecessary."),  # nopep8
        ("RR", "rail", "Shipments of fuel moved to consumers by rail (private or public/commercial). Included is coal hauled to or away from a railroad siding by truck if the truck did not use public roads."),  # nopep8
        ("RV", "river", "Shipments of fuel moved to consumers via river by barge.  Not included are shipments to Great Lakes coal loading docks, tidewater piers, or coastal ports."),  # nopep8
        ("PL", "pipeline", "Shipments of fuel moved to consumers by pipeline"),
        ("SP", "slurry_pipeline", "Shipments of coal moved to consumers by slurry pipeline."),  # nopep8
        ("TC", "tramway_conveyor", "Shipments of fuel moved to consumers by tramway or conveyor."),  # nopep8
        ("TP", "tidewater_ports", "Shipments of coal moved to Tidewater Piers and Coastal Ports for further shipments to consumers via coastal water or ocean."),  # nopep8
        ("TR", "truck", "Shipments of fuel moved to consumers by truck.  Not included is fuel hauled to or away from a railroad siding by truck on non-public roads."),  # nopep8
        ("WT", "other_waterways", "Shipments of fuel moved to consumers by other waterways."),  # nopep8
    ],
).convert_dtypes()
"""Descriptive labels for fuel transport modes reported in the EIA 923."""

ENERGY_SOURCES_EIA: pd.DataFrame = pd.DataFrame(
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
        ('MSW', 'municipal_solid_waste', 'short_tons', 9.0, 12.0, 'renewable', 'biomass', 'solid', 'waste', 'Municipal solid waste'),  # nopep8
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
).convert_dtypes()
"""
Descriptive labels and other metadata for energy sources reported by EIA.

This metadata was compiled from Table 28 in the EIA-860 instructions valid
through 2023-05-31, Table 8 in the EIA-923 instructions valid through
2023-05-31, and information in the "file layout" pages in the original
Excel spreadsheets containing fuel data.

"""

FERC_ACCOUNTS: pd.DataFrame = pd.DataFrame(
    columns=['row_number', 'ferc_account_id', 'ferc_account_description'],
    data=[
        # 1. Intangible Plant
        (2, '301', 'Intangible: Organization'),
        (3, '302', 'Intangible: Franchises and consents'),
        (4, '303', 'Intangible: Miscellaneous intangible plant'),
        (5, 'subtotal_intangible', 'Subtotal: Intangible Plant'),
        # 2. Production Plant
        #  A. steam production
        (8, '310', 'Steam production: Land and land rights'),
        (9, '311', 'Steam production: Structures and improvements'),
        (10, '312', 'Steam production: Boiler plant equipment'),
        (11, '313', 'Steam production: Engines and engine-driven generators'),
        (12, '314', 'Steam production: Turbogenerator units'),
        (13, '315', 'Steam production: Accessory electric equipment'),
        (14, '316', 'Steam production: Miscellaneous power plant equipment'),
        (15, '317', 'Steam production: Asset retirement costs for steam production plant'),
        (16, 'subtotal_steam_production', 'Subtotal: Steam Production Plant'),
        #  B. nuclear production
        (18, '320', 'Nuclear production: Land and land rights (Major only)'),
        (19, '321', 'Nuclear production: Structures and improvements (Major only)'),
        (20, '322', 'Nuclear production: Reactor plant equipment (Major only)'),
        (21, '323', 'Nuclear production: Turbogenerator units (Major only)'),
        (22, '324', 'Nuclear production: Accessory electric equipment (Major only)'),
        (23, '325', 'Nuclear production: Miscellaneous power plant equipment (Major only)'),
        (24, '326', 'Nuclear production: Asset retirement costs for nuclear production plant (Major only)'),
        (25, 'subtotal_nuclear_produciton', 'Subtotal: Nuclear Production Plant'),
        #  C. hydraulic production
        (27, '330', 'Hydraulic production: Land and land rights'),
        (28, '331', 'Hydraulic production: Structures and improvements'),
        (29, '332', 'Hydraulic production: Reservoirs, dams, and waterways'),
        (30, '333', 'Hydraulic production: Water wheels, turbines and generators'),
        (31, '334', 'Hydraulic production: Accessory electric equipment'),
        (32, '335', 'Hydraulic production: Miscellaneous power plant equipment'),
        (33, '336', 'Hydraulic production: Roads, railroads and bridges'),
        (34, '337', 'Hydraulic production: Asset retirement costs for hydraulic production plant'),
        (35, 'subtotal_hydraulic_production', 'Subtotal: Hydraulic Production Plant'),
        #  D. other production
        (37, '340', 'Other production: Land and land rights'),
        (38, '341', 'Other production: Structures and improvements'),
        (39, '342', 'Other production: Fuel holders, producers, and accessories'),
        (40, '343', 'Other production: Prime movers'),
        (41, '344', 'Other production: Generators'),
        (42, '345', 'Other production: Accessory electric equipment'),
        (43, '346', 'Other production: Miscellaneous power plant equipment'),
        (44, '347', 'Other production: Asset retirement costs for other production plant'),
        (None, '348', 'Other production: Energy Storage Equipment'),
        (45, 'subtotal_other_production', 'Subtotal: Other Production Plant'),
        (46, 'subtotal_production', 'Subtotal: Production Plant'),
        # 3. Transmission Plant,
        (48, '350', 'Transmission: Land and land rights'),
        (None, '351', 'Transmission: Energy Storage Equipment'),
        (49, '352', 'Transmission: Structures and improvements'),
        (50, '353', 'Transmission: Station equipment'),
        (51, '354', 'Transmission: Towers and fixtures'),
        (52, '355', 'Transmission: Poles and fixtures'),
        (53, '356', 'Transmission: Overhead conductors and devices'),
        (54, '357', 'Transmission: Underground conduit'),
        (55, '358', 'Transmission: Underground conductors and devices'),
        (56, '359', 'Transmission: Roads and trails'),
        (57, '359.1', 'Transmission: Asset retirement costs for transmission plant'),
        (58, 'subtotal_transmission', 'Subtotal: Transmission Plant'),
        # 4. Distribution Plant
        (60, '360', 'Distribution: Land and land rights'),
        (61, '361', 'Distribution: Structures and improvements'),
        (62, '362', 'Distribution: Station equipment'),
        (63, '363', 'Distribution: Storage battery equipment'),
        (64, '364', 'Distribution: Poles, towers and fixtures'),
        (65, '365', 'Distribution: Overhead conductors and devices'),
        (66, '366', 'Distribution: Underground conduit'),
        (67, '367', 'Distribution: Underground conductors and devices'),
        (68, '368', 'Distribution: Line transformers'),
        (69, '369', 'Distribution: Services'),
        (70, '370', 'Distribution: Meters'),
        (71, '371', 'Distribution: Installations on customers\' premises'),
        (72, '372', 'Distribution: Leased property on customers\' premises'),
        (73, '373', 'Distribution: Street lighting and signal systems'),
        (74, '374', 'Distribution: Asset retirement costs for distribution plant'),
        (75, 'subtotal_distribution', 'Subtotal: Distribution Plant'),
        # 5. Regional Transmission and Market Operation Plant
        (77, '380', 'Regional transmission: Land and land rights'),
        (78, '381', 'Regional transmission: Structures and improvements'),
        (79, '382', 'Regional transmission: Computer hardware'),
        (80, '383', 'Regional transmission: Computer software'),
        (81, '384', 'Regional transmission: Communication Equipment'),
        (82, '385', 'Regional transmission: Miscellaneous Regional Transmission and Market Operation Plant'),
        (83, '386', 'Regional transmission: Asset Retirement Costs for Regional Transmission and Market Operation Plant'),
        (84, 'subtotal_regional_transmission',
         'Subtotal: Transmission and Market Operation Plant'),
        (None, '387', 'Regional transmission: [Reserved]'),
        # 6. General Plant
        (86, '389', 'General: Land and land rights'),
        (87, '390', 'General: Structures and improvements'),
        (88, '391', 'General: Office furniture and equipment'),
        (89, '392', 'General: Transportation equipment'),
        (90, '393', 'General: Stores equipment'),
        (91, '394', 'General: Tools, shop and garage equipment'),
        (92, '395', 'General: Laboratory equipment'),
        (93, '396', 'General: Power operated equipment'),
        (94, '397', 'General: Communication equipment'),
        (95, '398', 'General: Miscellaneous equipment'),
        (96, 'subtotal_general', 'Subtotal: General Plant'),
        (97, '399', 'General: Other tangible property'),
        (98, '399.1', 'General: Asset retirement costs for general plant'),
        (99, 'total_general', 'TOTAL General Plant'),
        (100, '101_and_106', 'Electric plant in service (Major only)'),
        (101, '102_purchased', 'Electric plant purchased'),
        (102, '102_sold', 'Electric plant sold'),
        (103, '103', 'Experimental plant unclassified'),
        (104, 'total_electric_plant', 'TOTAL Electric Plant in Service')
    ]
)
"""
FERC electric plant account IDs with associated row numbers and descriptions.

From FERC Form 1 pages 204-207, Electric Plant in Service.
Descriptions from: https://www.law.cornell.edu/cfr/text/18/part-101
"""

FERC_DEPRECIATION_LINES: pd.DataFrame = pd.DataFrame(
    columns=['row_number', 'line_id', 'ferc_account_description'],
    data=[
        # Section A. Balances and Changes During Year
        (1, 'balance_beginning_of_year', 'Balance Beginning of Year'),
        (3, 'depreciation_expense', '(403) Depreciation Expense'),
        (4, 'depreciation_expense_asset_retirement',
         '(403.1) Depreciation Expense for Asset Retirement Costs'),
        (5, 'expense_electric_plant_leased_to_others',
         '(413) Exp. of Elec. Plt. Leas. to Others'),
        (6, 'transportation_expenses_clearing', 'Transportation Expenses-Clearing'),
        (7, 'other_clearing_accounts', 'Other Clearing Accounts'),
        (8, 'other_accounts_specified', 'Other Accounts (Specify, details in footnote):'),
        # blank: might also be other charges like line 17.
        (9, 'other_charges', 'Other Charges:'),
        (10, 'total_depreciation_provision_for_year',
         'TOTAL Deprec. Prov for Year (Enter Total of lines 3 thru 9)'),
        (11, 'net_charges_for_plant_retired', 'Net Charges for Plant Retired:'),
        (12, 'book_cost_of_plant_retired', 'Book Cost of Plant Retired'),
        (13, 'cost_of_removal', 'Cost of Removal'),
        (14, 'salvage_credit', 'Salvage (Credit)'),
        (15, 'total_net_charges_for_plant_retired',
         'TOTAL Net Chrgs. for Plant Ret. (Enter Total of lines 12 thru 14)'),
        (16, 'other_debit_or_credit_items',
         'Other Debit or Cr. Items (Describe, details in footnote):'),
        # blank: can be "Other Charges", e.g. in 2012 for PSCo.
        (17, 'other_charges_2', 'Other Charges 2'),
        (18, 'book_cost_or_asset_retirement_costs_retired',
         'Book Cost or Asset Retirement Costs Retired'),
        (19, 'balance_end_of_year',
         'Balance End of Year (Enter Totals of lines 1, 10, 15, 16, and 18)'),
        # Section B. Balances at End of Year According to Functional Classification
        (20, 'steam_production_end_of_year', 'Steam Production'),
        (21, 'nuclear_production_end_of_year', 'Nuclear Production'),
        (22, 'hydraulic_production_end_of_year', 'Hydraulic Production-Conventional'),
        (23, 'pumped_storage_end_of_year', 'Hydraulic Production-Pumped Storage'),
        (24, 'other_production', 'Other Production'),
        (25, 'transmission', 'Transmission'),
        (26, 'distribution', 'Distribution'),
        (27, 'regional_transmission_and_market_operation',
         'Regional Transmission and Market Operation'),
        (28, 'general', 'General'),
        (29, 'total', 'TOTAL (Enter Total of lines 20 thru 28)'),
    ],
)
"""
Row numbers, FERC account IDs, and FERC account descriptions.

From FERC Form 1 page 219, Accumulated Provision for Depreciation of electric
utility plant (Account 108).
"""
