# pudl.analysis.record_linkage.eia_ferc1_model_config

The model parameters for the FERC1 to EIA splink record linkage model.

This module enumerates the blocking rules as well as the comparison levels
for the matching columns that are used in the FERC1 to EIA record linkage
model.

## Attributes

| [`blocking_rule_1`](#pudl.analysis.record_linkage.eia_ferc1_model_config.blocking_rule_1)                |    |
|---------------------------------------------------------------------------------|----|
| [`blocking_rule_2`](#pudl.analysis.record_linkage.eia_ferc1_model_config.blocking_rule_2)                |    |
| [`blocking_rule_3`](#pudl.analysis.record_linkage.eia_ferc1_model_config.blocking_rule_3)                |    |
| [`blocking_rule_4`](#pudl.analysis.record_linkage.eia_ferc1_model_config.blocking_rule_4)                |    |
| [`blocking_rule_5`](#pudl.analysis.record_linkage.eia_ferc1_model_config.blocking_rule_5)                |    |
| [`blocking_rule_6`](#pudl.analysis.record_linkage.eia_ferc1_model_config.blocking_rule_6)                |    |
| [`blocking_rule_7`](#pudl.analysis.record_linkage.eia_ferc1_model_config.blocking_rule_7)                |    |
| [`blocking_rule_8`](#pudl.analysis.record_linkage.eia_ferc1_model_config.blocking_rule_8)                |    |
| [`blocking_rule_9`](#pudl.analysis.record_linkage.eia_ferc1_model_config.blocking_rule_9)                |    |
| [`blocking_rule_10`](#pudl.analysis.record_linkage.eia_ferc1_model_config.blocking_rule_10)               |    |
| [`BLOCKING_RULES`](#pudl.analysis.record_linkage.eia_ferc1_model_config.BLOCKING_RULES)                 |    |
| [`plant_name_comparison`](#pudl.analysis.record_linkage.eia_ferc1_model_config.plant_name_comparison)          |    |
| [`utility_name_comparison`](#pudl.analysis.record_linkage.eia_ferc1_model_config.utility_name_comparison)        |    |
| [`fuel_type_code_pudl_comparison`](#pudl.analysis.record_linkage.eia_ferc1_model_config.fuel_type_code_pudl_comparison) |    |
| [`capacity_comparison`](#pudl.analysis.record_linkage.eia_ferc1_model_config.capacity_comparison)            |    |
| [`net_gen_comparison`](#pudl.analysis.record_linkage.eia_ferc1_model_config.net_gen_comparison)             |    |
| [`installation_year_comparison`](#pudl.analysis.record_linkage.eia_ferc1_model_config.installation_year_comparison)   |    |
| [`construction_year_comparison`](#pudl.analysis.record_linkage.eia_ferc1_model_config.construction_year_comparison)   |    |
| [`COMPARISONS`](#pudl.analysis.record_linkage.eia_ferc1_model_config.COMPARISONS)                    |    |

## Functions

| [`get_date_comparison`](#pudl.analysis.record_linkage.eia_ferc1_model_config.get_date_comparison)(column_name)   | Get date comparison template for column.   |
|-------------------------------------------------------------------------------------|--------------------------------------------|

## Module Contents

### pudl.analysis.record_linkage.eia_ferc1_model_config.blocking_rule_1

### pudl.analysis.record_linkage.eia_ferc1_model_config.blocking_rule_2

### pudl.analysis.record_linkage.eia_ferc1_model_config.blocking_rule_3

### pudl.analysis.record_linkage.eia_ferc1_model_config.blocking_rule_4

### pudl.analysis.record_linkage.eia_ferc1_model_config.blocking_rule_5

### pudl.analysis.record_linkage.eia_ferc1_model_config.blocking_rule_6

### pudl.analysis.record_linkage.eia_ferc1_model_config.blocking_rule_7

### pudl.analysis.record_linkage.eia_ferc1_model_config.blocking_rule_8

### pudl.analysis.record_linkage.eia_ferc1_model_config.blocking_rule_9

### pudl.analysis.record_linkage.eia_ferc1_model_config.blocking_rule_10

### pudl.analysis.record_linkage.eia_ferc1_model_config.BLOCKING_RULES *: [list](https://docs.python.org/3/builtins/stdtypes.html#list)[splink.internals.blocking_rule_creator.BlockingRuleCreator | [dict](https://docs.python.org/3/builtins/stdtypes.html#dict)[[str](https://docs.python.org/3/builtins/stdtypes.html#str), Any]]*

### pudl.analysis.record_linkage.eia_ferc1_model_config.plant_name_comparison

### pudl.analysis.record_linkage.eia_ferc1_model_config.utility_name_comparison

### pudl.analysis.record_linkage.eia_ferc1_model_config.fuel_type_code_pudl_comparison

### pudl.analysis.record_linkage.eia_ferc1_model_config.capacity_comparison

### pudl.analysis.record_linkage.eia_ferc1_model_config.net_gen_comparison

### pudl.analysis.record_linkage.eia_ferc1_model_config.get_date_comparison(column_name)

Get date comparison template for column.

### pudl.analysis.record_linkage.eia_ferc1_model_config.installation_year_comparison

### pudl.analysis.record_linkage.eia_ferc1_model_config.construction_year_comparison

### pudl.analysis.record_linkage.eia_ferc1_model_config.COMPARISONS *: [list](https://docs.python.org/3/builtins/stdtypes.html#list)[splink.internals.comparison_creator.ComparisonCreator | [dict](https://docs.python.org/3/builtins/stdtypes.html#dict)[[str](https://docs.python.org/3/builtins/stdtypes.html#str), Any]]*
