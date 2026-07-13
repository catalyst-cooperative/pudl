# pudl.metadata.resources.eia861

Definitions of data tables primarily coming from EIA-861.

## Attributes

| [`DISTRIBUTED_GENERATION_NON_NET_METERING_TRANSITION`](#pudl.metadata.resources.eia861.DISTRIBUTED_GENERATION_NON_NET_METERING_TRANSITION)   |    |
|----------------------------------------------------------------------------------------------------------------------------------------------|----|
| [`EIA861_88888`](#pudl.metadata.resources.eia861.EIA861_88888)                                                                               |    |
| [`RESOURCE_METADATA`](#pudl.metadata.resources.eia861.RESOURCE_METADATA)                                                                     |    |

## Module Contents

### pudl.metadata.resources.eia861.DISTRIBUTED_GENERATION_NON_NET_METERING_TRANSITION *= Multiline-String*

<details><summary>Show Value</summary>
```python
"""The raw EIA861 distributed generation (DG) table (split into three normalized tables in
PUDL) was renamed in 2016 to Non-Net Metering to prevent double counting. The data in
the Non-Net Metering table (2016+) are split by sector, contain fuel cell information,
and convert capacity reported in DC units to AC units."""
```

</details>

### pudl.metadata.resources.eia861.EIA861_88888 *= Multiline-String*

<details><summary>Show Value</summary>
```python
"""Respondents are required to report this information to the EIA, but are not required
to disclose utility-level data to the public. When a respondent chooses to keep its
utility-level data proprietary, it files using EIA utility id 88888. For more details,
see :ref:`EIA-861 Notable Irregularities <eia861-notable-irregularities>`."""
```

</details>

### pudl.metadata.resources.eia861.RESOURCE_METADATA *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]]*
