"""Field metadata."""
from typing import Any, Dict, List

FIELD_LIST: List[Dict[str, Any]] = [
    {
        "name": "record_id",
        "type": "string",
        "description": "Identifier indicating original FERC Form 1 source record. format: {table_name}_{report_year}_{report_prd}_{respondent_id}_{spplmnt_num}_{row_number}. Unique within FERC Form 1 DB tables which are not row-mapped.",  # noqa: FS003
    },
    {
        "name": "utility_id_ferc1",
        "type": "integer",
        "description": "FERC assigned respondent_id, identifying the reporting entity. Stable from year to year.",
    },
    {
        "name": "report_year",
        "type": "year",
        "description": "Four-digit year in which the data was reported.",
    },
    {
        "name": "plant_name_ferc1",
        "type": "string",
        "description": "Name of the plant, as reported to FERC. This is a freeform string, not guaranteed to be consistent across references to the same plant.",
    },
    {
        "name": "fuel_type_code_pudl",
        "type": "string",
        "constraints": {
            "enum": [
                "coal",
                "oil",
                "gas",
                "solar",
                "wind",
                "hydro",
                "nuclear",
                "waste",
                "unknown",
            ]
        },
        "description": "PUDL assigned code indicating the general fuel type.",
    },
    {
        "name": "fuel_unit",
        "type": "string",
        "constraints": {
            "enum": [
                "ton",
                "mcf",
                "bbl",
                "gal",
                "kgal",
                "gramsU",
                "kgU",
                "klbs",
                "btu",
                "mmbtu",
                "mwdth",
                "mwhth",
                "unknown",
            ]
        },
        "description": "PUDL assigned code indicating reported fuel unit of measure.",
    },
    {
        "name": "fuel_qty_burned",
        "type": "number",
        "description": "Quantity of fuel consumed in the report year, in terms of the reported fuel units.",
    },
    {
        "name": "fuel_mmbtu_per_unit",
        "type": "number",
        "description": "Average heat content of fuel consumed in the report year, in mmBTU per reported fuel unit.",
    },
    {
        "name": "fuel_cost_per_unit_burned",
        "type": "number",
        "description": "Average cost of fuel consumed in the report year, in nominal USD per reported fuel unit.",
    },
    {
        "name": "fuel_cost_per_unit_delivered",
        "type": "number",
        "description": "Average cost of fuel delivered in the report year, in nominal USD per reported fuel unit.",
    },
    {
        "name": "fuel_cost_per_mmbtu",
        "type": "number",
        "description": "Average cost of fuel consumed in the report year, in nominal USD per mmBTU of fuel heat content.",
    },
]
"""
Field attributes.
TODO: Add missing fields.
"""

FIELDS: Dict[str, Dict[str, Any]] = {f["name"]: f for f in FIELD_LIST}
"""
Field attributes by PUDL identifier (`field.name`).
"""
