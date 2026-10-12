{{ config(materialized='view') }}

select distinct
    plant_id_eia,
    subplant_id
from {{ source('pudl', 'core_epa__assn_eia_epacamd_subplant_ids') }}
