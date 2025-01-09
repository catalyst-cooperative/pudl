SELECT *
FROM {{ source('pudl_nightly', 'out_vcerare__hourly_available_capacity_factor') }}
WHERE
    capacity_factor_offshore_wind > 1.02 OR
    capacity_factor_onshore_wind > 1.02
