{% test expect_eia930_operations_conserve_energy(
    model,
    atol=1.0,
    warn_fraction=0.01,
    error_fraction=0.05
) %}

{{ config(
    fail_calc="max(failing_fraction)",
    warn_if=">" ~ warn_fraction,
    error_if=">" ~ error_fraction
) }}

WITH eligible_rows AS (
    SELECT
        demand_reported_mwh,
        net_generation_reported_mwh,
        interchange_reported_mwh
    FROM {{ model }}
    WHERE demand_reported_mwh IS NOT NULL
        AND net_generation_reported_mwh IS NOT NULL
        AND interchange_reported_mwh IS NOT NULL
),

failure_summary AS (
    SELECT
        COUNT(*) AS eligible_records,
        SUM(
            CASE
                WHEN ABS(
                    demand_reported_mwh
                    - net_generation_reported_mwh
                    - interchange_reported_mwh
                ) > {{ atol }}
                    THEN 1
                ELSE 0
            END
        ) AS failing_records
    FROM eligible_rows
)

SELECT
    eligible_records,
    COALESCE(failing_records, 0) AS failing_records,
    COALESCE(COALESCE(failing_records, 0)::FLOAT / NULLIF(eligible_records, 0), 0.0)
        AS failing_fraction,
    {{ atol }} AS atol
FROM failure_summary

{% endtest %}
