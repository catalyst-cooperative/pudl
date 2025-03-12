{% test test_idle_capacity(model) %}

{% set max_idle_map = {"gas": 0.15, "coal": 0.075} %}

with
    wc as (
        SELECT fuel_type_code_pudl, SUM(capacity_mw) as working_capacity
        FROM {{ model }}
        WHERE capacity_factor > 0.0 OR capacity_factor IS NULL
        GROUP BY fuel_type_code_pudl
    ),
    tc as (
        SELECT fuel_type_code_pudl, SUM(capacity_mw) as total_capacity
        FROM {{ model }}
        GROUP BY fuel_type_code_pudl
    ),
    max_idle as (
        {% for fuel_type, upper_bound in max_idle_map.items() %}
        SELECT '{{ fuel_type }}' as fuel_type_code_pudl, {{ upper_bound }} as upper_bound
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
    )
SELECT
    wc.fuel_type_code_pudl,
    1 - (wc.working_capacity / tc.total_capacity) as idle_capacity,
    max_idle.upper_bound
FROM wc
INNER JOIN tc on wc.fuel_type_code_pudl=tc.fuel_type_code_pudl
INNER JOIN max_idle on wc.fuel_type_code_pudl=max_idle.fuel_type_code_pudl
WHERE idle_capacity > upper_bound

{% endtest %}
