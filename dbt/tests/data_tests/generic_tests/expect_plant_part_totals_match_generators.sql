{% test expect_plant_part_totals_match_generators(
    model,
    generators_table_name="out_eia__yearly_generators_by_ownership",
    atol=0.01,
    rtol=1e-6
) %}

{#- Argument documentation lives in schema.yml next to this file. -#}

{#- Columns identifying each plant part, beyond plant_id_eia. -#}
{%- set part_id_cols = {
    "plant": none,
    "plant_unit": "unit_id_pudl",
    "plant_prime_mover": "prime_mover_code",
    "plant_technology": "technology_description",
    "plant_prime_fuel": "energy_source_code_1",
    "plant_ferc_acct": "ferc_acct_name",
    "plant_operating_year": "generator_operating_year",
    "plant_gen": "generator_id",
} -%}
{%- set sum_cols = [
    "capacity_mw",
    "capacity_eoy_mw",
    "net_generation_mwh",
    "total_fuel_cost",
    "total_mmbtu",
] -%}
{%- set wtavg_cols = [
    "fuel_cost_per_mwh",
    "unit_heat_rate_mmbtu_per_mwh",
    "fuel_cost_per_mmbtu",
] -%}
{%- set weight_col = "capacity_mw" -%}
{%- set key_cols = [
    "plant_part",
    "plant_id_eia",
    "part_id",
    "report_date",
    "operational_status_pudl",
] -%}

WITH
-- One "total" record per generator, which carries the full generator's values.
generators AS (
    SELECT *
    FROM {{ source('pudl', generators_table_name) }}
    WHERE ownership_record_type = 'total'
    QUALIFY row_number() OVER (
        PARTITION BY plant_id_eia, generator_id, report_date
        ORDER BY utility_id_eia
    ) = 1
),

-- Generators whose "owned" records don't add up to the whole generator in the plant
-- parts, either because their reported ownership fractions don't sum to 1, or
-- because an owner is unknown and the "owned" aggregation drops its records.
incomplete_ownership AS (
    SELECT plant_id_eia, generator_id, report_date
    FROM {{ source('pudl', generators_table_name) }}
    WHERE ownership_record_type = 'owned'
    GROUP BY ALL
    HAVING abs(sum(fraction_owned) - 1) > 1e-6 OR bool_or(utility_id_eia IS NULL)
),

-- Aggregate each plant part directly from its distinct generators, the same way
-- the plant parts are aggregated: weighted averages ignore null values, and are null
-- when the weights sum to zero.
expected AS (
    {%- for part, id_col in part_id_cols.items() %}
    SELECT
        '{{ part }}' AS plant_part,
        g.plant_id_eia,
        {% if id_col is none %}NULL{% else %}CAST(g.{{ id_col }} AS VARCHAR){% endif %} AS part_id,
        g.report_date,
        g.operational_status_pudl,
        {%- for col in sum_cols %}
        sum(g.{{ col }}) AS {{ col }},
        {%- endfor %}
        {%- for col in wtavg_cols %}
        sum(g.{{ col }} * g.{{ weight_col }}) / nullif(
            sum(CASE WHEN g.{{ col }} IS NOT NULL THEN g.{{ weight_col }} END), 0
        ) AS {{ col }},
        {%- endfor %}
        bool_or(i.plant_id_eia IS NOT NULL) AS has_incomplete_ownership
    FROM generators AS g
    LEFT JOIN incomplete_ownership AS i
        USING (plant_id_eia, generator_id, report_date)
    GROUP BY ALL
    {% if not loop.last %}UNION ALL{% endif %}
    {%- endfor %}
),

plant_parts AS (
    SELECT
        *,
        CASE plant_part
            {%- for part, id_col in part_id_cols.items() if id_col is not none %}
            WHEN '{{ part }}' THEN CAST({{ id_col }} AS VARCHAR)
            {%- endfor %}
        END AS part_id
    FROM {{ model }}
    WHERE plant_part IN ('{{ part_id_cols.keys() | join("', '") }}')
),

-- Every owner of a plant part gets a "total" record describing the whole part, so
-- the minimum and maximum over owners should both match the expected values.
totals AS (
    SELECT
        {{ key_cols | join(', ') }},
        {%- for col in sum_cols + wtavg_cols %}
        min({{ col }}) AS min_total_{{ col }},
        max({{ col }}) AS max_total_{{ col }},
        {%- endfor %}
    FROM plant_parts
    WHERE ownership_record_type = 'total'
    GROUP BY ALL
),

owned AS (
    SELECT
        {{ key_cols | join(', ') }},
        {%- for col in sum_cols %}
        sum({{ col }}) AS owned_{{ col }},
        {%- endfor %}
    FROM plant_parts
    WHERE ownership_record_type = 'owned'
    GROUP BY ALL
)

SELECT
    e.*,
    t.* EXCLUDE ({{ key_cols | join(', ') }}),
    o.* EXCLUDE ({{ key_cols | join(', ') }})
FROM expected AS e
INNER JOIN totals AS t
    ON e.plant_part = t.plant_part
    AND e.plant_id_eia = t.plant_id_eia
    AND e.part_id IS NOT DISTINCT FROM t.part_id
    AND e.report_date = t.report_date
    AND e.operational_status_pudl = t.operational_status_pudl
LEFT JOIN owned AS o
    ON e.plant_part = o.plant_part
    AND e.plant_id_eia = o.plant_id_eia
    AND e.part_id IS NOT DISTINCT FROM o.part_id
    AND e.report_date = o.report_date
    AND e.operational_status_pudl = o.operational_status_pudl
WHERE
    {%- for col in sum_cols + wtavg_cols %}
    {{ plant_part_value_not_close("t.min_total_" ~ col, "e." ~ col, atol, rtol) }}
    OR {{ plant_part_value_not_close("t.max_total_" ~ col, "e." ~ col, atol, rtol) }}
    OR
    {%- endfor %}
    (
        NOT e.has_incomplete_ownership
        AND (
            {%- for col in sum_cols %}
            {{ plant_part_value_not_close("o.owned_" ~ col, "e." ~ col, atol, rtol) }}
            {%- if not loop.last %} OR{% endif %}
            {%- endfor %}
        )
    )

{% endtest %}

{% macro plant_part_value_not_close(a, b, atol, rtol) -%}
    {#-
        True when exactly one value is null or infinite, or the values differ by more
        than the tolerance. Equal infinite values (e.g. heat rates of generators with
        no net generation) are close, even though their difference is NaN.
    -#}
    (
        ({{ a }} IS NULL) != ({{ b }} IS NULL)
        OR isinf({{ a }}) != isinf({{ b }})
        OR (
            {{ a }} IS DISTINCT FROM {{ b }}
            AND NOT abs({{ a }} - {{ b }}) <= {{ atol }} + {{ rtol }} * abs({{ b }})
        )
    )
{%- endmacro %}
