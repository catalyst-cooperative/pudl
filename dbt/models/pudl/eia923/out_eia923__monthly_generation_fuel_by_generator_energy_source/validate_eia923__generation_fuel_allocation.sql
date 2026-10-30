{#-
    Compares original, plant-level generation_fuel table (more complete but
    less granular) against the fully allocated generator-level output per
    plant-year to check how close the allocated totals are to the originals.

    Two categories of report_year are excluded:

    * report_years for which the allocation process has not produced any output at
      all (e.g. the most recent, still-in-progress report_year).
    * 2001 and 2002, due to data quality issues. Plants typically retain only
      ~80-83% of their generation / fuel in these years, while for every other
      year it's >99.7%.
-#}
{% set metrics = ["net_generation_mwh", "fuel_consumed_mmbtu", "fuel_consumed_for_electricity_mmbtu"] %}

with original as (
    select
        extract(year from report_date) as report_year,
        plant_id_eia,
        {% for metric in metrics %}
        sum({{ metric }}) as original_{{ metric }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from {{ source('pudl', 'out_eia923__generation_fuel_combined') }}
    group by report_year, plant_id_eia
),

allocated as (
    select
        extract(year from report_date) as report_year,
        plant_id_eia,
        {% for metric in metrics %}
        sum({{ metric }}) as allocated_{{ metric }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from {{ source('pudl', 'out_eia923__monthly_generation_fuel_by_generator_energy_source') }}
    group by report_year, plant_id_eia
),

unallocated_report_years as (
    select distinct report_year from original
    except
    select distinct report_year from allocated
)

select
    coalesce(original.report_year, allocated.report_year) as report_year,
    coalesce(original.plant_id_eia, allocated.plant_id_eia) as plant_id_eia,
    {% for metric in metrics %}
    original.original_{{ metric }},
    allocated.allocated_{{ metric }},
    allocated.allocated_{{ metric }} / nullif(original.original_{{ metric }}, 0)
        as {{ metric }}_retained_fraction{% if not loop.last %},{% endif %}
    {% endfor %}
from original
full outer join allocated
    on original.report_year = allocated.report_year
    and original.plant_id_eia = allocated.plant_id_eia
where coalesce(original.report_year, allocated.report_year) >= 2003
and coalesce(original.report_year, allocated.report_year) not in (
    select report_year from unallocated_report_years
)
