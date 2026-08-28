{#-
    Compares the original, plant-level generation_fuel table (more complete but
    less granular) against the fully allocated generator-level output, at
    plant-year grain, so we can check how much of the original data was retained
    by the allocation process.

    Two categories of report_year are excluded here, for every downstream test:

    * report_years for which the allocation process has not produced any output at
      all (e.g. the most recent, still-in-progress report_year), since that's a
      structural side effect of the ETL/allocation timing rather than an
      allocation quality problem, and would otherwise permanently drag down every
      downstream check regardless of how well the allocation is working.
    * 2001 and 2002, a known, systemic early-data-quality era -- essentially every
      plant in those two years retains only ~80-83% of its data, orders of
      magnitude worse than every other year (which are all >99.7%). That's a
      different phenomenon than what these tests are meant to monitor.
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

-- report_years present in the original data but entirely absent from the
-- allocated output (e.g. the latest, still year-to-date report_year, which the
-- allocation process doesn't yet cover). A report_year with zero allocated rows
-- never appears as a group in `allocated` at all, so this has to be a set
-- difference on the distinct report_years themselves, not a null-sum check
-- within `allocated` -- there's no row there to be null in the first place.
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
