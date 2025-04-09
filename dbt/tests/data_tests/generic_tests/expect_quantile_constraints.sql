{% test expect_quantile_constraints(model, column_name,
                                           constraints,
                                           weight_column=None,
                                           group_by=None,
                                           row_condition=None,
                                           strictly=False
                                           ) %}
with
{% for constraint in constraints %}
constraint_{{ loop.index0 }} as (
{% if weight_column %}
{{ test_expect_column_weighted_quantile_values_to_be_between(
    model=model,
    column_name=column_name,
    quantile=constraint.quantile,
    weight_column=weight_column,
    min_value=constraint.min_value|default(None),
    max_value=constraint.max_value|default(None),
    group_by=group_by,
    row_condition=row_condition,
    strictly=strictly
) }}
{% else %}
{{ dbt_expectations.test_expect_column_quantile_values_to_be_between(
    model=model,
    column_name=column_name,
    quantile=constraint.quantile,
    min_value=constraint.min_value|default(None),
    max_value=constraint.max_value|default(None),
    group_by=group_by,
    row_condition=row_condition,
    strictly=strictly
) }}
{% endif %}
){%- if not loop.last %},{%- endif %} -- end constraint_{{ loop.index0 }}
{% endfor %}
{% for constraint in constraints %}
select distinct '{{ constraint.quantile }}' as quantile, expression from constraint_{{ loop.index0 }}
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
{% endtest %}
