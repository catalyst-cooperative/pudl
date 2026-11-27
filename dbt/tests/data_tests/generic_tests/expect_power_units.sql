{% test expect_power_units(
    model,
    unit_col,
    max_failure_rate,
    row_condition=None,
    allowed_unit_patterns=None
) %}

{# The selected rows should be measured or priced in units of power or capacity
(e.g. $/MW-MO), or a neutral unit like FLAT RATE, and not in anything else, such as
energy units (e.g. $/MWH). See unit_mismatch_check. #}

{{ unit_mismatch_check(
    model,
    unit_col,
    allowed_unit_patterns
    if allowed_unit_patterns is not none
    else power_unit_patterns() + neutral_unit_patterns(),
    max_failure_rate,
    row_condition,
) }}

{% endtest %}
