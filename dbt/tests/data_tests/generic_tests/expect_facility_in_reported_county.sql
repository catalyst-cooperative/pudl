-- Fails for rows whose latitude and longitude lie in a different county than
-- county_id_fips. See facility_outside_reported_region for details.
{% test expect_facility_in_reported_county(model, tolerance_meters=0) %}
{{
    facility_outside_reported_region(
        model,
        region_id_column='county_id_fips',
        regions=source('pudl', 'out_censusdp1tract__counties'),
        label_column='county',
        tolerance_meters=tolerance_meters,
    )
}}
{% endtest %}
