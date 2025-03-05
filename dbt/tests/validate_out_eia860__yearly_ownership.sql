with OwnSum as (
    select
        report_date,
        plant_id_eia,
        generator_id,
        sum(fraction_owned) as fraction_owned,
        1 as toy_join
    from {{ source('pudl', 'out_eia860__yearly_ownership') }}
    group by
        report_date,
        plant_id_eia,
        generator_id
    having fraction_owned not null
), PercentileScores as (
    select
        fraction_owned,
        row_number() over w as 'row_number',
        percent_rank() over w as 'percent_rank'
    from OwnSum
    window w as (order by fraction_owned)
), PercentileMissing as (
    select
        percent_rank as pct_missing,
        1 as toy_join
    from PercentileScores
    where fraction_owned >= 0.98
    limit 1
), Summary as (
    select *
    from OwnSum right join PercentileMissing using (toy_join)
    where fraction_owned > 1.02
)
select * from Summary where (report_date is not null) or (pct_missing >= 0.5)
