-- Weekly wave-height aggregate over the marine forecast's hourly series.
-- One row per (location, week) -- table materialization (dbt_project.yml)
-- rebuilds this fully each run, appropriate for a small aggregate over a
-- rolling forecast horizon rather than a growing history.
--
-- Aggregates the Current forecast, not raw staging output
-- -- staging carries one row per (call, hour), which would let hours
-- polled more often outweigh hours polled less often in these aggregates.
select
    location,
    date_trunc('week', forecast_time) as week_start,
    count(*) as hours_forecasted,
    avg(wave_height) as avg_wave_height_m,
    min(wave_height) as min_wave_height_m,
    max(wave_height) as max_wave_height_m

from {{ ref('int_marine__current_hourly') }}
group by location, date_trunc('week', forecast_time)
order by location, week_start
