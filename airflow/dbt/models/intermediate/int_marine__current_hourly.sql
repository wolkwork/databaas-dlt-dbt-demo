-- Current forecast: stg_marine__hourly has one row per
-- (call, hour) by design -- every poll of the marine Source appends a full
-- 7-day forecast, so an hour polled N times has N rows. This collapses that
-- down to one row per (location, hour), keeping only the most recently
-- fetched reading -- Open-Meteo may have revised its model between polls, so
-- the latest fetched_at wins.
--
-- row_number() in a CTE, not `qualify` -- this deployment's Trino doesn't
-- support the QUALIFY clause (SYNTAX_ERROR, "mismatched input 'row_number'"),
-- so the window function has to be filtered via an outer WHERE instead.
with ranked as (

    select
        location,
        fetched_at,
        latitude,
        longitude,
        forecast_time,
        wave_height,
        row_number() over (
            partition by location, forecast_time
            order by fetched_at desc
        ) as rn

    from {{ ref('stg_marine__hourly') }}

)

select
    location,
    fetched_at,
    latitude,
    longitude,
    forecast_time,
    wave_height

from ranked
where rn = 1
