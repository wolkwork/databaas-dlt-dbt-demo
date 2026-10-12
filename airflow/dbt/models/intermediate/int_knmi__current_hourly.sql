-- Current observation for the KNMI Source: stg_knmi__hourly
-- can carry more than one row per (location, hour) -- a retried or
-- overlapping run re-lists the same recent files (see knmi_pipeline/knmi.py,
-- "No cursor, no watcher") and KNMI itself can backfill/correct a station's
-- published value for up to 7 days after the fact. This collapses that down
-- to one row per (location, hour), keeping only the most recently fetched
-- reading -- same dedup mechanism as Current forecast, different reason:
-- KNMI backfill here, forecast revision there.
--
-- row_number() in a CTE, not `qualify` -- this deployment's Trino doesn't
-- support the QUALIFY clause (see int_marine__current_hourly.sql).
with ranked as (

    select
        location,
        fetched_at,
        observed_time,
        observed_wind_speed,
        observed_wind_direction,
        row_number() over (
            partition by location, observed_time
            order by fetched_at desc
        ) as rn

    from {{ ref('stg_knmi__hourly') }}

)

select
    location,
    fetched_at,
    observed_time,
    observed_wind_speed,
    observed_wind_direction

from ranked
where rn = 1
