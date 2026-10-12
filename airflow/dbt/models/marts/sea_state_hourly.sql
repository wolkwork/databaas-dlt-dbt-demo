-- Sea state: combined wave-height and wind conditions at a location and
-- hour. The first model that actually depends on more
-- than one Source -- inner join of marine and wind on (location,
-- forecast_time), since a sea state reading needs both a wave-height and a
-- wind reading for that hour. Joins each source's Current forecast,
-- not raw staging output -- staging carries one row per (call,
-- hour), which would give this merge multiple source rows per key.
--
-- KNMI's Current observation is left-joined onto the same
-- (location, hour) key, not inner-joined: an Observation only exists once
-- an hour has actually occurred, so every future hour in the forecast
-- horizon legitimately has NULL observed_wind_speed/observed_wind_direction
-- until KNMI publishes it -- that NULL is a fact ("not observed yet"), not
-- missing data an inner join should filter out.
--
-- incremental/merge on (location, forecast_timestamp): repeated polls of an
-- unchanged forecast hour update that row in place (Open-Meteo revises its
-- model between calls, and KNMI backfills/corrects an observation for up to
-- 7 days) instead of accumulating duplicate rows -- not a time-windowed
-- filter, since a merge over the current forecast horizon is cheap at this
-- data volume and every poll may revise any hour in it.
{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['location', 'forecast_timestamp']
  )
}}

select
    marine.location,
    marine.forecast_time as forecast_timestamp,
    marine.wave_height,
    wind.wind_speed_10m,
    wind.wind_direction_10m,
    knmi.observed_wind_speed,
    knmi.observed_wind_direction

from {{ ref('int_marine__current_hourly') }} as marine
inner join {{ ref('int_wind__current_hourly') }} as wind
    on marine.location = wind.location
    and marine.forecast_time = wind.forecast_time
left join {{ ref('int_knmi__current_hourly') }} as knmi
    on marine.location = knmi.location
    and marine.forecast_time = knmi.observed_time
