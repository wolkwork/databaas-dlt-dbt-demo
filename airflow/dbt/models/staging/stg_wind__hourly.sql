-- wind_table has one row per API call: location/fetched_at plus the
-- Open-Meteo Forecast API's own latitude/longitude/hourly fields, `hourly`
-- landing as a nested struct<list<...>> column (see
-- wind_pipeline/lakehouse.py). This unnests the parallel arrays into one row
-- per (call, hour).
--
-- Trino's tuple-form UNNEST: one CROSS JOIN UNNEST(...) AS t(...) unnests
-- all listed arrays together, positionally, into one row per element.
select
    raw.location,
    raw.fetched_at,
    raw.latitude,
    raw.longitude,
    -- The API's hourly.time strings are ISO 8601 with no seconds and no
    -- timezone (e.g. "2026-09-08T00:00") -- CAST(... AS timestamp) doesn't
    -- accept that format directly (INVALID_CAST_ARGUMENT), so parse it
    -- explicitly. timestamp(6), not the CAST default of (3): the Iceberg
    -- connector only accepts precision 6.
    cast(date_parse(t.forecast_time, '%Y-%m-%dT%H:%i') as timestamp(6)) as forecast_time,
    t.wind_speed_10m,
    t.wind_direction_10m

from {{ source('wind_forecast', env_var('WIND_TABLE', 'wind_table')) }} as raw
cross join unnest(
    raw.hourly.time,
    raw.hourly.wind_speed_10m,
    raw.hourly.wind_direction_10m
) as t(forecast_time, wind_speed_10m, wind_direction_10m)
