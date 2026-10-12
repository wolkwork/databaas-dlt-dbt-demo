-- marine_table has one row per API call: location/fetched_at plus the
-- marine API's own latitude/longitude/hourly fields, `hourly` landing as a
-- nested struct<list<...>> column (see marine_pipeline/lakehouse.py). This
-- unnests the parallel arrays into one row per (call, hour).
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
    t.wave_height

from {{ source('marine_forecast', env_var('MARINE_TABLE', 'marine_table')) }} as raw
cross join unnest(
    raw.hourly.time,
    raw.hourly.wave_height
) as t(forecast_time, wave_height)
