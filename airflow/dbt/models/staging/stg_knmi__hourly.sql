-- knmi_table already has one row per (file, hour) for our station -- unlike
-- the marine/wind Sources, a KNMI file covers a single hour already parsed
-- down to one station's reading (see knmi_pipeline/knmi.py), so there's no
-- nested array to UNNEST here. This model still exists for the raw ->
-- staging -> intermediate -> mart layering convention and
-- to cast/rename the raw payload.
--
-- observed_time, not forecast_time: this is an Observation, not a Forecast --
-- the column name says so even though it joins against
-- forecast_time-keyed rows downstream.
select
    location,
    fetched_at,
    -- observed_at is ISO 8601 with a 'T' separator and no UTC offset (from
    -- netCDF4.num2date().isoformat() -- see knmi_pipeline/knmi.py), e.g.
    -- "2026-09-13T06:00:00" -- CAST(... AS timestamp) doesn't accept the
    -- 'T' separator (INVALID_CAST_ARGUMENT), same reason the marine/wind
    -- staging models parse their own ISO strings explicitly instead of
    -- casting them directly. timestamp(6), not date_parse's default of (3):
    -- the Iceberg connector only accepts precision 6.
    cast(date_parse(observed_at, '%Y-%m-%dT%H:%i:%s') as timestamp(6)) as observed_time,
    observed_wind_speed,
    observed_wind_direction

from {{ source('knmi_observation', env_var('KNMI_TABLE', 'knmi_table')) }}
