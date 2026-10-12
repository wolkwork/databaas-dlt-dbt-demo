"""Parsing a KNMI hourly observation NetCDF file down to one station's row.

Split out of knmi.py on purpose: knmi.py decorates its file-listing resource
with `@dlt.resource` at module scope, so importing it -- regardless of which
function you actually call -- needs `dlt` installed. parse_knmi_observations
(see knmi_pipeline_dag.py) never lists or downloads files, only parses ones
ingest_knmi_files already landed, so it has no reason to need dlt at all.
Importing this module instead keeps that task's venv to just `netCDF4`.
"""

from __future__ import annotations

from pathlib import Path

from knmi_pipeline.models import STATION_CODE, Location


def extract_station_row(path: Path, location: Location) -> dict | None:
    """This station's single row out of a file covering every station.

    Verified against a real downloaded file (2026-09-14), not just the
    dataset's docs: `station` is the per-station WIGOS identifier array
    (e.g. "0-20000-0-06321", not the bare WMO number), `time` the file's one
    timestamp (its `long_name` says it marks the *end* of the hourly
    interval), and each observation variable is shaped (station, time) with
    exactly one time value per file -- hence the `[idx, 0]` indexing below,
    not a flat per-station array. `FH` ("Hourly mean wind speed") is the
    hourly-representative speed; `FF` ("past 10 minute mean wind speed") is
    a different, sub-hourly quantity the dataset happens to carry alongside
    it and is deliberately not used here, so as not to compare a 10-minute
    snapshot against Open-Meteo's hourly forecast. `DD` is mean wind
    direction. Returns None if this particular file doesn't carry our
    station -- offshore platforms occasionally drop a reading, and that's a
    fact to skip, not an error.
    """
    import netCDF4

    with netCDF4.Dataset(path) as ds:
        stations = [
            code.decode() if isinstance(code, bytes) else str(code)
            for code in ds.variables["station"][:]
        ]
        if STATION_CODE not in stations:
            return None
        idx = stations.index(STATION_CODE)

        time_var = ds.variables["time"]
        observed_at = netCDF4.num2date(
            time_var[:][0], units=time_var.units, calendar=time_var.calendar
        ).isoformat()

        def scalar(name: str) -> float | None:
            value = ds.variables[name][idx, 0]
            return None if getattr(value, "mask", False) else float(value)

        return {
            "location": location.name,
            "observed_at": observed_at,
            "observed_wind_speed": scalar("FH"),
            "observed_wind_direction": scalar("DD"),
        }
