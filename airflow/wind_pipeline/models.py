"""Where this pipeline points -- a single wind forecast location.

No LOCATIONS collection: `ingest_wind_forecast` makes a single call, not a
loop over several. Add a second constant here, and a second call at the DAG
call site, if a second location is ever wanted -- not a loop.
"""

from __future__ import annotations

from dataclasses import dataclass

# Open-Meteo Forecast API hourly variables requested, in the order the API
# returns them.
HOURLY_VARIABLES = ("wind_speed_10m", "wind_direction_10m")


@dataclass(frozen=True)
class Location:
    """One point forecast: a name and where to ask for it."""

    name: str
    latitude: float
    longitude: float


# Same point as the marine Source (marine_pipeline/models.py's
# EUROPLATFORM_51_9978_3_2750) -- wind and wave conditions at one point are
# what `sea_state_hourly` joins. Europlatform
# (WMO 06321), a North Sea offshore platform southwest of Hoek van Holland --
# also the point the KNMI Source (knmi_pipeline/models.py) observes.
EUROPLATFORM_51_9978_3_2750 = Location("europlatform_51_9978_3_2750", 51.9978, 3.2750)
