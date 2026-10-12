"""Where this pipeline points -- a single marine forecast location.

No LOCATIONS collection: `ingest_marine_forecast` makes a single call, not a
loop over several. Add a second constant here, and a second call at the DAG
call site, if a second location is ever wanted -- not a loop.
"""

from __future__ import annotations

from dataclasses import dataclass

# Open-Meteo Marine API hourly variables requested, in the order the API
# returns them.
HOURLY_VARIABLES = ("wave_height",)


@dataclass(frozen=True)
class Location:
    """One point forecast: a name and where to ask for it."""

    name: str
    latitude: float
    longitude: float


# Europlatform (WMO 06321) -- a North Sea offshore platform southwest of
# Hoek van Holland, and KNMI's own station code for this point (see
# knmi_pipeline/models.py) -- what the KNMI Source's Current observation
# joins onto.
EUROPLATFORM_51_9978_3_2750 = Location("europlatform_51_9978_3_2750", 51.9978, 3.2750)
