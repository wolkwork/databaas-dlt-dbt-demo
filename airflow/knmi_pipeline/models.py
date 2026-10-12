"""Where this pipeline points -- a single Dutch offshore weather station.

No LOCATIONS collection: mirrors marine_pipeline/models.py and
wind_pipeline/models.py -- one hardcoded station, not a loop over several.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Location:
    """One point observation: a name and where it is."""

    name: str
    latitude: float
    longitude: float


# Same point as marine_pipeline.models.EUROPLATFORM_51_9978_3_2750 and
# wind_pipeline.models.EUROPLATFORM_51_9978_3_2750 -- what sea_state_hourly
# left-joins this Source's Current observation onto.
EUROPLATFORM_51_9978_3_2750 = Location("europlatform_51_9978_3_2750", 51.9978, 3.2750)

# Europlatform's WIGOS Station Identifier (WSI) -- the `station` variable's
# actual format in the dataset's files, confirmed against a real downloaded
# file (not just the docs): "0-20000-0-06321", not the bare WMO number
# "06321". Each file from the dataset below covers every station KNMI
# operates, so this picks our one station's row out of it. See
# https://dataplatform.knmi.nl/dataset/docs/hourly-in-situ-meteorological-observations-1-0
STATION_CODE = "0-20000-0-06321"
