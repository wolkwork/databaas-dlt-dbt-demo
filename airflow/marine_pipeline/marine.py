"""Marine forecast API extraction via dlt.

No payload validation here -- dlt's rest_api resource is trusted to yield a
usable record, and its shape becomes the Iceberg schema exactly as returned,
inferred by pyarrow at write time (see marine_pipeline/lakehouse.py's
write_table()).

dlt ships in the Airflow runtime image the task pods run, but not in the
application image that parses DAG files -- so only task bodies import this.
"""

from __future__ import annotations

import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources

from marine_pipeline.models import HOURLY_VARIABLES, Location

BASE_URL = "https://marine-api.open-meteo.com/v1/"


@dlt.source(name="marine_forecast")
def marine_forecast_source(location: Location, forecast_days: int):
    config: RESTAPIConfig = {
        "client": {"base_url": BASE_URL},
        "resources": [
            {
                "name": "forecast",
                "endpoint": {
                    "path": "marine",
                    "params": {
                        "latitude": location.latitude,
                        "longitude": location.longitude,
                        "hourly": ",".join(HOURLY_VARIABLES),
                        "forecast_days": forecast_days,
                        "timezone": "UTC",
                    },
                    # The API returns one JSON object, not a list -- "$"
                    # yields the whole response as a single record, and the
                    # paginator is pinned rather than left to auto-detection,
                    # since the response is a single object.
                    "data_selector": "$",
                    "paginator": {"type": "single_page"},
                },
            },
        ],
    }
    yield from rest_api_resources(config)


def fetch_forecast(location: Location, forecast_days: int) -> dict:
    """One location's hourly wave-height forecast, exactly as dlt's rest_api
    resource returned it."""
    source = marine_forecast_source(location=location, forecast_days=forecast_days)
    records = list(source.resources["forecast"])
    if not records:
        raise RuntimeError(f"{location.name}: marine forecast API returned no records")
    return records[0]
