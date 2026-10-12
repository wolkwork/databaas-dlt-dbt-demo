"""Tests for wind_pipeline.wind.fetch_forecast -- the wind Source's dlt
fetch function. The underlying HTTP client is mocked via `responses`; no real
call reaches Open-Meteo's API.
"""

from __future__ import annotations

from urllib.parse import parse_qs, urlparse

import responses

from wind_pipeline.models import Location
from wind_pipeline.wind import BASE_URL, fetch_forecast

LOCATION = Location(name="test_point", latitude=1.23, longitude=4.56)

FAKE_RESPONSE = {
    "latitude": 1.23,
    "longitude": 4.56,
    "hourly": {
        "time": ["2026-01-01T00:00", "2026-01-01T01:00"],
        "wind_speed_10m": [12.3, 14.1],
        "wind_direction_10m": [180, 190],
    },
}


@responses.activate
def test_fetch_forecast_requests_expected_endpoint_and_params():
    responses.add(responses.GET, f"{BASE_URL}forecast", json=FAKE_RESPONSE, status=200)

    record = fetch_forecast(LOCATION, forecast_days=7)

    assert len(responses.calls) == 1
    request = responses.calls[0].request
    parsed = urlparse(request.url)
    assert f"{parsed.scheme}://{parsed.netloc}{parsed.path}" == f"{BASE_URL}forecast"

    params = parse_qs(parsed.query)
    assert params["latitude"] == ["1.23"]
    assert params["longitude"] == ["4.56"]
    assert params["hourly"] == ["wind_speed_10m,wind_direction_10m"]
    assert params["forecast_days"] == ["7"]
    assert params["timezone"] == ["UTC"]

    assert record == FAKE_RESPONSE
