"""KNMI Data Platform hourly observation extraction via dlt.

Unlike the marine/wind Sources' single REST call, KNMI's Open Data API is a
genuine file API: list files in a dataset version, resolve one filename to a
short-lived signed download URL, then GET the actual NetCDF bytes from that
URL. `rest_api_resources`' declarative config can't express that three-step
flow.

Split following dlt's "ingest as a file" pattern: listing files is
structured data (filename, size, lastModified), so it goes through a
`@dlt.resource` for schema inference same as the REST-based Sources;
downloading one file's bytes isn't structured, so
that's a plain function instead. Parsing a landed file lives in
knmi_pipeline/parsing.py, not here -- deliberately a separate module, not
just a separate function, so that parse_knmi_observations' `@task.virtualenv`
(see knmi_pipeline_dag.py) never needs to import this module and, with it,
`dlt` -- a package it never actually uses -- just to reach the one function
it does need.

Each file covers one hour for every station KNMI operates (not one file per
station), so each downloaded file is parsed down to the single row for our
station (see models.STATION_CODE), or skipped if that file doesn't carry it.

No cursor, no watcher: KNMI batches roughly 24 new hourly files once a night
(the dataset's own docs list its availability as "Overnight", not
near-real-time), so
"the latest file" alone would silently miss the other ~23 every day. Each
run instead re-lists and re-downloads the most recent FILES_PER_RUN files,
comfortably covering a full day plus slack for a delayed run. This is
idempotent by construction: a re-fetched hour just gets re-appended, and the
Current observation intermediate layer already dedupes to
one row per (location, hour), keeping whichever fetch was most recent --
same mechanism the marine/wind Sources use for forecast revision, here
covering KNMI backfilling/correcting a station's published value instead.

dlt ships in the Airflow runtime image the task pods run, but not in the
application image that parses DAG files -- so only task bodies import this.
"""

from __future__ import annotations

import logging
import re

import dlt
import requests

BASE_URL = "https://api.dataplatform.knmi.nl/open-data/v1"
DATASET = "hourly-in-situ-meteorological-observations"
VERSION = "1.0"

# See the "No cursor, no watcher" note above.
FILES_PER_RUN = 30

_FILENAME_HOUR_RE = re.compile(r"-(\d{2})\.nc$")

log = logging.getLogger(__name__)


def _list_recent_files_raw(api_key: str, limit: int):
    """The `limit` most recently published files, newest first, exactly as
    KNMI's list-files endpoint returns them (filename, size, created,
    lastModified)."""
    resp = requests.get(
        f"{BASE_URL}/datasets/{DATASET}/versions/{VERSION}/files",
        params={"maxKeys": limit, "sorting": "desc", "orderBy": "lastModified"},
        headers={"Authorization": api_key},
        timeout=30,
    )
    resp.raise_for_status()
    yield from resp.json()["files"]


@dlt.resource(name="recent_files")
def recent_files(api_key: str, files_per_run: int = FILES_PER_RUN):
    yield from _list_recent_files_raw(api_key, limit=files_per_run)


@dlt.source(name="knmi_files")
def knmi_files_source(api_key: str, files_per_run: int = FILES_PER_RUN):
    yield recent_files(api_key, files_per_run)


def list_recent_files(api_key: str, files_per_run: int = FILES_PER_RUN) -> list[dict]:
    """This dataset's most recently published files, newest first."""
    source = knmi_files_source(api_key, files_per_run)
    return list(source.resources["recent_files"])


def download_url(api_key: str, filename: str) -> str:
    """A short-lived signed URL for one file -- the bytes come from here, not
    from api.dataplatform.knmi.nl itself."""
    resp = requests.get(
        f"{BASE_URL}/datasets/{DATASET}/versions/{VERSION}/files/{filename}/url",
        headers={"Authorization": api_key},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["temporaryDownloadUrl"]


def download(api_key: str, filename: str) -> bytes:
    """One file's raw bytes -- not run through dlt, since a single binary
    download has no structure for dlt to normalize."""
    resp = requests.get(download_url(api_key, filename), timeout=60)
    resp.raise_for_status()
    return resp.content


def hour_from_filename(filename: str) -> int:
    """The hour this file covers, read from its name
    (hourly-observations-YYYYMMDD-HH.nc) rather than opening it -- the task
    that lands the raw file (knmi_pipeline_dag.ingest_knmi_files) has no
    reason to parse NetCDF at all; that's the parsing task's job.
    """
    match = _FILENAME_HOUR_RE.search(filename)
    if not match:
        raise ValueError(f"can't read the hour out of filename {filename!r}")
    return int(match.group(1))


def api_key_from_connection(conn_id: str = "KNMI_REGISTERED_API_KEY") -> str:
    """The registered KNMI Data Platform key from an Airflow connection.

    A Connection, not a Variable: use a connection for anything that
    authenticates you, and reserve Variables for non-secret pipeline state.
    Generic connection type --
    KNMI's API takes one bearer-style key in an Authorization header, with
    no host/login/extra structure worth a more specific type -- stored as
    the connection's password.
    """
    from airflow.sdk import BaseHook

    conn = BaseHook.get_connection(conn_id)
    value = conn.password
    if not value or not value.strip():
        raise RuntimeError(f"connection {conn_id!r} has no password (the KNMI API key)")
    # A trailing newline (e.g. from how the value was entered) makes
    # `requests` reject it outright as an invalid Authorization header --
    # strip whitespace defensively rather than trust it's clean.
    return value.strip()
