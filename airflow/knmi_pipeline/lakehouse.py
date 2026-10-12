"""Where KNMI data lands, in two stages matching the two-task split in
knmi_pipeline_dag.py.

Stage 1, `raw_store()`/`raw_index()`: the downloaded NetCDF files themselves,
untouched, in a Lakekeeper generic table (RAW_FILES_TABLE) -- not the
structured Iceberg table below. `ingest_knmi_files` stores each file and
records it in the index; `parse_knmi_observations` fetches each one back to
parse, following dlt's "ingest as a file" pattern.

Stage 2, `write_table()`: one Iceberg table, the parsed rows appended each
call -- same shape as marine_pipeline/lakehouse.py and
wind_pipeline/lakehouse.py, except a single call here can append many rows
(one per landed file that carried our station, typically ~24 -- see
knmi.py), not always exactly one.

Appends rather than overwrites: each run is more rows of history, and a
retried or overlapping run may re-append an hour already seen -- the Current
observation intermediate model collapses that down to one
row per (location, hour) before any mart consumes it, the same dedup
mechanism the marine/wind Sources' Current forecast models use.
"""

from __future__ import annotations

import logging

from databaas_utils.airflow import scoped

log = logging.getLogger(__name__)

SOURCE = "knmi_observation"
TABLE = "knmi_table"
RAW_FILES_TABLE = "raw_files"


def namespace() -> str:
    """`knmi_observation` in production, `knmi_observation_dev_<slug>` on a
    branch deploy."""
    return scoped(SOURCE)


def raw_store():
    """Where downloaded KNMI files land (stage 1) and get read back from for
    parsing (stage 2) -- a Lakekeeper generic table, distinct from the
    structured knmi_table below."""
    from databaas_utils.airflow import get_store

    return get_store(namespace=namespace())


def raw_index():
    """The record of which raw files have actually landed -- a generic
    table keeps no file list of its own, so this companion Iceberg table is
    what makes its contents queryable."""
    from databaas_utils.airflow import get_index

    return get_index(namespace=namespace())


def file_record(*, uri: str, filename: str, product: str, forecast_hour: int, size_bytes: int):
    """An index row for one landed KNMI file.

    `forecast_hour` doesn't quite fit an Observation source -- the field is
    shaped for forecast-run files -- but it's the closest slot the shared
    index schema has for "which hour this file covers", so the caller passes
    the hour read from the filename
    (see knmi.hour_from_filename) rather than leaving it unset.
    """
    from databaas_utils import FileRecord

    return FileRecord(
        uri=uri,
        key=filename,
        product=product,
        forecast_hour=forecast_hour,
        size_bytes=size_bytes,
    )


def write_table(rows: list[dict]) -> int:
    """Append the parsed KNMI observation row(s) -- one per landed file that
    carried our station -- growing the table's history. A run that found no
    rows (every recent file missed our station) appends nothing rather than
    writing an empty-schema batch."""
    if not rows:
        log.info("no rows to append -- no recent file carried our station")
        return 0

    import pyarrow as pa
    from databaas_utils.airflow import get_catalog

    cat = get_catalog()
    ns = namespace()
    cat.create_namespace_if_not_exists(ns)

    data = pa.Table.from_pylist(rows)
    table_id = f"{ns}.{TABLE}"
    table = cat.create_table_if_not_exists(table_id, schema=data.schema)
    table.append(data)
    log.info("appended %d row(s) to %s", data.num_rows, table_id)
    return data.num_rows
