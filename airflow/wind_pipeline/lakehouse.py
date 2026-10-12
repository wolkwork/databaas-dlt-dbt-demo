"""Where the wind forecast lands: one Iceberg table, the raw payload
untransformed, appended each call.

`write_table()` stores exactly what `wind.fetch_forecast()` returned -- the
API's own fields (`latitude`, `longitude`, `hourly`) become the row's columns
directly, plus `location`/`fetched_at` for provenance. `hourly` lands as a
nested struct<list<...>> column, so dbt reads it with Trino's UNNEST syntax
downstream, not a JSON parse. Reshaping into
one row per (call, hour) is dbt's job, not this module's.

Appends rather than overwrites: each call is one more row of history.
"""

from __future__ import annotations

import logging

from databaas_utils.airflow import scoped

log = logging.getLogger(__name__)

SOURCE = "wind_forecast"
TABLE = "wind_table"


def namespace() -> str:
    """`wind_forecast` in production, `wind_forecast_dev_<slug>` on a
    branch deploy."""
    return scoped(SOURCE)


def write_table(rows: list[dict]) -> int:
    """Append the raw, untransformed wind forecast row(s), growing the
    table's history."""
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
