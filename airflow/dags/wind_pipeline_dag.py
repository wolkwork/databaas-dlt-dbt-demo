"""Wind forecast ingestion (dlt) -> hourly staging (dbt via Cosmos).

Ingests the hourly wind speed/direction forecast for a single location via
dlt's REST API source and appends it, raw, into an Iceberg table
(`wind_pipeline/wind.py`, `wind_pipeline/lakehouse.py`). A dbt project then
unnests the raw hourly payload into one row per hour
(`airflow/dbt/models/staging/stg_wind__hourly.sql`).

Same Source structure as the marine Source, on its own 30-minute schedule so
the reference template demonstrates two Sources refreshing at genuinely
different cadences.

The dbt step is a Cosmos TaskGroup in watcher mode (`dbt_cosmos`), the same
as the marine Source: one producer task runs a single `dbt build` of
`stg_wind__hourly`, and each model and test in that selection shows up as its
own task reporting its own result.

On success, emits an Asset so the shared Marts DAG (`marts_pipeline_dag.py`)
can trigger off any Source's completion instead of polling on a fixed
schedule alone.

Task layout:
    ingest_wind_forecast ──> dbt (Cosmos TaskGroup) ──> transformed (emits an Asset)
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.sdk import Asset, dag, task
from databaas_utils.airflow.target import target
from dbt_cosmos import dbt_task_group

# Source schema/table names are owned by the pipeline code that actually
# writes them; namespace() already applies scoped() internally.
from wind_pipeline.lakehouse import TABLE as WIND_TABLE
from wind_pipeline.lakehouse import namespace as wind_namespace

# Scoped like `target.dag_id()` so a dev deployment's runs emit a distinct
# Asset from production's -- otherwise a dev branch's successful run would
# also satisfy production's Marts DAG trigger. Name matched by
# marts_pipeline_dag.py, which has no import route to this module's object.
WIND_FORECAST_ASSET = Asset(name=target.dag_id("wind_forecast_ingested"))


@dag(
    dag_id=target.dag_id("wind_pipeline"),
    # Genuinely different cadence from the marine Source's `@hourly` --
    # demonstrates two Sources refreshing independently.
    schedule="*/30 * * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=target.paused,
    tags=target.tags("wind", "dlt", "dbt", "iceberg"),
    doc_md=(__doc__ or "") + target.doc_suffix(),
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
)
def wind_pipeline():
    # ---- ingest: one task, extract + land raw ----

    @task(retries=3, retry_delay=timedelta(minutes=2))
    def ingest_wind_forecast() -> int:
        """Fetch the location's hourly wind speed/direction forecast via dlt
        and append it, untransformed, to wind_table -- one call, one row
        appended, no loop over locations. Reshaping into hourly rows is
        dbt's job; see wind_pipeline/lakehouse.py.
        """
        from datetime import datetime, timezone

        from wind_pipeline import lakehouse, wind
        from wind_pipeline.models import EUROPLATFORM_51_9978_3_2750

        payload = wind.fetch_forecast(EUROPLATFORM_51_9978_3_2750, forecast_days=7)
        # The API's own fields (latitude, longitude, hourly) become the
        # row's columns directly -- no wrapper -- plus location/fetched_at
        # for provenance, which the API response itself doesn't carry.
        row = {
            "location": EUROPLATFORM_51_9978_3_2750.name,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            **payload,
        }
        return lakehouse.write_table([row])

    # ---- hourly staging ----

    transform = dbt_task_group(
        group_id="dbt",
        # stg_wind__hourly selects only from a source(), so it has no
        # upstream model for a `+` to add.
        select=["stg_wind__hourly"],
        source_env={"WIND_SCHEMA": wind_namespace(), "WIND_TABLE": WIND_TABLE},
    )

    @task(outlets=[WIND_FORECAST_ASSET])
    def transformed() -> None:
        """Marks the whole dbt group done, for the Marts DAG's trigger."""

    ingest_wind_forecast() >> transform >> transformed()


_dag = wind_pipeline()

if __name__ == "__main__":
    import sys

    if "test" in sys.argv[1:]:
        _dag.test()
    else:
        print("parsed; pass 'test' to run it end to end")
