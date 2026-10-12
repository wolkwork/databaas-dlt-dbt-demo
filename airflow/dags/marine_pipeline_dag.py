"""Marine forecast ingestion (dlt) -> weekly wave-height aggregation (dbt via Cosmos).

Ingests the hourly wave-height forecast for a single location via dlt's REST
API source and appends it, raw, into an Iceberg table
(`marine_pipeline/marine.py`, `marine_pipeline/lakehouse.py`). A dbt project
then unnests the raw hourly payload into one row per hour and aggregates
wave height by week (`airflow/dbt/models/`).

The dbt step is a Cosmos TaskGroup in watcher mode (`dbt_cosmos`): one
producer task runs a single `dbt build` of `+marine_wave_height_weekly`, and
each model and test in that selection shows up as its own task reporting its
own result.

Task layout:
    ingest_marine_forecast ──> dbt (Cosmos TaskGroup) ──> transformed (emits an Asset)

On success, emits an Asset so the shared Marts DAG (`marts_pipeline_dag.py`)
can trigger off either Source's completion instead of polling on a fixed
schedule alone.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.sdk import Asset, dag, task
from databaas_target import target
from dbt_cosmos import dbt_task_group

# Source schema/table names are owned by the pipeline code that actually
# writes them; namespace() already applies scoped() internally.
from marine_pipeline.lakehouse import TABLE as MARINE_TABLE
from marine_pipeline.lakehouse import namespace as marine_namespace

# Scoped like `target.dag_id()` so a dev deployment's runs emit a distinct
# Asset from production's -- otherwise a dev branch's successful run would
# also satisfy production's Marts DAG trigger. Name matched by
# marts_pipeline_dag.py, which has no import route to this module's object.
MARINE_FORECAST_ASSET = Asset(name=target.dag_id("marine_forecast_ingested"))


@dag(
    dag_id=target.dag_id("marine_pipeline"),
    # Hourly so each run re-fetches the full 7-day forecast close to when
    # Open-Meteo last revised its model.
    schedule="@hourly",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=target.paused,
    tags=target.tags("marine", "dlt", "dbt", "iceberg"),
    doc_md=(__doc__ or "") + target.doc_suffix(),
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
)
def marine_pipeline():
    # ---- ingest: one task, extract + land raw ----

    @task(retries=3, retry_delay=timedelta(minutes=2))
    def ingest_marine_forecast() -> int:
        """Fetch the location's hourly wave-height forecast via dlt and
        append it, untransformed, to marine_table -- one call, one
        row appended, no loop over locations. Reshaping into hourly rows
        (and the weekly aggregation) is dbt's job; see
        marine_pipeline/lakehouse.py.
        """
        from datetime import datetime, timezone

        from marine_pipeline import lakehouse, marine
        from marine_pipeline.models import EUROPLATFORM_51_9978_3_2750

        payload = marine.fetch_forecast(EUROPLATFORM_51_9978_3_2750, forecast_days=7)
        # The API's own fields (latitude, longitude, hourly) become the
        # row's columns directly -- no wrapper -- plus location/fetched_at
        # for provenance, which the API response itself doesn't carry.
        row = {
            "location": EUROPLATFORM_51_9978_3_2750.name,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            **payload,
        }
        return lakehouse.write_table([row])

    # ---- weekly aggregation ----

    transform = dbt_task_group(
        group_id="dbt",
        # `+`: also builds the staging and intermediate views it selects
        # from, so a fresh (e.g. branch) namespace doesn't fail with
        # TABLE_NOT_FOUND.
        select=["+marine_wave_height_weekly"],
        source_env={"MARINE_SCHEMA": marine_namespace(), "MARINE_TABLE": MARINE_TABLE},
    )

    @task(outlets=[MARINE_FORECAST_ASSET])
    def transformed() -> None:
        """Marks the whole dbt group done, for the Marts DAG's trigger."""

    ingest_marine_forecast() >> transform >> transformed()


_dag = marine_pipeline()

if __name__ == "__main__":
    import sys

    if "test" in sys.argv[1:]:
        _dag.test()
    else:
        print("parsed; pass 'test' to run it end to end")
