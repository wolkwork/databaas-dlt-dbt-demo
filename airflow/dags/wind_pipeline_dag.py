"""Wind forecast ingestion (dlt) -> hourly staging (dbt).

Ingests the hourly wind speed/direction forecast for a single location via
dlt's REST API source and appends it, raw, into an Iceberg table
(`wind_pipeline/wind.py`, `wind_pipeline/lakehouse.py`). A dbt project then
unnests the raw hourly payload into one row per hour
(`airflow/dbt/models/staging/stg_wind__hourly.sql`).

Same Source structure as the marine Source, on its own 30-minute schedule so
the reference template demonstrates two Sources refreshing at genuinely
different cadences. dlt, dbt-core/dbt-trino and databaas-utils all ship in
the Airflow runtime image the task pods run, so both tasks are plain `@task`s;
only DAG-parse-time code is limited to the application image.

dbt execution is a `dbtRunner()` call, not a Cosmos TaskGroup, mirroring the
marine Source's own `dbt_run` task exactly.

On success, emits an Asset so the shared Marts DAG (`marts_pipeline_dag.py`)
can trigger off either Source's completion instead of polling on a fixed
schedule alone.

Task layout:
    ingest_wind_forecast ──> dbt_run (emits an Asset)

`dbt/` lives at `airflow/dbt/` -- inside the deployed bundle (the whole
`airflow/` tree is deployed), so the dbt task resolves it relative to this
file.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import Asset, dag, task
from databaas_target import target

# Ships inside the bundle at airflow/dbt/ -- see the module docstring.
DBT_PROJECT_DIR = Path(__file__).resolve().parents[1] / "dbt"

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

    @task(
        task_id="dbt_run",
        retries=2,
        retry_delay=timedelta(minutes=2),
        outlets=[WIND_FORECAST_ASSET],
    )
    def dbt_run(dbt_project_dir: str) -> None:
        import json
        import os
        from pathlib import Path

        from databaas_utils.airflow import get_settings, scoped

        # Source schema/table names are owned by the pipeline code that
        # actually writes them -- import directly instead of copying the
        # strings into dbt_config.json/sources.yml by hand, where they could
        # drift. namespace() already applies scoped() internally.
        from wind_pipeline.lakehouse import TABLE as WIND_TABLE
        from wind_pipeline.lakehouse import namespace as wind_namespace

        os.environ["WIND_SCHEMA"] = wind_namespace()
        os.environ["WIND_TABLE"] = WIND_TABLE

        # Catalog and dbt's own output namespace have no pipeline owner like
        # the above -- dbt_config.json is the one place for those, still
        # scoped here at run time since scoping is a deployment fact, not
        # configuration.
        config = json.loads((Path(dbt_project_dir) / "dbt_config.json").read_text())
        os.environ["TRINO_CATALOG"] = config["catalog"]
        for env_var_name, base_name in config["namespaces"].items():
            os.environ[env_var_name] = scoped(base_name)

        # Trino accepts only a Zitadel JWT: the lakehouse connection's own
        # service-account token, so Trino's principal is that account.
        os.environ["TRINO_JWT"] = get_settings().auth_header().removeprefix("Bearer ")

        from dbt.cli.main import dbtRunner

        result = dbtRunner().invoke(
            [
                "run",
                "--project-dir", dbt_project_dir,
                "--profiles-dir", dbt_project_dir,
                "--select", "stg_wind__hourly",
            ]
        )
        if not result.success:
            # Only the failed nodes' messages: the full result also carries the
            # task runner's argv, including its execution token.
            failed = [r.message for r in result.result or [] if r.status in ("error", "fail")]
            raise RuntimeError(f"dbt run failed: {result.exception or failed}")

    ingested = ingest_wind_forecast()
    transform_task = dbt_run(str(DBT_PROJECT_DIR))
    ingested >> transform_task


_dag = wind_pipeline()

if __name__ == "__main__":
    import sys

    if "test" in sys.argv[1:]:
        _dag.test()
    else:
        print("parsed; pass 'test' to run it end to end")
