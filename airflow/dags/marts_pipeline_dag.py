"""Shared Marts DAG: `sea_state_hourly`, the first dbt model that actually
depends on more than one Source.

Runs a dbt project against Trino (`airflow/dbt/`) that inner-joins the
marine Source's Current forecast (`int_marine__current_hourly`) and the wind
Source's Current forecast (`int_wind__current_hourly`) -- each deduped from
staging to one row per `(location, forecast_time)` -- on
`(location, forecast_time)`, then left-joins the KNMI Source's Current
observation (`int_knmi__current_hourly`) onto the same key: observed columns
are NULL for any hour KNMI hasn't published yet (every future hour) and get
filled in once it has. Merges into `sea_state_hourly` keyed on
`(location, forecast_timestamp)` -- see
`airflow/dbt/models/marts/sea_state_hourly.sql`.

Unlike a Source, this DAG has no dlt ingestion of its own: all three inputs
are landed by marine_pipeline_dag.py, wind_pipeline_dag.py and
knmi_pipeline_dag.py, each on their own schedule. dbt-core/dbt-trino ship
in the Airflow runtime image, so the dbt task is a plain `@task`, same as
every Source's own `dbt_run` task.

dbt execution is a `dbtRunner()` call selecting `sea_state_hourly` directly
by name, not a Cosmos TaskGroup scoped to `tag:marts`.

Scheduled on the OR of the Asset each Source's DAG emits on a successful
`dbt_run` (`marine_pipeline_dag.MARINE_FORECAST_ASSET`,
`wind_pipeline_dag.WIND_FORECAST_ASSET`, `knmi_pipeline_dag.
KNMI_OBSERVATION_ASSET` -- matched here by name, since a DAG file has no
import route to another DAG file's module), plus a daily fallback so a
stalled Source doesn't block this DAG from ever running. This is also what
makes the KNMI Source's daily run land before this DAG picks it up, without
a fixed time-of-day gap between the two: this DAG re-runs as soon as
knmi_pipeline's own dbt_run succeeds, rather than at a guessed clock time
that could race if that run is ever late.

Task layout:
    dbt_run
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import Asset, dag, task
from airflow.timetables.assets import AssetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable
from databaas_target import target

# Ships inside the bundle at airflow/dbt/.
DBT_PROJECT_DIR = Path(__file__).resolve().parents[1] / "dbt"

# Same names as marine_pipeline_dag.MARINE_FORECAST_ASSET,
# wind_pipeline_dag.WIND_FORECAST_ASSET and
# knmi_pipeline_dag.KNMI_OBSERVATION_ASSET -- Airflow matches Assets by name,
# not object identity, so this DAG doesn't need to import any of them.
MARINE_FORECAST_ASSET = Asset(name=target.dag_id("marine_forecast_ingested"))
WIND_FORECAST_ASSET = Asset(name=target.dag_id("wind_forecast_ingested"))
KNMI_OBSERVATION_ASSET = Asset(name=target.dag_id("knmi_observation_ingested"))


@dag(
    dag_id=target.dag_id("marts_pipeline"),
    # Any Source's Asset triggers a run immediately; the daily cron is only
    # a fallback for a stalled Source, not the primary cadence.
    schedule=AssetOrTimeSchedule(
        timetable=CronTriggerTimetable("@daily", timezone="UTC"),
        assets=(MARINE_FORECAST_ASSET | WIND_FORECAST_ASSET | KNMI_OBSERVATION_ASSET),
    ),
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=target.paused,
    tags=target.tags("marts", "dbt", "iceberg"),
    doc_md=(__doc__ or "") + target.doc_suffix(),
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
)
def marts_pipeline():
    @task(task_id="dbt_run", retries=2, retry_delay=timedelta(minutes=2))
    def dbt_run(dbt_project_dir: str) -> None:
        import json
        import os
        from pathlib import Path

        from databaas_utils.airflow import get_settings, scoped

        # `+sea_state_hourly` (below) rebuilds stg_marine__hourly,
        # stg_wind__hourly and stg_knmi__hourly too (via the
        # int_marine__current_hourly / int_wind__current_hourly / int_knmi__
        # current_hourly intermediate layer), and those resolve via
        # source() against sources.yml's MARINE_*/WIND_*/KNMI_* env vars --
        # same as each Source's own dbt_run task, and for the same reason:
        # unset, they'd default to the unscoped (production) schema names
        # even on a branch deployment.
        from knmi_pipeline.lakehouse import TABLE as KNMI_TABLE
        from knmi_pipeline.lakehouse import namespace as knmi_namespace
        from marine_pipeline.lakehouse import TABLE as MARINE_TABLE
        from marine_pipeline.lakehouse import namespace as marine_namespace
        from wind_pipeline.lakehouse import TABLE as WIND_TABLE
        from wind_pipeline.lakehouse import namespace as wind_namespace

        os.environ["MARINE_SCHEMA"] = marine_namespace()
        os.environ["MARINE_TABLE"] = MARINE_TABLE
        os.environ["WIND_SCHEMA"] = wind_namespace()
        os.environ["WIND_TABLE"] = WIND_TABLE
        os.environ["KNMI_SCHEMA"] = knmi_namespace()
        os.environ["KNMI_TABLE"] = KNMI_TABLE

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
                # "build", not "run" -- +sea_state_hourly now also builds
                # int_marine__current_hourly/int_wind__current_hourly, which
                # carry uniqueness tests; "run" alone would
                # silently never execute them.
                "build",
                "--project-dir", dbt_project_dir,
                "--profiles-dir", dbt_project_dir,
                # `+sea_state_hourly`, not the bare model name -- the bare
                # form only builds that model itself, not the upstream
                # staging views it joins, so a namespace without those yet
                # (e.g. the first run against a fresh branch namespace)
                # fails with TABLE_NOT_FOUND.
                "--select", "+sea_state_hourly",
            ]
        )
        if not result.success:
            # Only the failed nodes' messages: the full result also carries the
            # task runner's argv, including its execution token.
            failed = [r.message for r in result.result or [] if r.status in ("error", "fail")]
            raise RuntimeError(f"dbt build failed: {result.exception or failed}")

    dbt_run(str(DBT_PROJECT_DIR))


_dag = marts_pipeline()

if __name__ == "__main__":
    import sys

    if "test" in sys.argv[1:]:
        _dag.test()
    else:
        print("parsed; pass 'test' to run it end to end")
