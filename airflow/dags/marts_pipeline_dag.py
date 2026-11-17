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
knmi_pipeline_dag.py, each on their own schedule.

The dbt step is a Cosmos TaskGroup in watcher mode (`dbt_cosmos`), the same
as every Source's: one producer task runs a single `dbt build` of
`+sea_state_hourly`, and each model and test in that selection shows up as
its own task reporting its own result. The `+` rebuilds the upstream staging
and intermediate views too, so a fresh (e.g. branch) namespace doesn't fail
with TABLE_NOT_FOUND, and `build` runs the intermediate layer's uniqueness
tests.

Scheduled on the OR of the Asset each Source's DAG emits once its dbt group
succeeds (`marine_pipeline_dag.MARINE_FORECAST_ASSET`,
`wind_pipeline_dag.WIND_FORECAST_ASSET`, `knmi_pipeline_dag.
KNMI_OBSERVATION_ASSET` -- matched here by name, since a DAG file has no
import route to another DAG file's module), plus a daily fallback so a
stalled Source doesn't block this DAG from ever running. This is also what
makes the KNMI Source's daily run land before this DAG picks it up, without
a fixed time-of-day gap between the two: this DAG re-runs as soon as
knmi_pipeline's own dbt group succeeds, rather than at a guessed clock time
that could race if that run is ever late.

Task layout:
    dbt (Cosmos TaskGroup)
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.sdk import Asset, dag
from airflow.timetables.assets import AssetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable
from databaas_utils.airflow.target import target
from dbt_cosmos import dbt_task_group

# `+sea_state_hourly` builds all three Sources' staging models, which resolve
# via source() against sources.yml's MARINE_*/WIND_*/KNMI_* env vars. Unset,
# they'd default to the unscoped (production) schema names even on a branch
# deployment, so pass the names the pipeline code that writes them owns;
# namespace() already applies scoped() internally.
from knmi_pipeline.lakehouse import TABLE as KNMI_TABLE
from knmi_pipeline.lakehouse import namespace as knmi_namespace
from marine_pipeline.lakehouse import TABLE as MARINE_TABLE
from marine_pipeline.lakehouse import namespace as marine_namespace
from wind_pipeline.lakehouse import TABLE as WIND_TABLE
from wind_pipeline.lakehouse import namespace as wind_namespace

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
    dbt_task_group(
        group_id="dbt",
        select=["+sea_state_hourly"],
        source_env={
            "MARINE_SCHEMA": marine_namespace(),
            "MARINE_TABLE": MARINE_TABLE,
            "WIND_SCHEMA": wind_namespace(),
            "WIND_TABLE": WIND_TABLE,
            "KNMI_SCHEMA": knmi_namespace(),
            "KNMI_TABLE": KNMI_TABLE,
        },
    )


_dag = marts_pipeline()

if __name__ == "__main__":
    import sys

    if "test" in sys.argv[1:]:
        _dag.test()
    else:
        print("parsed; pass 'test' to run it end to end")
