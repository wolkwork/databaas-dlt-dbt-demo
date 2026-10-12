"""KNMI observation ingestion (dlt, as a file) -> hourly staging (dbt).

Ingests the most recently published hourly observation files for a single
Dutch offshore station (Europlatform, see knmi_pipeline/models.py). KNMI's
file API doesn't fit `rest_api_resources`' config shape the way the
marine/wind Sources' single JSON call does, so this follows dlt's "ingest
as a file" pattern instead, split across two tasks rather than downloading
and parsing in one:

- `ingest_knmi_files` lists the most recent files (a `@dlt.resource`, since
  that response is structured) and downloads each one, landing the raw
  NetCDF bytes in object storage (knmi_pipeline.lakehouse.raw_store()) and
  recording each in an index (knmi_pipeline.lakehouse.raw_index()), since a
  generic table keeps no file list of its own. It returns only each file's
  `{uri, key}`, not its bytes.
- `parse_knmi_observations` fetches each landed file back from object
  storage, parses out our station's reading (knmi_pipeline/parsing.py, a
  module deliberately separate from knmi.py so this task never imports
  `dlt`), and appends the parsed
  rows into knmi_table (knmi_pipeline/lakehouse.py's write_table()).

Landing the raw file first means a parsing bug found later can be fixed and
re-run against the already-landed files instead of re-hitting KNMI's API,
and a failure partway through a batch doesn't discard files already
downloaded. A dbt project then casts the parsed rows into one row per hour
(airflow/dbt/models/staging/stg_knmi__hourly.sql).

Daily, not hourly: KNMI batches this dataset's files once overnight rather
than publishing each hour as it happens -- an hourly trigger would just
re-poll for files that aren't there yet. `knmi.py`'s FILES_PER_RUN (30, not
1) is what actually covers the whole overnight batch; the daily schedule
just avoids running that fetch when nothing new can possibly exist yet.

Same Source structure otherwise as the marine/wind Sources: its own dbt step
is a `dbtRunner()` call, and it stops at staging only
-- mirroring the wind Source, not the marine one -- since KNMI has no
private mart of its own; only the shared sea_state_hourly (see
marts_pipeline_dag.py) consumes its Current observation.

On success, emits an Asset so the shared Marts DAG can trigger off this
Source's completion, same as the marine/wind Sources -- and per your
request, scheduled so this Source's own daily run lands before the Marts
DAG's Asset-triggered run picks it up, rather than racing a fixed time gap.

Task layout:
    ingest_knmi_files ──> parse_knmi_observations ──> dbt_run (emits an Asset)

dlt, dbt and databaas-utils ship in the Airflow runtime image the task pods
run, so those tasks are plain `@task`s. netCDF4 does not, so
`parse_knmi_observations` alone is a `@task.virtualenv` on top of the image's
packages.

`dbt/` lives at `airflow/dbt/` -- inside the deployed bundle (the whole
`airflow/` tree is deployed), so the dbt task resolves it relative to this
file, the same way `BUNDLE_IMPORT_ROOT` does.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import Asset, dag, task
from databaas_target import target

# The bundle is on the task process's sys.path, not on a @task.virtualenv
# body's fresh interpreter -- compute it once here and pass it into the venv
# task, which imports the pipeline's own package.
BUNDLE_IMPORT_ROOT = str(Path(__file__).resolve().parents[1])

# Ships inside the bundle at airflow/dbt/ -- see the module docstring.
DBT_PROJECT_DIR = Path(BUNDLE_IMPORT_ROOT) / "dbt"

# Scoped like `target.dag_id()` so a dev deployment's runs emit a distinct
# Asset from production's -- otherwise a dev branch's successful run would
# also satisfy production's Marts DAG trigger. Name matched by
# marts_pipeline_dag.py, which has no import route to this module's object.
KNMI_OBSERVATION_ASSET = Asset(name=target.dag_id("knmi_observation_ingested"))


@dag(
    dag_id=target.dag_id("knmi_pipeline"),
    # Not hourly -- see the module docstring: KNMI only publishes new files
    # for this dataset once a night.
    schedule="@daily",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=target.paused,
    tags=target.tags("knmi", "dlt", "dbt", "iceberg"),
    doc_md=(__doc__ or "") + target.doc_suffix(),
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
)
def knmi_pipeline():
    # ---- ingest, stage 1: list + land raw files in object storage ----

    @task(retries=3, retry_delay=timedelta(minutes=2))
    def ingest_knmi_files() -> list[dict]:
        """List the most recently published files and land each one, raw,
        in object storage -- parsing happens in parse_knmi_observations, not
        here. Returns each file's {uri, key}, not its bytes, so the XCom
        this task hands off stays small no matter how large the files are.
        """
        import tempfile
        from pathlib import Path

        from knmi_pipeline import knmi, lakehouse

        api_key = knmi.api_key_from_connection()
        store = lakehouse.raw_store()
        index = lakehouse.raw_index()

        landed = []
        records = []
        for file_info in knmi.list_recent_files(api_key):
            filename = file_info["filename"]
            content = knmi.download(api_key, filename)
            with tempfile.NamedTemporaryFile(suffix=".nc") as tmp:
                tmp.write(content)
                tmp.flush()
                uri = store.put(
                    table=lakehouse.RAW_FILES_TABLE, key=filename, source=Path(tmp.name)
                )
            records.append(
                lakehouse.file_record(
                    uri=uri,
                    filename=filename,
                    product=knmi.DATASET,
                    forecast_hour=knmi.hour_from_filename(filename),
                    size_bytes=len(content),
                )
            )
            landed.append({"uri": uri, "key": filename})

        index.record(*records)
        return landed

    # ---- ingest, stage 2: fetch each landed file back and parse it ----

    @task.virtualenv(
        # netCDF4 to read KNMI's binary file format, the one package this
        # needs that the runtime image doesn't carry; databaas-utils,
        # pyiceberg and obstore come from the image's site-packages.
        requirements=["netCDF4==1.7.2"],
        system_site_packages=True,
        venv_cache_path="/tmp/venvs",
        retries=3,
        retry_delay=timedelta(minutes=2),
    )
    def parse_knmi_observations(landed: list[dict], bundle_import_root: str) -> int:
        """Fetch each file ingest_knmi_files landed back from object
        storage, parse out our station's hourly reading, and append the
        results to knmi_table -- reshaping into cast hourly rows from there
        is dbt's job; see knmi_pipeline/lakehouse.py.
        """
        import sys

        sys.path.insert(0, bundle_import_root)

        import tempfile
        from datetime import datetime, timezone
        from pathlib import Path

        from knmi_pipeline import lakehouse, parsing
        from knmi_pipeline.models import EUROPLATFORM_51_9978_3_2750

        store = lakehouse.raw_store()
        fetched_at = datetime.now(timezone.utc).isoformat()

        rows = []
        for ref in landed:
            with tempfile.NamedTemporaryFile(suffix=".nc") as tmp:
                tmp_path = Path(tmp.name)
                store.get(table=lakehouse.RAW_FILES_TABLE, key=ref["key"], dest=tmp_path)
                row = parsing.extract_station_row(tmp_path, EUROPLATFORM_51_9978_3_2750)
            if row is not None:
                rows.append({**row, "fetched_at": fetched_at})

        return lakehouse.write_table(rows)

    # ---- hourly staging ----

    @task(
        task_id="dbt_run",
        retries=2,
        retry_delay=timedelta(minutes=2),
        outlets=[KNMI_OBSERVATION_ASSET],
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
        from knmi_pipeline.lakehouse import TABLE as KNMI_TABLE
        from knmi_pipeline.lakehouse import namespace as knmi_namespace

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
                "run",
                "--project-dir", dbt_project_dir,
                "--profiles-dir", dbt_project_dir,
                # `+stg_knmi__hourly`, not the bare model name
                # (a bare model name skips its `ref()` ancestors,
                # which can fail TABLE_NOT_FOUND on a fresh namespace) --
                # though stg_knmi__hourly only selects from a source(), not
                # a ref(), so it has no ancestor to miss and this is a no-op
                # today. Keeping the `+` anyway: cheap, consistent, and
                # correct automatically if this model ever grows a ref().
                "--select", "+stg_knmi__hourly",
            ]
        )
        if not result.success:
            # Only the failed nodes' messages: the full result also carries the
            # task runner's argv, including its execution token.
            failed = [r.message for r in result.result or [] if r.status in ("error", "fail")]
            raise RuntimeError(f"dbt run failed: {result.exception or failed}")

    landed = ingest_knmi_files()
    parsed = parse_knmi_observations(landed, BUNDLE_IMPORT_ROOT)
    transform_task = dbt_run(str(DBT_PROJECT_DIR))
    parsed >> transform_task


_dag = knmi_pipeline()

if __name__ == "__main__":
    import sys

    if "test" in sys.argv[1:]:
        _dag.test()
    else:
        print("parsed; pass 'test' to run it end to end")
