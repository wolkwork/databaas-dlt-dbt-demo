"""Cosmos wiring shared by every DAG that runs this repo's dbt project.

`dbt_task_group()` renders a dbt selection as an Airflow TaskGroup in Cosmos's
watcher mode: one producer task runs a single `dbt build` for the whole
selection, and every model and test gets a deferrable sensor that reports its
own result, so the UI shows per-node status without starting dbt once per node.

The task graph comes from `dbt/target/manifest.json`, which must be generated
with `dbt parse` before deploying the bundle: the dag-processor parses DAG
files without dbt installed, so Cosmos must not run `dbt ls` there.

Trino accepts only a Zitadel JWT, so the profile is built per task, in the task
pod, from the lakehouse connection's service account
(`LakehouseTrinoJWTProfileMapping`). Nothing at parse time fetches a token.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, LoadMode
from cosmos.profiles.trino.jwt import TrinoJWTProfileMapping
from databaas_utils.airflow import DEFAULT_CONN_ID, scoped

DBT_PROJECT_DIR = Path(__file__).resolve().parents[1] / "dbt"
MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"
TRINO_HOST = os.environ.get("TRINO_HOST", "trino.example.databaas.app")


class LakehouseTrinoJWTProfileMapping(TrinoJWTProfileMapping):
    """Trino over JWT, the token minted for the lakehouse connection's service account.

    Cosmos's stock mapping reads a static `jwt__token` from a Trino connection;
    a Zitadel token expires within the hour, so this asks databaas-utils for a
    fresh one whenever Cosmos builds the profile's environment. Every other
    profile field comes from `profile_args`, not from the connection.
    """

    airflow_connection_type = "databaas_lakehouse"
    airflow_param_mapping: dict[str, str | list[str]] = {}

    def get_dbt_value(self, name: str):
        if name == "jwt_token":
            from databaas_utils.airflow import get_settings

            return get_settings(self.conn_id).auth_header().removeprefix("Bearer ")
        return super().get_dbt_value(name)


def dbt_task_group(
    *,
    group_id: str,
    select: list[str],
    source_env: dict[str, str],
    conn_id: str = DEFAULT_CONN_ID,
) -> DbtTaskGroup:
    """`select` from the dbt project as a watcher-mode TaskGroup.

    `source_env` carries the env vars `sources.yml` reads (`MARINE_SCHEMA`, ...),
    owned by the pipelines that write those tables. The catalog and dbt's own
    output namespaces come from `dbt_config.json`, scoped to this deployment.
    """
    config = json.loads((DBT_PROJECT_DIR / "dbt_config.json").read_text())
    namespaces = {env: scoped(base) for env, base in config["namespaces"].items()}

    return DbtTaskGroup(
        group_id=group_id,
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_DIR,
            manifest_path=MANIFEST_PATH,
            install_dbt_deps=False,
            env_vars={"TRINO_CATALOG": config["catalog"], **namespaces, **source_env},
        ),
        profile_config=ProfileConfig(
            profile_name="marine_transform",
            target_name="trino",
            profile_mapping=LakehouseTrinoJWTProfileMapping(
                conn_id=conn_id,
                profile_args={
                    "host": TRINO_HOST,
                    "port": 443,
                    "http_scheme": "https",
                    "cert": True,
                    "database": config["catalog"],
                    "schema": namespaces["TRINO_NAMESPACE"],
                    "threads": 4,
                },
            ),
        ),
        execution_config=ExecutionConfig(execution_mode=ExecutionMode.WATCHER),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            select=select,
            # Each DAG emits one Asset of its own once the whole group is done.
            emit_datasets=False,
        ),
    )
