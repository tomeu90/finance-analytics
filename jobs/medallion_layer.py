# Upgrade Databricks SDK to the latest version and restart Python to see updated packages
%pip install --upgrade databricks-sdk==0.49.0
%restart_python

from databricks.sdk.service.jobs import JobSettings as Job


medallion_layer = Job.from_dict(
    {
        "name": "medallion_layer",
        "schedule": {
            "quartz_cron_expression": "0 5 23 ? * MON-FRI",
            "timezone_id": "America/New_York",
            "pause_status": "UNPAUSED",
        },
        "max_concurrent_runs": 2,
        "tasks": [
            {
                "task_key": "trigger_is_open",
                "spark_python_task": {
                    "python_file": "/Workspace/finance-analytics/utils/trg_holidays.py",
                },
                "environment_key": "Default",
            },
            {
                "task_key": "ingest_raw_index_components",
                "depends_on": [
                    {
                        "task_key": "trigger_is_open",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "/Workspace/finance-analytics/sources/src_ingest_raw_index_lists",
                    "source": "WORKSPACE",
                },
            },
            {
                "task_key": "ingest_raw_index_keys",
                "depends_on": [
                    {
                        "task_key": "trigger_is_open",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "/Workspace/finance-analytics/sources/src_ingest_raw_index_keys",
                    "source": "WORKSPACE",
                },
            },
            {
                "task_key": "ingest_insiders_trx",
                "depends_on": [
                    {
                        "task_key": "ingest_raw_index_keys",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "/Workspace/finance-analytics/sources/src_ingest_raw_insiders_trx",
                    "source": "WORKSPACE",
                },
            },
            {
                "task_key": "ingest_raw_prices",
                "depends_on": [
                    {
                        "task_key": "ingest_raw_index_keys",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "/Workspace/finance-analytics/sources/src_ingest_raw_prices",
                    "source": "WORKSPACE",
                },
            },
            {
                "task_key": "ingest_raw_sic_codes",
                "depends_on": [
                    {
                        "task_key": "trigger_is_open",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "/Workspace/finance-analytics/sources/src_ingest_raw_sic_codes",
                    "source": "WORKSPACE",
                },
            },
            {
                "task_key": "dbt_task",
                "depends_on": [
                    {
                        "task_key": "ingest_insiders_trx",
                    },
                    {
                        "task_key": "ingest_raw_prices",
                    },
                    {
                        "task_key": "ingest_raw_sic_codes",
                    },
                    {
                        "task_key": "ingest_raw_index_components",
                    },
                ],
                "dbt_task": {
                    "project_directory": "dbt",
                    "commands": [
                        "dbt deps",
                        "dbt build",
                    ],
                    "schema": "db",
                    "warehouse_id": "wh_id",
                    "catalog": "finance_catalog",
                    "source": "GIT",
                },
                "environment_key": "dbt-default",
            },
        ],
        "git_source": {
            "git_url": "https://github.com/tomeu90/finance-analytics",
            "git_provider": "gitHub",
            "git_branch": "main",
        },
        "queue": {
            "enabled": True,
        },
        "environments": [
            {
                "environment_key": "Default",
                "spec": {
                    "client": "2",
                },
            },
            {
                "environment_key": "dbt-default",
                "spec": {
                    "client": "2",
                    "dependencies": [
                        "dbt-databricks>=1.0.0,<2.0.0",
                    ],
                },
            },
        ],
        "performance_target": "STANDARD",
    }
)

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
w.jobs.reset(new_settings=medallion_layer, job_id=id)
# or create a new job using: w.jobs.create(**medallion_layer.as_shallow_dict())
