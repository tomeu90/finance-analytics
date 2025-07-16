# Upgrade Databricks SDK to the latest version and restart Python to see updated packages
%pip install --upgrade databricks-sdk==0.49.0
%restart_python

from databricks.sdk.service.jobs import JobSettings as Job


time_layer = Job.from_dict(
    {
        "name": "time_layer",
        "schedule": {
            "quartz_cron_expression": "0 0 23 1 1 ? *",
            "timezone_id": "America/Chicago",
            "pause_status": "UNPAUSED",
        },
        "tasks": [
            {
                "task_key": "ingest_raw_holidays",
                "notebook_task": {
                    "notebook_path": "/Workspace/finance-analytics/sources/src_ingest_raw_holidays",
                    "source": "WORKSPACE",
                },
            },
            {
                "task_key": "ingest_raw_time",
                "notebook_task": {
                    "notebook_path": "/Workspace/finance-analytics/sources/src_ingest_raw_time",
                    "source": "WORKSPACE",
                },
            },
        ],
        "queue": {
            "enabled": True,
        },
        "performance_target": "STANDARD",
    }
)

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
w.jobs.reset(new_settings=time_layer, job_id=53661287662690)
# or create a new job using: w.jobs.create(**time_layer.as_shallow_dict())
