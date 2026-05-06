"""
ETL DAG: MinIO (Data Lake) → ClickHouse OLAP.

Reads JSON batch files from user-events-lake, parses events, and inserts
into the ClickHouse user_events table.
"""

import json
from datetime import datetime, timezone

from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

BUCKET = "user-events-lake"
PREFIX = "user-events/"
PROCESSED_KEYS_VAR = "minio_to_clickhouse_processed_keys"


def check_clickhouse(**context):
    """Assert ClickHouse connectivity via a simple query."""
    hook = ClickHouseHook(clickhouse_conn_id="clickhouse_default")
    hook.execute("SELECT 1")
    context["ti"].xcom_push(key="clickhouse_ok", value=True)


def read_minio_files(**context):
    """Read only new MinIO objects for execution date and push events to XCom."""
    execution_date = context["execution_date"]
    year = execution_date.strftime("%Y")
    month = execution_date.strftime("%m")
    day = execution_date.strftime("%d")
    date_prefix = f"{PREFIX}year={year}/month={month}/day={day}/"

    hook = S3Hook(aws_conn_id="minio_s3")
    keys = hook.list_keys(bucket_name=BUCKET, prefix=date_prefix) or []
    processed_raw = Variable.get(PROCESSED_KEYS_VAR, default_var="[]")
    try:
        processed_keys = set(json.loads(processed_raw))
    except json.JSONDecodeError:
        processed_keys = set()

    new_keys = [key for key in keys if key not in processed_keys]
    events = []
    for key in new_keys:
        obj = hook.get_key(key=key, bucket_name=BUCKET)
        if obj:
            content = obj.get()["Body"].read().decode("utf-8")
            batch = json.loads(content)
            if isinstance(batch, list):
                events.extend(batch)
            else:
                events.append(batch)

    context["ti"].xcom_push(key="events", value=events)
    context["ti"].xcom_push(key="processed_keys_batch", value=new_keys)
    return len(events)


def insert_into_clickhouse(**context):
    """Insert events from XCom into ClickHouse user_events table."""
    events = context["ti"].xcom_pull(task_ids="read_minio_files", key="events") or []
    processed_keys_batch = context["ti"].xcom_pull(
        task_ids="read_minio_files", key="processed_keys_batch"
    ) or []
    if not events:
        print("No events to insert, skipping.")
        return 0

    rows = []
    for e in events:
        event_id = str(e.get("event_id", ""))
        event_type = str(e.get("event_type", ""))
        ts_str = e.get("timestamp", "")
        user_id = str(e.get("user_id", ""))
        props = json.dumps(e.get("properties", {}), ensure_ascii=False)

        try:
            ts_clickhouse = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            if ts_clickhouse.tzinfo is None:
                ts_clickhouse = ts_clickhouse.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            ts_clickhouse = datetime(1970, 1, 1, tzinfo=timezone.utc)

        rows.append((event_id, event_type, ts_clickhouse, user_id, props))

    hook = ClickHouseHook(clickhouse_conn_id="clickhouse_default")
    client = hook.get_conn()
    client.execute(
        "INSERT INTO default.user_events (event_id, event_type, ts, user_id, properties) VALUES",
        rows,
    )
    if processed_keys_batch:
        processed_raw = Variable.get(PROCESSED_KEYS_VAR, default_var="[]")
        try:
            processed_keys = set(json.loads(processed_raw))
        except json.JSONDecodeError:
            processed_keys = set()
        processed_keys.update(processed_keys_batch)
        Variable.set(PROCESSED_KEYS_VAR, json.dumps(sorted(processed_keys)))
    print(f"Inserted {len(rows)} rows into ClickHouse user_events")
    return len(rows)


with DAG(
    dag_id="minio_to_clickhouse",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["data_lake", "minio", "clickhouse", "etl"],
) as dag:
    task_check_clickhouse = PythonOperator(
        task_id="check_clickhouse",
        python_callable=check_clickhouse,
    )
    task_read_minio = PythonOperator(
        task_id="read_minio_files",
        python_callable=read_minio_files,
    )
    task_insert_clickhouse = PythonOperator(
        task_id="insert_into_clickhouse",
        python_callable=insert_into_clickhouse,
    )
    task_check_clickhouse >> task_read_minio >> task_insert_clickhouse
