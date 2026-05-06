"""
Skeleton DAG: verifies connectivity to MinIO (Data Lake).
Uses S3Hook with connection id 'minio_s3' (from AIRFLOW_CONN_MINIO_S3).
"""

from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime

BUCKET = "user-events-lake"
PREFIX = "user-events/"


def check_bucket_exists(**context):
    """Assert the data lake bucket exists."""
    hook = S3Hook(aws_conn_id="minio_s3")
    exists = hook.check_for_bucket(bucket_name=BUCKET)
    if not exists:
        raise ValueError(f"Bucket {BUCKET} does not exist")
    context["ti"].xcom_push(key="bucket_exists", value=True)


def list_lake_files(**context):
    """List keys under user-events/ prefix and log count + sample paths."""
    hook = S3Hook(aws_conn_id="minio_s3")
    keys = hook.list_keys(bucket_name=BUCKET, prefix=PREFIX)
    count = len(keys) if keys else 0
    sample = keys[-5:] if keys else []
    print(f"Found {count} file(s) under {PREFIX}")
    for k in sample:
        print(f"  - {k}")
    context["ti"].xcom_push(key="file_count", value=count)


with DAG(
    dag_id="minio_lake_connectivity_check",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["data_lake", "minio"],
) as dag:
    task_check_bucket = PythonOperator(
        task_id="check_bucket_exists",
        python_callable=check_bucket_exists,
    )
    task_list_files = PythonOperator(
        task_id="list_lake_files",
        python_callable=list_lake_files,
    )
    task_check_bucket >> task_list_files
