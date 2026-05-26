from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s


DATALAKE_BUCKET = "datn-financial-datalake-1773848181"
SYMBOLS = "BTCUSDT,ETHUSDT,BNBUSDT,ADAUSDT,SOLUSDT"
INTERVAL = "1m"
IMAGE = "khongducquang/binance-batch-scraper:v3"


default_args = {
    "owner": "kdquang",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}


with DAG(
    dag_id="binance_daily_bronze_to_gcs",
    description="Daily Binance kline scraper to GCS bronze parquet snappy",
    default_args=default_args,
    start_date=datetime(2026, 5, 26),
    schedule="20 0 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["batch", "binance", "bronze", "gcs"],
) as dag:

    scrape_binance_to_bronze = KubernetesPodOperator(
        task_id="scrape_binance_to_bronze",
        name="scrape-binance-to-bronze",
        namespace="airflow",
        image=IMAGE,
        image_pull_policy="Always",
        arguments=[
            "--symbols", SYMBOLS,
            "--interval", INTERVAL,
            "--start-date", "{{ macros.ds_add(ds, -1) }}",
            "--end-date", "{{ ds }}",
            "--bucket", DATALAKE_BUCKET,
            "--prefix", "bronze/binance/kline",
        ],
        env_vars={
            "GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/gcs-key.json",
        },
        volumes=[
            k8s.V1Volume(
                name="gcs-key",
                secret=k8s.V1SecretVolumeSource(secret_name="batch-gcs-key"),
            )
        ],
        volume_mounts=[
            k8s.V1VolumeMount(
                name="gcs-key",
                mount_path="/var/secrets/google",
                read_only=True,
            )
        ],
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "250m", "memory": "512Mi"},
            limits={"cpu": "1", "memory": "2Gi"},
        ),
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
        startup_timeout_seconds=300,
        log_events_on_failure=True,
    )