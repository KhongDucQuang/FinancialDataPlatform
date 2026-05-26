from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s


DATALAKE_BUCKET = "datn-financial-datalake-1773848181"
SYMBOLS = "BTCUSDT,ETHUSDT,BNBUSDT,ADAUSDT,SOLUSDT"
INTERVAL = "1m"

BRONZE_IMAGE = "khongducquang/binance-batch-scraper:v3"
SILVER_IMAGE = "khongducquang/spark-iceberg-batch:v2"


default_args = {
    "owner": "kdquang",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}


COMMON_NODE_SELECTOR = {
    "kubernetes.io/hostname": "data-platform-recover",
}

COMMON_TOLERATIONS = [
    k8s.V1Toleration(
        key="node-role.kubernetes.io/control-plane",
        operator="Exists",
        effect="NoSchedule",
    ),
    k8s.V1Toleration(
        key="node-role.kubernetes.io/master",
        operator="Exists",
        effect="NoSchedule",
    ),
]

GCS_SECRET_VOLUME = [
    k8s.V1Volume(
        name="gcs-key",
        secret=k8s.V1SecretVolumeSource(secret_name="batch-gcs-key"),
    )
]

GCS_SECRET_VOLUME_MOUNT = [
    k8s.V1VolumeMount(
        name="gcs-key",
        mount_path="/var/secrets/google",
        read_only=True,
    )
]

GCS_ENV = {
    "GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/gcs-key.json",
}


with DAG(
    dag_id="binance_daily_bronze_to_gcs",
    description="Daily Binance kline scraper to GCS bronze, then Spark bronze to silver Iceberg",
    default_args=default_args,
    start_date=datetime(2026, 5, 26),
    schedule="20 0 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["batch", "binance", "bronze", "silver", "gcs", "iceberg"],
) as dag:

    scrape_binance_to_bronze = KubernetesPodOperator(
        task_id="scrape_binance_to_bronze",
        name="scrape-binance-to-bronze",
        namespace="airflow",
        image=BRONZE_IMAGE,
        image_pull_policy="Always",
        node_selector=COMMON_NODE_SELECTOR,
        tolerations=COMMON_TOLERATIONS,
        arguments=[
            "--symbols", SYMBOLS,
            "--interval", INTERVAL,
            "--start-date", "{{ macros.ds_add(ds, -1) }}",
            "--end-date", "{{ ds }}",
            "--bucket", DATALAKE_BUCKET,
            "--prefix", "bronze/binance/kline",
        ],
        env_vars=GCS_ENV,
        volumes=GCS_SECRET_VOLUME,
        volume_mounts=GCS_SECRET_VOLUME_MOUNT,
        container_resources=k8s.V1ResourceRequirements(
            requests={
                "cpu": "50m",
                "memory": "256Mi",
            },
            limits={
                "cpu": "1",
                "memory": "2Gi",
            },
        ),
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
        startup_timeout_seconds=300,
        log_events_on_failure=True,
    )

    bronze_to_silver = KubernetesPodOperator(
        task_id="bronze_to_silver_iceberg",
        name="bronze-to-silver-iceberg",
        namespace="airflow",
        image=SILVER_IMAGE,
        image_pull_policy="Always",
        node_selector=COMMON_NODE_SELECTOR,
        tolerations=COMMON_TOLERATIONS,
        arguments=[
            "--bucket", DATALAKE_BUCKET,
            "--process-date", "{{ macros.ds_add(ds, -1) }}",
            "--interval", INTERVAL,
            "--mode", "merge",
        ],
        env_vars=GCS_ENV,
        volumes=GCS_SECRET_VOLUME,
        volume_mounts=GCS_SECRET_VOLUME_MOUNT,
        container_resources=k8s.V1ResourceRequirements(
            requests={
                "cpu": "20m",
                "memory": "512Mi",
            },
            limits={
                "cpu": "1",
                "memory": "2Gi",
            },
        ),
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
        startup_timeout_seconds=600,
        log_events_on_failure=True,
    )

    scrape_binance_to_bronze >> bronze_to_silver