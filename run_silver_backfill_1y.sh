#!/usr/bin/env bash
set -euo pipefail

START_DATE="2026-04-26"
END_DATE="2026-05-26"   # exclusive, không chạy ngày này
NAMESPACE="airflow"
TEMPLATE="infra/k8s-manifests/airflow/spark-bronze-to-silver-backfill.yaml"

current="$START_DATE"

echo "Start bronze -> silver backfill from $START_DATE to $END_DATE exclusive"

while [ "$current" != "$END_DATE" ]; do
  export PROCESS_DATE="$current"
  export PROCESS_DATE_COMPACT="$(echo "$PROCESS_DATE" | tr -d '-')"

  JOB_NAME="spark-b2s-${PROCESS_DATE_COMPACT}"

  echo "============================================================"
  echo "Processing date: $PROCESS_DATE"
  echo "Job name: $JOB_NAME"
  echo "============================================================"

  # Xóa job cũ nếu từng chạy rồi
  kubectl -n "$NAMESPACE" delete job "$JOB_NAME" --ignore-not-found

  # Tạo job mới
  envsubst < "$TEMPLATE" | kubectl apply -f -

  # Chờ job hoàn thành
  set +e
  kubectl -n "$NAMESPACE" wait --for=condition=complete "job/$JOB_NAME" --timeout=30m
  status=$?
  set -e

  if [ "$status" -ne 0 ]; then
    echo "Job failed or timeout for date $PROCESS_DATE"
    echo "Showing pod status and logs..."

    kubectl -n "$NAMESPACE" get pods | grep "$JOB_NAME" || true

    POD_NAME="$(kubectl -n "$NAMESPACE" get pods --selector=job-name="$JOB_NAME" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"

    if [ -n "$POD_NAME" ]; then
      kubectl -n "$NAMESPACE" describe pod "$POD_NAME" | sed -n '/Events/,$p' || true
      kubectl -n "$NAMESPACE" logs "$POD_NAME" || true
    fi

    echo "Stop backfill at failed date: $PROCESS_DATE"
    exit 1
  fi

  echo "Completed date: $PROCESS_DATE"

  # Có thể giữ job để audit, nhưng xóa sẽ đỡ đầy cluster
  kubectl -n "$NAMESPACE" delete job "$JOB_NAME" --ignore-not-found

  current="$(date -I -d "$current + 1 day")"
done

echo "Backfill bronze -> silver completed successfully."