
FROM apache/spark:3.5.3

USER root

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# Cài curl để tải JAR dependency
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Iceberg runtime cho Spark 3.5 + Scala 2.12
RUN curl -L -o ${SPARK_HOME}/jars/iceberg-spark-runtime-3.5_2.12-1.11.0.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.11.0/iceberg-spark-runtime-3.5_2.12-1.11.0.jar

# GCS connector cho Hadoop 3
RUN curl -L -o ${SPARK_HOME}/jars/gcs-connector-hadoop3-2.2.23-shaded.jar \
    https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.23/gcs-connector-hadoop3-2.2.23-shaded.jar

# Copy Spark job
WORKDIR /opt/spark/work-dir
COPY ./src/dags/bronze_to_silver_iceberg.py /opt/spark/work-dir/bronze_to_silver_iceberg.py

# Kiểm tra file tồn tại ngay lúc build
RUN ls -lah /opt/spark/work-dir/bronze_to_silver_iceberg.py \
    && ls -lah ${SPARK_HOME}/jars | grep -E "iceberg|gcs"

USER spark

ENTRYPOINT ["/opt/spark/bin/spark-submit", "/opt/spark/work-dir/bronze_to_silver_iceberg.py"]