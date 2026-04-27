# Sử dụng Flink chính thức
FROM flink:1.18.1

USER root

# Cài wget
RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

# =========================
# Prometheus metrics → /plugins (đúng chuẩn)
# =========================
RUN mkdir -p /opt/flink/plugins/metrics-prometheus && \
    wget -q -O /opt/flink/plugins/metrics-prometheus/flink-metrics-prometheus-1.18.1.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-metrics-prometheus/1.18.1/flink-metrics-prometheus-1.18.1.jar

# Xóa sạch prometheus khỏi lib nếu có
RUN rm -f /opt/flink/lib/flink-metrics-prometheus-*.jar || true

# =========================
# GCS connector (plugin đúng chuẩn)
# =========================
RUN mkdir -p /opt/flink/plugins/gs-fs-hadoop && \
    cp /opt/flink/opt/flink-gs-fs-hadoop-*.jar /opt/flink/plugins/gs-fs-hadoop/ || true

# =========================
# SQL connectors & drivers
# =========================
WORKDIR /opt/flink/lib

# Kafka
RUN wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar

# JDBC
RUN wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar

# PostgreSQL
RUN wget -q https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

# Format
RUN wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-json/1.18.1/flink-json-1.18.1.jar
RUN wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-avro/1.18.1/flink-avro-1.18.1.jar

# =========================
# App của bạn
# =========================
WORKDIR /opt/flink/usrlib
COPY sql-runner-1.0.jar /opt/flink/usrlib/sql-runner.jar

# SQL job
RUN mkdir -p /opt/flink/sql
COPY *.sql /opt/flink/sql/

# =========================
# Permission
# =========================
RUN chown -R flink:flink /opt/flink/lib /opt/flink/usrlib /opt/flink/sql /opt/flink/plugins

USER flink