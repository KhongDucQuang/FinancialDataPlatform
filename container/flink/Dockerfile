# Sử dụng bản Flink 1.18.1 chính thức
FROM flink:1.18.1

# Chuyển sang root để cài đặt và phân quyền
USER root

# Cài đặt wget để tải các connector
RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

# --- 1. Cài đặt các SQL Connectors & Drivers (vào thư mục /lib) ---
WORKDIR /opt/flink/lib

# Kafka Connector
RUN wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar

# JDBC Connector
RUN wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar

# PostgreSQL Driver (cho Sink DB)
RUN wget -q https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

RUN wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-json/1.18.1/flink-json-1.18.1.jar
RUN wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-avro/1.18.1/flink-avro-1.18.1.jar

WORKDIR /opt/flink/usrlib
RUN wget -q -O sql-runner.jar https://repo.maven.apache.org/maven2/org/apache/flink/flink-table-api-java-uber/1.18.1/flink-table-api-java-uber-1.18.1.jar

RUN chown -R flink:flink /opt/flink/lib /opt/flink/usrlib

USER flink