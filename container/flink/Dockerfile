# Sử dụng bản Flink 1.18.1 chính thức
FROM flink:1.18.1

# Chuyển sang root để cài đặt và phân quyền
USER root

# Cài đặt wget TRƯỚC để dùng cho các bước tải file
RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

# 1. Tạo thư mục và tải TRỰC TIẾP Prometheus plugin từ Maven
RUN mkdir -p /opt/flink/plugins/metrics-prometheus
RUN wget -q -O /opt/flink/plugins/metrics-prometheus/flink-metrics-prometheus-1.18.1.jar https://repo1.maven.org/maven2/org/apache/flink/flink-metrics-prometheus/1.18.1/flink-metrics-prometheus-1.18.1.jar

# 2. Xử lý GCS Hadoop (Tạm thời cứ dùng cp, nếu lỗi tiếp ta sẽ wget nốt)
RUN mkdir -p /opt/flink/plugins/gs-fs-hadoop && cp /opt/flink/opt/flink-gs-fs-hadoop-*.jar /opt/flink/plugins/gs-fs-hadoop/ || true

# --- Cài đặt các SQL Connectors & Drivers (vào thư mục /lib) ---
WORKDIR /opt/flink/lib

# Kafka Connector
RUN wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar

# JDBC Connector
RUN wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar

# PostgreSQL Driver (cho Sink DB)
RUN wget -q https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

RUN wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-json/1.18.1/flink-json-1.18.1.jar
RUN wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-avro/1.18.1/flink-avro-1.18.1.jar

# --- Đưa file SQL Runner do bạn code vào image (vào thư mục /usrlib) ---
WORKDIR /opt/flink/usrlib
COPY sql-runner-1.0.jar /opt/flink/usrlib/sql-runner.jar

# --- Đưa các file SQL Job vào image (vào thư mục /sql) ---
RUN mkdir -p /opt/flink/sql
COPY *.sql /opt/flink/sql/

# 3. Cấp lại quyền cho user flink
RUN chown -R flink:flink /opt/flink/lib /opt/flink/usrlib /opt/flink/sql /opt/flink/plugins

# Trả lại quyền cho user flink chạy ứng dụng
USER flink