# Sử dụng bản Flink 1.18.1 chính thức
FROM flink:1.18.1

# Chuyển sang root để cài đặt và phân quyền
USER root

# Cài đặt wget để tải các connector
RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/flink/plugins/gs-fs-hadoop && \
    cp /opt/flink/opt/flink-gs-fs-hadoop-*.jar /opt/flink/plugins/gs-fs-hadoop/
    
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

# --- 2. Đưa file SQL Runner do bạn code vào image (vào thư mục /usrlib) ---
WORKDIR /opt/flink/usrlib
COPY sql-runner-1.0.jar /opt/flink/usrlib/sql-runner.jar

# --- 3. Đưa các file SQL Job vào image (vào thư mục /sql) ---
# Tạo thư mục sql
RUN mkdir -p /opt/flink/sql

# Copy TẤT CẢ các file .sql từ máy bạn (nằm cùng thư mục Dockerfile) vào image
# Cách này tiện vì sau này bạn có job2.sql, job3.sql nó cũng tự động copy hết
COPY *.sql /opt/flink/sql/

# Cấp lại quyền cho user flink để không bị lỗi permission denied
RUN chown -R flink:flink /opt/flink/lib /opt/flink/usrlib /opt/flink/sql

# Trả lại quyền cho user flink chạy ứng dụng
USER flink