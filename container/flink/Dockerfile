FROM flink:1.18
WORKDIR /opt/flink/lib

# Kafka Connector
RUN wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.1.0-1.18/flink-connector-kafka-3.1.0-1.18.jar

# JDBC Connector
RUN wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar

# PostgreSQL Driver
RUN wget -q https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

# JSON format 
RUN wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-json/1.18.0/flink-json-1.18.0.jar

RUN wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-avro/1.18.0/flink-avro-1.18.0.jar

RUN apt-get clean || true

