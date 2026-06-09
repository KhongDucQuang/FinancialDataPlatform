FROM apache/spark:3.5.3

USER root

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    curl \
    ca-certificates \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

RUN pip install --no-cache-dir psycopg2-binary

RUN curl -L -o ${SPARK_HOME}/jars/iceberg-spark-runtime-3.5_2.12-1.11.0.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.11.0/iceberg-spark-runtime-3.5_2.12-1.11.0.jar

RUN curl -L -o ${SPARK_HOME}/jars/gcs-connector-hadoop3-2.2.23-shaded.jar \
    https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.23/gcs-connector-hadoop3-2.2.23-shaded.jar

WORKDIR /opt/spark/work-dir

COPY ./container/spark-gold/silver_to_gold.py /opt/spark/work-dir/silver_to_gold.py

RUN java -version \
    && ls -lah /opt/spark/work-dir/silver_to_gold.py \
    && ls -lah ${SPARK_HOME}/jars | grep -E "iceberg|gcs"

USER spark

ENTRYPOINT ["/opt/spark/bin/spark-submit", "/opt/spark/work-dir/silver_to_gold.py"]