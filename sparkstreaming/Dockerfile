FROM apache/spark:3.5.9-python3

USER root

ARG SPARK_VERSION=3.5.9
ARG SCALA_VERSION=2.12
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1
ARG POSTGRES_JDBC_VERSION=42.7.4

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl; \
    rm -rf /var/lib/apt/lists/*; \
    cd "${SPARK_HOME}/jars"; \
    base=https://repo1.maven.org/maven2; \
    curl -fsSLO "${base}/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar"; \
    curl -fsSLO "${base}/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar"; \
    curl -fsSLO "${base}/org/apache/spark/spark-avro_${SCALA_VERSION}/${SPARK_VERSION}/spark-avro_${SCALA_VERSION}-${SPARK_VERSION}.jar"; \
    curl -fsSLO "${base}/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar"; \
    curl -fsSLO "${base}/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar"; \
    curl -fsSLO "${base}/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"

COPY sparkstreaming/src/ /opt/app/

COPY finnhubproducer/src/schemas/trades.avsc /opt/app/schemas/trades.avsc

RUN mkdir -p /opt/spark-checkpoints && chown -R 185:0 /opt/spark-checkpoints /opt/app

USER 185

WORKDIR /opt/app

CMD ["/opt/spark/bin/spark-submit", "--master", "local[*]", "/opt/app/stream.py"]
