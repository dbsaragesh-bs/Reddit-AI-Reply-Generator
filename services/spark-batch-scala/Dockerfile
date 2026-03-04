FROM sbtscala/scala-sbt:eclipse-temurin-jammy-17.0.10_7_1.9.9_2.12.18 AS builder

WORKDIR /app

COPY build.sbt .
COPY project/ project/

RUN sbt update

COPY src/ src/
RUN sbt assembly

# Runtime image
FROM apache/spark:3.5.0

USER root

# Install curl for downloading JARs
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/scala-2.12/spark-batch-service.jar /opt/spark-app/spark-batch-service.jar

# Add Kafka client JARs
RUN curl -L -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -L -o /opt/spark/jars/kafka-clients-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar && \
    curl -L -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    curl -L -o /opt/spark/jars/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

EXPOSE 8085

ENTRYPOINT ["/opt/spark/bin/spark-submit", \
    "--master", "spark://spark-master:7077", \
    "--deploy-mode", "client", \
    "--class", "com.reddit.batch.BatchApp", \
    "--conf", "spark.driver.memory=2g", \
    "--conf", "spark.executor.memory=2g", \
    "--conf", "spark.driver.maxResultSize=1g", \
    "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", \
    "/opt/spark-app/spark-batch-service.jar"]
