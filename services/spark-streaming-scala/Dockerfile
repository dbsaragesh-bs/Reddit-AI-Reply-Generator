FROM sbtscala/scala-sbt:eclipse-temurin-jammy-17.0.10_7_1.9.9_2.12.18 AS builder

WORKDIR /app

# Copy build files first for caching
COPY build.sbt .
COPY project/ project/

# Download dependencies
RUN sbt update

# Copy source and build
COPY src/ src/
RUN sbt assembly

# Runtime image
FROM apache/spark:3.5.0

USER root

# Install curl for downloading JARs
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Copy the fat JAR
COPY --from=builder /app/target/scala-2.12/spark-streaming-service.jar /opt/spark-app/spark-streaming-service.jar

# Add Kafka client JARs
RUN curl -L -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -L -o /opt/spark/jars/kafka-clients-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar && \
    curl -L -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    curl -L -o /opt/spark/jars/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# Entrypoint
ENTRYPOINT ["/opt/spark/bin/spark-submit", \
    "--master", "spark://spark-master:7077", \
    "--deploy-mode", "client", \
    "--class", "com.reddit.streaming.StreamingApp", \
    "--conf", "spark.driver.memory=1g", \
    "--conf", "spark.executor.memory=1g", \
    "--conf", "spark.sql.streaming.checkpointLocation=/checkpoints", \
    "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", \
    "/opt/spark-app/spark-streaming-service.jar"]
