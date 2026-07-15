FROM bitnami/spark:3.4

USER root

# Connectors needed to read Parquet from MinIO (S3A) and write to PostgreSQL (JDBC).
# Versions matched to Spark 3.4 / Hadoop 3.3.4.
RUN mkdir -p /opt/bitnami/spark/jars && \
    curl -fsSL -o /opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fsSL -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -fsSL -o /opt/bitnami/spark/jars/postgresql-42.7.3.jar \
      https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

WORKDIR /app
COPY batch_processing.py .

ENTRYPOINT ["spark-submit", "--master", "local[*]", "/app/batch_processing.py"]
