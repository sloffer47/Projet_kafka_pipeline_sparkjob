FROM bitnami/spark:3.5

USER root

# Installer dépendances Python
RUN pip install --no-cache-dir pyspark psycopg2-binary

WORKDIR /opt/jobspark

COPY jobspark.py .

# Créer le répertoire des JARs et télécharger les dépendances
RUN mkdir -p /opt/spark/jars && \
    curl -f -L -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -f -L -o /opt/spark/jars/kafka-clients-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar && \
    curl -f -L -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    curl -f -L -o /opt/spark/jars/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# Vérifier que les JARs sont bien téléchargés
RUN ls -la /opt/spark/jars/

CMD ["spark-submit", \
     "--master", "spark://spark:7077", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", \
     "--driver-memory", "1g", \
     "--executor-memory", "1g", \
     "/opt/jobspark/jobspark.py"]