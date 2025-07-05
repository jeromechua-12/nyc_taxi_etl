FROM apache/airflow:3.0.2-python3.10

USER root 

# java installation and config
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk \
    wget \
    tar \
    && rm -rf "/var/lib/apt/lists/*"

ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64" \  
    PATH="$JAVA_HOME/bin:$PATH"

# spark installation and config
ENV SPARK_VERSION=3.4.4
RUN wget "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" && \
    tar xzf "spark-${SPARK_VERSION}-bin-hadoop3.tgz" -C /opt && \
    mv "/opt/spark-${SPARK_VERSION}-bin-hadoop3" "/opt/spark" && \
    rm -rf "spark-${SPARK_VERSION}-bin-hadoop3.tgz"

ENV SPARK_HOME="/opt/spark" \
    PATH="$SPARK_HOME/bin:$PATH"

USER airflow

COPY requirements.txt / 

RUN pip install --no-cache-dir -r /requirements.txt

