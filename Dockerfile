# images
FROM python:3.10.3-slim

# environment variables
ENV PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt \
    JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# install java 11
RUN apt update && \
    apt install -y "openjdk-11-jdk"

# ssl certificate for snowflake
RUN apt install -y "ca-certificates"

# working directory
WORKDIR /app
COPY . .

# install dependencies
RUN pip3 install --no-cache-dir -r requirements.txt

# run pipeline
CMD ["python3", "main.py"]
