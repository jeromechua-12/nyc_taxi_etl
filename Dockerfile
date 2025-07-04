# images
FROM openjdk:11-slim

# install python build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    "wget" \
    "curl" \
    "tar" \
    "make" \
    "build-essential" \
    "zlib1g-dev" \
    "libncursesw5-dev" \
    "libgdbm-dev" \
    "libnss3-dev" \
    "libssl-dev" \
    "libreadline-dev" \
    "libffi-dev" \
    "libsqlite3-dev" \
    "libbz2-dev" \
    "liblzma-dev" \
    "uuid-dev" \
    "tk-dev" \
    "ca-certificates" \
    && rm -rf "/var/lib/apt/lists/*"

# download and build python from source
ENV PYTHON_VERSION=3.10.18
RUN wget "https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz" && \
    tar xzf "Python-${PYTHON_VERSION}.tgz" && \
    cd Python-${PYTHON_VERSION} && \
    ./configure --enable-optimizations --with-ensurepip=install && \
    make -j$(nproc) && \
    make altinstall && \
    cd .. && rm -rf "Python-${PYTHON_VERSION} Python-${PYTHON_VERSION}.tgz"

# Symlink python and pip
RUN ln -sf "/usr/local/bin/python3.10" "/usr/bin/python" && \
    ln -sf "/usr/local/bin/python3.10" "/usr/bin/python3" && \
    ln -sf "/usr/local/bin/pip3.10" "/usr/bin/pip" && \
    ln -sf "/usr/local/bin/pip3.10" "/usr/bin/pip3"

# environment variables
ENV PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    REQUESTS_CA_BUNDLE="/etc/ssl/certs/ca-certificates.crt"

# working directory
WORKDIR /app
COPY . .

# install python dependencies
RUN python3 -m pip install --upgrade pip
RUN pip3 install --no-cache-dir -r requirements.txt
RUN pip3 install --no-cache-dir \
    "apache-airflow[postgres]==3.0.2" \
    "apache-airflow-providers-fab" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.2/constraints-3.10.txt"

# create airflow user
ARG AIRFLOW_UID=50000
RUN adduser --uid $AIRFLOW_UID --disabled-password --gecos "" airflow
