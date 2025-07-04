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
RUN ln -s "/usr/local/bin/python3.10 /usr/bin/python3" && \
    ln -s "/usr/local/bin/pip3.10 /usr/bin/pip3"

# environment variables
ENV PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    REQUESTS_CA_BUNDLE="/etc/ssl/certs/ca-certificates.crt"

# working directory
WORKDIR /app
COPY . .

# install python dependencies
RUN python3 -m pip install --upgrade "pip"
RUN pip3 install --no-cache-dir -r "requirements.txt"

