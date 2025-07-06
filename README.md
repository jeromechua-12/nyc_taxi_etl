# NYC Taxi Data ETL
![Build](https://img.shields.io/badge/build-passing-brightgreen)
![Python](https://img.shields.io/badge/python-v3.10-blue?logo=python&logoColor=white)
![Spark](https://img.shields.io/badge/apache--spark-grey?logo=apache-spark)
![Snowflake](https://img.shields.io/badge/snowflake-grey?logo=snowflake)
![Airflow](https://img.shields.io/badge/apache--airflow-grey?logo=apache-airflow)
![Docker](https://img.shields.io/badge/docker-grey?logo=docker)

This project implements a simple ETL pipeline using [NYC Taxi dataset](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

⚠️ Not fully cross platform supported out of the box. Docker image is configured for amd64.
Refer to [setup section](#setup) to configure for macOS.

## ETL Process

1. **Extract** 
    - Scrape and download monthly parquet files containing NYC Taxi Yellow data
    - Save parquet file to local storage for raw datasets
2. **Transform** (Pyspark)
    - Cast columns to appropriate data types
    - Remove rows with null values
    - Remove rows with negative numeric values
    - Save cleaned dataset to local storage for cleaned datasets
3. **Load** (Snowflake)
    - Used Snowflake connector to create database in Snowflake if it does not exist
    - Insert rows from cleaned dataset into database

## Airflow DAG
- Schedule and automate ETL pipeline monthly from Jan 2024 till May 2025.
- Perform backfill for missed months.
- Dynamically use the execution month to scrape data from the web.


## Setup 
- Project can be build using the Docker image.
- To build the container properly, ensure:
    - Snowflake account is created.
    - Fill up the credentials in .env file according to .env.template.
    - For macOS user, change "java-17-openjdk-amd64" to "java17-openjdk-arm64" in Dockerfile.
- For new Airflow UI, manually trigger the DAG in the UI once.
