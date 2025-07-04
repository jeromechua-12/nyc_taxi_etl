from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from pathlib import Path
import os
from functools import reduce
from operator import and_
from etl.schema import schema


def clean() -> None:
    '''
    Read raw data from local data folder
    and perform data cleaning on a dataframe.
    Store cleaned data in local data directory.

    Parameters:
        None

    Returns:
        None
    '''
    spark = SparkSession.builder \
        .appName("cleaner") \
        .getOrCreate()

    # get path to raw file
    cur_dir = Path.cwd()
    raw_file = f"{cur_dir}/data/raw/yellow_tripdata_2024-01.parquet"

    df = spark.read \
    .option("header", True) \
    .schema(schema) \
    .parquet(raw_file)

    no_na_filter = " AND ".join([f"{c} is NOT NULL" for c in df.columns])
    df_no_na = df.filter(no_na_filter)

    # drop rows where distance/filter columns is negative
    non_negative_cols = ['passenger_count', 'trip_distance',
                         'fare_amount', 'extra', 'mta_tax', 'tip_amount',
                         'tolls_amount', 'improvement_surcharge', 'total_amount',
                         'congestion_surcharge', 'Airport_fee']
    no_negative_fare_filter = reduce(and_, [col(c) >= 0 for c in non_negative_cols])
    df_filtered = df_no_na.filter(no_negative_fare_filter)

    # create directory to store cleaned file
    cur_dir = Path.cwd()
    os.makedirs(f"{cur_dir}/data/cleaned", exist_ok=True)
    file_path = f"{cur_dir}/data/cleaned/yellow_tripdata_2024-01_cleaned.parquet"

    # write cleaned parquet file
    df_filtered.write \
        .parquet(file_path, mode="overwrite")
    spark.stop()
