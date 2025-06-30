from pyspark.sql import SparkSession, DataFrame
from pathlib import Path
from pyspark.sql.functions import col
from functools import reduce
from operator import and_


def clean_data(file: str) -> DataFrame:
    '''
    Read a parquet file as a dataframe and perform data cleaning.
    Returns the cleaned dataframe.
    Parameters:
        file (str): a parquet file.
    Returns:
        DataFrame: cleaned dataframe.
    '''
    # create spark session 
    spark = (SparkSession.builder
             .appName("nyc_taxi_etl")
             .getOrCreate())

    # create dataframe
    df = spark.read.parquet(file, header=True, inferSchema=True)

    # drop rows with missing value
    no_na_filter = " AND ".join([f"{c} is NOT NULL" for c in df.columns])
    df_no_na = df.filter(no_na_filter)

    # drop rows where distance/filter columns is negative
    non_negative_cols = ['passenger_count', 'trip_distance',
                         'fare_amount', 'extra', 'mta_tax', 'tip_amount',
                         'tolls_amount', 'improvement_surcharge', 'total_amount',
                         'congestion_surcharge', 'Airport_fee']
    no_negative_fare_filter = reduce(and_, [col(c) >= 0 for c in non_negative_cols])
    df_filtered = df_no_na.filter(no_negative_fare_filter)
    return df_filtered
