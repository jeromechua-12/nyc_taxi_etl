from pyspark.sql import SparkSession
from pathlib import Path


spark = (SparkSession.builder
         .appName("nyc_taxi_etl")
         .getOrCreate())

# get data file
root_dir = Path(__file__).resolve().parents[1]
data_file = f"{root_dir}/data/yellow_tripdata_2024-01.parquet"

# read file
df = spark.read.parquet(data_file, header=True, inferSchema=True)
df.show()
