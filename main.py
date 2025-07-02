from data_extraction.extract import extract_data
from data_cleaning.clean import clean_data
from data_loading.load import create_table, insert_data
from pyspark.sql import SparkSession

def main():
    file_path = extract_data()
    if not file_path:
        print("No file extracted.")
        return
    create_table()
    # create spark session
    spark = SparkSession.builder\
        .appName("nyc_taxi_etl")\
        .getOrCreate()
    df = spark.read.parquet("data/yellow_tripdata_2024-01.parquet")
    df_cleaned = clean_data(df)
    print("Dataframe cleaned succesfully!")
    df_cleaned.printSchema()
    insert_data(df_cleaned)
    spark.stop()


if __name__ == "__main__":
    main()
