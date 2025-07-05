import snowflake.connector
from pyspark.sql import SparkSession, DataFrame
from pathlib import Path
from etl.config import SNOWFLAKE_USER, SNOWFLAKE_PWD, SNOWFLAKE_ACCOUNT,\
    SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA,\
    SNOWFLAKE_URL, SNOWFLAKE_ROLE
from etl.schema import schema


def _create_table() -> None:
    '''
    Build connection to snowflake and create a new table to store data.
    Parameters:
        None

    Returns:
        None
    '''
    ctx = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PWD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    cs = ctx.cursor()
    create_query = """
    CREATE OR REPLACE TABLE nyc_yellow_taxi (
        VendorID CHAR(1),
        tpep_pickup_datetime TIMESTAMPNTZ,
        tpep_dropoff_datetime TIMESTAMPNTZ,
        passenger_count INT,
        trip_distance DOUBLE,
        RatecodeID VARCHAR(2),
        store_and_fwd_flag CHAR(1),
        PULocationID VARCHAR(3),
        DOLocationID VARCHAR(3),
        payment_type CHAR(1),
        fare_amount DOUBLE,
        extra DOUBLE,
        mta_tax DOUBLE,
        tip_amount DOUBLE,
        tolls_amount DOUBLE,
        improvement_surcharge DOUBLE,
        total_amount DOUBLE,
        congestion_surcharge DOUBLE,
        Airport_fee DOUBLE
    );
    """
    try:
        cs.execute(create_query)
        print("Table created succesfully.")
    except Exception as e:
        print(e)
    cs.close()
    ctx.close()


def _insert_data(df: DataFrame) -> None:
    '''
    Insert rows of dataframe to an existing table in snowflake database.

    Parameters:
        df (DataFrame): dataframe containing data.

    Returns:
        None
    '''
    sfOptions = {
        "sfURL": SNOWFLAKE_URL,
        "sfUser": SNOWFLAKE_USER,
        "sfPassword": SNOWFLAKE_PWD,
        "sfDatabase": SNOWFLAKE_DATABASE,
        "sfSchema": SNOWFLAKE_SCHEMA,
        "sfWarehouse": SNOWFLAKE_WAREHOUSE,
        "sfRole": SNOWFLAKE_ROLE
    }
    SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
    print("Inserting rows into table...")
    try:
        df.write\
            .format(SNOWFLAKE_SOURCE_NAME)\
            .options(**sfOptions)\
            .option("dbtable", "NYC_YELLOW_TAXI")\
            .mode("append")\
            .save()
        print("Rows inserted succesfully!")
    except Exception as e:
        print("Failed to insert data.")
        print(e)


def load() -> None:
    '''
    Read cleaned parquet data and load the data into a snowflake database.

    Parameters:
        None

    Return:
        None
    '''
    spark = SparkSession.builder \
        .appName("loader") \
        .config("spark.jars.packages",
                "net.snowflake:snowflake-jdbc:3.13.30,net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4") \
        .getOrCreate()

    # get path to cleaned file
    cur_dir = Path.cwd()
    cleaned_file = f"{cur_dir}/data/cleaned/yellow_tripdata_2024-01_cleaned.parquet"

    df = spark.read \
        .option("header", True) \
        .schema(schema) \
        .parquet(cleaned_file)
    _create_table()
    _insert_data(df)
    spark.stop()
