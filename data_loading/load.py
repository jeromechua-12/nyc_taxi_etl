import snowflake.connector
from pyspark.sql import SparkSession, DataFrame
from data_loading.config import SNOWFLAKE_USER, SNOWFLAKE_PWD, SNOWFLAKE_ACCOUNT,\
    SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA,\
    SNOWFLAKE_URL, SNOWFLAKE_ROLE


def create_table() -> None:
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


def insert_data(df: DataFrame) -> None:
    '''
    Insert data of a dataframe to an existing table in snowflake database.
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
    print("Inserting data to table...")
    try:
        df.write\
            .format(SNOWFLAKE_SOURCE_NAME)\
            .options(**sfOptions)\
            .option("dbtable", "NYC_YELLOW_TAXI")\
            .option("truncate_table", "on")\
            .mode("overwrite")\
            .save()
        print("Rows inserted succesfully!")
    except Exception as e:
        print("Failed to insert data.")
        print(e)
