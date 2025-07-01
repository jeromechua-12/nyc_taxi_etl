import snowflake.connector
from config import SNOWFLAKE_USER, SNOWFLAKE_PWD, SNOWFLAKE_ACCOUNT,\
    SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_URL


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
        VendorID VARCHAR(1),
        tpep_pickup_datetime TIMESTAMPNTZ,
        tpep_dropoff_datetime TIMESTAMPNTZ,
        passenger_count INT,
        trip_distance DOUBLE,
        RatecodeID VARCHAR(2),
        store_and_fwd_flag CHAR(1),
        PULocationID INT,
        DOLocationID INT,
        payment_type INT,
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


def main():
    create_table()


if __name__ == "__main__":
    main()
