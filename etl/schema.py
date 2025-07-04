from pyspark.sql.types import StructType, StructField, \
    CharType, VarcharType, IntegerType, DoubleType, \
    TimestampNTZType


schema = StructType([
    StructField("VendorID", CharType(1), True),
    StructField("tpep_pickup_datetime", TimestampNTZType(), True),
    StructField("tpep_dropoff_datetime", TimestampNTZType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", VarcharType(2), True),
    StructField("store_and_fwd_flag", CharType(1), True),
    StructField("PULocationID", VarcharType(3), True),
    StructField("DOLocationID", VarcharType(3), True),
    StructField("payment_type", CharType(1), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("Airport_fee", DoubleType(), True)
])
