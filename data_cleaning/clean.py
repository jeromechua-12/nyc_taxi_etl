from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from functools import reduce
from operator import and_


def _cast_type(df: DataFrame) -> DataFrame:
    '''
    Helper function to perform type casting on
    the columns of dataframe to intended types.

    Parameters:
        df (DataFrame): dataframe to perform type casting.

    Returns:
        DataFrame: Dataframe with new data types.
    '''
    mapping = {
        "VendorID": col("VendorID").cast("string"),
        "RatecodeID": col("RatecodeID").cast("string"),
        "passenger_count": col("passenger_count").cast("int"),
        "PULocationID": col("PULocationID").cast("string"),
        "DOLocationID": col("DOLocationID").cast("string"),
        "payment_type": col("payment_type").cast("string"),
    }
    df_type_casted = df.withColumns(mapping)
    return df_type_casted


def clean_data(df: DataFrame) -> DataFrame:
    '''
    Perform data cleaning and type casting on a dataframe. 

    Parameters:
        df (DataFrame): Dataframe to perform cleaning on. 

    Returns:
        DataFrame: cleaned dataframe.
    '''
    no_na_filter = " AND ".join([f"{c} is NOT NULL" for c in df.columns])
    df_no_na = df.filter(no_na_filter)

    # drop rows where distance/filter columns is negative
    non_negative_cols = ['passenger_count', 'trip_distance',
                         'fare_amount', 'extra', 'mta_tax', 'tip_amount',
                         'tolls_amount', 'improvement_surcharge', 'total_amount',
                         'congestion_surcharge', 'Airport_fee']
    no_negative_fare_filter = reduce(and_, [col(c) >= 0 for c in non_negative_cols])
    df_filtered = df_no_na.filter(no_negative_fare_filter)
    df_final = _cast_type(df_filtered)
    return df_final 
