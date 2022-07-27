from pyspark.sql import DataFrame

def unique_records(df: DataFrame) -> DataFrame:
    return df.distinct()