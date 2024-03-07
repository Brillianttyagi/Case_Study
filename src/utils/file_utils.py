from pyspark.sql import DataFrame


def read_csv(spark, path: str) -> DataFrame:
    """
    Reads a CSV file into a DataFrame using Spark.

    Args:
        spark: The SparkSession object.
        path: The path to the CSV file.

    Returns:
        A DataFrame containing the data from the CSV file.
    """
    return spark.read.csv(path, header=True, inferSchema=True)
