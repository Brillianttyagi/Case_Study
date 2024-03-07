"""
This module provides utility functions for working with files.
"""

import yaml
from pyspark.sql import DataFrame, SparkSession


def read_yaml(path: str) -> dict:
    """
    Reads a YAML file into a dictionary.

    Args:
        path: The path to the YAML file.

    Returns:
        A dictionary containing the data from the YAML file.
    """
    with open(path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def read_csv(spark: SparkSession, path: str, fileformat: str) -> DataFrame:
    """
    Reads a CSV file into a DataFrame using Spark.

    Args:
        spark: The SparkSession object.
        path: The path to the CSV file.
        fileformat: The format of the file, e.g., "csv", "parquet", etc.

    Returns:
        A DataFrame containing the data from the CSV file.
    """
    return (
        spark.read.format(fileformat)
        .option("header", "true")
        .option("inferSchema", "true")
        .load(path)
    )


def write_csv(df: DataFrame, path: str, fileformat: str) -> None:
    """
    Writes a DataFrame to a CSV file using Spark.

    Args:
        df: The DataFrame to write.
        path: The path to the output CSV file.
        fileformat: The format of the file, e.g., "csv", "parquet", etc.
    """
    df.write.format(fileformat).mode("overwrite").option("header", "true").save(path)
