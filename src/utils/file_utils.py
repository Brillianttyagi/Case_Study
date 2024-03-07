"""
This module provides utility functions for working with files.
"""

import yaml
from pyspark.sql import DataFrame


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


def write_csv(df: DataFrame, path: str) -> None:
    """
    Writes a DataFrame to a CSV file.

    Args:
        df: The DataFrame to write.
        path: The path to the CSV file.
    """
    df.write.csv(path, header=True, mode="overwrite")
