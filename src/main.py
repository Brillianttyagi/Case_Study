"""
This module is the entry point of the application. 
It creates a SparkSession and runs the case study analysis job.
"""

from pyspark.sql import SparkSession

from src.jobs.case_study_analysis_job import CaseStudyAnalysis
from src.utils.file_utils import read_yaml

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Case Study Analysis").getOrCreate()

    # Set the log level to ERROR to avoid too much output
    spark.sparkContext.setLogLevel("ERROR")

    # Load the configuration
    config = read_yaml("config/config.yaml")

    # Run the case study analysis job
    case_study_analysis = CaseStudyAnalysis(spark, config)
    case_study_analysis.run()

    # Stop the SparkSession
    spark.stop()
