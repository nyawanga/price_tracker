import findspark

findspark.init()
import datetime, re

import pyspark
from pyspark.sql import SparkSession, functions as f, types as t, SQLContext, DataFrame
from pyspark import SparkConf, SparkContext
from pyspark.sql.window import Window

import boto3

from ruamel.yaml import YAML
from pathlib import Path
from utils.pyspark_helpers import (
    snake_case_string,
    convert_to_json,
    impute_timestamp,
    yaml_reader,
    ParquetWriter,
)

driver_path = "mysql-connector-java-8.0.23.jar"
spark = (
    SparkSession.builder.appName("xero")
    .config("spark.driver.extraClassPath", f"{driver_path}")
    .getOrCreate()
)


def light_processing(df: DataFrame, source: str, configs: dict) -> DataFrame:
    schema = configs[source]["schema"]
    for field in df.schema.fields:
        if field.name in schema["string"]:
            df = df.withColumn(field.name, f.trim(field.name))

    df = df.select([convert_to_json(field) for field in df.schema.fields])

    for column in df.columns:
        df = df.withColumnRenamed(column, snake_case_string(column))

    for field in df.schema.fields:
        if field.name in schema["datetime"]:
            df = impute_timestamp(df, field.name)

    return df


def main() -> None:
    ROOT_DIR = "/home/cube/development/projects/price_tracker"
    PROCESSING_DIR = "processing/configs"
    staging_configs = yaml_reader(
        bucket="",
        bucket_prefix=f"{ROOT_DIR}/{PROCESSING_DIR}/staging_configs.yml",
        source="local",
    )
    BASE_DIR = staging_configs["BASE_DIR"]
    STAGING_DIR = staging_configs["STAGING_DIR"]
    sources = staging_configs["sources"]

    for source_website in sources:
        print(f"\nINFO: Processing data for source : {source_website}\n")
        df = spark.read.option("multiline", "true").json(
            f"{ROOT_DIR}/{BASE_DIR}/{source_website}/*.json"
        )
        df = light_processing(df, source_website, staging_configs)

        df = df.withColumn("source_website", f.lit(source_website))

        # this is because you lose the column used when partitioning in parquet
        df = df.withColumn("source", f.col("source_website"))

        ParquetWriter().writer(
            df=df,
            partitions=["source", "year", "month", "day"],
            path=f"{ROOT_DIR}/{STAGING_DIR}/{source_website}",
            mode="overwrite",
            year_month_day_partitioning=True,
            date_col="created_at",
        )
        print(f"\nINFO: Processing finished for source : {source_website}\n")


if __name__ == "__main__":
    main()
