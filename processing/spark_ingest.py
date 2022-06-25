import findspark

findspark.init()
import datetime, re, os

import pyspark
from pyspark.sql import SparkSession, functions as f, types as t, SQLContext, DataFrame
from pyspark import SparkConf, SparkContext
from pyspark.sql.window import Window

from ruamel.yaml import YAML
from pathlib import Path

from utils.pyspark_helpers import (
    snake_case_string,
    convert_to_json,
    impute_timestamp,
    yaml_reader,
    ParquetWriter,
)

ROOT_DIR = "/home/cube/development/projects/price_tracker"
BASE_DIR = "datasets/base"
STAGING_DIR = "datasets/staging"
driver_path = f"{ROOT_DIR}/drivers/postgresql-42.4.0.jar"
spark = (
    SparkSession.builder.appName("xero")
    .config("spark.driver.extraClassPath", f"{driver_path}")
    .getOrCreate()
)


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
    SOURCES = staging_configs["sources"]

    HOST = os.environ["HOST"]
    PG_PORT = int(os.environ["PG_PORT"])
    DATABASE = os.environ["DATABASE"]
    PASSWORD = os.environ["PASSWORD"]
    USER = os.environ["USER"]

    for source_website in SOURCES:
        df = spark.read.parquet(f"{ROOT_DIR}/{STAGING_DIR}/{source_website}/*/*/*/**")
        df.printSchema()

        mode = "overwrite"
        url = f"jdbc:postgresql://{HOST}:{PG_PORT}/{DATABASE}"
        properties = {
            "user": USER,
            "password": PASSWORD,
            "driver": "org.postgresql.Driver",
        }
        print(f"\nINFO: writting source {source_website}")
        df.write.jdbc(
            url=url, table=f"{source_website}", mode=mode, properties=properties
        )
        print(f"\nINFO: done writting source {source_website}")


if __name__ == "__main__":
    main()
