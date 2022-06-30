from pathlib import Path
import argparse
import pandas as pd
from sqlalchemy import create_engine

import datetime, re, os
from ruamel.yaml import YAML
from pathlib import Path
from pyspark_helpers import (
    yaml_reader,
    gather_paths,
    date_file_splitter,
    pandas_parquet_stream,
    database_insert,
)

NOW = datetime.datetime.now()
PARSER = argparse.ArgumentParser()
PARSER.add_argument("--date", default=datetime.datetime.strftime(NOW, "%Y%m%d"))
PARSER.add_argument(
    "--target_sources",
    type=str,
    action="append",
    default=None,
    help="add the csources to process default is all of them",
)

ARGS, _ = PARSER.parse_known_args()
TARGET_SOURCES = ARGS.target_sources
# crawlers = ["carrefour"]
ARGS, _ = PARSER.parse_known_args()


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

    HOST = os.environ["PG_HOST"]
    PG_PORT = int(os.environ["PG_PORT"])
    DATABASE = os.environ["PG_DB"]
    PASSWORD = os.environ["PG_PASSWORD"]
    USER = os.environ["PG_USER"]

    engine = create_engine(
        f"postgresql://{USER}:{PASSWORD}@{HOST}:{PG_PORT}/{DATABASE}"
    )
    conn = engine.connect()

    # data_dir = Path("dir/to/parquet/files")
    # full_df = pd.concat(
    #     pd.read_parquet(parquet_file) for parquet_file in data_dir.glob("*.parquet")
    # )
    SOURCES = staging_configs["sources"]
    if TARGET_SOURCES:
        SOURCES = [source for source in SOURCES if source in TARGET_SOURCES]

    for source_website in SOURCES:
        print(f"\nINFO: Processing data for source : {source_website}\n")
        files = gather_paths(
            f"{ROOT_DIR}/{STAGING_DIR}/{source_website}/{ARGS.date}", "parquet"
        )
        date_files = date_file_splitter(files)
        # date_files
        dfs = pandas_parquet_stream(date_files.values())
        database_insert(dfs, source_website, conn, mode="append")


if __name__ == "__main__":
    main()
