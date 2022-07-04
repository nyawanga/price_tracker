from typing import Tuple
import re
from datetime import datetime, timedelta
from urllib.parse import urlparse
from pathlib import Path
from ruamel.yaml import YAML

import boto3
from dateutil.relativedelta import relativedelta
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.functions import udf
from pyspark.sql import types as t


def prefix_columns(
    data_frame: DataFrame, prefix: str, cols: Tuple[set, list]
) -> DataFrame:
    """
    Prefix columns of a dataframe with a specific keyword.
    """
    for col in cols:
        data_frame = data_frame.withColumnRenamed(col, f"{col}{prefix}")
    return data_frame


def convert_to_json(field: str) -> str:
    """
    Handles nested json strings that need to retain their structure.
    This also guarantees that data is not lost during schema inference.

    data_frame = data_frame.select(
        [convert_to_json(field) for field in data_frame.schema.fields]
    )
    """
    if isinstance(field.dataType, t.StructType) or isinstance(
        field.dataType, t.ArrayType
    ):
        return f.to_json(field.name).alias(field.name)
    return field.name


def get_data_from_s3(s3_path: str) -> str:
    """
    Gets a file from s3 and saves it to the current local directory.
    This is ideal for serverless environments like glue jobs that have temp
    working directories for authentication files and configs.
    :param s3_path: Full s3 path to the file.
    :returns: Path of the file saved in the current working directory.
    """
    url_parse = urlparse(s3_path)
    bucket = url_parse.netloc
    key = url_parse.path[1:]  # removed post-fixed '/' from path
    s3_resource = boto3.resource("s3")
    s3_resource.meta.client.download_file(bucket, key, key.split("/")[-1])
    return key.split("/")[-1]


def phrase_to_date(phrase: str) -> datetime:
    """
    A nifty tool that converts a  particular phrase structure into a
    standard python date object. For example, yesterday would return
    a date object of yesterday's date, today also works.
    The phrases are as follows:
        today
        yesterday
        <n>_days_ago
        <n>_weeks_ago
        <n>_months_ago
        <n>_years_ago
    :param phrase: String phrase.
    :return: python datetime object.
    """
    if phrase == "today":
        return datetime.now()

    if phrase == "yesterday":
        return datetime.now() - timedelta(days=1)

    try:
        phrase_constituents = phrase.split("_")
        phrase_increment = int(phrase_constituents[0])
        phrase_period = phrase_constituents[1]

        if phrase_period in ["day", "days"]:
            return datetime.now() - timedelta(days=phrase_increment)
        if phrase_period in ["week", "weeks"]:
            return datetime.now() - relativedelta(weeks=phrase_increment)
        if phrase_period in ["month", "months"]:
            return datetime.now() - relativedelta(months=phrase_increment)
        if phrase_period in ["year", "years"]:
            return datetime.now() - relativedelta(years=phrase_increment)

    except IndexError as err:
        raise ValueError(
            f"The given configuration date phrase: {phrase} is not valid, "
            f"please use the format: <integer>_<period>_ago"
        ) from err


def define_date_range(start_date, end_date, fmt="%Y-%m-%d") -> list:
    """
    Simply consumes a start date and end date, and returns a list
    of dates to iterate in whatever format required.
    :param start_date: Python datetime object.
    :param end_date: Python datetime object.
    :param fmt: String format consumes by .strftime()
    :return: List of dates.
    """
    start_date = phrase_to_date(start_date)
    end_date = phrase_to_date(end_date)
    delta = end_date - start_date
    return [
        (start_date + timedelta(days=i)).strftime(fmt) for i in range(delta.days + 1)
    ]


def snake_case_string(field: str, replace: tuple = None) -> str:
    """
    Converts any CamelCase string into snake_case with the added functionality
    to replace a string.
    :param field: The string object to be converted.
    :param replace: Optional set of two values where value 0 will be replaced with value 1 in the field string.
    :return: The snake_case string.
    """
    if replace is None:
        return re.sub(r"(?<!^)(?=[A-Z])", "_", field).lower()

    # if replace is not none, make sure it is a valid set
    if replace is not None and len(replace) != 2:
        raise ValueError(
            "Replace param only consumes one set of two values for the replace method."
        )

    return re.sub(
        r"(?<!^)(?=[A-Z])", "_", field.replace(replace[0], replace[1])
    ).lower()


def add_metadata_columns(df: DataFrame) -> DataFrame:
    """
    Adds two useful fields for the di environment to assess when the
    data was last updated and where the data is stored in s3.
    """
    return df.withColumn("last_updated", f.lit(datetime.now())).withColumn(
        "file_path", f.input_file_name()
    )


def process_data_types(
    schema: dict, data_frame: DataFrame, date_fmt="yyyyMMdd"
) -> DataFrame:
    """
    Upon providing a schema, the standard procedures of ensuring
    the data of the given fields
    are of the correct format and type is established.
    :param schema: The json object representing the data type,
                   and a list of fields that need to be converted
                   into that data type.
                   Example:
                   SCHEMA = {
                            "timestamp_fields": ["date_hour", "date_hour_minute"],
                            "date_fields": ["date"],
                            "int_fields": [
                                "session_duration",
                                "exits",
                                "total_events",
                                "sessions",
                                "page_views",
                                "users",
                                "bounces",
                                "avg_session_duration",
                                "session_count",
                            ],
                            "double_fields": ["bounce_rate", "pageviews_per_session"],
                        }
    :param data_frame: The Spark Dataframe where the shame will be applied.
    :param date_fmt: For the date fields, a string representing the native date format it is
                     in order to parse it to the correct spark date object.
    """
    for data_type, fields in schema.items():
        print(f"Cleaning up {data_type.upper()} types for fields {fields}.")
        if data_type == "int":
            for int_field in fields:
                if int_field in data_frame.columns:
                    data_frame = data_frame.withColumn(
                        int_field,
                        f.col(int_field).cast(t.IntegerType()).alias(int_field),
                    )

        if data_type == "double":
            for double_field in fields:
                if double_field in data_frame.columns:
                    data_frame = data_frame.withColumn(
                        double_field,
                        f.col(double_field).cast(t.DoubleType()).alias(double_field),
                    )

        if data_type == "date":
            for date_field in fields:
                if date_field in data_frame.columns:
                    data_frame = data_frame.withColumn(
                        date_field,
                        f.to_timestamp(f.unix_timestamp(date_field, date_fmt))
                        .cast(t.DateType())
                        .alias(date_field),
                    )

        if data_type == "timestamp":
            for timestamp_field in fields:
                if timestamp_field in data_frame.columns:
                    data_frame = data_frame.withColumn(
                        timestamp_field,
                        f.col(timestamp_field)
                        .cast(t.TimestampType())
                        .alias(timestamp_field),
                    )

        if data_type == "epoch":
            for epoch_field in fields:
                if epoch_field in data_frame.columns:
                    data_frame = data_frame.withColumn(
                        epoch_field, f.col(epoch_field).cast(t.IntegerType())
                    )
                    data_frame = data_frame.withColumn(
                        epoch_field,
                        f.from_unixtime(epoch_field),
                    )

                    # also create date version of the epoch
                    data_frame = data_frame.withColumn(
                        f"{epoch_field}_date",
                        f.to_date(data_frame[epoch_field]).cast(t.DateType()),
                    )

    return data_frame


def chunk_data_frame(df: DataFrame, row_count: int):
    """
    Chunk the data frame for processing in sets to
    reserve memory intensive workloads.
    :param df: dataframe to chunk.
    :param row_count: bucket sizes.
    :return: portion of the data frame.
    """
    count = df.count()

    if count > row_count:
        num_chunks = count // row_count
        chunk_percent = 1 / num_chunks  # 1% would become 0.01
        return df.randomSplit([chunk_percent] * num_chunks, seed=1234)
    return [df]


def generate_md5_hash(df: DataFrame, columns: dict) -> DataFrame:
    """
    Creates distinct has id by combining column(s).
    :param df: Dataframe to add the hash
    :param columns: A dict where key is result hash col name,
                    and values as list having cols to be combines
                    into the hash value.

                    {"my_new_hash": ["col1", "coln"]}
    :return: DataFrame
    """

    for key, value in columns.items():
        if len(value) > 1:
            temp_column_names = [f"{col}_tmp" for col in value]
            for idx, column in enumerate(temp_column_names):
                # replace null fields with empty string using coalesce
                df = df.withColumn(column, f.coalesce(value[idx], f.lit("")))

            # now create md5 and then drop the temp columns
            df = df.withColumn(key, f.md5(f.concat(*temp_column_names))).drop(
                *temp_column_names
            )

        # only coalesce and create md5  with the first item
        df = df.withColumn(key, f.md5(f.coalesce(value[0], f.lit(""))))
    return df


def unstack_df(df):
    for field in df.schema.fields:
        # print(field.dataType, T.ArrayType)
        if isinstance(field.dataType, t.ArrayType):
            df = df.withColumn(field.name, f.explode(field.name))

        if isinstance(field.dataType, t.StructType):
            try:
                df = (
                    df.withColumn(field.name, f.explode(f.array(f"{field.name}.*")))
                    .select("*", f"{field.name}.*")
                    .drop(field.name)
                )
            except Exception as e:
                msg = "Can only star expand struct data types"
                if msg in e.desc:
                    df = df.withColumn(
                        field.name, f.explode(f.array(f"{field.name}.*"))
                    )
    return df


def clean_url_string(df: DataFrame, col_name: str) -> DataFrame:
    """
    If the url has a trailing "/" then remove it.
    """
    df = df.withColumn(col_name, f.trim(f.col(col_name)))
    return df.withColumn(
        col_name,
        f.when(
            f.col(col_name).endswith("/"),
            f.expr(f"substring({col_name}, 1, length({col_name})-1)"),
        ).otherwise(f.col(col_name)),
    )


def generate_md5_hash(df: DataFrame, columns: dict) -> DataFrame:
    """_summary_

    Args:
        df DataFrame: pyspark Dataframe
        columns (dict): e.g {'new_name': ['col'], 'new_name_2': ['col1', 'col2']}

    Returns:
        DataFrame: pyspark Dataframe
    """

    for key, value in columns.items():
        if len(value) > 1:
            temp_column_names = [f"{col}_tmp" for col in value]
            for idx, column in enumerate(temp_column_names):
                # replace null fields with empty string using coalesce
                df = df.withColumn(column, f.coalesce(value[idx], f.lit("")))

            # now create md5 and then drop the temp columns
            df = df.withColumn(key, f.md5(f.concat(*temp_column_names))).drop(
                *temp_column_names
            )

        # only coalese and create md5  with the first item
        df = df.withColumn(key, f.md5(f.coalesce(value[0], f.lit(""))))
    return df


def deduplicate_by_max_date(
    df: DataFrame, group_by_columns: list, date_col: str
) -> DataFrame:
    df = generate_md5_hash(df, {"temp_hash_id": group_by_columns})

    latest_record_df = df.groupBy("temp_hash_id").agg(
        f.max(date_col).alias(f"max_{date_col}")
    )
    latest_record_df = latest_record_df.withColumnRenamed(
        "temp_hash_id", "max_temp_hash_id"
    )

    join_conditions = [
        df["temp_hash_id"] == latest_record_df["max_temp_hash_id"],
        df[date_col] == latest_record_df[f"max_{date_col}"],
    ]

    df = df.join(latest_record_df, join_conditions, "inner").drop(
        f"max_{date_col}", "temp_hash_id", "max_temp_hash_id"
    )

    return df.dropDuplicates()


def map_columns(df: DataFrame, col_map: dict, print_columns: bool = False) -> DataFrame:
    """maps columns with the preffered names from a dictionary passed in at run time

    Args:
        df (DataFrame): pyspark DataFrame
        col_map (dict): dictionary of columns {"source_col1":"destination_col1", "source_col2": "destination_col2"}
        print_columns (bool, optional): print the resulting columns or not. Defaults to True.

    Returns:
        DataFrame: _description_
    """
    # add columns that may miss from dataframe but are expected
    missing_columns = [i for i in col_map.keys() if i not in df.columns]
    for missing_col in missing_columns:
        df = df.withColumn(missing_col, f.lit(""))

    # reduce the columns to the ones we want
    df = df.select(*col_map.keys())

    # rename the columns to conform with warehouse schema
    df = df.select([f.col(c).alias(col_map.get(c, c)) for c in df.columns])
    if print_columns:
        print(df.columns)
    return df


# @udf
# def dotnet_ticker(ticks):
#     """
#     Converts a .NET Ticker into a python timestamp.
#     The format of the filename as we generate it:
#     {context.Configuration.ExportName}_{startTime.ToString("yyyy-MM-dd_hh-mm-ss")}_{DateTimeOffset.UtcNow.Ticks.ToString()}.json
#     You will notice that the first date is the “start time” of the export
#     And the second one is the “UTC Ticks” of the moment the file is generated
#     """
#     ticker_time = (
#         datetime.datetime(1, 1, 1) + datetime.timedelta(microseconds=(ticks // 10))
#     ).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

#     return ticker_time


def optional_column_imputing(df: DataFrame, col_pairs: dict) -> DataFrame:
    """
    use this compute columns with values from another when main is null

    :param df:pyspark DataFrame
    :param col_pairs : dictionary to process {'final_name':['col_1', 'col_2', 'col_n']}
    """
    if not isinstance(col_pairs, dict):
        raise Exception(
            "ERROR: Please provide atleast one column to compare in a dictionary {'final_name':['col_1', 'col_2', 'col_n']}"
        )
    for name, col_list in col_pairs.items():
        if len(col_list) == 0:
            print("\n\nINFO: No columns to do optional_column_processing\n\n")

        for idx, col_name in enumerate(col_list):
            if col_name in df.columns:
                if idx == 0 and name not in df.columns:
                    df = df.withColumn(name, f.lit(None))

                df = df.withColumn(
                    f"{name}",
                    f.when(
                        ((f.isnan(f.col(f"{name}"))) | (f.isnull(f.col(f"{name}"))))
                        & (
                            (~f.isnan(f.col(f"{col_name}")))
                            | (~f.isnull(f.col(f"{col_name}")))
                        ),
                        f.col(f"{col_name}"),
                    ).otherwise(f.col(f"{name}")),
                )
    return df


def impute_timestamp(
    df: DataFrame, date_col: str, default_timestamp: str = "2001-01-01 01:01:01.000"
) -> DataFrame:
    df = df.withColumn(
        f"{date_col}",
        f.when(
            (f.isnull(f.expr(f"DATEDIFF({date_col},{date_col})"))),
            f.to_timestamp(f.lit(f"{default_timestamp}")),
        ).otherwise(f.col(f"{date_col}")),
    )
    df = df.withColumn(f"{date_col}", f.to_timestamp(f"{date_col}"))
    return df


def yaml_reader(
    bucket: str, bucket_prefix: str, dest_file_name: str = None, source: str = "local"
) -> dict:
    try:
        if source == "local":
            with open(Path(f"{bucket}{bucket_prefix}"), "r") as f:
                yaml = YAML(typ="safe")
                configs = yaml.load(f.read())
            return configs

        elif source == "s3":
            s3_client = boto3.client("s3")
            s3_client.download_file(bucket, bucket_prefix, dest_file_name)
            with open(dest_file_name, "r") as f:
                yaml = YAML(typ="safe")
                configs = yaml.load(f.read())
            return configs
    except Exception as e:
        raise


def gather_paths(dir: str, file_format: str) -> list:
    if not dir.endswith("/"):
        dir = f"{dir}/"
    dir = Path(f"{dir}")
    return dir.glob(f"*.{file_format}")


def date_file_splitter(path_list: list) -> dict:
    date_path_dict = {}
    for path in path_list:
        date_value = str(path).split("/")[-1].split("-")[0]
        if date_path_dict.get(date_value):
            date_path_dict[date_value].append(path)
        else:
            date_path_dict[date_value] = [path]

    return date_path_dict


def pandas_parquet_stream(paths: str) -> pd.DataFrame:
    """A function used to read several parquet files from a glob object"""
    for path in paths:
        df = pd.read_parquet(path)
        yield df


def database_insert(
    df_list: list, dest_table: str, conn: create_engine, mode: str = "append"
) -> None:
    try:
        if mode == "truncate":
            conn.execute(f"TRUNCATE TABLE {dest_table}")
        else:
            iterator_insert(df_list, dest_table, conn, mode)
    except Exception as err:
        print(err)


def iterator_insert(
    df_list: list, dest_table: str, conn, mode: str = "append", drop_index=True
) -> None:
    while True:
        try:
            df = next(df_list)
            if drop_index:
                df = df.set_index(df.columns[0])
            if mode == "replace":
                df.head(0).to_sql(name=dest_table, con=conn, if_exists=mode)
            df.to_sql(name=dest_table, con=conn, if_exists=mode)
        except StopIteration:
            print("INFO: End of list all data streamed into database")
            break


class ParquetWriter:
    def __init__(self, max_records_per_file=200000):
        self.max_records_per_file = max_records_per_file

    def get_year_month_day(self, df: DataFrame, date_col: str) -> DataFrame:
        df = (
            df.withColumn("year", f.format_string("%02d", f.year(f.col(date_col))))
            .withColumn("month", f.format_string("%02d", f.month(f.col(date_col))))
            .withColumn("day", f.format_string("%02d", f.dayofmonth(f.col(date_col))))
        )

        return df

    def writer(
        self,
        df: DataFrame,
        partitions: list,
        path: str,
        mode: str = "overwrite",
        year_month_day_partitioning: bool = False,
        date_col: str = None,
    ):

        if not partitions:
            df.write.option("maxRecordsPerFile", self.max_records_per_file).parquet(
                path, mode=mode
            )
        else:
            if year_month_day_partitioning == True:
                if date_col is None:
                    raise Exception(
                        "WARNING: You did not give me a date column to generate year month day for the partitions"
                    )
                df = self.get_year_month_day(df, date_col)

            df.write.option("maxRecordsPerFile", self.max_records_per_file).partitionBy(
                *partitions
            ).parquet(path, mode=mode)
