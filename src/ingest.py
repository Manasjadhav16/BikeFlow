"""Read Citi Bike trip CSVs with an explicit schema and write Parquet partitioned by year/month.

Timestamps are read as strings and parsed with try_to_timestamp so a malformed
value becomes NULL instead of failing the job; clean.py counts and drops them.
Partitions follow the *source file* month (year=YYYY/month=M), not started_at.
"""
import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from src.common import TRIPS_DIR, get_spark

EXPECTED_COLUMNS = [
    "ride_id", "rideable_type", "started_at", "ended_at",
    "start_station_name", "start_station_id", "end_station_name", "end_station_id",
    "start_lat", "start_lng", "end_lat", "end_lng", "member_casual",
]

# Station ids look like "5329.03" — keep them as strings so trailing zeros survive.
TRIP_SCHEMA = StructType([
    StructField("ride_id", StringType(), True),
    StructField("rideable_type", StringType(), True),
    StructField("started_at", StringType(), True),
    StructField("ended_at", StringType(), True),
    StructField("start_station_name", StringType(), True),
    StructField("start_station_id", StringType(), True),
    StructField("end_station_name", StringType(), True),
    StructField("end_station_id", StringType(), True),
    StructField("start_lat", DoubleType(), True),
    StructField("start_lng", DoubleType(), True),
    StructField("end_lat", DoubleType(), True),
    StructField("end_lng", DoubleType(), True),
    StructField("member_casual", StringType(), True),
])


def _parse_ts(col: str):
    # Source looks like "2026-07-01 08:15:30.123"; fractional seconds are optional.
    return F.coalesce(
        F.try_to_timestamp(F.col(col), F.lit("yyyy-MM-dd HH:mm:ss.SSS")),
        F.try_to_timestamp(F.col(col), F.lit("yyyy-MM-dd HH:mm:ss")),
    )


def read_trips_csv(spark: SparkSession, csv_paths: list[str]) -> DataFrame:
    df = (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .schema(TRIP_SCHEMA)
        .csv(csv_paths)
    )
    return df.withColumn("started_at", _parse_ts("started_at")).withColumn(
        "ended_at", _parse_ts("ended_at"))


def ingest_csvs(spark: SparkSession, csv_paths: list[str], year: int, month: int,
                out_dir: Path = TRIPS_DIR) -> Path:
    """Append the given CSVs into the year/month partition and return its path."""
    df = read_trips_csv(spark, csv_paths)
    part = out_dir / f"year={year}" / f"month={month}"
    df.write.mode("append").parquet(str(part))
    return part


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--csv", nargs="+", required=True)
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--month", type=int, required=True)
    a = p.parse_args()
    spark = get_spark("bikeflow-ingest")
    part = ingest_csvs(spark, a.csv, a.year, a.month)
    print(f"{part} now has {spark.read.parquet(str(part)).count():,} rows")
    spark.stop()


if __name__ == "__main__":
    main()
