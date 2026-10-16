"""Shared paths, disk guard and SparkSession factory."""
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_RAW = ROOT / "data" / "raw"
DATA_PROCESSED = ROOT / "data" / "processed"
TRIPS_DIR = DATA_PROCESSED / "trips"
TRIPS_CLEAN_DIR = DATA_PROCESSED / "trips_clean"
STATIONS_DIR = DATA_PROCESSED / "stations"
DEMAND_DIR = DATA_PROCESSED / "demand_15min"
METRICS_DIR = ROOT / "outputs" / "metrics"
CHARTS_DIR = ROOT / "outputs" / "charts"

MIN_FREE_GB = 4.0
TIMEZONE = "America/New_York"


def free_gb(path: Path = ROOT) -> float:
    return shutil.disk_usage(path).free / 1e9


def check_disk(step: str) -> float:
    """Abort if free disk is below MIN_FREE_GB; otherwise log and return it."""
    free = free_gb()
    print(f"[disk] {step}: {free:.2f} GB free")
    if free < MIN_FREE_GB:
        sys.exit(f"[disk] STOP: only {free:.2f} GB free (< {MIN_FREE_GB} GB) before '{step}'")
    return free


def dir_size_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


def write_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, default=str) + "\n")


def _ensure_java_home() -> None:
    if os.environ.get("JAVA_HOME"):
        return
    for candidate in ("/opt/homebrew/opt/openjdk@17", "/usr/local/opt/openjdk@17"):
        if Path(candidate, "bin", "java").exists():
            os.environ["JAVA_HOME"] = candidate
            return
    try:
        os.environ["JAVA_HOME"] = subprocess.check_output(
            ["/usr/libexec/java_home", "-v", "17"], text=True).strip()
    except (OSError, subprocess.CalledProcessError):
        pass  # let PySpark report the missing JVM


def get_spark(app_name: str):
    _ensure_java_home()
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "3g")
        .config("spark.sql.session.timeZone", TIMEZONE)
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.parquet.compression.codec", "zstd")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
