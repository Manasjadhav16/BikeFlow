"""Shared paths, disk guard and SparkSession factory."""
import json
import os
import re
import shutil
import subprocess
import sys
import threading
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
RAW_SIZES_JSON = METRICS_DIR / "raw_sizes.json"
SPARK_TMP = ROOT / "data" / "tmp_spark"

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


def read_json(path: Path) -> dict:
    return json.loads(path.read_text()) if path.exists() else {}


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
    """SparkSession sized for an 8 GB laptop: 3 cores, 3 GB driver, small partitions, AQE."""
    _ensure_java_home()
    from pyspark.sql import SparkSession

    SPARK_TMP.mkdir(parents=True, exist_ok=True)
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[3]")
        .config("spark.driver.memory", "3g")
        .config("spark.sql.session.timeZone", TIMEZONE)
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.sql.files.maxPartitionBytes", "64m")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.local.dir", str(SPARK_TMP))
        .config("spark.sql.parquet.compression.codec", "zstd")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def stop_spark(spark) -> None:
    """Stop Spark and delete its scratch dir (shuffle/spill files)."""
    spark.stop()
    shutil.rmtree(SPARK_TMP, ignore_errors=True)


def swap_used_gb() -> float:
    out = subprocess.run(["sysctl", "-n", "vm.swapusage"], capture_output=True, text=True).stdout
    m = re.search(r"used = ([\d.]+)M", out)
    return float(m[1]) / 1024 if m else float("nan")


class ResourceMonitor:
    """Samples RSS of this Python process + the Spark JVM, and swap, once a second; keeps peaks."""

    def __init__(self, spark, interval: float = 1.0):
        self.pids = [str(os.getpid()), str(spark._jvm.java.lang.ProcessHandle.current().pid())]
        self.interval = interval
        self.peak_rss_gb = 0.0
        self.peak_swap_gb = swap_used_gb()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        while not self._stop.is_set():
            out = subprocess.run(["ps", "-o", "rss=", "-p", ",".join(self.pids)],
                                 capture_output=True, text=True).stdout
            rss = sum(int(x) for x in out.split()) / 1024 / 1024
            self.peak_rss_gb = max(self.peak_rss_gb, rss)
            self.peak_swap_gb = max(self.peak_swap_gb, swap_used_gb())
            self._stop.wait(self.interval)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._thread.join()


def log_progress(step: str, label: str, rows: int, seconds: float, mon: "ResourceMonitor | None" = None) -> dict:
    rec = {"step": step, "month": label, "rows": rows, "seconds": round(seconds, 1),
           "free_disk_gb": round(free_gb(), 2), "swap_used_gb": round(swap_used_gb(), 2)}
    if mon:
        rec.update(peak_rss_gb=round(mon.peak_rss_gb, 2), peak_swap_gb=round(mon.peak_swap_gb, 2))
    print(f"[progress] {step} {label}: {rows:,} rows in {seconds:.0f}s | free disk {rec['free_disk_gb']} GB"
          f" | swap {rec['swap_used_gb']} GB"
          + (f" | peak RSS {rec['peak_rss_gb']} GB, peak swap {rec['peak_swap_gb']} GB" if mon else ""),
          flush=True)
    return rec
