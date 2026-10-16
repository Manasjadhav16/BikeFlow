# Live collector notes (Stage 3a)

`src/collector.py` polls the Citi Bike GBFS feed and stores station status snapshots for
later replay and evaluation. It's plain Python (`requests` + `pyarrow`), with no Spark, and
is meant to run for days on the 8 GB Mac next to other work. It uses about 60–80 MB RAM and
a fraction of one CPU (measured below).

## Start, check, stop

```bash
# start in the background (nohup), PID in data/live/collector.pid, log in data/live/collector.log
scripts/run_collector.sh

# is it alive? last few polls?
kill -0 "$(cat data/live/collector.pid)" && echo running
tail -3 outputs/metrics/collector_health.jsonl

# stop cleanly (SIGTERM -> flushes buffered rows, saves de-dup state)
scripts/stop_collector.sh
```

- **Bounded run:** `scripts/run_collector.sh --max-minutes 600`.
- **One test poll:** `.venv/bin/python -m src.collector --once`.
- **Restarting is safe:** the collector adds new Parquet files and reloads its de-dup state
  from `data/live/collector_state.json`. A second `run_collector.sh` while one is running
  does nothing.
- **Station mapping report:** `.venv/bin/python -m src.live_map`.

## What it does

- **Discovery:** reads `https://gbfs.citibikenyc.com/gbfs/gbfs.json` (GBFS 1.1, served by Lyft) and
  takes the `station_status` and `station_information` URLs from it.
- **Polling:** every 60 s, or the feed's `ttl` if that is longer (currently `ttl = 60`).
  If the feed's `last_updated` hasn't changed since the previous poll, nothing is
  written (`feed_unchanged: true` in the health log).
- **De-duplication:** a station row is written only when that station's `last_reported`
  changed since the previous poll. Unchanged stations are counted as skipped.
- **Robustness:**
  - requests time out after 10 s;
  - errors are retried with exponential backoff (1, 2, 4, 8, 16 s; 5 retries), then the
    cycle is skipped and logged as `poll_skipped`;
  - a malformed payload is logged as `poll_bad_payload` and never crashes the loop;
  - Ctrl+C and SIGTERM stop the loop and flush the buffer;
  - Parquet files are written to a temporary name and renamed, so a reader never sees a
    half-written file.
- **station_information** is saved at start and then every 24 h.

## What is stored (`data/live/`, git-ignored)

| path | content |
|---|---|
| `status/date=YYYY-MM-DD/hour=HH/part-<flush time>-<pid>.parquet` | status rows, flushed every 5 min; **date/hour are UTC** (from `fetched_at`) |
| `station_info/station_information_<UTC time>.json` | raw `station_information.json`, once per 24 h |
| `collector_state.json` | de-dup state (`station_id → last_reported`) and the last feed `last_updated` |
| `collector.log`, `collector.pid` | process log and PID (from the run script) |

Health log: `outputs/metrics/collector_health.jsonl`, one JSON line per event (`start`, `poll`,
`poll_skipped`, `poll_bad_payload`, `station_info_saved`, `stop`). Poll lines carry the HTTP
status, latency, station count, rows kept and skipped, the feed's `last_updated` and `ttl`, and retries.

Status row schema:

| column | type | note |
|---|---|---|
| `fetched_at` | string | UTC ISO time of the poll, e.g. `2026-10-16T17:43:33Z` |
| `station_id` | string | GBFS id (usually a UUID; a few are numeric strings) |
| `num_bikes_available`, `num_ebikes_available`, `num_docks_available`, `num_bikes_disabled`, `num_docks_disabled` | int32 | |
| `is_installed`, `is_renting`, `is_returning` | bool | feed sends 0/1 |
| `last_reported` | int64 | epoch seconds, as published |

To read everything back:
`pyarrow.dataset.dataset("data/live/status", format="parquet", partitioning="hive")`.

## Mapping live stations to the model (`src/live_map.py`)

Live GBFS ids are mapped to trip short ids with the existing station table
(`data/processed/stations/stations.parquet`; fallback `models/station_index.json`). On the
first live snapshot (2026-10-16):

- **Matched:** 2,477 of 2,518 live stations (98.4%), and **all 2,477 model stations are live**.
- **Unmatched:** the 41 others have no trips in Oct 2025 – Sep 2026 (new or reopened
  stations, including several Jersey City sites such as Port Liberté Ferry Terminal). All
  41 have a live `short_name`, so they can be mapped once trip history exists.
- **Cross-check:** for matched stations, the live `short_name` (after the trailing-zero padding)
  agrees with the table 100%.

## Test run (2026-10-16)

`python -m src.collector --once`, then `--max-minutes 10` (17:43–17:53 UTC, an evening
peak in New York). The second run reloaded the de-dup state written by `--once`, so its first
poll correctly wrote nothing. Summary in `outputs/metrics/collector_test_run.json`.

| metric | value |
|---|---|
| polls | 10 (all HTTP 200, 0 retries, 0 skipped cycles) |
| polls where the feed hadn't changed (`last_updated` same) | 5 of 10 |
| rows written | 11,391 |
| rows skipped as unchanged | 13,789 |
| rows kept per poll | 0, 1721, 0, 2429, 0, 2421, 0, 2377, 0, 2443 |
| latency | mean 980 ms (min 189, max 1771) |
| Parquet written | 141 KB in 2 files over 542 s |
| size per hour | **0.94 MB** (status) |
| projected per day | **about 22.5 MB** status + about 1.3 MB station_information, roughly 0.17 GB/week |
| process | about 78 MB RSS, 0.37 s CPU in the first 71 s (under 1% of one core) |

What the test shows about the feed:

- **The feed refreshes about every 2 minutes even though `ttl = 60`.** Every other 60-second
  poll sees the same `last_updated` and writes nothing.
- **When it does refresh, about 95% of stations have a new `last_reported`.** Stations
  heartbeat whether or not anything changed, so de-duplicating on `last_reported` only
  removes about half the rows. De-duplicating on the *counts* would remove far more,
  but would lose the "still reporting" signal. That's a possible later change.
- **The projection is from an evening-peak hour.** Because of the heartbeats, night hours
  are probably only a little smaller. Check after the first full day:
  `du -sh data/live/status/date=*`.

## Known caveats

- **NYC night hours.** From about 01:00 to 06:00 New York time, few stations report changes,
  so most rows are skipped as unchanged and hourly files are small. That's expected and
  not a fault. Partition hours are UTC, so New York 01:00–06:00 is `hour=05`–`hour=10`
  (EDT) or `hour=06`–`hour=11` (EST).
- **The Mac must stay awake.** `run_collector.sh` starts `caffeinate -i -s -w <pid>`, which
  stops idle sleep, and on AC power system sleep, while the collector runs. **Closing the
  lid still sleeps a MacBook** unless it's in clamshell mode with power and an external
  display. A sleep shows up as a gap in `collector_health.jsonl`. After wake the loop just
  continues (no catch-up; missed minutes are lost).
- **Rebalancing by operator vans changes counts.** Station counts change from rides *and*
  from Citi Bike's rebalancing trucks and bike-train vans, which move many bikes at once.
  Trip data (and the Stage 2 models) only see rides. A 30-minute drop in
  `num_bikes_available` therefore isn't the same thing as the trip-based
  `bike_change_last_30m`. Large step changes (about ±10 bikes in one poll) are probably
  rebalancing and should be flagged before they feed live features.
- **Other count changes not caused by riders:** docks or bikes marked disabled (`num_*_disabled`),
  stations switched off (`is_installed/is_renting/is_returning = False`), and e-bike
  battery swaps.
- **Placeholder records:** some feed records are decommissioned or placeholder stations,
  with `is_installed = False` and `last_reported = 86400` (Jan 2, 1970). Filter on
  `is_installed` and a recent `last_reported` before using counts.
- **`last_reported` granularity:** a station whose counts change without a new report would be
  missed by the de-dup rule. In practice counts only change with a report.
- **Partition hour is the poll time**, not `last_reported`. A row records the state as seen at
  `fetched_at`; the station's own report time is `last_reported`.
- **Small files:** 5-minute flushes make up to about 12 files per hour, roughly 288 per day.
  That's fine for days of collection; compact them later (e.g. one file per day) if it
  runs for months.
- **Clock:** `fetched_at` uses the Mac's clock (UTC). Keep automatic time sync on.
