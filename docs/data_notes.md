# Data notes: Citi Bike trips (Stage 1)

Things that are not obvious from the code but matter for anyone who uses
`data/processed/` downstream. The numbers come from `outputs/metrics/*.json` for
Oct 2025 – Sep 2026 (11 months loaded; February 2026 is quarantined, see §7).

## 1. Station IDs that lost their trailing zero

Station IDs look like decimals (`5980.10`), and some rows in the trip CSVs have
them with the trailing zero dropped (`5980.1`). This happens in every month, mostly in
`end_station_id` (12 months: 643,381 end IDs and 93,237 start IDs). It's probably
a numeric round-trip somewhere upstream.

Left alone, one physical station appears under two IDs, which splits its
demand (mostly arrivals) and makes the station unmatchable in GBFS. In the first 3 months
this split 94 stations and cut the GBFS match rate from 99.1% to 95.5%.

**Fix:** `src/clean.py` restores the zero for any ID matching `^\d+\.\d$`
(`5980.1` → `5980.10`) in both station columns. The counts are in
`data_quality.json → station_id_normalized`. GBFS `station_information.json`
has the same defect in a few `short_name` values, and `src/stations.py`
applies the same padding there. IDs are always kept as strings, never cast to
numbers.

## 2. Timezone: everything is America/New_York

Trip timestamps are New York wall-clock time with no offset. The Spark session runs
in `America/New_York` (`src/common.py`), so `hour`, `day_of_week`,
the 15-minute buckets and month boundaries are all New York local time,
whatever timezone the machine is in.

Two bugs came from the host timezone (the dev Mac is in IST, UTC+5:30), and both are fixed:

- Month bounds built from naive Python timestamps were read in IST, which
  shifted month edges by 9.5 h. In the test run, September had 31 days and 2,023
  demand rows were wrongly dropped. Month bounds are now built inside Spark from
  strings or `date_format`, which use the session timezone.
- Timestamps collected to Python (`.first()`, `.collect()`) are rendered in
  the host timezone. Any timestamp written to a report is formatted inside Spark
  first (`date_format`). The inspection strings already written for 11 months
  were corrected from IST to New York time.

`bucket_start` is stored in Parquet as a UTC instant. Spark with the NYC session
timezone reads it back correctly. **pandas reads it as a naive UTC value**: for
example, `2026-06-01 11:00` is the 07:00 New York bucket. Convert it with
`.dt.tz_localize("UTC").dt.tz_convert("America/New_York")`, or use the
`hour`, `day_of_week`, `month` columns, which are already New York local.
DST: a bucket on the autumn "fall back" night can cover two
wall-clock hours.

## 3. Unmatched stations (trip ID not in GBFS)

Over 12 months, 2,477 of 2,531 trip stations match a GBFS `short_name`
(**97.9% of stations, 98.4% of trips**). The 54 unmatched stations carry
1.39M trips (1.6%). The reason: GBFS `station_information.json` is a
**snapshot of today's network**, while the trips span a year. Stations
that were retired, moved or renumbered during the year have no current entry. Examples:

- High-volume IDs that GBFS no longer lists at all: `5788.13 Lafayette St & E 8 St`
  (124k trips), `6925.09 2 Ave & E 72 St` (112k), `5847.01 Christopher St & Greenwich St`
  (105k).
- Relocations to a nearby corner with a new ID, for example `5329.08 Murray St & West St`
  is now `5329.09 … (NW Corner)` and `5329.13 … (NE Corner)`.
- Only 8 unmatched stations (157k trips) exist in GBFS under the same name with a
  different ID, for example `5561.06` → `5561.08 Lafayette St & Jersey St`.
- Citi Bike operational sites, not public stations: `1234.56 Morgan HCT Charging`
  and `SYS038 Morgan Loading Docks`.

Matched stations agree with GBFS exactly: 99.9% name agreement, 0 m median and p95
coordinate distance. That confirms the join on `short_name` is right. The unmatched
stations stay in `stations.parquet` with `matched = False`. They're listed in
`station_mapping.json → top_unmatched_by_trips`. A name-plus-distance fallback, or
archived GBFS snapshots, would close most of the gap if a later stage needs GBFS fields
(such as capacity) for these stations.

## 4. Month edges: which rides are missing near a boundary

Inspection showed that **each monthly file holds the rides that *ended* in that month**:
0 rides in any file end outside its month. A few hundred per file *start* on
the last day of the previous month. **April 2026 is the exception**: its file is cut by
*start* time, so its 495 rides that ended on May 1 also appear in the May file. Those
495 rides are the only cross-file duplicates (`data_quality.json → observations`).
`trips_clean` keeps both copies, because each month was cleaned and verified on its
own. `demand_15min` counts each of them once (it dedupes `ride_id` among rides that
cross a month boundary).

Consequences for `demand_15min`, where each target month is built from the files for
months T-1, T and T+1:

- **Arrivals are complete** for every loaded month: 0 dropped.
- **Departures are missing where the *next* month's file isn't loaded.** A ride that starts
  on the last evening of month T and ends after midnight is in month T+1's file. So
  the late evening of **2026-09-30** (the October file isn't loaded) and of
  **2026-01-31** (February is quarantined) lack the departures of rides that crossed
  midnight, roughly a few hundred each. That under-counts departures and
  `net_outflow` in those last buckets only.
- Rides that started in a month that isn't loaded (Sep 2025, Feb 2026) but ended in a
  loaded month count only as arrivals. 1,028 departures fall outside the loaded
  months and are dropped (`eda_summary.json → dropped_edge_rides`).

(The Stage 1 note said "first-day arrivals" were missing. That assumed files split by
start time, and it was the wrong way round: the gap is departures before a missing month.)

## 5. Citi Bike data arrives (almost) pre-cleaned

Across the 11 loaded months (44,164,661 raw rows → 44,129,501 clean), the cleaning
rules remove 0.08% of rows:

| rule | removed | share |
|---|---:|---:|
| null or unparseable timestamp | 0 | 0% |
| `ended_at <= started_at` | 482 | 0.001% |
| duration > 24 h | 9,677 | 0.022% |
| null `start_station_id` (dockless e-bike starts) | 25,001 | 0.057% |
| duplicate `ride_id` within a month | 0 | 0% |

Only 4 rides last under 60 s (Lyft filters these out before publishing).
`end_station_missing` is flagged, not dropped: 112,697 rides (0.26%) count as
departures with no arrival. So the cleaning step is mostly a guard. The real
problems are the station-ID, timezone and file-boundary issues above.

## 6. Source-file quirks

- Monthly zips are **stored uncompressed** (zip size = CSV size, about 1 GB in summer),
  except April 2026, which is deflate-compressed (164 MB zip → 753 MB CSV). April is
  also the only month cut by start time (see §4), so it came from a different
  export process.
- Each month is split into ~195 MB CSV members. They are converted one at a time, so
  the full month is never on disk at once.
- S3 downloads can drop mid-file. `scripts/download_trips.py` resumes with
  HTTP Range requests.

## 7. Month inspection and quarantine

Before cleaning, every month is checked: rows, min/max `started_at`, and days with
at least 100 rides. A month with uncovered days, or more than 1% of rides starting outside the month, is
**quarantined**: its raw Parquet is kept, it's left out of `trips_clean`, and the reason
is recorded in `data_quality.json → quarantined_months`. Include it with
`python -m src.clean --include YYYY-MM`, then rerun `scripts/build_features.sh`.

**February 2026 is quarantined.** Feb 23 has 0 rides in New York time, while every
other February day has 13k–76k. In the raw file, rides taper off on the evening of
Feb 22, stop at about 20:00, and resume at about 06:00 on Feb 24 (around 34 hours with
almost no rides, New York time). That is a
system-wide service suspension (it looks like a storm), not a truncated file. The
other 27 days look normal (1.22M rides). Whether to include it is a modelling
decision: including it teaches a model that shutdown days happen; excluding it
leaves a 28-day gap.

**April 2026** looked suspicious (a 164 MB zip against about 1 GB for other months), but it
passed: all 30 days covered, 39k–179k rides per day, 3.86M rows. The small zip is
only compression (§6).

**Low days that passed:** Jan 25, 2026 had 3,050 rides (the quietest loaded day; it
looks like another storm), and Sep 27, 2026 had 33,299. Both are above the 100-ride
floor, so they're included as real low-demand days.

## 8. Memory: building on an 8 GB machine

A global exact `countDistinct(ride_id)` over all ~44M rows (the old summary in
`clean.py`) held tens of millions of IDs in a 3 GB heap. With 8 concurrent tasks it
pushed macOS deep into swap, and the job stalled. Everything now runs one month
per Spark job (`local[3]`, 3 GB driver, 32 shuffle partitions, 64 MB input splits,
AQE, scratch in `data/tmp_spark`, deleted after each run), and only small per-month
aggregates are combined in pandas. Cross-month duplicates are checked exactly
with adjacent-month joins. A full rebuild (`scripts/build_features.sh`) takes
about 4 minutes, with peak RSS under 1.9 GB and no swap growth. Per-month timings
are in `outputs/metrics/build_progress.json`.
