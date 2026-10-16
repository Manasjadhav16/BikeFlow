# Streaming alert engine (Stage 3b)

Kafka + Spark Structured Streaming turn live GBFS station status into "this station is likely
to run out of bikes / docks in the next 30 minutes" alerts. Everything runs on one 8 GB Mac
next to the collector. Kafka is a single node in KRaft mode (no ZooKeeper).

## Architecture

```
 GBFS station_status (every ~2 min)
        │  src/collector.py --kafka           (or src/replay.py: recorded data, Nx speed)
        ▼
 Kafka topic station_status  (3 partitions, key = GBFS station_id, timestamp = last_reported)
        │
        ▼  src/stream_alerts.py  (Spark Structured Streaming, local[2], 2 GB driver)
   parse JSON → event time = last_reported, watermark 5 min
   per station (applyInPandasWithState): last 35 min of bikes/docks →
        current bikes, docks, service flags, change over 5 / 15 / 30 min
   every micro-batch (60 s, foreachBatch):
        expected departures/arrivals for the next 30 min      ← reference/expected_demand.parquet
        × weather factor for current conditions               ← weather_live.py + weather_adjustment.json
        Skellam → P(empty), P(full) → level HIGH / MEDIUM
        │
        ├─► Kafka topic bikeflow_alerts (1 partition)        one JSON message per alerting station
        ├─► data/live/alerts/date=YYYY-MM-DD/batch-N.parquet  alert history
        ├─► data/live/latest_state.parquet                    every station's current risk (for a dashboard)
        └─► data/live/alert_batches.jsonl                     one line per batch (for scoring / health)
```

Collector Parquet (`data/live/status/`) is still written as before; it is the ground truth for
`src/score_alerts.py`.

## Start and stop everything

```bash
scripts/start_kafka.sh                       # broker (512 MB heap) + topics; data in data/kafka/
scripts/stop_collector.sh                    # if the plain collector is running...
scripts/run_collector.sh --kafka             # ...restart it with Kafka publishing
scripts/start_stream.sh                      # alert engine (live: latest offsets, 60 s batches)

tail -f data/live/stream_alerts.log          # one plain-language line per batch

scripts/stop_stream.sh                       # graceful (SIGINT); resumes from its checkpoint
scripts/stop_collector.sh
scripts/stop_kafka.sh
```

- **Start order:** Kafka → collector → alert engine. **Stop order:** the reverse.
- **If Kafka is down,** the collector keeps writing Parquet and logs `kafka_unavailable` or
  delivery failures.
- **The checkpoint** lives in `data/checkpoints/stream_alerts/`. Deleting it makes the engine
  start fresh from the latest offsets.
- **Wiping Kafka:** `scripts/stop_kafka.sh && rm -rf data/kafka`.
- **Test on recorded data** (never touches `data/live/status` or the real topics):
  `scripts/test_stream_replay.sh [status dir] [speed]`. It copies the data, creates temporary topics,
  runs the engine alongside a replay, scores the alerts, and deletes the temporary topics.

**Memory** (measured): Kafka about 400 MB RSS, alert engine about 1.1–1.3 GB (JVM + Python
workers), collector about 80 MB.

## Message formats

`station_status` (from the collector or replay), key = GBFS `station_id`, Kafka timestamp =
`last_reported` × 1000:

```json
{"fetched_at": "2026-10-16T18:06:38Z", "station_id": "77e31650-e86a-4b06-b2de-b92df2a7a7f5",
 "num_bikes_available": 22, "num_ebikes_available": 9, "num_docks_available": 6,
 "num_bikes_disabled": 2, "num_docks_disabled": 0, "is_installed": true, "is_renting": true,
 "is_returning": true, "last_reported": 1792173842, "event_time": "2026-10-16T18:04:02Z",
 "source": "collector"}
```

`bikeflow_alerts`, key = GBFS `station_id`. Each alerting station's row from `latest_state`:
`as_of` (data time), `processed_at`, `station_id`, `short_id`, `name`, `lat`, `lng`, `capacity`,
`bikes`, `docks`, `ebikes`, `alert_type` (`empty` / `full`), `alert_kind` (`risk_*` or
`already_*`), `probability`, `level`, `p_empty`, `p_full`, `exp_departures_30m`, `exp_arrivals_30m`,
`weather_factor_dep` / `_arr`, `temperature_2m`, `precipitation`, `change_5m` / `15m` / `30m`.

## The alert formula

At each micro-batch, **now** is the latest event time (`last_reported`) seen. Live, that is
the current time; in a replay it's the replayed time, so replays behave like live.

1. **Expected flow.** From `expected_demand.parquet`, take expected departures and arrivals
   for the window [now, now + 30 min): the three 15-minute New York buckets it overlaps,
   weighted by overlap (e.g. now = 10:07 → 8/15 of 10:00, all of 10:15, 7/15 of 10:30).
2. **Weather.** Get the conditions at `now` from Open-Meteo, using the record at the latest
   hour mark at or before `now`, exactly as in training. Look up its precipitation bin ×
   temperature band in the adjustment table, then
   μ_dep = factor_dep × expected departures and μ_arr = factor_arr × expected arrivals.
3. **Probability.** D ~ Poisson(μ_dep) and A ~ Poisson(μ_arr), independent, so D − A is Skellam:
   - P(empty risk) = P(D − A ≥ bikes_now − 1)
   - P(full risk) = P(A − D ≥ docks_now − 1)

   Computed with scipy `skellam.sf`.
4. **Level.** HIGH if the larger probability is ≥ 0.7, MEDIUM if it's ≥ 0.5. The alert type is
   whichever risk is larger.
5. **No alert** if the station is not installed, not renting or not returning (out of
   service), or hasn't reported for 60 minutes (stale).
6. **Already at the threshold.** With bikes ≤ 1 (or docks ≤ 1) the threshold is ≤ 0 and P = 1.
   These are labelled `already_empty` / `already_full`. They're operationally useful but
   not predictions, so they're counted and scored separately from `risk_*` alerts.

## Reference tables (`src/reference_tables.py`, built once in about 45 s)

**Expected demand** (`data/processed/reference/expected_demand.parquet`): 2,477 live-matched
stations × 672 weekly slots (weekday × hour × quarter-hour, New York time) = 1,664,544 rows.
- **Mean per slot:** departures and arrivals over all 12 months, with disrupted slots excluded.
- **Zero buckets count:** demand_15min only has buckets with activity, so the denominator is
  the number of non-disrupted occurrences of the slot between the station's first and last
  ride. Zero-activity buckets count as zeros.
- **Check:** the mean is 0.56 departures per station-bucket, about 134k a day, matching
  normal-day volume.

**Weather adjustment** (`weather_adjustment.json`, copy in `outputs/metrics/`):
- **Factor:** system-wide actual ÷ expected departures (and arrivals) per 15-minute bucket, by
  precipitation in the previous hour (none 0 / light < 1 / moderate 1–4 / heavy ≥ 4 mm) and
  temperature (< 0 / 0–10 / 10–20 / 20–28 / ≥ 28 °C).
- **Smoothing:** each cell is shrunk toward the product of its marginal ratios with weight
  n_hours / (n_hours + 24), then clipped to [0.2, 1.6].
- **Exclusions:** holidays and the Feb 22–24 shutdown are excluded; weather-driven low days
  are kept.

Departure factors (arrivals are within 0.02):

| precip \ temp | < 0 | 0–10 | 10–20 | 20–28 | ≥ 28 |
|---|---:|---:|---:|---:|---:|
| none | 0.39 | 0.73 | 1.15 | 1.37 | 1.37 |
| light | 0.25 | 0.43 | 0.76 | 1.13 | 1.28 |
| moderate | 0.20 | 0.29 | 0.43 | 0.81 | 0.82 (4 h) |
| heavy | (0.30) | 0.30 (3 h) | 0.41 | 0.61 | (0.61) |

- **Temperature also carries season.** Expected demand is a year-round average, so cold dry
  hours sit at 0.39× and warm dry hours at about 1.37×.
- **Rain** cuts demand within each band.
- **Combinations never seen** (shown in parentheses) use the nearest temperature band in the
  same precipitation bin.

**Live weather** (`src/weather_live.py`): Open-Meteo forecast endpoint (no key), hourly series
with the past 7 days, cached 15 minutes in memory and in `data/cache/`. It uses the same
mapping as training.

## Assumptions and caveats

- **The demand model is the historical average** (station × weekday × quarter-hour) with a
  weather factor. Stage 2b showed the GBT models did not beat the historical average on
  drain/fill events and were worse calibrated, so the historical average is the production
  model here (`docs/model_notes.md` §9).
- **Rebalancing by operator vans:** trucks and bike trains move many bikes at once. The
  expected-demand table only knows rider trips, so a station about to be restocked or cleared
  by staff will raise false alarms, and an unexpected truck removal is invisible to the model.
  The `change_*` columns show sudden jumps (likely rebalancing) for a later rule.
- **The feed refreshes about every 2 minutes** (`ttl = 60`, but `last_updated` moves every second poll).
  A 60-second batch therefore often sees no change for half the stations. Change-over-5-minutes
  values are built from 2–3 reports.
- **Poisson / Skellam independence:** departures and arrivals are assumed independent and
  Poisson. Group rides and commute waves are overdispersed, so probabilities in the
  tails are optimistic. The empty threshold is "≤ 1 bike" because one remaining bike is often
  broken.
- **E-bikes** count as bikes (`num_bikes_available` includes them). A station with only e-bikes
  is "not empty".
- **Stations without history** (41 new live stations) get no expected demand and no alerts
  until the reference table is rebuilt with newer trip data.
- **Weather** is a single city-wide point (lat 40.71, lon −74.01).

## Test on recorded data (2026-10-16)

`scripts/test_stream_replay.sh data/replay_src/status 20`.
- **Input:** a copy of 58 minutes of recorded status (18:02–19:00 UTC, a weekday afternoon in
  New York; 75,149 rows). It combines the bounded test collection (`data/test_live`) with a
  read-only copy of `data/live/status`.
- **Kafka:** temporary topics only. They were deleted afterwards.
- **Run:** replayed at 20× (75,092 messages in 217 s) while the engine ran with a 5-second
  trigger. Summary: `outputs/metrics/stream_test_run.json`.

| metric | value |
|---|---|
| micro-batches | 43 (≈ 100 s of data each) |
| micro-batch duration | mean **3.39 s**, max 4.9 s (the live trigger is 60 s) |
| alert step inside foreachBatch | mean 3.13 s, max 4.79 s |
| peak memory (Spark JVM + Python workers + driver) | **1.21 GB**; swap flat at 1.04 GB |
| Kafka broker | about 400 MB RSS |
| alerts written (18,519 rows over 39 batches) | HIGH 17,997 · MEDIUM 522 |
| of which already empty / already full | 6,685 / 11,304 (all HIGH, P = 1) |
| forward-looking risk alerts | risk_empty: 7 HIGH, 252 MEDIUM · risk_full: 1 HIGH, 270 MEDIUM (about 12 per batch) |

Sample alerts (risk alerts plus one already-full):

| as_of (UTC) | station | bikes | docks | kind | P | level | exp dep / arr 30 min |
|---|---|---:|---:|---|---:|---|---|
| 18:23:00 | Park Ave & E 40 St | 2 | 2 | risk_empty | 0.835 | HIGH | 5.26 / 2.15 |
| 18:59:39 | Greenwich St & Hubert St | 2 | 29 | risk_empty | 0.809 | HIGH | 6.04 / 2.95 |
| 18:56:20 | E 84 St & 1 Ave | 24 | 2 | risk_full | 0.755 | HIGH | 4.09 / 6.87 |
| 18:04:40 | E 50 St & Park Ave | 2 | 35 | risk_empty | 0.711 | HIGH | 4.07 / 2.17 |
| 17:57:54 | E 3 St & Cortelyou Rd | 11 | 1 | already_full | 1.000 | HIGH | 0.42 / 0.13 |

The weather factor was 1.15 throughout (dry, 10–20 °C).

**First alert scores** (`outputs/metrics/alert_scores.json`). Evaluation covers 16 batch times
from 18:02 to 18:29 (each needs 30 minutes of future data): 39,268
in-service station-times. In 30 minutes, 10.2% of them reached ≤ 1
bike and 16.5% reached ≤ 1 dock.

| | empty: precision | empty: recall of new events | full: precision | full: recall of new events |
|---|---|---|---|---|
| model, all alerts | 0.976 (2,972) | 0.44 (88/198) | 0.978 (5,128) | 0.52 (150/291) |
| model, already at threshold | 0.987 (2,856) | | 0.984 (5,021) | |
| model, risk HIGH | 1.0 (4) | | – (0) | |
| model, risk MEDIUM | **0.70** (112) | | **0.69** (107) | |
| baseline "≤ 2 now", all | 0.781 (4,548) | **0.85** (169/198) | 0.811 (7,171) | **0.83** (242/291) |
| baseline, stations at exactly 2 | 0.41 (1,612) | | 0.38 (2,054) | |

How to read this (one hour, one afternoon, so indicative only):
- **Most alerts are "already empty/full".** They're nearly always still at the threshold
  30 minutes later. That's useful operationally, but it isn't prediction.
- **Forward-looking risk alerts are much more precise than the naive rule.** MEDIUM risk alerts
  were right about 70% of the time. The "≤ 2" rule's real predictions (stations at exactly 2)
  were right about 40% of the time.
- **But risk alerts are rare,** so recall of new empty/full events is about half the
  baseline's (44–52% vs 83–85%). Recall counts any alert of the right type in the 30 minutes
  before the event, including "already" alerts at stations that bounced 1 → 2 → 1.
- **Precision/recall trade-off:** lower thresholds (e.g. MEDIUM ≥ 0.3) or a combined rule
  (risk alert OR ≤ 2 bikes during peaks) are the obvious knobs.
- **Needs days of data:** settle this on several days of the long-run collection before
  tuning, covering mornings, nights, weekends and rain.
- **Replay start-up:** early batches have a "now" slightly before the first poll (17:57).
  Old reports from rarely-reporting stations are replayed first. Scoring starts at the first poll.
