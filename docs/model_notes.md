# Model notes: Stage 2 demand model

**Bottom line:** the best model (random forest) beats the historical-average baseline by
only **1.5% MAE** on the test month and is **slightly worse on RMSE**. Most of what it
knows comes from the station's own 4-week history. It clearly beats persistence, but
persistence is a weak baseline for this target. Details and caveats below.

## 1. Task

For every GBFS-matched station (2,477) and every 15-minute prediction time `ts`, predict

    bike_change_next_30m = (arrivals - departures) over [ts, ts + 30 min)

using only trip data before `ts`. "Bike change" is net flow from trips. It ignores
rebalancing trucks and docks out of service (see §5).

The target is mostly zero (about 60% of rows; standard deviation about 1.8 bikes) with
heavy tails (±60 around events). So **predicting 0 everywhere already scores MAE 1.011 on
the test month**, and that is the floor to read every MAE below against.

## 2. Features (`src/build_features.py`)

| feature | definition | available live? |
|---|---|---|
| `hour`, `day_of_week` (ISO), `month`, `is_weekend` | of `ts`, New York time | yes |
| `station_idx` | index into `models/station_index.json` (sorted trip short IDs) | yes |
| `lat`, `lng` | station coordinates | yes |
| `bike_change_last_15m` | net change in slot `[ts-15m, ts)` | from live counts (§5) |
| `bike_change_last_30m` | net change in `[ts-30m, ts)` | from live counts (§5) |
| `hist_same_slot_4w` | mean of the target at `ts - 1, 2, 3, 4 weeks` | needs a history store |

Rows are densified per station with zeros between its first and last observed ride
(nothing before a station opened or after it closed). Building them takes 21–32 s
per month, with peak memory about 2.2 GB.

## 3. How leakage is avoided

- **Past-only features, checked by an independent recomputation.** For one station-month
  (2,884 rows, including the DST-change day), all four computed features and the target
  were recomputed from raw `demand_15min` in pandas and matched on 100% of rows.
- **`hist_same_slot_4w` uses the target at `ts - k weeks`.** That covers
  `[ts - k weeks, ts - k weeks + 30 min)`, which ends 6 days 23.5 h (or more) before `ts`. The
  current week is never included. The feature is null unless all four weeks exist, so
  training starts in November 2025 (October only supplies history).
- **Month edges are built from the months on either side**, so lags, leads and history
  at a month boundary use the true neighbouring slots. The split is still strictly by time.
- **Time-based split, never random:** train Nov 2025 – Jul 2026, validate Aug 2026,
  test Sep 2026. The historical-average baseline, the "top-20% busiest" station list
  and the training sample are all taken from the training months only.
- **Known approximations, not leaks:**
  - The 4-week lag is 7 × 24 h of absolute time, so for one week after each daylight-saving
    change it points one local hour off.
  - Weeks that contain a disrupted day feed into `hist_same_slot_4w` for later weeks.

## 4. Training setup (`src/train_model.py`)

- **Sample:** 20% of station-days, chosen deterministically by
  `xxhash64(station_id, date, seed=42)`, from Nov–Jul. That is **10,771,345 rows**,
  without disrupted rows and without rows lacking 4-week history. A 1-month dry run
  (10% sample) took about 2 min end to end. On all ~57M rows the cached tree data would
  overflow the 3 GB heap. **Evaluation always uses every row** of the validation (7.05M)
  and test (6.83M) months.
- **Missing history at evaluation:** 129,792 test rows (1.9%, stations newer than 4 weeks)
  have `hist_same_slot_4w` imputed as 0. The historical-average baseline has no value for
  222,702 test rows and predicts 0 there.
- **Models (not tuned; one fixed configuration each, same 10 features):**
  - Linear regression (no regularisation).
  - Random forest: 40 trees, depth 10, min 20 per leaf, 50% subsampling.
  - GBT: 50 iterations, depth 6, step size 0.1, 50% subsampling.
  - The best model is chosen by validation MAE (excluding disrupted rows): **random forest**.
- **Cost on the 8 GB machine:**
  - Linear regression: 26 s.
  - Random forest: 8.6 min.
  - GBT: 18.5 min, with swap rising to a 2.4 GB peak. This was the only step under real memory
    pressure. Use 10% or fewer GBT iterations if this needs re-running often.
  - Scoring a full month: about 40 s.

## 5. Training / live consistency (for Stage 3)

Live GBFS gives `num_bikes_available` per station, not trips. The live versions of the
trend features are therefore

    bike_change_last_15m ≈ bikes_available(now) - bikes_available(now - 15 min)
    bike_change_last_30m ≈ bikes_available(now) - bikes_available(now - 30 min)

These match the training definition (arrivals minus departures) **only when no truck
rebalancing, dock outage or e-bike battery swap happens in that window.** Live deltas
include rebalancing; trip-based training data does not. Stage 3 should:

- log live deltas next to trip-based deltas for a while and measure the gap;
- clip, or flag, live deltas that look like truck moves (large jumps);
- keep the same `station_idx` map (`models/station_index.json`). A station missing from
  it can't use the model and should fall back to the historical average
  (`models/hist_baseline/`, station × weekday × hour);
- build `hist_same_slot_4w` from a rolling store of observed 30-min changes, not from
  the trip files.

## 6. Results (test = Sep 2026; disrupted rows excluded unless stated)

MAE / RMSE in bikes over the next 30 minutes. All rows of the month are scored.

| segment | rows | always 0 | persistence | hist avg | linear reg. | random forest | GBT |
|---|---:|---:|---:|---:|---:|---:|---:|
| overall | 6,369,678 | 1.011 / 2.215 | 1.427 / 2.703 | 0.993 / **1.924** | 1.039 / 1.954 | **0.978** / 1.942 | 0.990 / 1.935 |
| top-20% busiest | 1,254,190 | – | 3.119 / 4.816 | **2.190** / **3.438** | 2.253 / 3.488 | 2.193 / 3.486 | 2.201 / 3.473 |
| rush hours 7–10, 16–19 | 1,596,495 | – | 2.034 / 3.644 | 1.463 / **2.675** | 1.494 / 2.676 | **1.453** / 2.697 | 1.468 / 2.683 |
| overall *incl. disrupted* | 6,830,635 | – | 1.375 / 2.635 | 0.960 / 1.876 | 1.009 / 1.908 | 0.943 / 1.892 | 0.956 / 1.886 |

Validation (Aug 2026, no disrupted days in the month): RF 0.976 / 1.896, hist avg
0.991 / 1.875, GBT 0.990 / 1.894, linear regression 1.039 / 1.911, persistence 1.433 / 2.663.
Full tables, including MAE by hour, are in `outputs/metrics/model_results.json`.

### Honest comparison

- **Persistence is beaten by about 31%, but that's easy.** At 15-minute resolution the
  last 30 minutes is a noisy draw, so copying it doubles the noise.
- **The historical average is the real bar, and the models barely clear it.** RF is 1.5%
  better on MAE overall and 0.7% better in rush hours. It is *worse* on RMSE everywhere,
  and *not better* on the busiest 20% of stations (2.193 vs 2.190). GBT is roughly a tie,
  and linear regression is worse than the baseline.
- **Against always-0:** RF improves MAE by only 3.3% (MAE rewards predicting the median,
  which is 0). It improves RMSE by 12%, so it does capture the larger, predictable
  commute swings.
- **Why:** feature importance is dominated by the history feature (RF 52%, GBT 31%,
  and the largest linear coefficient). The recent-trend features add about 28% (RF).
  In practice the model is a smoothed historical average plus a small trend correction.
  The big errors are one-off spikes (events, group rides, weather) that no feature here can
  see (`outputs/charts/pred_vs_actual_week.png`).
- **Not tried yet:** tuning, weather, holidays and events, longer lags (1–2 h, same day),
  neighbouring-station flows, and count-aware losses (Poisson or quantile).

## 7. Strong drain / fill alerts (`src/eval_alerts.py`)

Strong drain = predicted change ≤ −X; strong fill = predicted change ≥ +X. Each is scored
against the actual change with the same X (test month, disrupted excluded). Precision / recall:

| alert | actual events | persistence | hist avg | random forest | GBT |
|---|---:|---:|---:|---:|---:|
| drain X=3 | 362,607 | 0.26 / 0.26 | **0.70** / 0.09 | 0.68 / 0.09 | 0.63 / **0.11** |
| drain X=5 | 139,912 | 0.25 / 0.25 | **0.71** / 0.09 | 0.67 / 0.09 | 0.64 / 0.10 |
| drain X=8 | 46,876 | 0.25 / 0.25 | **0.70** / 0.09 | 0.64 / 0.08 | 0.64 / 0.09 |
| fill X=3 | 354,799 | 0.26 / 0.26 | 0.68 / 0.09 | **0.69** / 0.09 | 0.68 / **0.10** |
| fill X=5 | 139,655 | 0.25 / 0.25 | 0.69 / 0.10 | **0.70** / 0.13 | 0.67 / **0.14** |
| fill X=8 | 48,897 | 0.26 / 0.26 | 0.68 / 0.10 | **0.68** / 0.18 | 0.66 / **0.19** |

- **Smoothed models:** about 2 in 3 of their alerts are right, but they catch only 8–19% of
  events, because they rarely predict large values.
- **Persistence:** catches about 25%, with about 75% false alarms.
- **RF vs the baseline:** RF only beats the historical average for large *fills* (recall
  0.18 vs 0.10 at X=8, at the same precision). For drains the baseline is as good or better.

**These are flow alerts, not empty/full alerts.** A drain of 8 bikes matters at a station
holding 10 bikes and not at one holding 40. Real "will be empty / will be full" alerts need
the live `num_bikes_available` / `num_docks_available` from GBFS `station_status`, and those
come in Stage 3.

## 8. Artifacts

- `models/best_model/`: Spark `PipelineModel` (VectorAssembler + random forest).
- `models/model_meta.json`: feature order, training months, sample rate, validation MAE.
- `models/station_index.json`: the `station_idx` map with GBFS UUIDs.
- `models/hist_baseline/`: station × weekday × hour mean (also the fallback).
- `outputs/metrics/model_results.json`, `feature_importances.json`, `alert_eval.json`,
  `model_results_dryrun.json`.
- `outputs/charts/pred_vs_actual_week.png`, `mae_by_hour.png`, `model_comparison.png`.
- **Rebuild:** `scripts/run_stage2.sh`. It runs `src.build_features`,
  `src.train_model --sample-pct 20`, `src.eval_alerts` and `src.model_charts`,
  and stops at the first failing step.
