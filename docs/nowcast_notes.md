# Nowcast experiment on live snapshots: PRELIMINARY

> **PRELIMINARY.** About **31 hours** of live data survived the coverage rules, over three New York
> days. Training and validation come from one Friday evening and night. The test block is
> Sunday Oct 18, 00:39–20:39 New York. These numbers show which way the comparison leans. They are not
> a reliable evaluation. Re-run `python -m src.nowcast` when the collector has several continuous days.
> The first run (one evening, about 11 hours) is kept in `outputs/metrics/nowcast_metrics_prelim_backup.json`.

## Question

For a station at a snapshot time *t*, will it be **EMPTY** (≤ 1 bike) or **FULL** (≤ 1 free dock) at
any snapshot in the next 30 minutes? Each label is built only for rows where the station is *not
already* empty (respectively full) at *t*, so every row is a real forward-looking prediction.

## Setup (`src/nowcast.py`)

- **Data:**
  - **Source:** all of `data/live/status`, read-only.
  - **Snapshots:** the collector's poll times.
  - **State at time t:** the collector stores a station only when its report changes, so a station's state at *t* is its latest report at or before *t*.
  - **Excluded:** stations out of service, or silent for more than 60 minutes, at *t*.
- **Gap rule:** a row is kept only if snapshots cover **[t − 30 min, t]** (for the change features) and **[t, t + 30 min]** (for the label) with no gap longer than **5 minutes**.
- **Features (12):**
  - stock: bikes, docks, capacity;
  - recent change in bikes over the last 5, 15 and 30 minutes;
  - expected departures and arrivals for the next 30 minutes (the alert engine's own lookup: historical average for the overlapping 15-minute buckets × weather factor);
  - weather: temperature and precipitation;
  - time: hour and weekday, in New York time.
- **Check:** the expected-demand lookup was compared with the replay engine's stored values for batch 72 (2,430 stations) and matched exactly (difference 0).
- **Models,** all compared on the same test rows:
  1. persistence: the state stays as is, so it never alerts;
  2. simple rule: bikes ≤ 2 (empty) or docks ≤ 2 (full); among rows not already at ≤ 1, that means exactly 2;
  3. the alert engine score: Skellam probability from historical average × weather;
  4. logistic regression (Spark MLlib, standardised features);
  5. random forest (MLlib, 30 trees, depth 8);
  6. gradient-boosted trees (MLlib, 40 iterations, depth 5).
- **Training:** negatives are subsampled to 4 per positive, in the training set only. Validation and test are untouched.
- **Threshold:** for each scored model (3–6), the threshold that maximises F1 on validation, then applied unchanged to test.
- **Resources:**
  - Spark: 3 GB driver, `local[3]`, 32 shuffle partitions.
  - Scratch: the job's own folder (`data/scratch_nowcast/`, emptied on exit), not the shared `data/tmp_spark`.
  - The 4 GB free-disk guard was checked before the run and before Spark.
  - Output is written to temporary files and moved onto `nowcast_metrics.json` and `nowcast_comparison.png` only after the run finishes.
  - Runtime: 163 s.

## Data, coverage and dropped rows

| | count |
|---|---:|
| snapshots collected | 1,422 (Fri 16 Oct 13:43 → Sun 18 Oct 21:05 New York) |
| snapshots dropped: past 30 min not continuous | 92 |
| snapshots dropped: next 30 min not continuous | 58 |
| **usable snapshots** | **1,272** (about 31 hours) |
| usable snapshots per New York day | Fri 16 Oct: 385 · Sat 17 Oct: 70 · Sun 18 Oct: 817 |
| station rows dropped (unknown state / out of service or silent) | 306 / 16,341 |
| station-snapshot rows built | 3,078,129 (2,433 stations) |
| excluded from the EMPTY label (already ≤ 1 bike) | 243,103 |
| excluded from the FULL label (already ≤ 1 dock) | 546,628 |

The usable data falls in two blocks: Fri 16 Oct 14:58 → Sat 17 Oct 01:57 (455 snapshots) and
Sun 18 Oct 00:39 → 20:39 (817 snapshots). **On Saturday the machine slept repeatedly**, so almost
none of Saturday has 30 continuous minutes on both sides of a snapshot.

## Split (by time, never random)

The planned rule (test on the latest full New York day, validate on the day before, train on
earlier days) needs three full days. A day counts as full with at least 576 usable snapshots
(80 % of a day at one snapshot every ~2 minutes). Only **Sunday 18 Oct (817)** qualifies.
The script therefore used its middle rule: **test = the latest full day; the usable snapshots
before it are split by time, first 75 % train, last 25 % validation.** It will switch to the
day-based rule automatically once three full days exist.

| part | New York time | snapshots | EMPTY rows (positives) | FULL rows (positives) |
|---|---|---:|---:|---:|
| train | Fri 16 Oct 14:58 – 22:54 | 341 | 759,854 (26,215) → 131,075 after subsampling | 692,048 (37,479) → 187,395 after subsampling |
| validation | Fri 16 Oct 22:56 – Sat 17 Oct 01:57 | 114 | 254,963 (3,979; 1.6 %) | 219,543 (8,719; 4.0 %) |
| test | Sun 18 Oct 00:39 – 20:39 | 817 | 1,820,209 (40,145; **2.2 %**) | 1,619,910 (64,091; **4.0 %**) |

## Results on the test block

Precision, recall and F1 at the validation-chosen threshold; PR-AUC = average precision. The
confusion matrix counts TP / FP / FN / TN.

**EMPTY within 30 minutes** (test base rate 2.2 %, 40,145 positives)

| model | threshold | precision | recall | F1 | PR-AUC | TP / FP / FN / TN |
|---|---:|---:|---:|---:|---:|---|
| persistence (no alert) | – | – | 0.000 | 0.000 | 0.022 | 0 / 0 / 40,145 / 1,780,064 |
| simple rule (bikes ≤ 2) | – | 0.285 | 0.565 | 0.379 | 0.170 | 22,667 / 56,905 / 17,478 / 1,723,159 |
| **alert engine score** | 0.153 | 0.394 | **0.650** | **0.490** | **0.440** | 26,077 / 40,170 / 14,068 / 1,739,894 |
| logistic regression | 0.485 | 0.282 | 0.197 | 0.232 | 0.240 | 7,890 / 20,109 / 32,255 / 1,759,955 |
| random forest | 0.652 | 0.432 | 0.537 | 0.478 | 0.392 | 21,538 / 28,380 / 18,607 / 1,751,684 |
| gradient-boosted trees | 0.758 | **0.439** | 0.452 | 0.445 | 0.416 | 18,136 / 23,222 / 22,009 / 1,756,842 |

**FULL within 30 minutes** (test base rate 4.0 %, 64,091 positives)

| model | threshold | precision | recall | F1 | PR-AUC | TP / FP / FN / TN |
|---|---:|---:|---:|---:|---:|---|
| persistence (no alert) | – | – | 0.000 | 0.000 | 0.040 | 0 / 0 / 64,091 / 1,555,819 |
| simple rule (docks ≤ 2) | – | 0.313 | 0.527 | 0.392 | 0.183 | 33,746 / 74,180 / 30,345 / 1,481,639 |
| **alert engine score** | 0.150 | **0.511** | 0.655 | **0.574** | **0.580** | 41,976 / 40,109 / 22,115 / 1,515,710 |
| logistic regression | 0.363 | 0.494 | 0.480 | 0.487 | 0.456 | 30,764 / 31,498 / 33,327 / 1,524,321 |
| random forest | 0.527 | 0.473 | **0.707** | 0.567 | 0.579 | 45,331 / 50,549 / 18,760 / 1,505,270 |
| gradient-boosted trees | 0.587 | 0.458 | 0.699 | 0.553 | 0.577 | 44,767 / 53,071 / 19,324 / 1,502,748 |

Chart: `outputs/charts/nowcast_comparison.png` (PR curves for both labels and F1 bars).
All numbers: `outputs/metrics/nowcast_metrics.json`.

## Plain statements

- **The baseline still holds.** The **alert engine score has the best F1 and PR-AUC on both labels.**
  The ML models do **not** beat it.
  - EMPTY: random forest and GBT reach F1 0.478 / 0.445 against the engine's 0.490, and PR-AUC 0.392 / 0.416 against 0.440.
  - FULL: random forest comes within a hair (F1 0.567 vs 0.574, PR-AUC 0.579 vs 0.580), with GBT just behind.
  - Logistic regression is clearly worse.
  - As instructed, I did not tune the ML models to try to beat the engine.
- **Every scored model except logistic regression beats the simple rule.** The rule's precision is 0.29
  (empty) and 0.31 (full): when a station is at exactly 2, it usually does *not* reach ≤ 1 within 30 minutes.
- **Persistence** never alerts, so its recall and F1 are 0.
- **The engine's own alert threshold (0.5, "MEDIUM") is conservative.** At 0.5 the engine gets
  precision 0.61 / 0.82 but recall only 0.06 / 0.07 (empty / full). The F1-best threshold on
  validation was much lower again (0.15 / 0.15), the same as in the first run. That's worth
  re-checking on several full days before changing anything live.
- **Compared with the first run** (test = Friday night): the ranking is the same. Every scored model has
  a higher F1 on the Sunday test block; the simple rule's FULL F1 fell (0.438 → 0.392). The gap between
  the engine and the best tree model narrowed on both labels (EMPTY 0.039 → 0.012, FULL 0.014 → 0.007).

## Why accuracy is not a headline metric

Positives are rare: 2.2 % (empty) and 4.0 % (full) of test rows. **Persistence, which never alerts
and is therefore useless, is 97.8 % (empty) and 96.0 % (full) accurate.** Accuracy rewards saying
"nothing will happen". Precision (how many alerts are right), recall (how many events were caught),
F1 and the area under the precision-recall curve measure what matters for alerts. The base rate is
reported so PR-AUC can be read against it: a useless model scores roughly the base rate.

## Caveats

- **About 31 hours of usable data, and train/test come from different kinds of day.**
  - Training and validation cover only Friday 15:00 to Saturday 02:00. The ML models never saw a weekend
    daytime before being tested on Sunday, so the weekday feature carries no usable signal. This works
    against the ML models. The engine's historical average already knows Sundays.
  - Sunday 18 Oct counts as a full day by the 80 % rule, but the test block ends at 20:39 because the
    collector was still running.
- **Rows are highly correlated.** Neighbouring snapshots of the same station are nearly identical, so the
  effective sample is much smaller than the row counts suggest. No confidence intervals are reported.
- **Thresholds were picked on a 3-hour late-evening validation block.** That is noisy, and it is a
  different time of day from most of the test block.
- **The models learn station identity only through capacity and expected demand.** There is no station
  id feature, deliberately, so the result is not about memorising stations from one evening.
- **Operator vans and dock outages are not modelled.** A restock can make a correct "empty" prediction look
  wrong; a van emptying a station can create an unpredictable event.
- **Weather is a single city-wide point** from the Open-Meteo forecast endpoint's recent history, the
  same source the live engine uses.
- **The experiment doesn't depend on the live stream engine.** It rebuilds the engine's score from the
  same tables, so the stream engine's outage on Saturday 17 Oct only matters for live alerts, not here.
