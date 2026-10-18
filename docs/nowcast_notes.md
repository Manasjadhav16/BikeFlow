# Nowcast experiment on live snapshots: PRELIMINARY

> **PRELIMINARY.** Only about **11 hours** of live data (one Friday evening into the night)
> survived the coverage rules. The test block is a night block (23:39–01:57 New York). These numbers
> are a first look at whether the pipeline works and which way the comparison leans. They are not
> an evaluation. Re-run `python -m src.nowcast` when the collector has several continuous days.

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
  - Runtime: 100 s.

## Data, coverage and dropped rows

| | count |
|---|---:|
| snapshots collected | 592 (Fri 16 Oct 13:43 → Sun 18 Oct 00:31 New York) |
| snapshots dropped: past 30 min not continuous | 92 |
| snapshots dropped: next 30 min not continuous | 45 |
| **usable snapshots** | **455**, all in one block: Fri 16 Oct 14:58 → Sat 17 Oct 01:57 New York |
| station rows dropped (unknown state / out of service or silent) | 306 / 3,198 |
| station-snapshot rows built | 1,103,511 (2,433 stations) |
| excluded from the EMPTY label (already ≤ 1 bike) | 88,694 |
| excluded from the FULL label (already ≤ 1 dock) | 191,920 |

**Why so little:** the collector polled through Friday evening. **On Saturday the machine slept
repeatedly:** 24 gaps over 5 minutes, the longest 174 minutes. None of Saturday's daytime fragments has
30 continuous minutes on both sides of a snapshot.

## Split (by time, never random)

Under the planned rule (test on the latest full New York day, validate on the day before, train on
earlier days) there is **no full day** to use. The script therefore applied its documented
**fallback: a chronological 60 / 20 / 20 split** of the usable snapshot times. It will switch to the
day-based rule automatically once three usable days exist.

| part | New York time | snapshots | EMPTY rows (positives) | FULL rows (positives) |
|---|---|---:|---:|---:|
| train | Fri 16 Oct 14:58 – 21:29 | 273 | 607,849 (22,907) → 114,535 after subsampling | 560,774 (32,152) → 160,760 after subsampling |
| validation | Fri 16 Oct 21:30 – 23:38 | 91 | 203,243 (4,305; 2.1 %) | 176,139 (7,395; 4.2 %) |
| test | Fri 16 Oct 23:39 – Sat 17 Oct 01:57 | 91 | 203,725 (2,982; **1.5 %**) | 174,678 (6,651; **3.8 %**) |

## Results on the test block

Precision, recall and F1 at the validation-chosen threshold; PR-AUC = average precision. The
confusion matrix counts TP / FP / FN / TN.

**EMPTY within 30 minutes** (test base rate 1.5 %, 2,982 positives)

| model | threshold | precision | recall | F1 | PR-AUC | TP / FP / FN / TN |
|---|---:|---:|---:|---:|---:|---|
| persistence (no alert) | – | – | 0.000 | 0.000 | 0.015 | 0 / 0 / 2,982 / 200,743 |
| simple rule (bikes ≤ 2) | – | 0.193 | 0.595 | 0.291 | 0.121 | 1,774 / 7,421 / 1,208 / 193,322 |
| **alert engine score** | 0.193 | **0.342** | 0.525 | **0.414** | **0.315** | 1,566 / 3,014 / 1,416 / 197,729 |
| logistic regression | 0.299 | 0.132 | 0.821 | 0.228 | 0.256 | 2,447 / 16,055 / 535 / 184,688 |
| random forest | 0.607 | 0.266 | 0.617 | 0.372 | 0.304 | 1,841 / 5,071 / 1,141 / 195,672 |
| gradient-boosted trees | 0.651 | 0.292 | 0.526 | 0.375 | 0.309 | 1,567 / 3,807 / 1,415 / 196,936 |

**FULL within 30 minutes** (test base rate 3.8 %, 6,651 positives)

| model | threshold | precision | recall | F1 | PR-AUC | TP / FP / FN / TN |
|---|---:|---:|---:|---:|---:|---|
| persistence (no alert) | – | – | 0.000 | 0.000 | 0.038 | 0 / 0 / 6,651 / 168,027 |
| simple rule (docks ≤ 2) | – | 0.341 | 0.613 | 0.438 | 0.224 | 4,075 / 7,886 / 2,576 / 160,141 |
| **alert engine score** | 0.140 | 0.465 | **0.662** | **0.546** | **0.516** | 4,403 / 5,061 / 2,248 / 162,966 |
| logistic regression | 0.409 | 0.389 | 0.340 | 0.363 | 0.320 | 2,264 / 3,563 / 4,387 / 164,464 |
| random forest | 0.591 | 0.511 | 0.554 | 0.532 | 0.479 | 3,684 / 3,524 / 2,967 / 164,503 |
| gradient-boosted trees | 0.681 | **0.542** | 0.507 | 0.524 | 0.489 | 3,371 / 2,847 / 3,280 / 165,180 |

Chart: `outputs/charts/nowcast_comparison.png` (PR curves for both labels and F1 bars).
All numbers: `outputs/metrics/nowcast_metrics.json`.

## Plain statements

- **The baseline holds.** The **alert engine score has the best F1 and PR-AUC on both labels.** The ML
  models do **not** beat it. Random forest and GBT come close (EMPTY F1 0.372 / 0.375 vs 0.414;
  FULL 0.532 / 0.524 vs 0.546). Logistic regression is clearly worse. As instructed, I did not tune
  the ML models to try to beat it.
- **Every scored model beats the simple rule.** The rule's precision is 0.19 (empty) and 0.34
  (full): when a station is at exactly 2, it usually does *not* reach ≤ 1 within 30 minutes.
- **Persistence** never alerts, so its recall and F1 are 0.
- **The engine's own alert threshold (0.5, "MEDIUM") is conservative here.** At 0.5 the engine gets
  precision 0.47 / 0.71 but recall only 0.08 / 0.06 (empty / full). The F1-best threshold on
  validation was much lower (0.19 / 0.14). This is worth re-checking on daytime data before changing
  anything. A night test block is not a basis for moving the live threshold.

## Why accuracy is not a headline metric

Positives are rare: 1.5 % (empty) and 3.8 % (full) of test rows. **Persistence, which never alerts
and is therefore useless, is 98.5 % (empty) and 96.2 % (full) accurate.** Accuracy rewards saying
"nothing will happen". Precision (how many alerts are right), recall (how many events were caught),
F1 and the area under the precision-recall curve measure what matters for alerts. The base rate is
reported so PR-AUC can be read against it: a useless model scores roughly the base rate.

## Caveats

- **About 11 hours of usable data, from one evening.**
  - Training covers Friday 15:00–21:30.
  - Validation and test are late evening and night, with few events and quieter dynamics than a
    weekday rush. The ranking could change on daytime or weekday data.
- **Test positives are few** (2,982 / 6,651 station-snapshots, from far fewer distinct station
  episodes). Neighbouring snapshots of the same station are highly correlated, so the effective
  sample is much smaller than the row counts suggest. No confidence intervals are reported.
- **Thresholds were picked on a 2-hour validation block.** That is noisy, especially for the empty label.
- **The models learn station identity only through capacity and expected demand.** There is no station
  id feature, deliberately, so the result is not about memorising stations from one evening.
- **Operator vans and dock outages are not modelled.** A restock can make a correct "empty" prediction look
  wrong; a van emptying a station can create an unpredictable event.
- **Weather is a single city-wide point** from the Open-Meteo forecast endpoint's recent history, the
  same source the live engine uses.
- **The live stream engine was not running during this work.** It stopped on Sat 17 Oct at about 02:41 New
  York time with a Kafka `TimeoutException` after the machine slept. The experiment doesn't depend on
  it (it rebuilds the engine's score from the same tables), but no live alerts were produced after that.
