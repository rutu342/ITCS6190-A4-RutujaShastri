# Assignment #4: Report

**Name:** Rutuja Hemant Shastri
**Student ID:** 801484366
**Email:** rshastri@charlotte.edu

---

## Seed and commands

Seed used for `datagen.py`: **801484366** (my student ID).

The commands you ran, in order:

```bash
# generate the data (60000 trips, 30 stream files with 1195 rides)
python3 datagen.py 801484366

# start the cluster (first attempts failed, see "Problems and fixes")
docker rm -f spark-master spark-worker-1 spark-worker-2
docker compose down
docker compose up -d

# first run of part 1a failed on numpy, fixed with:
docker exec -u root spark-master pip install numpy

# part 1a
docker cp main.py spark-master:/opt/spark/work-dir/
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/main.py part1a /opt/spark/work-dir/shared 801484366

# part 1b
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/main.py part1b /opt/spark/work-dir/shared 801484366

# part 2
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/main.py part2 /opt/spark/work-dir/shared 801484366

# part 3
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/main.py part3 /opt/spark/work-dir/shared 801484366

# planted anomalies (written to a separate folder so my data was not touched)
python3 datagen.py 801484366 /tmp/a4check /tmp/truth.csv

# precision / recall check
cat shared-folder/output/part3/*.csv | grep -v ride_id | cut -d, -f1 | sort > /tmp/flagged.txt
tail -n +2 /tmp/truth.csv | cut -d, -f1 | sort > /tmp/planted.txt
comm -12 /tmp/flagged.txt /tmp/planted.txt | wc -l
comm -13 /tmp/flagged.txt /tmp/planted.txt
grep -F -f <(comm -23 /tmp/flagged.txt /tmp/planted.txt) shared-folder/output/part3/*.csv
```

---

## Part 1a: trips and revenue per zone per window

The micro-batch summary (batches, watermark, state rows):

```
=== part1a: micro-batch summary ===
batch input rows            watermark state rows
    0         39                    -         29
    1         40  2026-10-27 07:57:56         32
    2         41  2026-10-27 07:58:47         33
    3         36  2026-10-27 07:59:54         33
    4         45  2026-10-27 08:00:58         23
    5         35  2026-10-27 08:01:54         39
    6         38  2026-10-27 08:02:56         46
    7         38  2026-10-27 08:03:53         48
    8         44  2026-10-27 08:04:49         49
    9         38  2026-10-27 08:05:54         24
   10         45  2026-10-27 08:06:47         39
   11         42  2026-10-27 08:07:53         45
   12         37  2026-10-27 08:08:56         49
   13         37  2026-10-27 08:09:55         49
   14         35  2026-10-27 08:10:47         25
   15         41  2026-10-27 08:11:50         42
   16         37  2026-10-27 08:12:50         48
   17         40  2026-10-27 08:13:55         49
   18         40  2026-10-27 08:14:58         49
   19         44  2026-10-27 08:15:56         24
   20         41  2026-10-27 08:16:57         37
   21         37  2026-10-27 08:17:51         43
   22         45  2026-10-27 08:18:55         46
   23         35  2026-10-27 08:19:54         47
   24         44  2026-10-27 08:20:55         25
   25         37  2026-10-27 08:21:44         39
   26         44  2026-10-27 08:22:58         46
   27         37  2026-10-27 08:23:56         46
   28         43  2026-10-27 08:24:53         48
   29         40  2026-10-27 08:25:58         23
   30          0  2026-10-27 08:26:46         23
```

The 30 batches read 1195 rides in total, the same number `datagen.py` reported.

The stream wrote the windows that start at 07:55, 08:00, 08:05, 08:10, 08:15 and 08:20. The batch table also has the **08:25 to 08:30 window**, which is missing from the stream output. The last watermark is 08:26:46, and the 08:25 window only closes when the watermark passes 08:30, so in append mode Spark never emits it. (The 07:55 window exists because the late rides in the first files have an `event_time` of 07:58 or 07:59.)

Besides the missing window, the stream also has **fewer trips inside windows it did write**. For example, in the 08:00 window for zone 10 the stream has 6 trips and 109.12 revenue, while the batch table has 8 trips and 165.09. The 08:10 window for zone 5 has 23 trips in the stream and 24 in the batch table (778.30 vs 805.81). In the first 100 rows of the two tables (windows 08:00 to 08:15) I count 13 trips that the stream lost. The difference comes from how `datagen.py` makes late events: 79 rides arrive 1 to 2 minutes late, and 19 rides arrive 8 to 10 minutes late. The watermark is the latest event time minus 3 minutes. A ride that is 1 to 2 minutes late is still inside the watermark, so it is counted. A ride that is 8 to 10 minutes late arrives after the watermark has already passed the end of its window, so Spark has dropped that window's state and ignores the ride. The batch query has no watermark and counts every ride, which is why its numbers are larger.

`state rows` drops every five batches because each batch is one minute, and one 5-minute window closes every five minutes. While windows are open, the state grows (29 up to 49 rows). When the watermark passes the end of a window, Spark writes the finished window to the output and deletes its state rows, so the count falls back to about 23 to 25 (batches 4, 9, 14, 19, 24 and 29). Batch 30 has 0 input rows, but it still moves the watermark forward and flushes the last closed windows.

## Part 1b: alerts

How many alerts, of which kind, and the first five lines of the output:

```
=== part1b: 8 alerts ===
+-------------+-----+
|       reason|count|
+-------------+-----+
|    high_fare|    1|
|zero_distance|    7|
+-------------+-----+

R00105,2026-10-27 08:02:21,1,0.0,9.5,15.58,zero_distance
R00127,2026-10-27 08:03:36,20,0.0,27.7,31.53,zero_distance
R00226,2026-10-27 08:05:45,20,20.84,53.3,184.25,high_fare
R00260,2026-10-27 08:06:44,2,0.0,19.8,27.06,zero_distance
R00278,2026-10-27 08:07:32,23,0.0,7.8,8.64,zero_distance
```

There are 8 alerts: 1 `high_fare` (fare above 150) and 7 `zero_distance`, which matches the 7 zero-distance rides that `datagen.py` reported. Only one fare is above 150, so most of the planted bad fares are subtler than this rule and are left for part 3. The micro-batch summary of this query shows `-` for watermark and state rows.

This query has no watermark and no state because each ride is judged on its own: the filter looks only at that ride's fare and distance and keeps nothing between rides. There is nothing to wait for and nothing to remember, so a late ride is still alerted correctly, and Spark does not need to know when to forget old data. If I also wanted "more than three alerts for the same driver in ten minutes", the query would become stateful. It would need to group the alerts by `driver_id` over a 10-minute event-time window and count them, and Spark would keep a count per driver and window in state. That requires a watermark so that old windows can be dropped (otherwise the state grows forever, and append mode would not be allowed on the aggregation), and some late alerts would then be ignored, as in part 1a.

---

## Part 2: the fare model

The three lines printed by `evaluate()` (also in `shared-folder/output/part2/metrics`), the "tuned" line with the best parameters, and the "best model" line:

```
seed 801484366: 48085 training rows, 11915 test rows
baseline   RMSE   3.67   R2  0.856
full       RMSE   1.67   R2  0.970
tuned: 2 grid values x 3 folds = 6 fits, plus 1 final refit; CV RMSE per maxDepth value: [2.68, 2.4]
tuned      RMSE   2.27   R2  0.945
saving 'full' to /opt/spark/work-dir/shared/models/fare
```

The CV RMSE list is in the order of the grid, `maxDepth` 5 then 8, so the best parameter was `maxDepth = 8` (CV RMSE 2.40). The best model by test RMSE is the **full linear pipeline** (RMSE 1.67), and that is the one saved under `shared-folder/models/fare`.

My tuned model is a `RandomForestRegressor` (30 trees) inside a pipeline with the same `StringIndexer` and a `VectorAssembler` over the index columns plus `distance_km` and `duration_min`. The grid has two values of one parameter, `maxDepth` in [5, 8]. I chose `maxDepth` because it controls how complex each tree is, and I wanted to see whether deeper trees could capture more of the fare structure than shallow ones. The CrossValidator (3 folds, parallelism 2) ran 2 x 3 = 6 fits, plus 1 final refit of the best setting on the whole training set, so 7 fits for the tuned model (9 fits in the whole part, with the baseline and the full pipeline). Part 2 took under 30 seconds on the cluster (the Spark log runs from 01:55:12 to about 01:55:30).

The tuning did **not** beat the full linear pipeline: the tuned forest has RMSE 2.27 and R2 0.945, against 1.67 and 0.970 for the full linear model. This tells me the fare is close to a linear function of distance and duration, plus separate additive effects for zone, hour and payment type, with some noise. A linear model with the right features matches that structure directly, while trees approximate a straight line with steps and need many splits to do it. The deeper forest did improve (2.68 to 2.40 in CV), so more depth or more trees might narrow the gap, but there is little reason to expect it to pass the linear model.

`hour` is turned into a string before the `StringIndexer` (see `features_of`) because the fare does not grow with the hour: hour 23 is not "more" than hour 8, and rush hours can cost more than the hours around them. If `hour` were a number, the linear model would have to give one slope to the whole day (a later hour always adds or always subtracts the same amount). As a category, `StringIndexer` and `OneHotEncoder` give each hour its own coefficient, so the model can learn that 08:00 is different from 03:00.

---

## Part 3: scoring the stream

The threshold used, the number of rides flagged, the ten rides with the largest deviation and the per-window counts:

```
saved model: full, test RMSE 1.67; flagging rides with deviation > 6.68

=== part3: 69 rides flagged ===
+-------+-------------------+-----------+-----------+------------+------------+------+----------+---------+
|ride_id|event_time         |pickup_zone|distance_km|duration_min|payment_type|fare  |prediction|deviation|
+-------+-------------------+-----------+-----------+------------+------------+------+----------+---------+
|R00226 |2026-10-27 08:05:45|20         |20.84      |53.3        |card        |184.25|60.29     |123.96   |
|R01083 |2026-10-27 08:26:55|1          |10.48      |30.3        |card        |118.01|37.07     |80.94    |
|R00590 |2026-10-27 08:13:47|5          |13.1       |36.1        |app         |126.17|50.03     |76.14    |
|R01137 |2026-10-27 08:28:20|14         |9.07       |38.7        |card        |109.44|35.27     |74.17    |
|R00034 |2026-10-27 08:00:27|5          |5.65       |21.8        |app         |95.36 |32.37     |62.99    |
|R01191 |2026-10-27 08:28:42|4          |26.69      |99.7        |app         |32.6  |87.01     |54.41    |
|R00139 |2026-10-27 08:03:11|22         |5.74       |24.7        |card        |68.29 |24.57     |43.72    |
|R01066 |2026-10-27 08:26:29|18         |8.07       |22.3        |app         |69.94 |27.63     |42.31    |
|R00701 |2026-10-27 08:16:01|11         |5.64       |20.0        |card        |63.75 |22.65     |41.1     |
|R00977 |2026-10-27 08:23:46|25         |4.73       |18.3        |card        |61.2  |20.52     |40.68    |
+-------+-------------------+-----------+-----------+------------+------------+------+----------+---------+

=== part3: flagged rides per 5-minute window (batch over the output) ===
+-------------------+-----+
|       window_start|count|
+-------------------+-----+
|2026-10-27 08:00:00|   12|
|2026-10-27 08:05:00|    8|
|2026-10-27 08:10:00|    8|
|2026-10-27 08:15:00|   12|
|2026-10-27 08:20:00|   10|
|2026-10-27 08:25:00|   19|
+-------------------+-----+
```

The threshold is 4 x 1.67 = **6.68**, and **69 rides** were flagged.

The generator planted **48** anomalies. My job flagged all 48 of them (**recall = 48/48 = 100%**, none missed), plus 21 rides that were not planted (**precision = 48/69 = about 70%**). In plain words: everything the generator planted was found, but about 3 of every 10 alerts are false alarms that a fraud team would have to read and dismiss.

The 21 false positives have two things in common:

- **5 are zero-distance rides** (R00127, R00260, R00382, R00396, R01149). Their fares look normal (about 26 to 31), but the model sees `distance_km = 0` and predicts about 14 to 16, so the deviation is 11 to 15.
- **16 are long, expensive trips** (11.7 to 27.7 km, 30 to 105 minutes, fares of 43 to 105). Their deviations are 6.79 to 13.79, mostly just above the 6.68 threshold. The noise in a fare grows with the size of the trip, and a single fixed threshold of 4 x RMSE treats a 5 km trip and a 28 km trip the same.

To reduce them I would change two things. First, in part 2, I would let the model handle the variance that grows with trip size, for example by predicting the log of the fare or by comparing the *relative* deviation (`|fare - prediction| / prediction`) instead of a fixed dollar threshold. Second, I would handle zero-distance rides separately (part 1b already alerts on them), and I might add an interaction feature such as distance x duration. A better tuned model would also help, since my forest was worse than the linear pipeline.

There is no `fit()` in part 3 because training already happened in part 2 and the result is saved: part 3 only loads the PipelineModel and calls `transform`, which applies what was learned to new rides. A `fit()` on the stream would not work (Spark does not allow batch operations like `fit` on a streaming DataFrame, and fails with an error such as "Queries with streaming sources must be executed with writeStream.start()"). Even if it could run, it would train on one minute of data that contains the anomalies, and the model would change under the running job.

Three stages of the loaded model are fitted models, and each one learned something from the training data:

- `StringIndexerModel`: for `zone`, `payment_type` and `hour`, the mapping from each category value to an index number (ordered by frequency), plus the extra slot for unseen values (`handleInvalid="keep"`).
- `OneHotEncoderModel`: how many categories each index column has, so it knows the size of each one-hot vector.
- `LinearRegressionModel`: the coefficients and the intercept that predict the fare.

The `VectorAssembler` is not a fitted model: it only joins columns into one vector and learns nothing.

---

## Design: this pipeline in production

In production the rides would arrive in a Kafka topic, and the scoring job would be a long-running Structured Streaming query that reads from Kafka (`readStream`) on a cluster such as Kubernetes or YARN, applies `features_of`, scores with the saved PipelineModel, and writes the flagged rides to an alerts topic or table that the fraud team reads. Its checkpoint (the Kafka offsets and the query state) lives on durable shared storage such as S3 or HDFS, not on a container's disk, so the job can restart and continue exactly where it stopped without losing or repeating events. The training job is a separate nightly batch job (scheduled by something like Airflow) that reads the last month of completed trips from the data lake, trains the three pipelines, evaluates them on a held-out set and saves the best PipelineModel to a versioned path (for example `models/fare/2026-11-15/`) together with its RMSE, which gives the threshold. A new model reaches the running scoring job without losing events in one of two ways: the job is stopped and restarted with the new path, and because the Kafka offsets are in the checkpoint it resumes from the next unread event; or, better, the scoring runs in `foreachBatch`, which checks a "current version" pointer at the start of each micro-batch and reloads the model (and its threshold) only when the pointer changes, so the stream never stops. A new model is promoted only if it is not worse than the current one on recent data. To know that the model has gone stale I would monitor the distribution of the deviation (the RMSE on recent rides) and the share of rides flagged (here 69 of 1195, about 5.8%, against the 4% planted rate), the drift of the input features (distance, duration, zone and hour mix, and how often an unseen category falls in the `keep` bucket), Kafka consumer lag, the rate of late events and dropped windows, and the age of the model, and I would alert when the flag rate or the error moves away from what the training metrics promised.

---

## Problems and fixes

1. **Old containers blocked the cluster.** `docker compose up -d` failed with:
   `Error response from daemon: Conflict. The container name "/spark-master" is already in use by container "07a99bc3836693490e5db7fc3447df9b55fc0138298be71a6b3a3b9db4a9462e". You have to remove (or rename) that container to be able to reuse that name.`
   These were left over from an earlier hands-on. I removed them with `docker rm -f spark-master spark-worker-1 spark-worker-2`. My first command had a typo (`spark-worker-2v`), so the old worker was still there and the next start failed with `Conflict. The container name "/spark-worker-2" is already in use by container "76af5bda7c1c..."`. Running `docker rm -f spark-worker-2`, then `docker compose down` and `docker compose up -d` fixed it.
2. **Docker was paused.** `Error response from daemon: Docker Desktop is manually paused. Unpause it through the Whale menu or Dashboard.` I unpaused Docker Desktop.
3. **Missing numpy.** The first `spark-submit` stopped at `from pyspark.ml import Pipeline, PipelineModel` with `ModuleNotFoundError: No module named 'numpy'`. I installed it in the master container with `docker exec -u root spark-master pip install numpy`, and the parts ran afterwards. The container loses it if it is recreated, so it has to be installed again after `docker compose down`.
4. **Warnings in part 2 (not fixed, the model is fine).** The full pipeline printed `Cholesky solver failed due to singular covariance matrix. Retrying with Quasi-Newton solver.` followed by `ERROR LBFGS: Failure! Resetting history: breeze.optimize.FirstOrderException: Line search zoom failed`. The one-hot columns are strongly correlated, so Spark switched to another solver, and the model still reached R2 0.970. Spark also printed `regParam is zero, which might cause numerical instability and overfitting` and `netlib-blas: JNI_OnLoad: dlopen(libblas.so.3) failed`, which only means it used its built-in math library instead of a native one.