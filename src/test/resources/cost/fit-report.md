# Cost coefficient fit

Fitted on 349 measured rows (7 rows excluded, see the note column) by non negative least squares on residuals scaled by 1 / measured latency (relative error). Coefficients are rounded to two significant digits; the rounded values are what CostCoefficients.java carries and what the residual and choice tables below use.

## Coefficients

| coefficient | fitted | rounded | unit |
|---|---|---|---|
| PUSHED_FIXED_MS | 19.02 | 19 | ms per request |
| OBJECT_STORE_OPEN_MS | 73.79 | 74 | ms per request |
| OBJECT_STORE_READ_MS_PER_GB_PER_NODE | 165.4 | 170 | ms per GB of column bytes one node reads from the object store |
| PUSHED_DECODE_MS_PER_MROW_THREAD_PER_8_BYTES | 8.954 | 9 | ms per million rows per thread per 8 bytes of row width |
| PUSHED_STRING_KEY_MS_PER_MROW_THREAD | 17.46 | 17 | ms per million rows one thread processes |
| PUSHED_NUMERIC_KEY_MS_PER_MROW_THREAD | 1.773 | 1.8 | ms per million rows one thread processes |
| PUSHED_DATE_KEY_MS_PER_MROW_THREAD | 44.75 | 45 | ms per million rows one thread processes |
| PUSHED_RANGE_KEY_MS_PER_MROW_THREAD | 17.76 | 18 | ms per million rows one thread processes |
| PUSHED_FILTERS_KEY_MS_PER_MROW_THREAD | 12.36 | 12 | ms per million rows one thread processes |
| PUSHED_COMPOSITE_DATE_KEY_MS_PER_MROW_THREAD | 103 | 100 | ms per million rows one thread processes |
| PUSHED_EXTENDED_STATS_MS_PER_MROW_THREAD | 0.8888 | 0.89 | ms per million rows one thread processes |
| PUSHED_PERCENTILES_MS_PER_MROW_THREAD | 17.41 | 17 | ms per million rows one thread processes |
| PUSHED_LARGE_GROUPS_MS_PER_MROW_THREAD | 100.4 | 100 | ms per million rows one thread processes |
| PUSHED_FILTER_EVAL_MS_PER_MROW_THREAD | 11.78 | 12 | ms per million rows one thread processes |
| PUSHED_FILTER_MATCH_MS_PER_MROW | 16.64 | 17 | ms per million rows of the whole table |
| PUSHED_MERGE_MS_PER_MGROUP | 460.5 | 460 | ms per million group rows merged |
| PUSHED_CARDINALITY_HASH_MS_PER_MROW_THREAD | 18.78 | 19 | ms per million rows one thread processes |
| PUSHED_CARDINALITY_MS_PER_MVALUE | 293.4 | 290 | ms per million distinct values fed to the sketch |
| LUCENE_FIXED_MS | 18.26 | 18 | ms per request |
| LUCENE_COLUMN_MS_PER_MROW_THREAD | 8.326 | 8.3 | ms per million rows one thread processes |
| LUCENE_NUMERIC_KEY_MS_PER_MROW_THREAD | 16.58 | 17 | ms per million rows one thread processes |
| LUCENE_DATE_KEY_MS_PER_MROW_THREAD | 14.48 | 14 | ms per million rows one thread processes |
| LUCENE_RANGE_KEY_MS_PER_MROW_THREAD | 25.86 | 26 | ms per million rows one thread processes |
| LUCENE_FILTERS_KEY_MS_PER_MROW_THREAD | 31.32 | 31 | ms per million rows one thread processes |
| LUCENE_COMPOSITE_SOURCE_MS_PER_MROW_THREAD | 18.27 | 18 | ms per million rows one thread processes |
| LUCENE_NESTED_LEVEL_MS_PER_MROW_THREAD | 16.51 | 17 | ms per million rows one thread processes |
| LUCENE_SIMPLE_METRIC_MS_PER_MROW_THREAD | 3.643 | 3.6 | ms per million rows one thread processes |
| LUCENE_BUCKET_METRIC_MS_PER_MROW_THREAD | 31.76 | 32 | ms per million rows one thread processes |
| LUCENE_EXTENDED_STATS_MS_PER_MROW_THREAD | 18.69 | 19 | ms per million rows one thread processes |
| LUCENE_PERCENTILES_MS_PER_MROW_THREAD | 97.28 | 97 | ms per million rows one thread processes |
| LUCENE_CARDINALITY_MS_PER_MROW_THREAD | 132.5 | 130 | ms per million rows one thread processes |
| LUCENE_LARGE_GROUPS_MS_PER_MROW_NODE | 95.41 | 95 | ms per million rows one node holds |
| LUCENE_FILTER_EVAL_MS_PER_MROW_THREAD | 196.5 | 200 | ms per million rows one thread processes |

## Residuals

Per fitted row: predicted / measured with the rounded coefficients. Rows are in CSV order.

| round | table | cluster | shape | path | slices | measured | predicted | pred / meas |
|---|---|---|---|---|---|---|---|---|
| r15 | 20M | 1node4xl | terms(category) | pushed | 8 | 69 ms | 67 ms | 0.98 |
| r15 | 20M | 1node4xl | terms(category) | lucene | 1 | 204 ms | 184 ms | 0.90 |
| r15 | 20M | 1node4xl | terms(rating) | pushed | 8 | 37 ms | 35 ms | 0.94 |
| r15 | 20M | 1node4xl | terms(rating) | lucene | 1 | 526 ms | 524 ms | 1.00 |
| r15 | 20M | 1node4xl | sum(price) | pushed | 8 | 30 ms | 42 ms | 1.38 |
| r15 | 20M | 1node4xl | sum(price) | lucene | 1 | 255 ms | 256 ms | 1.00 |
| r15 | 20M | 1node4xl | date_histogram month + sum(price) | pushed | 8 | 161 ms | 177 ms | 1.10 |
| r15 | 20M | 1node4xl | date_histogram month + sum(price) | lucene | 1 | 1470 ms | 1342 ms | 0.91 |
| r15 | 20M | 1node16xl | terms(category) | pushed | 32 | 36 ms | 33 ms | 0.90 |
| r15 | 20M | 1node16xl | terms(category) | lucene | 1 | 157 ms | 184 ms | 1.17 |
| r15 | 20M | 1node16xl | terms(rating) | pushed | 32 | 24 ms | 23 ms | 0.96 |
| r15 | 20M | 1node16xl | terms(rating) | lucene | 1 | 530 ms | 524 ms | 0.99 |
| r15 | 20M | 1node16xl | sum(price) | pushed | 32 | 23 ms | 25 ms | 1.07 |
| r15 | 20M | 1node16xl | sum(price) | lucene | 1 | 255 ms | 256 ms | 1.00 |
| r15 | 20M | 1node16xl | date_histogram month + sum(price) | pushed | 32 | 63 ms | 59 ms | 0.93 |
| r15 | 20M | 1node16xl | date_histogram month + sum(price) | lucene | 1 | 1460 ms | 1342 ms | 0.92 |
| r15 | 20M | 3node | terms(category) | pushed | 8 | 38 ms | 35 ms | 0.93 |
| r15 | 20M | 3node | terms(category) | lucene | 1 | 81 ms | 73 ms | 0.91 |
| r15 | 20M | 3node | terms(rating) | pushed | 8 | 26 ms | 24 ms | 0.93 |
| r15 | 20M | 3node | terms(rating) | lucene | 1 | 188 ms | 187 ms | 0.99 |
| r15 | 20M | 3node | sum(price) | pushed | 8 | 23 ms | 27 ms | 1.15 |
| r15 | 20M | 3node | sum(price) | lucene | 1 | 96 ms | 97 ms | 1.01 |
| r15 | 20M | 3node | date_histogram month + sum(price) | pushed | 8 | 80 ms | 72 ms | 0.89 |
| r15 | 20M | 3node | date_histogram month + sum(price) | lucene | 1 | 503 ms | 459 ms | 0.91 |
| r15 | 20M | 6node | terms(category) | pushed | 8 | 27 ms | 27 ms | 1.01 |
| r15 | 20M | 6node | terms(category) | lucene | 1 | 48 ms | 46 ms | 0.95 |
| r15 | 20M | 6node | terms(rating) | pushed | 8 | 20 ms | 22 ms | 1.08 |
| r15 | 20M | 6node | terms(rating) | lucene | 1 | 103 ms | 102 ms | 0.99 |
| r15 | 20M | 6node | sum(price) | pushed | 8 | 18 ms | 23 ms | 1.26 |
| r15 | 20M | 6node | sum(price) | lucene | 1 | 57 ms | 58 ms | 1.01 |
| r15 | 20M | 6node | date_histogram month + sum(price) | pushed | 8 | 51 ms | 45 ms | 0.89 |
| r15 | 20M | 6node | date_histogram month + sum(price) | lucene | 1 | 270 ms | 239 ms | 0.88 |
| r15 | 100M | 1node4xl | terms(category) | pushed | 8 | 307 ms | 260 ms | 0.85 |
| r15 | 100M | 1node4xl | terms(category) | lucene | 1 | 1010 ms | 848 ms | 0.84 |
| r15 | 100M | 1node4xl | terms(rating) | pushed | 8 | 162 ms | 98 ms | 0.60 |
| r15 | 100M | 1node4xl | terms(rating) | lucene | 1 | 2630 ms | 2548 ms | 0.97 |
| r15 | 100M | 1node4xl | sum(price) | pushed | 8 | 118 ms | 132 ms | 1.11 |
| r15 | 100M | 1node4xl | sum(price) | lucene | 1 | 1260 ms | 1208 ms | 0.96 |
| r15 | 100M | 1node4xl | date_histogram month + sum(price) | pushed | 8 | 763 ms | 807 ms | 1.06 |
| r15 | 100M | 1node4xl | date_histogram month + sum(price) | lucene | 1 | 7390 ms | 6638 ms | 0.90 |
| r15 | 100M | 1node16xl | terms(category) | pushed | 32 | 136 ms | 81 ms | 0.59 |
| r15 | 100M | 1node16xl | terms(category) | lucene | 1 | 775 ms | 848 ms | 1.09 |
| r15 | 100M | 1node16xl | terms(rating) | pushed | 32 | 91 ms | 39 ms | 0.43 |
| r15 | 100M | 1node16xl | terms(rating) | lucene | 1 | 2630 ms | 2548 ms | 0.97 |
| r15 | 100M | 1node16xl | sum(price) | pushed | 32 | 94 ms | 47 ms | 0.50 |
| r15 | 100M | 1node16xl | sum(price) | lucene | 1 | 1260 ms | 1208 ms | 0.96 |
| r15 | 100M | 1node16xl | date_histogram month + sum(price) | pushed | 32 | 260 ms | 216 ms | 0.83 |
| r15 | 100M | 1node16xl | date_histogram month + sum(price) | lucene | 1 | 7270 ms | 6638 ms | 0.91 |
| r15 | 100M | 3node | terms(category) | pushed | 8 | 126 ms | 100 ms | 0.79 |
| r15 | 100M | 3node | terms(category) | lucene | 1 | 354 ms | 295 ms | 0.83 |
| r15 | 100M | 3node | terms(rating) | pushed | 8 | 72 ms | 45 ms | 0.63 |
| r15 | 100M | 3node | terms(rating) | lucene | 1 | 888 ms | 861 ms | 0.97 |
| r15 | 100M | 3node | sum(price) | pushed | 8 | 58 ms | 57 ms | 0.97 |
| r15 | 100M | 3node | sum(price) | lucene | 1 | 439 ms | 415 ms | 0.94 |
| r15 | 100M | 3node | date_histogram month + sum(price) | pushed | 8 | 283 ms | 282 ms | 1.00 |
| r15 | 100M | 3node | date_histogram month + sum(price) | lucene | 1 | 2500 ms | 2225 ms | 0.89 |
| r15 | 100M | 6node | terms(category) | pushed | 8 | 77 ms | 59 ms | 0.77 |
| r15 | 100M | 6node | terms(category) | lucene | 1 | 190 ms | 156 ms | 0.82 |
| r15 | 100M | 6node | terms(rating) | pushed | 8 | 48 ms | 32 ms | 0.67 |
| r15 | 100M | 6node | terms(rating) | lucene | 1 | 457 ms | 440 ms | 0.96 |
| r15 | 100M | 6node | sum(price) | pushed | 8 | 41 ms | 38 ms | 0.92 |
| r15 | 100M | 6node | sum(price) | lucene | 1 | 234 ms | 216 ms | 0.92 |
| r15 | 100M | 6node | date_histogram month + sum(price) | pushed | 8 | 165 ms | 150 ms | 0.91 |
| r15 | 100M | 6node | date_histogram month + sum(price) | lucene | 1 | 1260 ms | 1121 ms | 0.89 |
| r15 | 1B | 1node4xl | terms(category) | pushed | 8 | 2750 ms | 2840 ms | 1.03 |
| r15 | 1B | 1node4xl | terms(category) | lucene | 1 | 6950 ms | 8392 ms | 1.21 |
| r15 | 1B | 1node4xl | terms(rating) | pushed | 8 | 1110 ms | 1561 ms | 1.41 |
| r15 | 1B | 1node4xl | terms(rating) | lucene | 1 | 25.13 s | 25.39 s | 1.01 |
| r15 | 1B | 1node4xl | sum(price) | pushed | 8 | 2530 ms | 2578 ms | 1.02 |
| r15 | 1B | 1node4xl | sum(price) | lucene | 1 | 12.26 s | 11.99 s | 0.98 |
| r15 | 1B | 1node4xl | date_histogram month + sum(price) | pushed | 8 | 12.43 s | 10.69 s | 0.86 |
| r15 | 1B | 1node4xl | date_histogram month + sum(price) | lucene | 1 | 89.24 s | 66.29 s | 0.74 |
| r15 | 1B | 1node16xl | terms(category) | pushed | 32 | 716 ms | 622 ms | 0.87 |
| r15 | 1B | 1node16xl | terms(category) | lucene | 1 | 8860 ms | 8318 ms | 0.94 |
| r15 | 1B | 1node16xl | terms(rating) | pushed | 32 | 282 ms | 216 ms | 0.77 |
| r15 | 1B | 1node16xl | terms(rating) | lucene | 1 | 24.17 s | 25.32 s | 1.05 |
| r15 | 1B | 1node16xl | sum(price) | pushed | 32 | 337 ms | 300 ms | 0.89 |
| r15 | 1B | 1node16xl | sum(price) | lucene | 1 | 11.75 s | 11.92 s | 1.01 |
| r15 | 1B | 1node16xl | date_histogram month + sum(price) | pushed | 32 | 1890 ms | 1988 ms | 1.05 |
| r15 | 1B | 1node16xl | date_histogram month + sum(price) | lucene | 1 | 72.58 s | 66.22 s | 0.91 |
| r15 | 1B | 3node | terms(category) | pushed | 8 | 1030 ms | 1009 ms | 0.98 |
| r15 | 1B | 3node | terms(category) | lucene | 1 | 3010 ms | 2859 ms | 0.95 |
| r15 | 1B | 3node | terms(rating) | pushed | 8 | 531 ms | 582 ms | 1.10 |
| r15 | 1B | 3node | terms(rating) | lucene | 1 | 8330 ms | 8525 ms | 1.02 |
| r15 | 1B | 3node | sum(price) | pushed | 8 | 959 ms | 921 ms | 0.96 |
| r15 | 1B | 3node | sum(price) | lucene | 1 | 3990 ms | 4059 ms | 1.02 |
| r15 | 1B | 3node | date_histogram month + sum(price) | pushed | 8 | 4440 ms | 3625 ms | 0.82 |
| r15 | 1B | 3node | date_histogram month + sum(price) | lucene | 1 | 24.72 s | 22.16 s | 0.90 |
| r15 | 1B | 6node | terms(category) | pushed | 8 | 598 ms | 551 ms | 0.92 |
| r15 | 1B | 6node | terms(category) | lucene | 1 | 1530 ms | 1475 ms | 0.96 |
| r15 | 1B | 6node | terms(rating) | pushed | 8 | 356 ms | 338 ms | 0.95 |
| r15 | 1B | 6node | terms(rating) | lucene | 1 | 4210 ms | 4309 ms | 1.02 |
| r15 | 1B | 6node | sum(price) | pushed | 8 | 589 ms | 507 ms | 0.86 |
| r15 | 1B | 6node | sum(price) | lucene | 1 | 2030 ms | 2075 ms | 1.02 |
| r15 | 1B | 6node | date_histogram month + sum(price) | pushed | 8 | 2400 ms | 1859 ms | 0.77 |
| r15 | 1B | 6node | date_histogram month + sum(price) | lucene | 1 | 12.27 s | 11.13 s | 0.91 |
| r15 | 1B | 1node16xl | terms(user_id) size 10 | lucene | 1 | 140.89 s | 103.32 s | 0.73 |
| r15 | 1B | 3node | terms(user_id) size 10 | lucene | 1 | 70.20 s | 34.53 s | 0.49 |
| r15 | 1B | 6node | terms(user_id) size 10 | lucene | 1 | 28.64 s | 17.31 s | 0.60 |
| r15 | 20M | 1node4xl | terms(category) > terms(rating) > avg(price) | pushed | 8 | 97 ms | 109 ms | 1.12 |
| r15 | 20M | 1node4xl | terms(category) > terms(rating) > avg(price) | lucene | 1 | 1760 ms | 1908 ms | 1.08 |
| r15 | 20M | 1node4xl | terms(category) > date_histogram month > sum(price) | pushed | 8 | 219 ms | 242 ms | 1.11 |
| r15 | 20M | 1node4xl | terms(category) > date_histogram month > sum(price) | lucene | 1 | 1850 ms | 1848 ms | 1.00 |
| r15 | 20M | 1node4xl | composite(category, rating) size 10 | pushed | 8 | 84 ms | 87 ms | 1.03 |
| r15 | 20M | 1node4xl | composite(category, rating) size 10 | lucene | 1 | 1050 ms | 1070 ms | 1.02 |
| r15 | 20M | 1node4xl | composite(category, rating) size 10 page 2 | pushed | 8 | 83 ms | 87 ms | 1.04 |
| r15 | 20M | 1node4xl | composite(category, rating) size 10 page 2 | lucene | 1 | 1110 ms | 1070 ms | 0.96 |
| r15 | 20M | 1node4xl | composite(category, ts 1d) size 10 | pushed | 8 | 941 ms | 877 ms | 0.93 |
| r15 | 20M | 1node4xl | composite(category, ts 1d) size 10 | lucene | 1 | 1030 ms | 1070 ms | 1.04 |
| r15 | 20M | 1node16xl | terms(category) > terms(rating) > avg(price) | pushed | 32 | 49 ms | 55 ms | 1.13 |
| r15 | 20M | 1node16xl | terms(category) > terms(rating) > avg(price) | lucene | 1 | 1830 ms | 1908 ms | 1.04 |
| r15 | 20M | 1node16xl | terms(category) > date_histogram month > sum(price) | pushed | 32 | 125 ms | 141 ms | 1.13 |
| r15 | 20M | 1node16xl | terms(category) > date_histogram month > sum(price) | lucene | 1 | 1850 ms | 1848 ms | 1.00 |
| r15 | 20M | 1node16xl | composite(category, rating) size 10 | pushed | 32 | 45 ms | 50 ms | 1.10 |
| r15 | 20M | 1node16xl | composite(category, rating) size 10 | lucene | 1 | 1060 ms | 1070 ms | 1.01 |
| r15 | 20M | 1node16xl | composite(category, rating) size 10 page 2 | pushed | 32 | 44 ms | 50 ms | 1.13 |
| r15 | 20M | 1node16xl | composite(category, rating) size 10 page 2 | lucene | 1 | 1130 ms | 1070 ms | 0.95 |
| r15 | 20M | 1node16xl | composite(category, ts 1d) size 10 | pushed | 32 | 1810 ms | 2248 ms | 1.24 |
| r15 | 20M | 1node16xl | composite(category, ts 1d) size 10 | lucene | 1 | 1050 ms | 1070 ms | 1.02 |
| r15 | 20M | 3node | terms(category) > terms(rating) > avg(price) | pushed | 8 | 48 ms | 51 ms | 1.07 |
| r15 | 20M | 3node | terms(category) > terms(rating) > avg(price) | lucene | 1 | 632 ms | 648 ms | 1.03 |
| r15 | 20M | 3node | terms(category) > date_histogram month > sum(price) | pushed | 8 | 113 ms | 105 ms | 0.93 |
| r15 | 20M | 3node | terms(category) > date_histogram month > sum(price) | lucene | 1 | 634 ms | 628 ms | 0.99 |
| r15 | 20M | 3node | composite(category, rating) size 10 | pushed | 8 | 46 ms | 44 ms | 0.96 |
| r15 | 20M | 3node | composite(category, rating) size 10 | lucene | 1 | 360 ms | 369 ms | 1.02 |
| r15 | 20M | 3node | composite(category, rating) size 10 page 2 | pushed | 8 | 46 ms | 44 ms | 0.96 |
| r15 | 20M | 3node | composite(category, rating) size 10 page 2 | lucene | 1 | 388 ms | 369 ms | 0.95 |
| r15 | 20M | 3node | composite(category, ts 1d) size 10 | pushed | 8 | 862 ms | 663 ms | 0.77 |
| r15 | 20M | 3node | composite(category, ts 1d) size 10 | lucene | 1 | 383 ms | 369 ms | 0.96 |
| r15 | 20M | 6node | terms(category) > terms(rating) > avg(price) | pushed | 8 | 37 ms | 37 ms | 1.00 |
| r15 | 20M | 6node | terms(category) > terms(rating) > avg(price) | lucene | 1 | 335 ms | 333 ms | 0.99 |
| r15 | 20M | 6node | terms(category) > date_histogram month > sum(price) | pushed | 8 | 77 ms | 71 ms | 0.92 |
| r15 | 20M | 6node | terms(category) > date_histogram month > sum(price) | lucene | 1 | 344 ms | 323 ms | 0.94 |
| r15 | 20M | 6node | composite(category, rating) size 10 | pushed | 8 | 32 ms | 33 ms | 1.04 |
| r15 | 20M | 6node | composite(category, rating) size 10 | lucene | 1 | 197 ms | 193 ms | 0.98 |
| r15 | 20M | 6node | composite(category, rating) size 10 page 2 | pushed | 8 | 33 ms | 33 ms | 1.01 |
| r15 | 20M | 6node | composite(category, rating) size 10 page 2 | lucene | 1 | 206 ms | 193 ms | 0.94 |
| r15 | 20M | 6node | composite(category, ts 1d) size 10 | pushed | 8 | 660 ms | 610 ms | 0.92 |
| r15 | 20M | 6node | composite(category, ts 1d) size 10 | lucene | 1 | 194 ms | 193 ms | 1.00 |
| r15 | 100M | 1node4xl | terms(category) > terms(rating) > avg(price) | pushed | 8 | 424 ms | 455 ms | 1.07 |
| r15 | 100M | 1node4xl | terms(category) > terms(rating) > avg(price) | lucene | 1 | 9280 ms | 9468 ms | 1.02 |
| r15 | 100M | 1node4xl | terms(category) > date_histogram month > sum(price) | pushed | 8 | 990 ms | 1065 ms | 1.08 |
| r15 | 100M | 1node4xl | terms(category) > date_histogram month > sum(price) | lucene | 1 | 9320 ms | 9168 ms | 0.98 |
| r15 | 100M | 1node4xl | composite(category, rating) size 10 | pushed | 8 | 373 ms | 342 ms | 0.92 |
| r15 | 100M | 1node4xl | composite(category, rating) size 10 | lucene | 1 | 5300 ms | 5278 ms | 1.00 |
| r15 | 100M | 1node4xl | composite(category, rating) size 10 page 2 | pushed | 8 | 378 ms | 342 ms | 0.90 |
| r15 | 100M | 1node4xl | composite(category, rating) size 10 page 2 | lucene | 1 | 5400 ms | 5278 ms | 0.98 |
| r15 | 100M | 1node4xl | composite(category, ts 1d) size 10 | pushed | 8 | 2000 ms | 2159 ms | 1.08 |
| r15 | 100M | 1node4xl | composite(category, ts 1d) size 10 | lucene | 1 | 5080 ms | 5278 ms | 1.04 |
| r15 | 100M | 1node16xl | terms(category) > terms(rating) > avg(price) | pushed | 32 | 181 ms | 142 ms | 0.78 |
| r15 | 100M | 1node16xl | terms(category) > terms(rating) > avg(price) | lucene | 1 | 9270 ms | 9468 ms | 1.02 |
| r15 | 100M | 1node16xl | terms(category) > date_histogram month > sum(price) | pushed | 32 | 375 ms | 347 ms | 0.92 |
| r15 | 100M | 1node16xl | terms(category) > date_histogram month > sum(price) | lucene | 1 | 9300 ms | 9168 ms | 0.99 |
| r15 | 100M | 1node16xl | composite(category, rating) size 10 | pushed | 32 | 165 ms | 114 ms | 0.69 |
| r15 | 100M | 1node16xl | composite(category, rating) size 10 | lucene | 1 | 5390 ms | 5278 ms | 0.98 |
| r15 | 100M | 1node16xl | composite(category, rating) size 10 page 2 | pushed | 32 | 166 ms | 114 ms | 0.68 |
| r15 | 100M | 1node16xl | composite(category, rating) size 10 page 2 | lucene | 1 | 5630 ms | 5278 ms | 0.94 |
| r15 | 100M | 1node16xl | composite(category, ts 1d) size 10 | pushed | 32 | 2580 ms | 2569 ms | 1.00 |
| r15 | 100M | 1node16xl | composite(category, ts 1d) size 10 | lucene | 1 | 5180 ms | 5278 ms | 1.02 |
| r15 | 100M | 3node | terms(category) > terms(rating) > avg(price) | pushed | 8 | 167 ms | 167 ms | 1.00 |
| r15 | 100M | 3node | terms(category) > terms(rating) > avg(price) | lucene | 1 | 3120 ms | 3168 ms | 1.02 |
| r15 | 100M | 3node | terms(category) > date_histogram month > sum(price) | pushed | 8 | 372 ms | 379 ms | 1.02 |
| r15 | 100M | 3node | terms(category) > date_histogram month > sum(price) | lucene | 1 | 3160 ms | 3068 ms | 0.97 |
| r15 | 100M | 3node | composite(category, rating) size 10 | pushed | 8 | 148 ms | 129 ms | 0.87 |
| r15 | 100M | 3node | composite(category, rating) size 10 | lucene | 1 | 1920 ms | 1771 ms | 0.92 |
| r15 | 100M | 3node | composite(category, rating) size 10 page 2 | pushed | 8 | 146 ms | 129 ms | 0.88 |
| r15 | 100M | 3node | composite(category, rating) size 10 page 2 | lucene | 1 | 1920 ms | 1771 ms | 0.92 |
| r15 | 100M | 3node | composite(category, ts 1d) size 10 | pushed | 8 | 1310 ms | 1091 ms | 0.83 |
| r15 | 100M | 3node | composite(category, ts 1d) size 10 | lucene | 1 | 1880 ms | 1771 ms | 0.94 |
| r15 | 100M | 6node | terms(category) > terms(rating) > avg(price) | pushed | 8 | 104 ms | 95 ms | 0.91 |
| r15 | 100M | 6node | terms(category) > terms(rating) > avg(price) | lucene | 1 | 1600 ms | 1593 ms | 1.00 |
| r15 | 100M | 6node | terms(category) > date_histogram month > sum(price) | pushed | 8 | 220 ms | 208 ms | 0.95 |
| r15 | 100M | 6node | terms(category) > date_histogram month > sum(price) | lucene | 1 | 1630 ms | 1543 ms | 0.95 |
| r15 | 100M | 6node | composite(category, rating) size 10 | pushed | 8 | 93 ms | 76 ms | 0.82 |
| r15 | 100M | 6node | composite(category, rating) size 10 | lucene | 1 | 938 ms | 895 ms | 0.95 |
| r15 | 100M | 6node | composite(category, rating) size 10 page 2 | pushed | 8 | 93 ms | 76 ms | 0.82 |
| r15 | 100M | 6node | composite(category, rating) size 10 page 2 | lucene | 1 | 964 ms | 895 ms | 0.93 |
| r15 | 100M | 6node | composite(category, ts 1d) size 10 | pushed | 8 | 1030 ms | 823 ms | 0.80 |
| r15 | 100M | 6node | composite(category, ts 1d) size 10 | lucene | 1 | 931 ms | 895 ms | 0.96 |
| r15 | 1B | 1node4xl | terms(category) > terms(rating) > avg(price) | pushed | 8 | 6150 ms | 6795 ms | 1.10 |
| r15 | 1B | 1node4xl | terms(category) > terms(rating) > avg(price) | lucene | 1 | 102.98 s | 94.59 s | 0.92 |
| r15 | 1B | 1node4xl | terms(category) > date_histogram month > sum(price) | pushed | 8 | 15.15 s | 13.45 s | 0.89 |
| r15 | 1B | 1node4xl | terms(category) > date_histogram month > sum(price) | lucene | 1 | 108.12 s | 91.59 s | 0.85 |
| r15 | 1B | 1node4xl | composite(category, rating) size 10 | pushed | 8 | 3580 ms | 4310 ms | 1.20 |
| r15 | 1B | 1node4xl | composite(category, rating) size 10 | lucene | 1 | 51.08 s | 52.69 s | 1.03 |
| r15 | 1B | 1node4xl | composite(category, rating) size 10 page 2 | pushed | 8 | 3580 ms | 4310 ms | 1.20 |
| r15 | 1B | 1node4xl | composite(category, rating) size 10 page 2 | lucene | 1 | 51.69 s | 52.69 s | 1.02 |
| r15 | 1B | 1node4xl | composite(category, ts 1d) size 10 | pushed | 8 | 16.46 s | 18.36 s | 1.12 |
| r15 | 1B | 1node4xl | composite(category, ts 1d) size 10 | lucene | 1 | 48.97 s | 52.69 s | 1.08 |
| r15 | 1B | 1node16xl | terms(category) > terms(rating) > avg(price) | pushed | 32 | 1020 ms | 1113 ms | 1.09 |
| r15 | 1B | 1node16xl | terms(category) > terms(rating) > avg(price) | lucene | 1 | 89.13 s | 94.52 s | 1.06 |
| r15 | 1B | 1node16xl | terms(category) > date_histogram month > sum(price) | pushed | 32 | 2480 ms | 2660 ms | 1.07 |
| r15 | 1B | 1node16xl | terms(category) > date_histogram month > sum(price) | lucene | 1 | 90.17 s | 91.52 s | 1.01 |
| r15 | 1B | 1node16xl | composite(category, rating) size 10 | pushed | 32 | 867 ms | 832 ms | 0.96 |
| r15 | 1B | 1node16xl | composite(category, rating) size 10 | lucene | 1 | 52.22 s | 52.62 s | 1.01 |
| r15 | 1B | 1node16xl | composite(category, rating) size 10 page 2 | pushed | 32 | 864 ms | 832 ms | 0.96 |
| r15 | 1B | 1node16xl | composite(category, rating) size 10 page 2 | lucene | 1 | 54.32 s | 52.62 s | 0.97 |
| r15 | 1B | 1node16xl | composite(category, ts 1d) size 10 | pushed | 32 | 6390 ms | 6176 ms | 0.97 |
| r15 | 1B | 1node16xl | composite(category, ts 1d) size 10 | lucene | 1 | 49.39 s | 52.62 s | 1.07 |
| r15 | 1B | 3node | terms(category) > terms(rating) > avg(price) | pushed | 8 | 2170 ms | 2330 ms | 1.07 |
| r15 | 1B | 3node | terms(category) > terms(rating) > avg(price) | lucene | 1 | 30.76 s | 31.59 s | 1.03 |
| r15 | 1B | 3node | terms(category) > date_histogram month > sum(price) | pushed | 8 | 5260 ms | 4558 ms | 0.87 |
| r15 | 1B | 3node | terms(category) > date_histogram month > sum(price) | lucene | 1 | 30.99 s | 30.59 s | 0.99 |
| r15 | 1B | 3node | composite(category, rating) size 10 | pushed | 8 | 1320 ms | 1501 ms | 1.14 |
| r15 | 1B | 3node | composite(category, rating) size 10 | lucene | 1 | 17.48 s | 17.63 s | 1.01 |
| r15 | 1B | 3node | composite(category, rating) size 10 page 2 | pushed | 8 | 1380 ms | 1501 ms | 1.09 |
| r15 | 1B | 3node | composite(category, rating) size 10 page 2 | lucene | 1 | 17.60 s | 17.63 s | 1.00 |
| r15 | 1B | 3node | composite(category, ts 1d) size 10 | pushed | 8 | 6750 ms | 6541 ms | 0.97 |
| r15 | 1B | 3node | composite(category, ts 1d) size 10 | lucene | 1 | 16.58 s | 17.63 s | 1.06 |
| r15 | 1B | 6node | terms(category) > terms(rating) > avg(price) | pushed | 8 | 1200 ms | 1213 ms | 1.01 |
| r15 | 1B | 6node | terms(category) > terms(rating) > avg(price) | lucene | 1 | 15.44 s | 15.84 s | 1.03 |
| r15 | 1B | 6node | terms(category) > date_histogram month > sum(price) | pushed | 8 | 2810 ms | 2334 ms | 0.83 |
| r15 | 1B | 6node | terms(category) > date_histogram month > sum(price) | lucene | 1 | 15.54 s | 15.34 s | 0.99 |
| r15 | 1B | 6node | composite(category, rating) size 10 | pushed | 8 | 844 ms | 799 ms | 0.95 |
| r15 | 1B | 6node | composite(category, rating) size 10 | lucene | 1 | 8800 ms | 8859 ms | 1.01 |
| r15 | 1B | 6node | composite(category, rating) size 10 page 2 | pushed | 8 | 848 ms | 799 ms | 0.94 |
| r15 | 1B | 6node | composite(category, rating) size 10 page 2 | lucene | 1 | 8920 ms | 8859 ms | 0.99 |
| r15 | 1B | 6node | composite(category, ts 1d) size 10 | pushed | 8 | 3870 ms | 3585 ms | 0.93 |
| r15 | 1B | 6node | composite(category, ts 1d) size 10 | lucene | 1 | 8440 ms | 8859 ms | 1.05 |
| r16 | 1B | 1node4xl | terms(category) | pushed | 8 | 2730 ms | 2840 ms | 1.04 |
| r16 | 1B | 4node | terms(category) | pushed | 8 | 848 ms | 780 ms | 0.92 |
| r16 | 1B | 6node | terms(category) | pushed | 8 | 636 ms | 551 ms | 0.87 |
| r16 | 1B | 1node16xl | terms(category) | pushed | 32 | 726 ms | 622 ms | 0.86 |
| r16 | 1B | 4node | terms(category) | lucene | 8 | 346 ms | 351 ms | 1.02 |
| r16 | 1B | 1node4xl | terms(rating) | pushed | 8 | 1100 ms | 1561 ms | 1.42 |
| r16 | 1B | 4node | terms(rating) | pushed | 8 | 346 ms | 460 ms | 1.33 |
| r16 | 1B | 6node | terms(rating) | pushed | 8 | 386 ms | 338 ms | 0.87 |
| r16 | 1B | 1node16xl | terms(rating) | pushed | 32 | 263 ms | 216 ms | 0.82 |
| r16 | 1B | 4node | terms(rating) | lucene | 8 | 780 ms | 883 ms | 1.13 |
| r16 | 1B | 1node4xl | terms(user_id) size 10 | pushed | 8 | 19.50 s | 19.69 s | 1.01 |
| r16 | 1B | 4node | terms(user_id) size 10 | pushed | 8 | 4740 ms | 4992 ms | 1.05 |
| r16 | 1B | 6node | terms(user_id) size 10 | pushed | 8 | 3260 ms | 3359 ms | 1.03 |
| r16 | 1B | 1node16xl | terms(user_id) size 10 | pushed | 32 | 5290 ms | 4239 ms | 0.80 |
| r16 | 1B | 4node | terms(user_id) size 10 | lucene | 8 | 17.80 s | 24.10 s | 1.35 |
| r16 | 1B | 1node4xl | sum(price) | pushed | 8 | 2640 ms | 2578 ms | 0.98 |
| r16 | 1B | 4node | sum(price) | pushed | 8 | 784 ms | 714 ms | 0.91 |
| r16 | 1B | 6node | sum(price) | pushed | 8 | 586 ms | 507 ms | 0.87 |
| r16 | 1B | 1node16xl | sum(price) | pushed | 32 | 321 ms | 300 ms | 0.94 |
| r16 | 1B | 4node | sum(price) | lucene | 8 | 442 ms | 464 ms | 1.05 |
| r16 | 1B | 1node4xl | avg(price) | pushed | 8 | 2640 ms | 2578 ms | 0.98 |
| r16 | 1B | 4node | avg(price) | pushed | 8 | 793 ms | 714 ms | 0.90 |
| r16 | 1B | 6node | avg(price) | pushed | 8 | 587 ms | 507 ms | 0.86 |
| r16 | 1B | 1node16xl | avg(price) | pushed | 32 | 321 ms | 300 ms | 0.94 |
| r16 | 1B | 4node | avg(price) | lucene | 8 | 448 ms | 464 ms | 1.04 |
| r16 | 1B | 1node4xl | stats(price) | pushed | 8 | 2730 ms | 2578 ms | 0.94 |
| r16 | 1B | 4node | stats(price) | pushed | 8 | 816 ms | 714 ms | 0.88 |
| r16 | 1B | 6node | stats(price) | pushed | 8 | 590 ms | 507 ms | 0.86 |
| r16 | 1B | 1node16xl | stats(price) | pushed | 32 | 322 ms | 300 ms | 0.93 |
| r16 | 1B | 4node | stats(price) | lucene | 8 | 475 ms | 464 ms | 0.98 |
| r16 | 1B | 1node4xl | extended_stats(price) | pushed | 8 | 2890 ms | 2689 ms | 0.93 |
| r16 | 1B | 4node | extended_stats(price) | pushed | 8 | 826 ms | 742 ms | 0.90 |
| r16 | 1B | 6node | extended_stats(price) | pushed | 8 | 611 ms | 526 ms | 0.86 |
| r16 | 1B | 1node16xl | extended_stats(price) | pushed | 32 | 291 ms | 328 ms | 1.13 |
| r16 | 1B | 4node | extended_stats(price) | lucene | 8 | 1050 ms | 1058 ms | 1.01 |
| r16 | 1B | 1node4xl | date_histogram 1d + sum(price) | pushed | 8 | 9770 ms | 10.69 s | 1.09 |
| r16 | 1B | 4node | date_histogram 1d + sum(price) | pushed | 8 | 2650 ms | 2744 ms | 1.04 |
| r16 | 1B | 6node | date_histogram 1d + sum(price) | pushed | 8 | 1870 ms | 1862 ms | 1.00 |
| r16 | 1B | 1node16xl | date_histogram 1d + sum(price) | pushed | 32 | 1310 ms | 1998 ms | 1.53 |
| r16 | 1B | 4node | date_histogram 1d + sum(price) | lucene | 8 | 1660 ms | 2161 ms | 1.30 |
| r16 | 1B | 1node4xl | date_histogram month + sum(price) | pushed | 8 | 12.60 s | 10.69 s | 0.85 |
| r16 | 1B | 4node | date_histogram month + sum(price) | pushed | 8 | 3300 ms | 2742 ms | 0.83 |
| r16 | 1B | 6node | date_histogram month + sum(price) | pushed | 8 | 2310 ms | 1859 ms | 0.80 |
| r16 | 1B | 1node16xl | date_histogram month + sum(price) | pushed | 32 | 1900 ms | 1988 ms | 1.05 |
| r16 | 1B | 4node | date_histogram month + sum(price) | lucene | 8 | 2450 ms | 2161 ms | 0.88 |
| r16 | 1B | 1node4xl | range(price 4 band) | pushed | 8 | 4770 ms | 4828 ms | 1.01 |
| r16 | 1B | 4node | range(price 4 band) | pushed | 8 | 1360 ms | 1277 ms | 0.94 |
| r16 | 1B | 6node | range(price 4 band) | pushed | 8 | 955 ms | 882 ms | 0.92 |
| r16 | 1B | 1node16xl | range(price 4 band) | pushed | 32 | 781 ms | 863 ms | 1.10 |
| r16 | 1B | 4node | range(price 4 band) | lucene | 8 | 1110 ms | 1164 ms | 1.05 |
| r16 | 1B | 1node4xl | filters(rating=5, category=cat150, price>=500) | pushed | 8 | 5730 ms | 5942 ms | 1.04 |
| r16 | 1B | 4node | filters(rating=5, category=cat150, price>=500) | pushed | 8 | 1550 ms | 1555 ms | 1.00 |
| r16 | 1B | 6node | filters(rating=5, category=cat150, price>=500) | pushed | 8 | 1120 ms | 1068 ms | 0.95 |
| r16 | 1B | 1node16xl | filters(rating=5, category=cat150, price>=500) | pushed | 32 | 888 ms | 886 ms | 1.00 |
| r16 | 1B | 4node | filters(rating=5, category=cat150, price>=500) | lucene | 8 | 1610 ms | 1839 ms | 1.14 |
| r16 | 1B | 1node4xl | cardinality(user_id) | pushed | 8 | 52.60 s | 30.64 s | 0.58 |
| r16 | 1B | 4node | cardinality(user_id) | pushed | 8 | 22.60 s | 25.13 s | 1.11 |
| r16 | 1B | 6node | cardinality(user_id) | pushed | 8 | 17.60 s | 24.52 s | 1.39 |
| r16 | 1B | 1node16xl | cardinality(user_id) | pushed | 32 | 199.30 s | 93.98 s | 0.47 |
| r16 | 1B | 4node | cardinality(user_id) | lucene | 8 | 4030 ms | 4414 ms | 1.10 |
| r16 | 1B | 1node4xl | percentiles(price) | pushed | 8 | 7070 ms | 7188 ms | 1.02 |
| r16 | 1B | 4node | percentiles(price) | pushed | 8 | 2040 ms | 1867 ms | 0.92 |
| r16 | 1B | 6node | percentiles(price) | pushed | 8 | 1470 ms | 1276 ms | 0.87 |
| r16 | 1B | 1node16xl | percentiles(price) | pushed | 32 | 1020 ms | 1113 ms | 1.09 |
| r16 | 1B | 4node | percentiles(price) | lucene | 8 | 3350 ms | 3383 ms | 1.01 |
| r16 | 1B | 1node4xl | terms(category) > terms(rating) > avg(price) | pushed | 8 | 5970 ms | 6795 ms | 1.14 |
| r16 | 1B | 4node | terms(category) > terms(rating) > avg(price) | pushed | 8 | 1650 ms | 1771 ms | 1.07 |
| r16 | 1B | 6node | terms(category) > terms(rating) > avg(price) | pushed | 8 | 1210 ms | 1213 ms | 1.00 |
| r16 | 1B | 1node16xl | terms(category) > terms(rating) > avg(price) | pushed | 32 | 961 ms | 1113 ms | 1.16 |
| r16 | 1B | 4node | terms(category) > terms(rating) > avg(price) | lucene | 8 | 3050 ms | 3045 ms | 1.00 |
| r16 | 1B | 1node4xl | composite(category, rating) size 10 | pushed | 8 | 3560 ms | 4310 ms | 1.21 |
| r16 | 1B | 4node | composite(category, rating) size 10 | pushed | 8 | 1090 ms | 1150 ms | 1.06 |
| r16 | 1B | 6node | composite(category, rating) size 10 | pushed | 8 | 836 ms | 799 ms | 0.96 |
| r16 | 1B | 1node16xl | composite(category, rating) size 10 | pushed | 32 | 844 ms | 832 ms | 0.99 |
| r16 | 1B | 4node | composite(category, rating) size 10 | lucene | 8 | 1870 ms | 1736 ms | 0.93 |
| r16 | 1B | 1node4xl | filter rating=5 + terms(category) | pushed | 8 | 7030 ms | 7282 ms | 1.04 |
| r16 | 1B | 4node | filter rating=5 + terms(category) | pushed | 8 | 4260 ms | 4441 ms | 1.04 |
| r16 | 1B | 6node | filter rating=5 + terms(category) | pushed | 8 | 3830 ms | 4125 ms | 1.08 |
| r16 | 1B | 1node16xl | filter rating=5 + terms(category) | pushed | 32 | 4770 ms | 4113 ms | 0.86 |
| r16 | 1B | 1node16xl | cardinality(user_id) | lucene | 32 | 4340 ms | 4340 ms | 1.00 |
| r16 | 20M | 1node4xl | cardinality(user_id) | pushed | 8 | 14.10 s | 5912 ms | 0.42 |
| r16 | 20M | 1node4xl | cardinality(user_id) | lucene | 8 | 446 ms | 364 ms | 0.82 |
| r16 | 1B | 1node16xl | percentiles(price) | lucene | 1 | 100.00 s | 105.32 s | 1.05 |
| r16 | 1B | 1node16xl | percentiles(price) | lucene | 32 | 3600 ms | 3309 ms | 0.92 |
| r16 | 1B | 1node16xl | date_histogram 1d + stats(price) | lucene | 1 | 54.40 s | 66.22 s | 1.22 |
| r16 | 1B | 1node16xl | date_histogram 1d + stats(price) | lucene | 32 | 1790 ms | 2087 ms | 1.17 |
| r16 | 1B | 1node16xl | range(price 4 band) | lucene | 1 | 34.20 s | 34.32 s | 1.00 |
| r16 | 1B | 1node16xl | range(price 4 band) | lucene | 32 | 1140 ms | 1090 ms | 0.96 |
| r16 | 1B | 1node16xl | filters(rating=5, category=cat150, price>=500) | lucene | 1 | 60.90 s | 55.92 s | 0.92 |
| r16 | 1B | 1node16xl | filters(rating=5, category=cat150, price>=500) | lucene | 32 | 1990 ms | 1765 ms | 0.89 |
| r17 | 1B | 4node | terms(category) | pushed | 8 | 822 ms | 780 ms | 0.95 |
| r18 | 1B | 4node | terms(category) | lucene | 8 | 363 ms | 351 ms | 0.97 |
| r17 | 1B | 6node | terms(category) | pushed | 8 | 621 ms | 551 ms | 0.89 |
| r18 | 1B | 6node | terms(category) | lucene | 8 | 227 ms | 265 ms | 1.17 |
| r17 | 1B | 4node | terms(user_id) size 10 | pushed | 8 | 4820 ms | 4992 ms | 1.04 |
| r18 | 1B | 4node | terms(user_id) size 10 | lucene | 8 | 22.70 s | 24.10 s | 1.06 |
| r17 | 1B | 6node | terms(user_id) size 10 | pushed | 8 | 3220 ms | 3359 ms | 1.04 |
| r18 | 1B | 6node | terms(user_id) size 10 | lucene | 8 | 15.10 s | 16.10 s | 1.07 |
| r17 | 1B | 4node | date_histogram 1d + sum(price) | pushed | 8 | 2660 ms | 2744 ms | 1.03 |
| r18 | 1B | 4node | date_histogram 1d + sum(price) | lucene | 8 | 1660 ms | 2161 ms | 1.30 |
| r17 | 1B | 6node | date_histogram 1d + sum(price) | pushed | 8 | 1870 ms | 1862 ms | 1.00 |
| r18 | 1B | 6node | date_histogram 1d + sum(price) | lucene | 8 | 1270 ms | 1471 ms | 1.16 |
| r18 | 1B | 6node | cardinality(user_id) | lucene | 8 | 3060 ms | 2973 ms | 0.97 |
| r17 | 1B | 4node | terms(category) > terms(rating) > avg(price) | pushed | 8 | 1690 ms | 1771 ms | 1.05 |
| r18 | 1B | 4node | terms(category) > terms(rating) > avg(price) | lucene | 8 | 2860 ms | 3045 ms | 1.06 |
| r17 | 1B | 6node | terms(category) > terms(rating) > avg(price) | pushed | 8 | 1190 ms | 1213 ms | 1.02 |
| r18 | 1B | 6node | terms(category) > terms(rating) > avg(price) | lucene | 8 | 2210 ms | 2061 ms | 0.93 |
| r17 | 1B | 4node | composite(category, rating) size 10 | pushed | 8 | 1110 ms | 1150 ms | 1.04 |
| r18 | 1B | 4node | composite(category, rating) size 10 | lucene | 8 | 1800 ms | 1736 ms | 0.96 |
| r17 | 1B | 6node | composite(category, rating) size 10 | pushed | 8 | 810 ms | 799 ms | 0.99 |
| r18 | 1B | 6node | composite(category, rating) size 10 | lucene | 8 | 1350 ms | 1188 ms | 0.88 |
| r17 | 1B | 4node | filter rating=5 + terms(category) | lucene | 8 | 5580 ms | 6446 ms | 1.16 |
| r17 | 1B | 6node | filter rating=5 + terms(category) | lucene | 8 | 5270 ms | 4328 ms | 0.82 |
| r18 | 1B | 6node | filter rating=5 + terms(category) | pushed | 8 | 3900 ms | 4125 ms | 1.06 |
| r17 | 20M | 1node4xl | terms(category) | pushed | 8 | 67 ms | 67 ms | 1.01 |
| r18 | 20M | 1node4xl | terms(category) | lucene | 8 | 38 ms | 39 ms | 1.02 |
| r17 | 20M | 1node4xl | terms(category) size 5 + avg(rating) depth_first | pushed | 8 | 70 ms | 79 ms | 1.12 |
| r18 | 20M | 1node4xl | terms(category) size 5 + avg(rating) depth_first | lucene | 8 | 201 ms | 148 ms | 0.74 |
| r17 | 20M | 1node4xl | terms(category) size 5 + avg(rating) | pushed | 8 | 73 ms | 79 ms | 1.08 |
| r18 | 20M | 1node4xl | terms(category) size 5 + avg(rating) | lucene | 8 | 154 ms | 148 ms | 0.96 |
| r17 | 20M | 1node4xl | terms(category) size 3 + sum(id) | pushed | 8 | 69 ms | 90 ms | 1.30 |
| r18 | 20M | 1node4xl | terms(category) size 3 + sum(id) | lucene | 8 | 146 ms | 148 ms | 1.02 |
| r17 | 20M | 1node4xl | terms(rating) > max(id) + terms(category) size 2 | pushed | 8 | 91 ms | 109 ms | 1.20 |
| r18 | 20M | 1node4xl | terms(rating) > max(id) + terms(category) size 2 | lucene | 8 | 240 ms | 254 ms | 1.06 |
| r17 | 20M | 1node4xl | cardinality(category) | lucene | 8 | 35 ms | 39 ms | 1.11 |
| r18 | 20M | 1node4xl | cardinality(category) | pushed | 8 | 71 ms | 73 ms | 1.02 |
| r19 | 1B | 4node | terms(category) > terms(rating) > avg(price) | lucene | 8 | 2860 ms | 3045 ms | 1.06 |
| r19 | 1B | 4node | composite(category, rating) size 10 | lucene | 8 | 1810 ms | 1736 ms | 0.96 |
| r19 | 20M | 1node4xl | terms(rating) > max(id) + terms(category) size 2 | lucene | 8 | 223 ms | 254 ms | 1.14 |
| r17 | 20M | 1node4xl | date_histogram month | pushed | 8 | 148 ms | 154 ms | 1.04 |

Median absolute log ratio 0.069 (a factor of 1.07); 345 of 349 rows within a factor of 2.

## Choices

Every (shape, table, cluster) with both a pushed and a Lucene measurement (excluded rows on either side stay out). Pairs whose measured latencies differ by more than 1.3x are asserted by CostModelTests; pairs inside that band are reported only. A pair is listed once per Lucene slices value measured.

| table | cluster | shape | slices | measured pushed | measured lucene | measured winner | model pushed | model lucene | model winner | verdict |
|---|---|---|---|---|---|---|---|---|---|---|
| 20M | 1node16xl | composite(category, rating) size 10 | 1 | 45 ms | 1060 ms | pushed | 50 ms | 1070 ms | pushed | agrees |
| 20M | 1node16xl | composite(category, rating) size 10 page 2 | 1 | 44 ms | 1130 ms | pushed | 50 ms | 1070 ms | pushed | agrees |
| 20M | 1node16xl | composite(category, ts 1d) size 10 | 1 | 1810 ms | 1050 ms | lucene | 2248 ms | 1070 ms | lucene | agrees |
| 20M | 1node16xl | date_histogram month + sum(price) | 1 | 63 ms | 1460 ms | pushed | 59 ms | 1342 ms | pushed | agrees |
| 20M | 1node16xl | sum(price) | 1 | 23 ms | 255 ms | pushed | 25 ms | 256 ms | pushed | agrees |
| 20M | 1node16xl | terms(category) | 1 | 36 ms | 157 ms | pushed | 33 ms | 184 ms | pushed | agrees |
| 20M | 1node16xl | terms(category) > date_histogram month > sum(price) | 1 | 125 ms | 1850 ms | pushed | 141 ms | 1848 ms | pushed | agrees |
| 20M | 1node16xl | terms(category) > terms(rating) > avg(price) | 1 | 49 ms | 1830 ms | pushed | 55 ms | 1908 ms | pushed | agrees |
| 20M | 1node16xl | terms(rating) | 1 | 24 ms | 530 ms | pushed | 23 ms | 524 ms | pushed | agrees |
| 20M | 1node4xl | cardinality(category) | 8 | 71 ms | 35 ms | lucene | 73 ms | 39 ms | lucene | agrees |
| 20M | 1node4xl | cardinality(user_id) | 8 | 14.10 s | 446 ms | lucene | 5912 ms | 364 ms | lucene | agrees |
| 20M | 1node4xl | composite(category, rating) size 10 | 1 | 84 ms | 1050 ms | pushed | 87 ms | 1070 ms | pushed | agrees |
| 20M | 1node4xl | composite(category, rating) size 10 page 2 | 1 | 83 ms | 1110 ms | pushed | 87 ms | 1070 ms | pushed | agrees |
| 20M | 1node4xl | composite(category, ts 1d) size 10 | 1 | 941 ms | 1030 ms | pushed | 877 ms | 1070 ms | pushed | reported (1.09x) |
| 20M | 1node4xl | date_histogram month + sum(price) | 1 | 161 ms | 1470 ms | pushed | 177 ms | 1342 ms | pushed | agrees |
| 20M | 1node4xl | sum(price) | 1 | 30 ms | 255 ms | pushed | 42 ms | 256 ms | pushed | agrees |
| 20M | 1node4xl | terms(category) | 1 | 67 ms | 204 ms | pushed | 67 ms | 184 ms | pushed | agrees |
| 20M | 1node4xl | terms(category) | 8 | 67 ms | 38 ms | lucene | 67 ms | 39 ms | lucene | agrees |
| 20M | 1node4xl | terms(category) > date_histogram month > sum(price) | 1 | 219 ms | 1850 ms | pushed | 242 ms | 1848 ms | pushed | agrees |
| 20M | 1node4xl | terms(category) > terms(rating) > avg(price) | 1 | 97 ms | 1760 ms | pushed | 109 ms | 1908 ms | pushed | agrees |
| 20M | 1node4xl | terms(category) size 3 + sum(id) | 8 | 69 ms | 146 ms | pushed | 90 ms | 148 ms | pushed | agrees |
| 20M | 1node4xl | terms(category) size 5 + avg(rating) | 8 | 73 ms | 154 ms | pushed | 79 ms | 148 ms | pushed | agrees |
| 20M | 1node4xl | terms(category) size 5 + avg(rating) depth_first | 8 | 70 ms | 201 ms | pushed | 79 ms | 148 ms | pushed | agrees |
| 20M | 1node4xl | terms(rating) | 1 | 37 ms | 526 ms | pushed | 35 ms | 524 ms | pushed | agrees |
| 20M | 1node4xl | terms(rating) > max(id) + terms(category) size 2 | 8 | 91 ms | 240 ms | pushed | 109 ms | 254 ms | pushed | agrees |
| 20M | 1node4xl | terms(rating) > max(id) + terms(category) size 2 | 8 | 91 ms | 223 ms | pushed | 109 ms | 254 ms | pushed | agrees |
| 20M | 3node | composite(category, rating) size 10 | 1 | 46 ms | 360 ms | pushed | 44 ms | 369 ms | pushed | agrees |
| 20M | 3node | composite(category, rating) size 10 page 2 | 1 | 46 ms | 388 ms | pushed | 44 ms | 369 ms | pushed | agrees |
| 20M | 3node | composite(category, ts 1d) size 10 | 1 | 862 ms | 383 ms | lucene | 663 ms | 369 ms | lucene | agrees |
| 20M | 3node | date_histogram month + sum(price) | 1 | 80 ms | 503 ms | pushed | 72 ms | 459 ms | pushed | agrees |
| 20M | 3node | sum(price) | 1 | 23 ms | 96 ms | pushed | 27 ms | 97 ms | pushed | agrees |
| 20M | 3node | terms(category) | 1 | 38 ms | 81 ms | pushed | 35 ms | 73 ms | pushed | agrees |
| 20M | 3node | terms(category) > date_histogram month > sum(price) | 1 | 113 ms | 634 ms | pushed | 105 ms | 628 ms | pushed | agrees |
| 20M | 3node | terms(category) > terms(rating) > avg(price) | 1 | 48 ms | 632 ms | pushed | 51 ms | 648 ms | pushed | agrees |
| 20M | 3node | terms(rating) | 1 | 26 ms | 188 ms | pushed | 24 ms | 187 ms | pushed | agrees |
| 20M | 6node | composite(category, rating) size 10 | 1 | 32 ms | 197 ms | pushed | 33 ms | 193 ms | pushed | agrees |
| 20M | 6node | composite(category, rating) size 10 page 2 | 1 | 33 ms | 206 ms | pushed | 33 ms | 193 ms | pushed | agrees |
| 20M | 6node | composite(category, ts 1d) size 10 | 1 | 660 ms | 194 ms | lucene | 610 ms | 193 ms | lucene | agrees |
| 20M | 6node | date_histogram month + sum(price) | 1 | 51 ms | 270 ms | pushed | 45 ms | 239 ms | pushed | agrees |
| 20M | 6node | sum(price) | 1 | 18 ms | 57 ms | pushed | 23 ms | 58 ms | pushed | agrees |
| 20M | 6node | terms(category) | 1 | 27 ms | 48 ms | pushed | 27 ms | 46 ms | pushed | agrees |
| 20M | 6node | terms(category) > date_histogram month > sum(price) | 1 | 77 ms | 344 ms | pushed | 71 ms | 323 ms | pushed | agrees |
| 20M | 6node | terms(category) > terms(rating) > avg(price) | 1 | 37 ms | 335 ms | pushed | 37 ms | 333 ms | pushed | agrees |
| 20M | 6node | terms(rating) | 1 | 20 ms | 103 ms | pushed | 22 ms | 102 ms | pushed | agrees |
| 100M | 1node16xl | composite(category, rating) size 10 | 1 | 165 ms | 5390 ms | pushed | 114 ms | 5278 ms | pushed | agrees |
| 100M | 1node16xl | composite(category, rating) size 10 page 2 | 1 | 166 ms | 5630 ms | pushed | 114 ms | 5278 ms | pushed | agrees |
| 100M | 1node16xl | composite(category, ts 1d) size 10 | 1 | 2580 ms | 5180 ms | pushed | 2569 ms | 5278 ms | pushed | agrees |
| 100M | 1node16xl | date_histogram month + sum(price) | 1 | 260 ms | 7270 ms | pushed | 216 ms | 6638 ms | pushed | agrees |
| 100M | 1node16xl | sum(price) | 1 | 94 ms | 1260 ms | pushed | 47 ms | 1208 ms | pushed | agrees |
| 100M | 1node16xl | terms(category) | 1 | 136 ms | 775 ms | pushed | 81 ms | 848 ms | pushed | agrees |
| 100M | 1node16xl | terms(category) > date_histogram month > sum(price) | 1 | 375 ms | 9300 ms | pushed | 347 ms | 9168 ms | pushed | agrees |
| 100M | 1node16xl | terms(category) > terms(rating) > avg(price) | 1 | 181 ms | 9270 ms | pushed | 142 ms | 9468 ms | pushed | agrees |
| 100M | 1node16xl | terms(rating) | 1 | 91 ms | 2630 ms | pushed | 39 ms | 2548 ms | pushed | agrees |
| 100M | 1node4xl | composite(category, rating) size 10 | 1 | 373 ms | 5300 ms | pushed | 342 ms | 5278 ms | pushed | agrees |
| 100M | 1node4xl | composite(category, rating) size 10 page 2 | 1 | 378 ms | 5400 ms | pushed | 342 ms | 5278 ms | pushed | agrees |
| 100M | 1node4xl | composite(category, ts 1d) size 10 | 1 | 2000 ms | 5080 ms | pushed | 2159 ms | 5278 ms | pushed | agrees |
| 100M | 1node4xl | date_histogram month + sum(price) | 1 | 763 ms | 7390 ms | pushed | 807 ms | 6638 ms | pushed | agrees |
| 100M | 1node4xl | sum(price) | 1 | 118 ms | 1260 ms | pushed | 132 ms | 1208 ms | pushed | agrees |
| 100M | 1node4xl | terms(category) | 1 | 307 ms | 1010 ms | pushed | 260 ms | 848 ms | pushed | agrees |
| 100M | 1node4xl | terms(category) > date_histogram month > sum(price) | 1 | 990 ms | 9320 ms | pushed | 1065 ms | 9168 ms | pushed | agrees |
| 100M | 1node4xl | terms(category) > terms(rating) > avg(price) | 1 | 424 ms | 9280 ms | pushed | 455 ms | 9468 ms | pushed | agrees |
| 100M | 1node4xl | terms(rating) | 1 | 162 ms | 2630 ms | pushed | 98 ms | 2548 ms | pushed | agrees |
| 100M | 3node | composite(category, rating) size 10 | 1 | 148 ms | 1920 ms | pushed | 129 ms | 1771 ms | pushed | agrees |
| 100M | 3node | composite(category, rating) size 10 page 2 | 1 | 146 ms | 1920 ms | pushed | 129 ms | 1771 ms | pushed | agrees |
| 100M | 3node | composite(category, ts 1d) size 10 | 1 | 1310 ms | 1880 ms | pushed | 1091 ms | 1771 ms | pushed | agrees |
| 100M | 3node | date_histogram month + sum(price) | 1 | 283 ms | 2500 ms | pushed | 282 ms | 2225 ms | pushed | agrees |
| 100M | 3node | sum(price) | 1 | 58 ms | 439 ms | pushed | 57 ms | 415 ms | pushed | agrees |
| 100M | 3node | terms(category) | 1 | 126 ms | 354 ms | pushed | 100 ms | 295 ms | pushed | agrees |
| 100M | 3node | terms(category) > date_histogram month > sum(price) | 1 | 372 ms | 3160 ms | pushed | 379 ms | 3068 ms | pushed | agrees |
| 100M | 3node | terms(category) > terms(rating) > avg(price) | 1 | 167 ms | 3120 ms | pushed | 167 ms | 3168 ms | pushed | agrees |
| 100M | 3node | terms(rating) | 1 | 72 ms | 888 ms | pushed | 45 ms | 861 ms | pushed | agrees |
| 100M | 6node | composite(category, rating) size 10 | 1 | 93 ms | 938 ms | pushed | 76 ms | 895 ms | pushed | agrees |
| 100M | 6node | composite(category, rating) size 10 page 2 | 1 | 93 ms | 964 ms | pushed | 76 ms | 895 ms | pushed | agrees |
| 100M | 6node | composite(category, ts 1d) size 10 | 1 | 1030 ms | 931 ms | lucene | 823 ms | 895 ms | pushed | reported (1.11x) |
| 100M | 6node | date_histogram month + sum(price) | 1 | 165 ms | 1260 ms | pushed | 150 ms | 1121 ms | pushed | agrees |
| 100M | 6node | sum(price) | 1 | 41 ms | 234 ms | pushed | 38 ms | 216 ms | pushed | agrees |
| 100M | 6node | terms(category) | 1 | 77 ms | 190 ms | pushed | 59 ms | 156 ms | pushed | agrees |
| 100M | 6node | terms(category) > date_histogram month > sum(price) | 1 | 220 ms | 1630 ms | pushed | 208 ms | 1543 ms | pushed | agrees |
| 100M | 6node | terms(category) > terms(rating) > avg(price) | 1 | 104 ms | 1600 ms | pushed | 95 ms | 1593 ms | pushed | agrees |
| 100M | 6node | terms(rating) | 1 | 48 ms | 457 ms | pushed | 32 ms | 440 ms | pushed | agrees |
| 1B | 1node16xl | cardinality(user_id) | 32 | 199.30 s | 4340 ms | lucene | 93.98 s | 4340 ms | lucene | agrees |
| 1B | 1node16xl | composite(category, rating) size 10 | 1 | 844 ms | 52.22 s | pushed | 832 ms | 52.62 s | pushed | agrees |
| 1B | 1node16xl | composite(category, rating) size 10 page 2 | 1 | 864 ms | 54.32 s | pushed | 832 ms | 52.62 s | pushed | agrees |
| 1B | 1node16xl | composite(category, ts 1d) size 10 | 1 | 6390 ms | 49.39 s | pushed | 6176 ms | 52.62 s | pushed | agrees |
| 1B | 1node16xl | date_histogram month + sum(price) | 1 | 1900 ms | 72.58 s | pushed | 1988 ms | 66.22 s | pushed | agrees |
| 1B | 1node16xl | filters(rating=5, category=cat150, price>=500) | 1 | 888 ms | 60.90 s | pushed | 886 ms | 55.92 s | pushed | agrees |
| 1B | 1node16xl | filters(rating=5, category=cat150, price>=500) | 32 | 888 ms | 1990 ms | pushed | 886 ms | 1765 ms | pushed | agrees |
| 1B | 1node16xl | percentiles(price) | 1 | 1020 ms | 100.00 s | pushed | 1113 ms | 105.32 s | pushed | agrees |
| 1B | 1node16xl | percentiles(price) | 32 | 1020 ms | 3600 ms | pushed | 1113 ms | 3309 ms | pushed | agrees |
| 1B | 1node16xl | range(price 4 band) | 1 | 781 ms | 34.20 s | pushed | 863 ms | 34.32 s | pushed | agrees |
| 1B | 1node16xl | range(price 4 band) | 32 | 781 ms | 1140 ms | pushed | 863 ms | 1090 ms | pushed | agrees |
| 1B | 1node16xl | sum(price) | 1 | 321 ms | 11.75 s | pushed | 300 ms | 11.92 s | pushed | agrees |
| 1B | 1node16xl | terms(category) | 1 | 726 ms | 8860 ms | pushed | 622 ms | 8318 ms | pushed | agrees |
| 1B | 1node16xl | terms(category) > date_histogram month > sum(price) | 1 | 2480 ms | 90.17 s | pushed | 2660 ms | 91.52 s | pushed | agrees |
| 1B | 1node16xl | terms(category) > terms(rating) > avg(price) | 1 | 961 ms | 89.13 s | pushed | 1113 ms | 94.52 s | pushed | agrees |
| 1B | 1node16xl | terms(rating) | 1 | 263 ms | 24.17 s | pushed | 216 ms | 25.32 s | pushed | agrees |
| 1B | 1node16xl | terms(user_id) size 10 | 1 | 5290 ms | 140.89 s | pushed | 4239 ms | 103.32 s | pushed | agrees |
| 1B | 1node4xl | composite(category, rating) size 10 | 1 | 3560 ms | 51.08 s | pushed | 4310 ms | 52.69 s | pushed | agrees |
| 1B | 1node4xl | composite(category, rating) size 10 page 2 | 1 | 3580 ms | 51.69 s | pushed | 4310 ms | 52.69 s | pushed | agrees |
| 1B | 1node4xl | composite(category, ts 1d) size 10 | 1 | 16.46 s | 48.97 s | pushed | 18.36 s | 52.69 s | pushed | agrees |
| 1B | 1node4xl | date_histogram month + sum(price) | 1 | 12.60 s | 89.24 s | pushed | 10.69 s | 66.29 s | pushed | agrees |
| 1B | 1node4xl | sum(price) | 1 | 2640 ms | 12.26 s | pushed | 2578 ms | 11.99 s | pushed | agrees |
| 1B | 1node4xl | terms(category) | 1 | 2730 ms | 6950 ms | pushed | 2840 ms | 8392 ms | pushed | agrees |
| 1B | 1node4xl | terms(category) > date_histogram month > sum(price) | 1 | 15.15 s | 108.12 s | pushed | 13.45 s | 91.59 s | pushed | agrees |
| 1B | 1node4xl | terms(category) > terms(rating) > avg(price) | 1 | 5970 ms | 102.98 s | pushed | 6795 ms | 94.59 s | pushed | agrees |
| 1B | 1node4xl | terms(rating) | 1 | 1100 ms | 25.13 s | pushed | 1561 ms | 25.39 s | pushed | agrees |
| 1B | 3node | composite(category, rating) size 10 | 1 | 1320 ms | 17.48 s | pushed | 1501 ms | 17.63 s | pushed | agrees |
| 1B | 3node | composite(category, rating) size 10 page 2 | 1 | 1380 ms | 17.60 s | pushed | 1501 ms | 17.63 s | pushed | agrees |
| 1B | 3node | composite(category, ts 1d) size 10 | 1 | 6750 ms | 16.58 s | pushed | 6541 ms | 17.63 s | pushed | agrees |
| 1B | 3node | date_histogram month + sum(price) | 1 | 4440 ms | 24.72 s | pushed | 3625 ms | 22.16 s | pushed | agrees |
| 1B | 3node | sum(price) | 1 | 959 ms | 3990 ms | pushed | 921 ms | 4059 ms | pushed | agrees |
| 1B | 3node | terms(category) | 1 | 1030 ms | 3010 ms | pushed | 1009 ms | 2859 ms | pushed | agrees |
| 1B | 3node | terms(category) > date_histogram month > sum(price) | 1 | 5260 ms | 30.99 s | pushed | 4558 ms | 30.59 s | pushed | agrees |
| 1B | 3node | terms(category) > terms(rating) > avg(price) | 1 | 2170 ms | 30.76 s | pushed | 2330 ms | 31.59 s | pushed | agrees |
| 1B | 3node | terms(rating) | 1 | 531 ms | 8330 ms | pushed | 582 ms | 8525 ms | pushed | agrees |
| 1B | 4node | avg(price) | 8 | 793 ms | 448 ms | lucene | 714 ms | 464 ms | lucene | agrees |
| 1B | 4node | cardinality(user_id) | 8 | 22.60 s | 4030 ms | lucene | 25.13 s | 4414 ms | lucene | agrees |
| 1B | 4node | composite(category, rating) size 10 | 8 | 1110 ms | 1870 ms | pushed | 1150 ms | 1736 ms | pushed | agrees |
| 1B | 4node | composite(category, rating) size 10 | 8 | 1110 ms | 1800 ms | pushed | 1150 ms | 1736 ms | pushed | agrees |
| 1B | 4node | composite(category, rating) size 10 | 8 | 1110 ms | 1810 ms | pushed | 1150 ms | 1736 ms | pushed | agrees |
| 1B | 4node | date_histogram 1d + sum(price) | 8 | 2660 ms | 1660 ms | lucene | 2744 ms | 2161 ms | lucene | agrees |
| 1B | 4node | date_histogram 1d + sum(price) | 8 | 2660 ms | 1660 ms | lucene | 2744 ms | 2161 ms | lucene | agrees |
| 1B | 4node | date_histogram month + sum(price) | 8 | 3300 ms | 2450 ms | lucene | 2742 ms | 2161 ms | lucene | agrees |
| 1B | 4node | extended_stats(price) | 8 | 826 ms | 1050 ms | pushed | 742 ms | 1058 ms | pushed | reported (1.27x) |
| 1B | 4node | filter rating=5 + terms(category) | 8 | 4260 ms | 5580 ms | pushed | 4441 ms | 6446 ms | pushed | agrees |
| 1B | 4node | filters(rating=5, category=cat150, price>=500) | 8 | 1550 ms | 1610 ms | pushed | 1555 ms | 1839 ms | pushed | reported (1.04x) |
| 1B | 4node | percentiles(price) | 8 | 2040 ms | 3350 ms | pushed | 1867 ms | 3383 ms | pushed | agrees |
| 1B | 4node | range(price 4 band) | 8 | 1360 ms | 1110 ms | lucene | 1277 ms | 1164 ms | lucene | reported (1.23x) |
| 1B | 4node | stats(price) | 8 | 816 ms | 475 ms | lucene | 714 ms | 464 ms | lucene | agrees |
| 1B | 4node | sum(price) | 8 | 784 ms | 442 ms | lucene | 714 ms | 464 ms | lucene | agrees |
| 1B | 4node | terms(category) | 8 | 822 ms | 346 ms | lucene | 780 ms | 351 ms | lucene | agrees |
| 1B | 4node | terms(category) | 8 | 822 ms | 363 ms | lucene | 780 ms | 351 ms | lucene | agrees |
| 1B | 4node | terms(category) > terms(rating) > avg(price) | 8 | 1690 ms | 3050 ms | pushed | 1771 ms | 3045 ms | pushed | agrees |
| 1B | 4node | terms(category) > terms(rating) > avg(price) | 8 | 1690 ms | 2860 ms | pushed | 1771 ms | 3045 ms | pushed | agrees |
| 1B | 4node | terms(category) > terms(rating) > avg(price) | 8 | 1690 ms | 2860 ms | pushed | 1771 ms | 3045 ms | pushed | agrees |
| 1B | 4node | terms(rating) | 8 | 346 ms | 780 ms | pushed | 460 ms | 883 ms | pushed | agrees |
| 1B | 4node | terms(user_id) size 10 | 8 | 4820 ms | 17.80 s | pushed | 4992 ms | 24.10 s | pushed | agrees |
| 1B | 4node | terms(user_id) size 10 | 8 | 4820 ms | 22.70 s | pushed | 4992 ms | 24.10 s | pushed | agrees |
| 1B | 6node | cardinality(user_id) | 8 | 17.60 s | 3060 ms | lucene | 24.52 s | 2973 ms | lucene | agrees |
| 1B | 6node | composite(category, rating) size 10 | 1 | 810 ms | 8800 ms | pushed | 799 ms | 8859 ms | pushed | agrees |
| 1B | 6node | composite(category, rating) size 10 | 8 | 810 ms | 1350 ms | pushed | 799 ms | 1188 ms | pushed | agrees |
| 1B | 6node | composite(category, rating) size 10 page 2 | 1 | 848 ms | 8920 ms | pushed | 799 ms | 8859 ms | pushed | agrees |
| 1B | 6node | composite(category, ts 1d) size 10 | 1 | 3870 ms | 8440 ms | pushed | 3585 ms | 8859 ms | pushed | agrees |
| 1B | 6node | date_histogram 1d + sum(price) | 8 | 1870 ms | 1270 ms | lucene | 1862 ms | 1471 ms | lucene | agrees |
| 1B | 6node | date_histogram month + sum(price) | 1 | 2310 ms | 12.27 s | pushed | 1859 ms | 11.13 s | pushed | agrees |
| 1B | 6node | filter rating=5 + terms(category) | 8 | 3900 ms | 5270 ms | pushed | 4125 ms | 4328 ms | pushed | agrees |
| 1B | 6node | sum(price) | 1 | 586 ms | 2030 ms | pushed | 507 ms | 2075 ms | pushed | agrees |
| 1B | 6node | terms(category) | 1 | 621 ms | 1530 ms | pushed | 551 ms | 1475 ms | pushed | agrees |
| 1B | 6node | terms(category) | 8 | 621 ms | 227 ms | lucene | 551 ms | 265 ms | lucene | agrees |
| 1B | 6node | terms(category) > date_histogram month > sum(price) | 1 | 2810 ms | 15.54 s | pushed | 2334 ms | 15.34 s | pushed | agrees |
| 1B | 6node | terms(category) > terms(rating) > avg(price) | 1 | 1190 ms | 15.44 s | pushed | 1213 ms | 15.84 s | pushed | agrees |
| 1B | 6node | terms(category) > terms(rating) > avg(price) | 8 | 1190 ms | 2210 ms | pushed | 1213 ms | 2061 ms | pushed | agrees |
| 1B | 6node | terms(rating) | 1 | 386 ms | 4210 ms | pushed | 338 ms | 4309 ms | pushed | agrees |
| 1B | 6node | terms(user_id) size 10 | 1 | 3220 ms | 28.64 s | pushed | 3359 ms | 17.31 s | pushed | agrees |
| 1B | 6node | terms(user_id) size 10 | 8 | 3220 ms | 15.10 s | pushed | 3359 ms | 16.10 s | pushed | agrees |

150 pairs agree, 0 disagree, 5 inside the 1.3x band.
