# Cost coefficient fit

Fitted on 310 measured rows (4 rows excluded, see the note column) by non negative least squares on residuals scaled by 1 / measured latency (relative error). Coefficients are rounded to two significant digits; the rounded values are what CostCoefficients.java carries and what the residual and choice tables below use.

## Coefficients

| coefficient | fitted | rounded | unit |
|---|---|---|---|
| PUSHED_FIXED_MS | 19.18 | 19 | ms per request |
| OBJECT_STORE_OPEN_MS | 89.62 | 90 | ms per request |
| OBJECT_STORE_READ_MS_PER_GB_PER_NODE | 156.7 | 160 | ms per GB of column bytes one node reads from the object store |
| PUSHED_DECODE_MS_PER_MROW_THREAD_PER_8_BYTES | 9.178 | 9.2 | ms per million rows per thread per 8 bytes of row width |
| PUSHED_STRING_KEY_MS_PER_MROW_THREAD | 17.85 | 18 | ms per million rows one thread processes |
| PUSHED_NUMERIC_KEY_MS_PER_MROW_THREAD | 1.588 | 1.6 | ms per million rows one thread processes |
| PUSHED_DATE_KEY_MS_PER_MROW_THREAD | 44.53 | 45 | ms per million rows one thread processes |
| PUSHED_RANGE_KEY_MS_PER_MROW_THREAD | 17.6 | 18 | ms per million rows one thread processes |
| PUSHED_FILTERS_KEY_MS_PER_MROW_THREAD | 12.24 | 12 | ms per million rows one thread processes |
| PUSHED_COMPOSITE_DATE_KEY_MS_PER_MROW_THREAD | 102.8 | 100 | ms per million rows one thread processes |
| PUSHED_EXTENDED_STATS_MS_PER_MROW_THREAD | 0.7009 | 0.7 | ms per million rows one thread processes |
| PUSHED_PERCENTILES_MS_PER_MROW_THREAD | 17.27 | 17 | ms per million rows one thread processes |
| PUSHED_LARGE_GROUPS_MS_PER_MROW_THREAD | 136.6 | 140 | ms per million rows one thread processes |
| PUSHED_FILTER_EVAL_MS_PER_MROW_THREAD | 65.15 | 65 | ms per million rows one thread processes |
| PUSHED_MERGE_MS_PER_MGROUP | 457.9 | 460 | ms per million group rows merged |
| PUSHED_CARDINALITY_MS_PER_MVALUE | 307 | 310 | ms per million distinct values fed to the sketch |
| LUCENE_FIXED_MS | 21.02 | 21 | ms per request |
| LUCENE_COLUMN_MS_PER_MROW_THREAD | 8.282 | 8.3 | ms per million rows one thread processes |
| LUCENE_NUMERIC_KEY_MS_PER_MROW_THREAD | 17.51 | 18 | ms per million rows one thread processes |
| LUCENE_DATE_KEY_MS_PER_MROW_THREAD | 42.56 | 43 | ms per million rows one thread processes |
| LUCENE_RANGE_KEY_MS_PER_MROW_THREAD | 25.68 | 26 | ms per million rows one thread processes |
| LUCENE_FILTERS_KEY_MS_PER_MROW_THREAD | 31.18 | 31 | ms per million rows one thread processes |
| LUCENE_COMPOSITE_SOURCE_MS_PER_MROW_THREAD | 18.13 | 18 | ms per million rows one thread processes |
| LUCENE_NESTED_LEVEL_MS_PER_MROW_THREAD | 35.29 | 35 | ms per million rows one thread processes |
| LUCENE_SIMPLE_METRIC_MS_PER_MROW_THREAD | 3.67 | 3.7 | ms per million rows one thread processes |
| LUCENE_EXTENDED_STATS_MS_PER_MROW_THREAD | 18.11 | 18 | ms per million rows one thread processes |
| LUCENE_PERCENTILES_MS_PER_MROW_THREAD | 97.1 | 97 | ms per million rows one thread processes |
| LUCENE_CARDINALITY_MS_PER_MROW_THREAD | 131.5 | 130 | ms per million rows one thread processes |
| LUCENE_LARGE_GROUPS_MS_PER_MROW_THREAD | 150.6 | 150 | ms per million rows one thread processes |
| LUCENE_FILTER_EVAL_MS_PER_MROW_THREAD | 501.9 | 500 | ms per million rows one thread processes |

## Residuals

Per fitted row: predicted / measured with the rounded coefficients. Rows are in CSV order.

| round | table | cluster | shape | path | slices | measured | predicted | pred / meas |
|---|---|---|---|---|---|---|---|---|
| r15 | 20M | 1node4xl | terms(category) | pushed | 8 | 69 ms | 70 ms | 1.02 |
| r15 | 20M | 1node4xl | terms(category) | lucene | 1 | 204 ms | 187 ms | 0.92 |
| r15 | 20M | 1node4xl | terms(rating) | pushed | 8 | 37 ms | 35 ms | 0.93 |
| r15 | 20M | 1node4xl | terms(rating) | lucene | 1 | 526 ms | 547 ms | 1.04 |
| r15 | 20M | 1node4xl | sum(price) | pushed | 8 | 30 ms | 42 ms | 1.40 |
| r15 | 20M | 1node4xl | sum(price) | lucene | 1 | 255 ms | 261 ms | 1.02 |
| r15 | 20M | 1node4xl | date_histogram month + sum(price) | pushed | 8 | 161 ms | 178 ms | 1.10 |
| r15 | 20M | 1node4xl | date_histogram month + sum(price) | lucene | 1 | 1470 ms | 1287 ms | 0.88 |
| r15 | 20M | 1node16xl | terms(category) | pushed | 32 | 36 ms | 33 ms | 0.92 |
| r15 | 20M | 1node16xl | terms(category) | lucene | 1 | 157 ms | 187 ms | 1.19 |
| r15 | 20M | 1node16xl | terms(rating) | pushed | 32 | 24 ms | 23 ms | 0.96 |
| r15 | 20M | 1node16xl | terms(rating) | lucene | 1 | 530 ms | 547 ms | 1.03 |
| r15 | 20M | 1node16xl | sum(price) | pushed | 32 | 23 ms | 25 ms | 1.08 |
| r15 | 20M | 1node16xl | sum(price) | lucene | 1 | 255 ms | 261 ms | 1.02 |
| r15 | 20M | 1node16xl | date_histogram month + sum(price) | pushed | 32 | 63 ms | 59 ms | 0.94 |
| r15 | 20M | 1node16xl | date_histogram month + sum(price) | lucene | 1 | 1460 ms | 1287 ms | 0.88 |
| r15 | 20M | 3node | terms(category) | pushed | 8 | 38 ms | 36 ms | 0.95 |
| r15 | 20M | 3node | terms(category) | lucene | 1 | 81 ms | 76 ms | 0.94 |
| r15 | 20M | 3node | terms(rating) | pushed | 8 | 26 ms | 24 ms | 0.93 |
| r15 | 20M | 3node | terms(rating) | lucene | 1 | 188 ms | 196 ms | 1.04 |
| r15 | 20M | 3node | sum(price) | pushed | 8 | 23 ms | 27 ms | 1.16 |
| r15 | 20M | 3node | sum(price) | lucene | 1 | 96 ms | 101 ms | 1.05 |
| r15 | 20M | 3node | date_histogram month + sum(price) | pushed | 8 | 80 ms | 72 ms | 0.90 |
| r15 | 20M | 3node | date_histogram month + sum(price) | lucene | 1 | 503 ms | 443 ms | 0.88 |
| r15 | 20M | 6node | terms(category) | pushed | 8 | 27 ms | 28 ms | 1.03 |
| r15 | 20M | 6node | terms(category) | lucene | 1 | 48 ms | 49 ms | 1.01 |
| r15 | 20M | 6node | terms(rating) | pushed | 8 | 20 ms | 22 ms | 1.08 |
| r15 | 20M | 6node | terms(rating) | lucene | 1 | 103 ms | 109 ms | 1.06 |
| r15 | 20M | 6node | sum(price) | pushed | 8 | 18 ms | 23 ms | 1.27 |
| r15 | 20M | 6node | sum(price) | lucene | 1 | 57 ms | 61 ms | 1.07 |
| r15 | 20M | 6node | date_histogram month + sum(price) | pushed | 8 | 51 ms | 46 ms | 0.89 |
| r15 | 20M | 6node | date_histogram month + sum(price) | lucene | 1 | 270 ms | 232 ms | 0.86 |
| r15 | 100M | 1node4xl | terms(category) | pushed | 8 | 307 ms | 273 ms | 0.89 |
| r15 | 100M | 1node4xl | terms(category) | lucene | 1 | 1010 ms | 851 ms | 0.84 |
| r15 | 100M | 1node4xl | terms(rating) | pushed | 8 | 162 ms | 97 ms | 0.60 |
| r15 | 100M | 1node4xl | terms(rating) | lucene | 1 | 2630 ms | 2651 ms | 1.01 |
| r15 | 100M | 1node4xl | sum(price) | pushed | 8 | 118 ms | 134 ms | 1.14 |
| r15 | 100M | 1node4xl | sum(price) | lucene | 1 | 1260 ms | 1221 ms | 0.97 |
| r15 | 100M | 1node4xl | date_histogram month + sum(price) | pushed | 8 | 763 ms | 812 ms | 1.06 |
| r15 | 100M | 1node4xl | date_histogram month + sum(price) | lucene | 1 | 7390 ms | 6351 ms | 0.86 |
| r15 | 100M | 1node16xl | terms(category) | pushed | 32 | 136 ms | 84 ms | 0.62 |
| r15 | 100M | 1node16xl | terms(category) | lucene | 1 | 775 ms | 851 ms | 1.10 |
| r15 | 100M | 1node16xl | terms(rating) | pushed | 32 | 91 ms | 38 ms | 0.42 |
| r15 | 100M | 1node16xl | terms(rating) | lucene | 1 | 2630 ms | 2651 ms | 1.01 |
| r15 | 100M | 1node16xl | sum(price) | pushed | 32 | 94 ms | 48 ms | 0.51 |
| r15 | 100M | 1node16xl | sum(price) | lucene | 1 | 1260 ms | 1221 ms | 0.97 |
| r15 | 100M | 1node16xl | date_histogram month + sum(price) | pushed | 32 | 260 ms | 217 ms | 0.84 |
| r15 | 100M | 1node16xl | date_histogram month + sum(price) | lucene | 1 | 7270 ms | 6351 ms | 0.87 |
| r15 | 100M | 3node | terms(category) | pushed | 8 | 126 ms | 104 ms | 0.83 |
| r15 | 100M | 3node | terms(category) | lucene | 1 | 354 ms | 298 ms | 0.84 |
| r15 | 100M | 3node | terms(rating) | pushed | 8 | 72 ms | 45 ms | 0.62 |
| r15 | 100M | 3node | terms(rating) | lucene | 1 | 888 ms | 898 ms | 1.01 |
| r15 | 100M | 3node | sum(price) | pushed | 8 | 58 ms | 57 ms | 0.99 |
| r15 | 100M | 3node | sum(price) | lucene | 1 | 439 ms | 421 ms | 0.96 |
| r15 | 100M | 3node | date_histogram month + sum(price) | pushed | 8 | 283 ms | 283 ms | 1.00 |
| r15 | 100M | 3node | date_histogram month + sum(price) | lucene | 1 | 2500 ms | 2131 ms | 0.85 |
| r15 | 100M | 6node | terms(category) | pushed | 8 | 77 ms | 62 ms | 0.80 |
| r15 | 100M | 6node | terms(category) | lucene | 1 | 190 ms | 159 ms | 0.84 |
| r15 | 100M | 6node | terms(rating) | pushed | 8 | 48 ms | 32 ms | 0.67 |
| r15 | 100M | 6node | terms(rating) | lucene | 1 | 457 ms | 459 ms | 1.01 |
| r15 | 100M | 6node | sum(price) | pushed | 8 | 41 ms | 38 ms | 0.93 |
| r15 | 100M | 6node | sum(price) | lucene | 1 | 234 ms | 221 ms | 0.94 |
| r15 | 100M | 6node | date_histogram month + sum(price) | pushed | 8 | 165 ms | 151 ms | 0.92 |
| r15 | 100M | 6node | date_histogram month + sum(price) | lucene | 1 | 1260 ms | 1076 ms | 0.85 |
| r15 | 1B | 1node4xl | terms(category) | pushed | 8 | 2750 ms | 2967 ms | 1.08 |
| r15 | 1B | 1node4xl | terms(category) | lucene | 1 | 6950 ms | 8411 ms | 1.21 |
| r15 | 1B | 1node4xl | terms(rating) | pushed | 8 | 1110 ms | 1524 ms | 1.37 |
| r15 | 1B | 1node4xl | terms(rating) | lucene | 1 | 25.13 s | 26.41 s | 1.05 |
| r15 | 1B | 1node4xl | sum(price) | pushed | 8 | 2530 ms | 2539 ms | 1.00 |
| r15 | 1B | 1node4xl | sum(price) | lucene | 1 | 12.26 s | 12.11 s | 0.99 |
| r15 | 1B | 1node4xl | date_histogram month + sum(price) | pushed | 8 | 12.43 s | 10.59 s | 0.85 |
| r15 | 1B | 1node4xl | date_histogram month + sum(price) | lucene | 1 | 89.24 s | 63.41 s | 0.71 |
| r15 | 1B | 1node16xl | terms(category) | pushed | 32 | 716 ms | 655 ms | 0.91 |
| r15 | 1B | 1node16xl | terms(category) | lucene | 1 | 8860 ms | 8321 ms | 0.94 |
| r15 | 1B | 1node16xl | terms(rating) | pushed | 32 | 282 ms | 213 ms | 0.75 |
| r15 | 1B | 1node16xl | terms(rating) | lucene | 1 | 24.17 s | 26.32 s | 1.09 |
| r15 | 1B | 1node16xl | sum(price) | pushed | 32 | 337 ms | 307 ms | 0.91 |
| r15 | 1B | 1node16xl | sum(price) | lucene | 1 | 11.75 s | 12.02 s | 1.02 |
| r15 | 1B | 1node16xl | date_histogram month + sum(price) | pushed | 32 | 1890 ms | 2001 ms | 1.06 |
| r15 | 1B | 1node16xl | date_histogram month + sum(price) | lucene | 1 | 72.58 s | 63.32 s | 0.87 |
| r15 | 1B | 3node | terms(category) | pushed | 8 | 1030 ms | 1062 ms | 1.03 |
| r15 | 1B | 3node | terms(category) | lucene | 1 | 3010 ms | 2878 ms | 0.96 |
| r15 | 1B | 3node | terms(rating) | pushed | 8 | 531 ms | 581 ms | 1.09 |
| r15 | 1B | 3node | terms(rating) | lucene | 1 | 8330 ms | 8878 ms | 1.07 |
| r15 | 1B | 3node | sum(price) | pushed | 8 | 959 ms | 919 ms | 0.96 |
| r15 | 1B | 3node | sum(price) | lucene | 1 | 3990 ms | 4111 ms | 1.03 |
| r15 | 1B | 3node | date_histogram month + sum(price) | pushed | 8 | 4440 ms | 3604 ms | 0.81 |
| r15 | 1B | 3node | date_histogram month + sum(price) | lucene | 1 | 24.72 s | 21.21 s | 0.86 |
| r15 | 1B | 6node | terms(category) | pushed | 8 | 598 ms | 586 ms | 0.98 |
| r15 | 1B | 6node | terms(category) | lucene | 1 | 1530 ms | 1494 ms | 0.98 |
| r15 | 1B | 6node | terms(rating) | pushed | 8 | 356 ms | 345 ms | 0.97 |
| r15 | 1B | 6node | terms(rating) | lucene | 1 | 4210 ms | 4494 ms | 1.07 |
| r15 | 1B | 6node | sum(price) | pushed | 8 | 589 ms | 514 ms | 0.87 |
| r15 | 1B | 6node | sum(price) | lucene | 1 | 2030 ms | 2111 ms | 1.04 |
| r15 | 1B | 6node | date_histogram month + sum(price) | pushed | 8 | 2400 ms | 1857 ms | 0.77 |
| r15 | 1B | 6node | date_histogram month + sum(price) | lucene | 1 | 12.27 s | 10.66 s | 0.87 |
| r15 | 1B | 1node16xl | terms(user_id) size 10 | lucene | 1 | 140.89 s | 176.32 s | 1.25 |
| r15 | 1B | 3node | terms(user_id) size 10 | lucene | 1 | 70.20 s | 58.88 s | 0.84 |
| r15 | 1B | 6node | terms(user_id) size 10 | lucene | 1 | 28.64 s | 29.49 s | 1.03 |
| r15 | 20M | 1node4xl | terms(category) > terms(rating) > avg(price) | pushed | 8 | 97 ms | 112 ms | 1.15 |
| r15 | 20M | 1node4xl | terms(category) > terms(rating) > avg(price) | lucene | 1 | 1760 ms | 1653 ms | 0.94 |
| r15 | 20M | 1node4xl | terms(category) > date_histogram month > sum(price) | pushed | 8 | 219 ms | 246 ms | 1.12 |
| r15 | 20M | 1node4xl | terms(category) > date_histogram month > sum(price) | lucene | 1 | 1850 ms | 2153 ms | 1.16 |
| r15 | 20M | 1node4xl | composite(category, rating) size 10 | pushed | 8 | 84 ms | 89 ms | 1.06 |
| r15 | 20M | 1node4xl | composite(category, rating) size 10 | lucene | 1 | 1050 ms | 1073 ms | 1.02 |
| r15 | 20M | 1node4xl | composite(category, rating) size 10 page 2 | pushed | 8 | 83 ms | 89 ms | 1.07 |
| r15 | 20M | 1node4xl | composite(category, rating) size 10 page 2 | lucene | 1 | 1110 ms | 1073 ms | 0.97 |
| r15 | 20M | 1node4xl | composite(category, ts 1d) size 10 | pushed | 8 | 941 ms | 880 ms | 0.94 |
| r15 | 20M | 1node4xl | composite(category, ts 1d) size 10 | lucene | 1 | 1030 ms | 1073 ms | 1.04 |
| r15 | 20M | 1node16xl | terms(category) > terms(rating) > avg(price) | pushed | 32 | 49 ms | 56 ms | 1.14 |
| r15 | 20M | 1node16xl | terms(category) > terms(rating) > avg(price) | lucene | 1 | 1830 ms | 1653 ms | 0.90 |
| r15 | 20M | 1node16xl | terms(category) > date_histogram month > sum(price) | pushed | 32 | 125 ms | 142 ms | 1.14 |
| r15 | 20M | 1node16xl | terms(category) > date_histogram month > sum(price) | lucene | 1 | 1850 ms | 2153 ms | 1.16 |
| r15 | 20M | 1node16xl | composite(category, rating) size 10 | pushed | 32 | 45 ms | 50 ms | 1.12 |
| r15 | 20M | 1node16xl | composite(category, rating) size 10 | lucene | 1 | 1060 ms | 1073 ms | 1.01 |
| r15 | 20M | 1node16xl | composite(category, rating) size 10 page 2 | pushed | 32 | 44 ms | 50 ms | 1.14 |
| r15 | 20M | 1node16xl | composite(category, rating) size 10 page 2 | lucene | 1 | 1130 ms | 1073 ms | 0.95 |
| r15 | 20M | 1node16xl | composite(category, ts 1d) size 10 | pushed | 32 | 1810 ms | 2249 ms | 1.24 |
| r15 | 20M | 1node16xl | composite(category, ts 1d) size 10 | lucene | 1 | 1050 ms | 1073 ms | 1.02 |
| r15 | 20M | 3node | terms(category) > terms(rating) > avg(price) | pushed | 8 | 48 ms | 52 ms | 1.09 |
| r15 | 20M | 3node | terms(category) > terms(rating) > avg(price) | lucene | 1 | 632 ms | 565 ms | 0.89 |
| r15 | 20M | 3node | terms(category) > date_histogram month > sum(price) | pushed | 8 | 113 ms | 106 ms | 0.94 |
| r15 | 20M | 3node | terms(category) > date_histogram month > sum(price) | lucene | 1 | 634 ms | 732 ms | 1.15 |
| r15 | 20M | 3node | composite(category, rating) size 10 | pushed | 8 | 46 ms | 45 ms | 0.97 |
| r15 | 20M | 3node | composite(category, rating) size 10 | lucene | 1 | 360 ms | 372 ms | 1.03 |
| r15 | 20M | 3node | composite(category, rating) size 10 page 2 | pushed | 8 | 46 ms | 45 ms | 0.97 |
| r15 | 20M | 3node | composite(category, rating) size 10 page 2 | lucene | 1 | 388 ms | 372 ms | 0.96 |
| r15 | 20M | 3node | composite(category, ts 1d) size 10 | pushed | 8 | 862 ms | 664 ms | 0.77 |
| r15 | 20M | 3node | composite(category, ts 1d) size 10 | lucene | 1 | 383 ms | 372 ms | 0.97 |
| r15 | 20M | 6node | terms(category) > terms(rating) > avg(price) | pushed | 8 | 37 ms | 38 ms | 1.01 |
| r15 | 20M | 6node | terms(category) > terms(rating) > avg(price) | lucene | 1 | 335 ms | 293 ms | 0.87 |
| r15 | 20M | 6node | terms(category) > date_histogram month > sum(price) | pushed | 8 | 77 ms | 72 ms | 0.93 |
| r15 | 20M | 6node | terms(category) > date_histogram month > sum(price) | lucene | 1 | 344 ms | 376 ms | 1.09 |
| r15 | 20M | 6node | composite(category, rating) size 10 | pushed | 8 | 32 ms | 34 ms | 1.05 |
| r15 | 20M | 6node | composite(category, rating) size 10 | lucene | 1 | 197 ms | 196 ms | 1.00 |
| r15 | 20M | 6node | composite(category, rating) size 10 page 2 | pushed | 8 | 33 ms | 34 ms | 1.02 |
| r15 | 20M | 6node | composite(category, rating) size 10 page 2 | lucene | 1 | 206 ms | 196 ms | 0.95 |
| r15 | 20M | 6node | composite(category, ts 1d) size 10 | pushed | 8 | 660 ms | 610 ms | 0.92 |
| r15 | 20M | 6node | composite(category, ts 1d) size 10 | lucene | 1 | 194 ms | 196 ms | 1.01 |
| r15 | 100M | 1node4xl | terms(category) > terms(rating) > avg(price) | pushed | 8 | 424 ms | 469 ms | 1.11 |
| r15 | 100M | 1node4xl | terms(category) > terms(rating) > avg(price) | lucene | 1 | 9280 ms | 8181 ms | 0.88 |
| r15 | 100M | 1node4xl | terms(category) > date_histogram month > sum(price) | pushed | 8 | 990 ms | 1083 ms | 1.09 |
| r15 | 100M | 1node4xl | terms(category) > date_histogram month > sum(price) | lucene | 1 | 9320 ms | 10.68 s | 1.15 |
| r15 | 100M | 1node4xl | composite(category, rating) size 10 | pushed | 8 | 373 ms | 354 ms | 0.95 |
| r15 | 100M | 1node4xl | composite(category, rating) size 10 | lucene | 1 | 5300 ms | 5281 ms | 1.00 |
| r15 | 100M | 1node4xl | composite(category, rating) size 10 page 2 | pushed | 8 | 378 ms | 354 ms | 0.94 |
| r15 | 100M | 1node4xl | composite(category, rating) size 10 page 2 | lucene | 1 | 5400 ms | 5281 ms | 0.98 |
| r15 | 100M | 1node4xl | composite(category, ts 1d) size 10 | pushed | 8 | 2000 ms | 2175 ms | 1.09 |
| r15 | 100M | 1node4xl | composite(category, ts 1d) size 10 | lucene | 1 | 5080 ms | 5281 ms | 1.04 |
| r15 | 100M | 1node16xl | terms(category) > terms(rating) > avg(price) | pushed | 32 | 181 ms | 145 ms | 0.80 |
| r15 | 100M | 1node16xl | terms(category) > terms(rating) > avg(price) | lucene | 1 | 9270 ms | 8181 ms | 0.88 |
| r15 | 100M | 1node16xl | terms(category) > date_histogram month > sum(price) | pushed | 32 | 375 ms | 351 ms | 0.94 |
| r15 | 100M | 1node16xl | terms(category) > date_histogram month > sum(price) | lucene | 1 | 9300 ms | 10.68 s | 1.15 |
| r15 | 100M | 1node16xl | composite(category, rating) size 10 | pushed | 32 | 165 ms | 117 ms | 0.71 |
| r15 | 100M | 1node16xl | composite(category, rating) size 10 | lucene | 1 | 5390 ms | 5281 ms | 0.98 |
| r15 | 100M | 1node16xl | composite(category, rating) size 10 page 2 | pushed | 32 | 166 ms | 117 ms | 0.70 |
| r15 | 100M | 1node16xl | composite(category, rating) size 10 page 2 | lucene | 1 | 5630 ms | 5281 ms | 0.94 |
| r15 | 100M | 1node16xl | composite(category, ts 1d) size 10 | pushed | 32 | 2580 ms | 2573 ms | 1.00 |
| r15 | 100M | 1node16xl | composite(category, ts 1d) size 10 | lucene | 1 | 5180 ms | 5281 ms | 1.02 |
| r15 | 100M | 3node | terms(category) > terms(rating) > avg(price) | pushed | 8 | 167 ms | 171 ms | 1.03 |
| r15 | 100M | 3node | terms(category) > terms(rating) > avg(price) | lucene | 1 | 3120 ms | 2741 ms | 0.88 |
| r15 | 100M | 3node | terms(category) > date_histogram month > sum(price) | pushed | 8 | 372 ms | 385 ms | 1.04 |
| r15 | 100M | 3node | terms(category) > date_histogram month > sum(price) | lucene | 1 | 3160 ms | 3574 ms | 1.13 |
| r15 | 100M | 3node | composite(category, rating) size 10 | pushed | 8 | 148 ms | 133 ms | 0.90 |
| r15 | 100M | 3node | composite(category, rating) size 10 | lucene | 1 | 1920 ms | 1774 ms | 0.92 |
| r15 | 100M | 3node | composite(category, rating) size 10 page 2 | pushed | 8 | 146 ms | 133 ms | 0.91 |
| r15 | 100M | 3node | composite(category, rating) size 10 page 2 | lucene | 1 | 1920 ms | 1774 ms | 0.92 |
| r15 | 100M | 3node | composite(category, ts 1d) size 10 | pushed | 8 | 1310 ms | 1096 ms | 0.84 |
| r15 | 100M | 3node | composite(category, ts 1d) size 10 | lucene | 1 | 1880 ms | 1774 ms | 0.94 |
| r15 | 100M | 6node | terms(category) > terms(rating) > avg(price) | pushed | 8 | 104 ms | 97 ms | 0.93 |
| r15 | 100M | 6node | terms(category) > terms(rating) > avg(price) | lucene | 1 | 1600 ms | 1381 ms | 0.86 |
| r15 | 100M | 6node | terms(category) > date_histogram month > sum(price) | pushed | 8 | 220 ms | 211 ms | 0.96 |
| r15 | 100M | 6node | terms(category) > date_histogram month > sum(price) | lucene | 1 | 1630 ms | 1798 ms | 1.10 |
| r15 | 100M | 6node | composite(category, rating) size 10 | pushed | 8 | 93 ms | 78 ms | 0.84 |
| r15 | 100M | 6node | composite(category, rating) size 10 | lucene | 1 | 938 ms | 898 ms | 0.96 |
| r15 | 100M | 6node | composite(category, rating) size 10 page 2 | pushed | 8 | 93 ms | 78 ms | 0.84 |
| r15 | 100M | 6node | composite(category, rating) size 10 page 2 | lucene | 1 | 964 ms | 898 ms | 0.93 |
| r15 | 100M | 6node | composite(category, ts 1d) size 10 | pushed | 8 | 1030 ms | 826 ms | 0.80 |
| r15 | 100M | 6node | composite(category, ts 1d) size 10 | lucene | 1 | 931 ms | 898 ms | 0.96 |
| r15 | 1B | 1node4xl | terms(category) > terms(rating) > avg(price) | pushed | 8 | 6150 ms | 6815 ms | 1.11 |
| r15 | 1B | 1node4xl | terms(category) > terms(rating) > avg(price) | lucene | 1 | 102.98 s | 81.71 s | 0.79 |
| r15 | 1B | 1node4xl | terms(category) > date_histogram month > sum(price) | pushed | 8 | 15.15 s | 13.47 s | 0.89 |
| r15 | 1B | 1node4xl | terms(category) > date_histogram month > sum(price) | lucene | 1 | 108.12 s | 106.71 s | 0.99 |
| r15 | 1B | 1node4xl | composite(category, rating) size 10 | pushed | 8 | 3580 ms | 4385 ms | 1.22 |
| r15 | 1B | 1node4xl | composite(category, rating) size 10 | lucene | 1 | 51.08 s | 52.71 s | 1.03 |
| r15 | 1B | 1node4xl | composite(category, rating) size 10 page 2 | pushed | 8 | 3580 ms | 4385 ms | 1.22 |
| r15 | 1B | 1node4xl | composite(category, rating) size 10 page 2 | lucene | 1 | 51.69 s | 52.71 s | 1.02 |
| r15 | 1B | 1node4xl | composite(category, ts 1d) size 10 | pushed | 8 | 16.46 s | 18.43 s | 1.12 |
| r15 | 1B | 1node4xl | composite(category, ts 1d) size 10 | lucene | 1 | 48.97 s | 52.71 s | 1.08 |
| r15 | 1B | 1node16xl | terms(category) > terms(rating) > avg(price) | pushed | 32 | 1020 ms | 1149 ms | 1.13 |
| r15 | 1B | 1node16xl | terms(category) > terms(rating) > avg(price) | lucene | 1 | 89.13 s | 81.62 s | 0.92 |
| r15 | 1B | 1node16xl | terms(category) > date_histogram month > sum(price) | pushed | 32 | 2480 ms | 2705 ms | 1.09 |
| r15 | 1B | 1node16xl | terms(category) > date_histogram month > sum(price) | lucene | 1 | 90.17 s | 106.62 s | 1.18 |
| r15 | 1B | 1node16xl | composite(category, rating) size 10 | pushed | 32 | 867 ms | 862 ms | 0.99 |
| r15 | 1B | 1node16xl | composite(category, rating) size 10 | lucene | 1 | 52.22 s | 52.62 s | 1.01 |
| r15 | 1B | 1node16xl | composite(category, rating) size 10 page 2 | pushed | 32 | 864 ms | 862 ms | 1.00 |
| r15 | 1B | 1node16xl | composite(category, rating) size 10 page 2 | lucene | 1 | 54.32 s | 52.62 s | 0.97 |
| r15 | 1B | 1node16xl | composite(category, ts 1d) size 10 | pushed | 32 | 6390 ms | 6215 ms | 0.97 |
| r15 | 1B | 1node16xl | composite(category, ts 1d) size 10 | lucene | 1 | 49.39 s | 52.62 s | 1.07 |
| r15 | 1B | 3node | terms(category) > terms(rating) > avg(price) | pushed | 8 | 2170 ms | 2347 ms | 1.08 |
| r15 | 1B | 3node | terms(category) > terms(rating) > avg(price) | lucene | 1 | 30.76 s | 27.31 s | 0.89 |
| r15 | 1B | 3node | terms(category) > date_histogram month > sum(price) | pushed | 8 | 5260 ms | 4574 ms | 0.87 |
| r15 | 1B | 3node | terms(category) > date_histogram month > sum(price) | lucene | 1 | 30.99 s | 35.64 s | 1.15 |
| r15 | 1B | 3node | composite(category, rating) size 10 | pushed | 8 | 1320 ms | 1537 ms | 1.16 |
| r15 | 1B | 3node | composite(category, rating) size 10 | lucene | 1 | 17.48 s | 17.64 s | 1.01 |
| r15 | 1B | 3node | composite(category, rating) size 10 page 2 | pushed | 8 | 1380 ms | 1537 ms | 1.11 |
| r15 | 1B | 3node | composite(category, rating) size 10 page 2 | lucene | 1 | 17.60 s | 17.64 s | 1.00 |
| r15 | 1B | 3node | composite(category, ts 1d) size 10 | pushed | 8 | 6750 ms | 6575 ms | 0.97 |
| r15 | 1B | 3node | composite(category, ts 1d) size 10 | lucene | 1 | 16.58 s | 17.64 s | 1.06 |
| r15 | 1B | 6node | terms(category) > terms(rating) > avg(price) | pushed | 8 | 1200 ms | 1230 ms | 1.02 |
| r15 | 1B | 6node | terms(category) > terms(rating) > avg(price) | lucene | 1 | 15.44 s | 13.71 s | 0.89 |
| r15 | 1B | 6node | terms(category) > date_histogram month > sum(price) | pushed | 8 | 2810 ms | 2350 ms | 0.84 |
| r15 | 1B | 6node | terms(category) > date_histogram month > sum(price) | lucene | 1 | 15.54 s | 17.88 s | 1.15 |
| r15 | 1B | 6node | composite(category, rating) size 10 | pushed | 8 | 844 ms | 825 ms | 0.98 |
| r15 | 1B | 6node | composite(category, rating) size 10 | lucene | 1 | 8800 ms | 8878 ms | 1.01 |
| r15 | 1B | 6node | composite(category, rating) size 10 page 2 | pushed | 8 | 848 ms | 825 ms | 0.97 |
| r15 | 1B | 6node | composite(category, rating) size 10 page 2 | lucene | 1 | 8920 ms | 8878 ms | 1.00 |
| r15 | 1B | 6node | composite(category, ts 1d) size 10 | pushed | 8 | 3870 ms | 3611 ms | 0.93 |
| r15 | 1B | 6node | composite(category, ts 1d) size 10 | lucene | 1 | 8440 ms | 8878 ms | 1.05 |
| r16 | 1B | 1node4xl | terms(category) | pushed | 8 | 2730 ms | 2967 ms | 1.09 |
| r16 | 1B | 4node | terms(category) | pushed | 8 | 848 ms | 824 ms | 0.97 |
| r16 | 1B | 6node | terms(category) | pushed | 8 | 636 ms | 586 ms | 0.92 |
| r16 | 1B | 1node16xl | terms(category) | pushed | 32 | 726 ms | 655 ms | 0.90 |
| r16 | 1B | 4node | terms(category) | lucene | 8 | 346 ms | 370 ms | 1.07 |
| r16 | 1B | 1node4xl | terms(rating) | pushed | 8 | 1100 ms | 1524 ms | 1.39 |
| r16 | 1B | 4node | terms(rating) | pushed | 8 | 346 ms | 463 ms | 1.34 |
| r16 | 1B | 6node | terms(rating) | pushed | 8 | 386 ms | 345 ms | 0.89 |
| r16 | 1B | 1node16xl | terms(rating) | pushed | 32 | 263 ms | 213 ms | 0.81 |
| r16 | 1B | 4node | terms(rating) | lucene | 8 | 780 ms | 933 ms | 1.20 |
| r16 | 1B | 1node4xl | terms(user_id) size 10 | pushed | 8 | 19.50 s | 20.24 s | 1.04 |
| r16 | 1B | 4node | terms(user_id) size 10 | pushed | 8 | 4740 ms | 5142 ms | 1.08 |
| r16 | 1B | 6node | terms(user_id) size 10 | pushed | 8 | 3260 ms | 3464 ms | 1.06 |
| r16 | 1B | 1node16xl | terms(user_id) size 10 | pushed | 32 | 5290 ms | 4733 ms | 0.89 |
| r16 | 1B | 4node | terms(user_id) size 10 | lucene | 8 | 17.80 s | 5620 ms | 0.32 |
| r16 | 1B | 1node4xl | sum(price) | pushed | 8 | 2640 ms | 2539 ms | 0.96 |
| r16 | 1B | 4node | sum(price) | pushed | 8 | 784 ms | 717 ms | 0.91 |
| r16 | 1B | 6node | sum(price) | pushed | 8 | 586 ms | 514 ms | 0.88 |
| r16 | 1B | 1node16xl | sum(price) | pushed | 32 | 321 ms | 307 ms | 0.95 |
| r16 | 1B | 4node | sum(price) | lucene | 8 | 442 ms | 486 ms | 1.10 |
| r16 | 1B | 1node4xl | avg(price) | pushed | 8 | 2640 ms | 2539 ms | 0.96 |
| r16 | 1B | 4node | avg(price) | pushed | 8 | 793 ms | 717 ms | 0.90 |
| r16 | 1B | 6node | avg(price) | pushed | 8 | 587 ms | 514 ms | 0.88 |
| r16 | 1B | 1node16xl | avg(price) | pushed | 32 | 321 ms | 307 ms | 0.95 |
| r16 | 1B | 4node | avg(price) | lucene | 8 | 448 ms | 486 ms | 1.08 |
| r16 | 1B | 1node4xl | stats(price) | pushed | 8 | 2730 ms | 2539 ms | 0.93 |
| r16 | 1B | 4node | stats(price) | pushed | 8 | 816 ms | 717 ms | 0.88 |
| r16 | 1B | 6node | stats(price) | pushed | 8 | 590 ms | 514 ms | 0.87 |
| r16 | 1B | 1node16xl | stats(price) | pushed | 32 | 322 ms | 307 ms | 0.95 |
| r16 | 1B | 4node | stats(price) | lucene | 8 | 475 ms | 486 ms | 1.02 |
| r16 | 1B | 1node4xl | extended_stats(price) | pushed | 8 | 2890 ms | 2627 ms | 0.91 |
| r16 | 1B | 4node | extended_stats(price) | pushed | 8 | 826 ms | 738 ms | 0.89 |
| r16 | 1B | 6node | extended_stats(price) | pushed | 8 | 611 ms | 529 ms | 0.87 |
| r16 | 1B | 1node16xl | extended_stats(price) | pushed | 32 | 291 ms | 328 ms | 1.13 |
| r16 | 1B | 4node | extended_stats(price) | lucene | 8 | 1050 ms | 1048 ms | 1.00 |
| r16 | 1B | 1node4xl | date_histogram 1d + sum(price) | pushed | 8 | 9770 ms | 10.60 s | 1.08 |
| r16 | 1B | 4node | date_histogram 1d + sum(price) | pushed | 8 | 2650 ms | 2733 ms | 1.03 |
| r16 | 1B | 6node | date_histogram 1d + sum(price) | pushed | 8 | 1870 ms | 1859 ms | 0.99 |
| r16 | 1B | 1node16xl | date_histogram 1d + sum(price) | pushed | 32 | 1310 ms | 2011 ms | 1.54 |
| r16 | 1B | 4node | date_histogram 1d + sum(price) | lucene | 8 | 1660 ms | 2089 ms | 1.26 |
| r16 | 1B | 1node4xl | date_histogram month + sum(price) | pushed | 8 | 12.60 s | 10.59 s | 0.84 |
| r16 | 1B | 4node | date_histogram month + sum(price) | pushed | 8 | 3300 ms | 2730 ms | 0.83 |
| r16 | 1B | 6node | date_histogram month + sum(price) | pushed | 8 | 2310 ms | 1857 ms | 0.80 |
| r16 | 1B | 1node16xl | date_histogram month + sum(price) | pushed | 32 | 1900 ms | 2001 ms | 1.05 |
| r16 | 1B | 4node | date_histogram month + sum(price) | lucene | 8 | 2450 ms | 2089 ms | 0.85 |
| r16 | 1B | 1node4xl | range(price 4 band) | pushed | 8 | 4770 ms | 4789 ms | 1.00 |
| r16 | 1B | 4node | range(price 4 band) | pushed | 8 | 1360 ms | 1279 ms | 0.94 |
| r16 | 1B | 6node | range(price 4 band) | pushed | 8 | 955 ms | 889 ms | 0.93 |
| r16 | 1B | 1node16xl | range(price 4 band) | pushed | 32 | 781 ms | 869 ms | 1.11 |
| r16 | 1B | 4node | range(price 4 band) | lucene | 8 | 1110 ms | 1183 ms | 1.07 |
| r16 | 1B | 1node4xl | filters(rating=5, category=cat150, price>=500) | pushed | 8 | 5730 ms | 5862 ms | 1.02 |
| r16 | 1B | 4node | filters(rating=5, category=cat150, price>=500) | pushed | 8 | 1550 ms | 1547 ms | 1.00 |
| r16 | 1B | 6node | filters(rating=5, category=cat150, price>=500) | pushed | 8 | 1120 ms | 1068 ms | 0.95 |
| r16 | 1B | 1node16xl | filters(rating=5, category=cat150, price>=500) | pushed | 32 | 888 ms | 897 ms | 1.01 |
| r16 | 1B | 4node | filters(rating=5, category=cat150, price>=500) | lucene | 8 | 1610 ms | 1858 ms | 1.15 |
| r16 | 1B | 1node4xl | cardinality(user_id) | pushed | 8 | 52.60 s | 27.34 s | 0.52 |
| r16 | 1B | 4node | cardinality(user_id) | pushed | 8 | 22.60 s | 25.52 s | 1.13 |
| r16 | 1B | 6node | cardinality(user_id) | pushed | 8 | 17.60 s | 25.31 s | 1.44 |
| r16 | 1B | 1node16xl | cardinality(user_id) | pushed | 32 | 199.30 s | 99.51 s | 0.50 |
| r16 | 1B | 4node | cardinality(user_id) | lucene | 8 | 4030 ms | 4433 ms | 1.10 |
| r16 | 1B | 1node4xl | percentiles(price) | pushed | 8 | 7070 ms | 7094 ms | 1.00 |
| r16 | 1B | 4node | percentiles(price) | pushed | 8 | 2040 ms | 1855 ms | 0.91 |
| r16 | 1B | 6node | percentiles(price) | pushed | 8 | 1470 ms | 1273 ms | 0.87 |
| r16 | 1B | 1node16xl | percentiles(price) | pushed | 32 | 1020 ms | 1125 ms | 1.10 |
| r16 | 1B | 4node | percentiles(price) | lucene | 8 | 3350 ms | 3402 ms | 1.02 |
| r16 | 1B | 1node4xl | terms(category) > terms(rating) > avg(price) | pushed | 8 | 5970 ms | 6815 ms | 1.14 |
| r16 | 1B | 4node | terms(category) > terms(rating) > avg(price) | pushed | 8 | 1650 ms | 1788 ms | 1.08 |
| r16 | 1B | 6node | terms(category) > terms(rating) > avg(price) | pushed | 8 | 1210 ms | 1230 ms | 1.02 |
| r16 | 1B | 1node16xl | terms(category) > terms(rating) > avg(price) | pushed | 32 | 961 ms | 1149 ms | 1.20 |
| r16 | 1B | 4node | terms(category) > terms(rating) > avg(price) | lucene | 8 | 3050 ms | 2661 ms | 0.87 |
| r16 | 1B | 1node4xl | composite(category, rating) size 10 | pushed | 8 | 3560 ms | 4385 ms | 1.23 |
| r16 | 1B | 4node | composite(category, rating) size 10 | pushed | 8 | 1090 ms | 1181 ms | 1.08 |
| r16 | 1B | 6node | composite(category, rating) size 10 | pushed | 8 | 836 ms | 825 ms | 0.99 |
| r16 | 1B | 1node16xl | composite(category, rating) size 10 | pushed | 32 | 844 ms | 862 ms | 1.02 |
| r16 | 1B | 4node | composite(category, rating) size 10 | lucene | 8 | 1870 ms | 1755 ms | 0.94 |
| r16 | 1B | 1node4xl | filter rating=5 + terms(category) | pushed | 8 | 7030 ms | 10.51 s | 1.49 |
| r16 | 1B | 4node | filter rating=5 + terms(category) | pushed | 8 | 4260 ms | 2709 ms | 0.64 |
| r16 | 1B | 6node | filter rating=5 + terms(category) | pushed | 8 | 3830 ms | 1842 ms | 0.48 |
| r16 | 1B | 1node16xl | filter rating=5 + terms(category) | pushed | 32 | 4770 ms | 2380 ms | 0.50 |
| r16 | 1B | 4node | filter rating=5 + terms(category) | lucene | 8 | 15.90 s | 15.84 s | 1.00 |
| r16 | 1B | 1node16xl | cardinality(user_id) | lucene | 32 | 4340 ms | 4343 ms | 1.00 |
| r16 | 20M | 1node4xl | cardinality(user_id) | pushed | 8 | 14.10 s | 6242 ms | 0.44 |
| r16 | 20M | 1node4xl | cardinality(user_id) | lucene | 8 | 446 ms | 367 ms | 0.82 |
| r16 | 1B | 1node16xl | percentiles(price) | lucene | 1 | 100.00 s | 105.32 s | 1.05 |
| r16 | 1B | 1node16xl | percentiles(price) | lucene | 32 | 3600 ms | 3312 ms | 0.92 |
| r16 | 1B | 1node16xl | date_histogram 1d + stats(price) | lucene | 1 | 54.40 s | 63.32 s | 1.16 |
| r16 | 1B | 1node16xl | date_histogram 1d + stats(price) | lucene | 32 | 1790 ms | 1999 ms | 1.12 |
| r16 | 1B | 1node16xl | range(price 4 band) | lucene | 1 | 34.20 s | 34.32 s | 1.00 |
| r16 | 1B | 1node16xl | range(price 4 band) | lucene | 32 | 1140 ms | 1093 ms | 0.96 |
| r16 | 1B | 1node16xl | filters(rating=5, category=cat150, price>=500) | lucene | 1 | 60.90 s | 55.92 s | 0.92 |
| r16 | 1B | 1node16xl | filters(rating=5, category=cat150, price>=500) | lucene | 32 | 1990 ms | 1768 ms | 0.89 |

Median absolute log ratio 0.083 (a factor of 1.09); 304 of 310 rows within a factor of 2.

## Choices

Every (shape, table, cluster) with both a pushed and a Lucene measurement. Pairs whose measured latencies differ by more than 1.3x are asserted by CostModelTests; pairs inside that band are reported only. A pair is listed once per Lucene slices value measured.

| table | cluster | shape | slices | measured pushed | measured lucene | measured winner | model pushed | model lucene | model winner | verdict |
|---|---|---|---|---|---|---|---|---|---|---|
| 20M | 1node16xl | composite(category, rating) size 10 | 1 | 45 ms | 1060 ms | pushed | 50 ms | 1073 ms | pushed | agrees |
| 20M | 1node16xl | composite(category, rating) size 10 page 2 | 1 | 44 ms | 1130 ms | pushed | 50 ms | 1073 ms | pushed | agrees |
| 20M | 1node16xl | composite(category, ts 1d) size 10 | 1 | 1810 ms | 1050 ms | lucene | 2249 ms | 1073 ms | lucene | agrees |
| 20M | 1node16xl | date_histogram month + sum(price) | 1 | 63 ms | 1460 ms | pushed | 59 ms | 1287 ms | pushed | agrees |
| 20M | 1node16xl | sum(price) | 1 | 23 ms | 255 ms | pushed | 25 ms | 261 ms | pushed | agrees |
| 20M | 1node16xl | terms(category) | 1 | 36 ms | 157 ms | pushed | 33 ms | 187 ms | pushed | agrees |
| 20M | 1node16xl | terms(category) > date_histogram month > sum(price) | 1 | 125 ms | 1850 ms | pushed | 142 ms | 2153 ms | pushed | agrees |
| 20M | 1node16xl | terms(category) > terms(rating) > avg(price) | 1 | 49 ms | 1830 ms | pushed | 56 ms | 1653 ms | pushed | agrees |
| 20M | 1node16xl | terms(rating) | 1 | 24 ms | 530 ms | pushed | 23 ms | 547 ms | pushed | agrees |
| 20M | 1node4xl | cardinality(user_id) | 8 | 14.10 s | 446 ms | lucene | 6242 ms | 367 ms | lucene | agrees |
| 20M | 1node4xl | composite(category, rating) size 10 | 1 | 84 ms | 1050 ms | pushed | 89 ms | 1073 ms | pushed | agrees |
| 20M | 1node4xl | composite(category, rating) size 10 page 2 | 1 | 83 ms | 1110 ms | pushed | 89 ms | 1073 ms | pushed | agrees |
| 20M | 1node4xl | composite(category, ts 1d) size 10 | 1 | 941 ms | 1030 ms | pushed | 880 ms | 1073 ms | pushed | reported (1.09x) |
| 20M | 1node4xl | date_histogram month + sum(price) | 1 | 161 ms | 1470 ms | pushed | 178 ms | 1287 ms | pushed | agrees |
| 20M | 1node4xl | sum(price) | 1 | 30 ms | 255 ms | pushed | 42 ms | 261 ms | pushed | agrees |
| 20M | 1node4xl | terms(category) | 1 | 69 ms | 204 ms | pushed | 70 ms | 187 ms | pushed | agrees |
| 20M | 1node4xl | terms(category) > date_histogram month > sum(price) | 1 | 219 ms | 1850 ms | pushed | 246 ms | 2153 ms | pushed | agrees |
| 20M | 1node4xl | terms(category) > terms(rating) > avg(price) | 1 | 97 ms | 1760 ms | pushed | 112 ms | 1653 ms | pushed | agrees |
| 20M | 1node4xl | terms(rating) | 1 | 37 ms | 526 ms | pushed | 35 ms | 547 ms | pushed | agrees |
| 20M | 3node | composite(category, rating) size 10 | 1 | 46 ms | 360 ms | pushed | 45 ms | 372 ms | pushed | agrees |
| 20M | 3node | composite(category, rating) size 10 page 2 | 1 | 46 ms | 388 ms | pushed | 45 ms | 372 ms | pushed | agrees |
| 20M | 3node | composite(category, ts 1d) size 10 | 1 | 862 ms | 383 ms | lucene | 664 ms | 372 ms | lucene | agrees |
| 20M | 3node | date_histogram month + sum(price) | 1 | 80 ms | 503 ms | pushed | 72 ms | 443 ms | pushed | agrees |
| 20M | 3node | sum(price) | 1 | 23 ms | 96 ms | pushed | 27 ms | 101 ms | pushed | agrees |
| 20M | 3node | terms(category) | 1 | 38 ms | 81 ms | pushed | 36 ms | 76 ms | pushed | agrees |
| 20M | 3node | terms(category) > date_histogram month > sum(price) | 1 | 113 ms | 634 ms | pushed | 106 ms | 732 ms | pushed | agrees |
| 20M | 3node | terms(category) > terms(rating) > avg(price) | 1 | 48 ms | 632 ms | pushed | 52 ms | 565 ms | pushed | agrees |
| 20M | 3node | terms(rating) | 1 | 26 ms | 188 ms | pushed | 24 ms | 196 ms | pushed | agrees |
| 20M | 6node | composite(category, rating) size 10 | 1 | 32 ms | 197 ms | pushed | 34 ms | 196 ms | pushed | agrees |
| 20M | 6node | composite(category, rating) size 10 page 2 | 1 | 33 ms | 206 ms | pushed | 34 ms | 196 ms | pushed | agrees |
| 20M | 6node | composite(category, ts 1d) size 10 | 1 | 660 ms | 194 ms | lucene | 610 ms | 196 ms | lucene | agrees |
| 20M | 6node | date_histogram month + sum(price) | 1 | 51 ms | 270 ms | pushed | 46 ms | 232 ms | pushed | agrees |
| 20M | 6node | sum(price) | 1 | 18 ms | 57 ms | pushed | 23 ms | 61 ms | pushed | agrees |
| 20M | 6node | terms(category) | 1 | 27 ms | 48 ms | pushed | 28 ms | 49 ms | pushed | agrees |
| 20M | 6node | terms(category) > date_histogram month > sum(price) | 1 | 77 ms | 344 ms | pushed | 72 ms | 376 ms | pushed | agrees |
| 20M | 6node | terms(category) > terms(rating) > avg(price) | 1 | 37 ms | 335 ms | pushed | 38 ms | 293 ms | pushed | agrees |
| 20M | 6node | terms(rating) | 1 | 20 ms | 103 ms | pushed | 22 ms | 109 ms | pushed | agrees |
| 100M | 1node16xl | composite(category, rating) size 10 | 1 | 165 ms | 5390 ms | pushed | 117 ms | 5281 ms | pushed | agrees |
| 100M | 1node16xl | composite(category, rating) size 10 page 2 | 1 | 166 ms | 5630 ms | pushed | 117 ms | 5281 ms | pushed | agrees |
| 100M | 1node16xl | composite(category, ts 1d) size 10 | 1 | 2580 ms | 5180 ms | pushed | 2573 ms | 5281 ms | pushed | agrees |
| 100M | 1node16xl | date_histogram month + sum(price) | 1 | 260 ms | 7270 ms | pushed | 217 ms | 6351 ms | pushed | agrees |
| 100M | 1node16xl | sum(price) | 1 | 94 ms | 1260 ms | pushed | 48 ms | 1221 ms | pushed | agrees |
| 100M | 1node16xl | terms(category) | 1 | 136 ms | 775 ms | pushed | 84 ms | 851 ms | pushed | agrees |
| 100M | 1node16xl | terms(category) > date_histogram month > sum(price) | 1 | 375 ms | 9300 ms | pushed | 351 ms | 10.68 s | pushed | agrees |
| 100M | 1node16xl | terms(category) > terms(rating) > avg(price) | 1 | 181 ms | 9270 ms | pushed | 145 ms | 8181 ms | pushed | agrees |
| 100M | 1node16xl | terms(rating) | 1 | 91 ms | 2630 ms | pushed | 38 ms | 2651 ms | pushed | agrees |
| 100M | 1node4xl | composite(category, rating) size 10 | 1 | 373 ms | 5300 ms | pushed | 354 ms | 5281 ms | pushed | agrees |
| 100M | 1node4xl | composite(category, rating) size 10 page 2 | 1 | 378 ms | 5400 ms | pushed | 354 ms | 5281 ms | pushed | agrees |
| 100M | 1node4xl | composite(category, ts 1d) size 10 | 1 | 2000 ms | 5080 ms | pushed | 2175 ms | 5281 ms | pushed | agrees |
| 100M | 1node4xl | date_histogram month + sum(price) | 1 | 763 ms | 7390 ms | pushed | 812 ms | 6351 ms | pushed | agrees |
| 100M | 1node4xl | sum(price) | 1 | 118 ms | 1260 ms | pushed | 134 ms | 1221 ms | pushed | agrees |
| 100M | 1node4xl | terms(category) | 1 | 307 ms | 1010 ms | pushed | 273 ms | 851 ms | pushed | agrees |
| 100M | 1node4xl | terms(category) > date_histogram month > sum(price) | 1 | 990 ms | 9320 ms | pushed | 1083 ms | 10.68 s | pushed | agrees |
| 100M | 1node4xl | terms(category) > terms(rating) > avg(price) | 1 | 424 ms | 9280 ms | pushed | 469 ms | 8181 ms | pushed | agrees |
| 100M | 1node4xl | terms(rating) | 1 | 162 ms | 2630 ms | pushed | 97 ms | 2651 ms | pushed | agrees |
| 100M | 3node | composite(category, rating) size 10 | 1 | 148 ms | 1920 ms | pushed | 133 ms | 1774 ms | pushed | agrees |
| 100M | 3node | composite(category, rating) size 10 page 2 | 1 | 146 ms | 1920 ms | pushed | 133 ms | 1774 ms | pushed | agrees |
| 100M | 3node | composite(category, ts 1d) size 10 | 1 | 1310 ms | 1880 ms | pushed | 1096 ms | 1774 ms | pushed | agrees |
| 100M | 3node | date_histogram month + sum(price) | 1 | 283 ms | 2500 ms | pushed | 283 ms | 2131 ms | pushed | agrees |
| 100M | 3node | sum(price) | 1 | 58 ms | 439 ms | pushed | 57 ms | 421 ms | pushed | agrees |
| 100M | 3node | terms(category) | 1 | 126 ms | 354 ms | pushed | 104 ms | 298 ms | pushed | agrees |
| 100M | 3node | terms(category) > date_histogram month > sum(price) | 1 | 372 ms | 3160 ms | pushed | 385 ms | 3574 ms | pushed | agrees |
| 100M | 3node | terms(category) > terms(rating) > avg(price) | 1 | 167 ms | 3120 ms | pushed | 171 ms | 2741 ms | pushed | agrees |
| 100M | 3node | terms(rating) | 1 | 72 ms | 888 ms | pushed | 45 ms | 898 ms | pushed | agrees |
| 100M | 6node | composite(category, rating) size 10 | 1 | 93 ms | 938 ms | pushed | 78 ms | 898 ms | pushed | agrees |
| 100M | 6node | composite(category, rating) size 10 page 2 | 1 | 93 ms | 964 ms | pushed | 78 ms | 898 ms | pushed | agrees |
| 100M | 6node | composite(category, ts 1d) size 10 | 1 | 1030 ms | 931 ms | lucene | 826 ms | 898 ms | pushed | reported (1.11x) |
| 100M | 6node | date_histogram month + sum(price) | 1 | 165 ms | 1260 ms | pushed | 151 ms | 1076 ms | pushed | agrees |
| 100M | 6node | sum(price) | 1 | 41 ms | 234 ms | pushed | 38 ms | 221 ms | pushed | agrees |
| 100M | 6node | terms(category) | 1 | 77 ms | 190 ms | pushed | 62 ms | 159 ms | pushed | agrees |
| 100M | 6node | terms(category) > date_histogram month > sum(price) | 1 | 220 ms | 1630 ms | pushed | 211 ms | 1798 ms | pushed | agrees |
| 100M | 6node | terms(category) > terms(rating) > avg(price) | 1 | 104 ms | 1600 ms | pushed | 97 ms | 1381 ms | pushed | agrees |
| 100M | 6node | terms(rating) | 1 | 48 ms | 457 ms | pushed | 32 ms | 459 ms | pushed | agrees |
| 1B | 1node16xl | cardinality(user_id) | 32 | 199.30 s | 4340 ms | lucene | 99.51 s | 4343 ms | lucene | agrees |
| 1B | 1node16xl | composite(category, rating) size 10 | 1 | 844 ms | 52.22 s | pushed | 862 ms | 52.62 s | pushed | agrees |
| 1B | 1node16xl | composite(category, rating) size 10 page 2 | 1 | 864 ms | 54.32 s | pushed | 862 ms | 52.62 s | pushed | agrees |
| 1B | 1node16xl | composite(category, ts 1d) size 10 | 1 | 6390 ms | 49.39 s | pushed | 6215 ms | 52.62 s | pushed | agrees |
| 1B | 1node16xl | date_histogram month + sum(price) | 1 | 1900 ms | 72.58 s | pushed | 2001 ms | 63.32 s | pushed | agrees |
| 1B | 1node16xl | filters(rating=5, category=cat150, price>=500) | 1 | 888 ms | 60.90 s | pushed | 897 ms | 55.92 s | pushed | agrees |
| 1B | 1node16xl | filters(rating=5, category=cat150, price>=500) | 32 | 888 ms | 1990 ms | pushed | 897 ms | 1768 ms | pushed | agrees |
| 1B | 1node16xl | percentiles(price) | 1 | 1020 ms | 100.00 s | pushed | 1125 ms | 105.32 s | pushed | agrees |
| 1B | 1node16xl | percentiles(price) | 32 | 1020 ms | 3600 ms | pushed | 1125 ms | 3312 ms | pushed | agrees |
| 1B | 1node16xl | range(price 4 band) | 1 | 781 ms | 34.20 s | pushed | 869 ms | 34.32 s | pushed | agrees |
| 1B | 1node16xl | range(price 4 band) | 32 | 781 ms | 1140 ms | pushed | 869 ms | 1093 ms | pushed | agrees |
| 1B | 1node16xl | sum(price) | 1 | 321 ms | 11.75 s | pushed | 307 ms | 12.02 s | pushed | agrees |
| 1B | 1node16xl | terms(category) | 1 | 726 ms | 8860 ms | pushed | 655 ms | 8321 ms | pushed | agrees |
| 1B | 1node16xl | terms(category) > date_histogram month > sum(price) | 1 | 2480 ms | 90.17 s | pushed | 2705 ms | 106.62 s | pushed | agrees |
| 1B | 1node16xl | terms(category) > terms(rating) > avg(price) | 1 | 961 ms | 89.13 s | pushed | 1149 ms | 81.62 s | pushed | agrees |
| 1B | 1node16xl | terms(rating) | 1 | 263 ms | 24.17 s | pushed | 213 ms | 26.32 s | pushed | agrees |
| 1B | 1node16xl | terms(user_id) size 10 | 1 | 5290 ms | 140.89 s | pushed | 4733 ms | 176.32 s | pushed | agrees |
| 1B | 1node4xl | composite(category, rating) size 10 | 1 | 3560 ms | 51.08 s | pushed | 4385 ms | 52.71 s | pushed | agrees |
| 1B | 1node4xl | composite(category, rating) size 10 page 2 | 1 | 3580 ms | 51.69 s | pushed | 4385 ms | 52.71 s | pushed | agrees |
| 1B | 1node4xl | composite(category, ts 1d) size 10 | 1 | 16.46 s | 48.97 s | pushed | 18.43 s | 52.71 s | pushed | agrees |
| 1B | 1node4xl | date_histogram month + sum(price) | 1 | 12.60 s | 89.24 s | pushed | 10.59 s | 63.41 s | pushed | agrees |
| 1B | 1node4xl | sum(price) | 1 | 2640 ms | 12.26 s | pushed | 2539 ms | 12.11 s | pushed | agrees |
| 1B | 1node4xl | terms(category) | 1 | 2730 ms | 6950 ms | pushed | 2967 ms | 8411 ms | pushed | agrees |
| 1B | 1node4xl | terms(category) > date_histogram month > sum(price) | 1 | 15.15 s | 108.12 s | pushed | 13.47 s | 106.71 s | pushed | agrees |
| 1B | 1node4xl | terms(category) > terms(rating) > avg(price) | 1 | 5970 ms | 102.98 s | pushed | 6815 ms | 81.71 s | pushed | agrees |
| 1B | 1node4xl | terms(rating) | 1 | 1100 ms | 25.13 s | pushed | 1524 ms | 26.41 s | pushed | agrees |
| 1B | 3node | composite(category, rating) size 10 | 1 | 1320 ms | 17.48 s | pushed | 1537 ms | 17.64 s | pushed | agrees |
| 1B | 3node | composite(category, rating) size 10 page 2 | 1 | 1380 ms | 17.60 s | pushed | 1537 ms | 17.64 s | pushed | agrees |
| 1B | 3node | composite(category, ts 1d) size 10 | 1 | 6750 ms | 16.58 s | pushed | 6575 ms | 17.64 s | pushed | agrees |
| 1B | 3node | date_histogram month + sum(price) | 1 | 4440 ms | 24.72 s | pushed | 3604 ms | 21.21 s | pushed | agrees |
| 1B | 3node | sum(price) | 1 | 959 ms | 3990 ms | pushed | 919 ms | 4111 ms | pushed | agrees |
| 1B | 3node | terms(category) | 1 | 1030 ms | 3010 ms | pushed | 1062 ms | 2878 ms | pushed | agrees |
| 1B | 3node | terms(category) > date_histogram month > sum(price) | 1 | 5260 ms | 30.99 s | pushed | 4574 ms | 35.64 s | pushed | agrees |
| 1B | 3node | terms(category) > terms(rating) > avg(price) | 1 | 2170 ms | 30.76 s | pushed | 2347 ms | 27.31 s | pushed | agrees |
| 1B | 3node | terms(rating) | 1 | 531 ms | 8330 ms | pushed | 581 ms | 8878 ms | pushed | agrees |
| 1B | 4node | avg(price) | 8 | 793 ms | 448 ms | lucene | 717 ms | 486 ms | lucene | agrees |
| 1B | 4node | cardinality(user_id) | 8 | 22.60 s | 4030 ms | lucene | 25.52 s | 4433 ms | lucene | agrees |
| 1B | 4node | composite(category, rating) size 10 | 8 | 1090 ms | 1870 ms | pushed | 1181 ms | 1755 ms | pushed | agrees |
| 1B | 4node | date_histogram 1d + sum(price) | 8 | 2650 ms | 1660 ms | lucene | 2733 ms | 2089 ms | lucene | agrees |
| 1B | 4node | date_histogram month + sum(price) | 8 | 3300 ms | 2450 ms | lucene | 2730 ms | 2089 ms | lucene | agrees |
| 1B | 4node | extended_stats(price) | 8 | 826 ms | 1050 ms | pushed | 738 ms | 1048 ms | pushed | reported (1.27x) |
| 1B | 4node | filter rating=5 + terms(category) | 8 | 4260 ms | 15.90 s | pushed | 2709 ms | 15.84 s | pushed | agrees |
| 1B | 4node | filters(rating=5, category=cat150, price>=500) | 8 | 1550 ms | 1610 ms | pushed | 1547 ms | 1858 ms | pushed | reported (1.04x) |
| 1B | 4node | percentiles(price) | 8 | 2040 ms | 3350 ms | pushed | 1855 ms | 3402 ms | pushed | agrees |
| 1B | 4node | range(price 4 band) | 8 | 1360 ms | 1110 ms | lucene | 1279 ms | 1183 ms | lucene | reported (1.23x) |
| 1B | 4node | stats(price) | 8 | 816 ms | 475 ms | lucene | 717 ms | 486 ms | lucene | agrees |
| 1B | 4node | sum(price) | 8 | 784 ms | 442 ms | lucene | 717 ms | 486 ms | lucene | agrees |
| 1B | 4node | terms(category) | 8 | 848 ms | 346 ms | lucene | 824 ms | 370 ms | lucene | agrees |
| 1B | 4node | terms(category) > terms(rating) > avg(price) | 8 | 1650 ms | 3050 ms | pushed | 1788 ms | 2661 ms | pushed | agrees |
| 1B | 4node | terms(rating) | 8 | 346 ms | 780 ms | pushed | 463 ms | 933 ms | pushed | agrees |
| 1B | 4node | terms(user_id) size 10 | 8 | 4740 ms | 17.80 s | pushed | 5142 ms | 5620 ms | pushed | agrees |
| 1B | 6node | composite(category, rating) size 10 | 1 | 836 ms | 8800 ms | pushed | 825 ms | 8878 ms | pushed | agrees |
| 1B | 6node | composite(category, rating) size 10 page 2 | 1 | 848 ms | 8920 ms | pushed | 825 ms | 8878 ms | pushed | agrees |
| 1B | 6node | composite(category, ts 1d) size 10 | 1 | 3870 ms | 8440 ms | pushed | 3611 ms | 8878 ms | pushed | agrees |
| 1B | 6node | date_histogram month + sum(price) | 1 | 2310 ms | 12.27 s | pushed | 1857 ms | 10.66 s | pushed | agrees |
| 1B | 6node | sum(price) | 1 | 586 ms | 2030 ms | pushed | 514 ms | 2111 ms | pushed | agrees |
| 1B | 6node | terms(category) | 1 | 636 ms | 1530 ms | pushed | 586 ms | 1494 ms | pushed | agrees |
| 1B | 6node | terms(category) > date_histogram month > sum(price) | 1 | 2810 ms | 15.54 s | pushed | 2350 ms | 17.88 s | pushed | agrees |
| 1B | 6node | terms(category) > terms(rating) > avg(price) | 1 | 1210 ms | 15.44 s | pushed | 1230 ms | 13.71 s | pushed | agrees |
| 1B | 6node | terms(rating) | 1 | 386 ms | 4210 ms | pushed | 345 ms | 4494 ms | pushed | agrees |
| 1B | 6node | terms(user_id) size 10 | 1 | 3260 ms | 28.64 s | pushed | 3464 ms | 29.49 s | pushed | agrees |

129 pairs agree, 0 disagree, 5 inside the 1.3x band.
