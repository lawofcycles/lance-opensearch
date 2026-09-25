#!/usr/bin/env python3
"""Fit the planner's latency cost model to the measured aggregation shapes.

Reads src/test/resources/cost/measurements.csv (one row per measured
(table, cluster, shape, path, latency)), builds the same per operator
features CostModel.java computes, fits every coefficient by non negative
least squares, and prints the coefficient table, the per row residuals and
the (shape, table, cluster) pairs where the model with the rounded
coefficients picks the other path than the measurement.

The fit is linear in the coefficients (the model is a sum of
coefficient x quantity terms) and every row is scaled by 1 / measured
latency before the least squares, so the objective is the sum of squared
relative errors: a 30 ms row and a 50 s row weigh the same. A plain linear
least squares would fit the 1B rows alone, and a fit on the log of the
latency would not be linear in the coefficients. Non negativity is a
physical constraint (no term of the model makes a request faster).

Most quantities are rows per thread (the node's row share divided by the
path's parallelism). Three are not: the object store transfer and the
Lucene hash table misses above LARGE_GROUPS are per node (bandwidth bound,
the parallel threads share the node's memory and network), and the row
address set a pushed filter materialises from a scalar index covers the
whole table on every node, so it is per table row. The structural
constants (LARGE_GROUPS, LUCENE_CARDINALITY_ORDINALS_MAX_DISTINCT) mirror
CostCoefficients.java and are not fitted.

Standard library only. Usage:

    python3 scripts/fit-cost-coefficients.py [--csv PATH] [--report PATH]

Without --report the report is written to stdout; the committed report is
src/test/resources/cost/fit-report.md and must match this script's output.
"""

import argparse
import csv
import math
import sys
from collections import defaultdict

LARGE_GROUPS = 1_000_000
LUCENE_CARDINALITY_ORDINALS_MAX_DISTINCT = 32_768
CHOICE_MARGIN = 1.3

PUSHED_NAMES = [
    "PUSHED_FIXED_MS",
    "OBJECT_STORE_OPEN_MS",
    "OBJECT_STORE_READ_MS_PER_GB_PER_NODE",
    "PUSHED_DECODE_MS_PER_MROW_THREAD_PER_8_BYTES",
    "PUSHED_STRING_KEY_MS_PER_MROW_THREAD",
    "PUSHED_NUMERIC_KEY_MS_PER_MROW_THREAD",
    "PUSHED_DATE_KEY_MS_PER_MROW_THREAD",
    "PUSHED_RANGE_KEY_MS_PER_MROW_THREAD",
    "PUSHED_FILTERS_KEY_MS_PER_MROW_THREAD",
    "PUSHED_COMPOSITE_DATE_KEY_MS_PER_MROW_THREAD",
    "PUSHED_EXTENDED_STATS_MS_PER_MROW_THREAD",
    "PUSHED_PERCENTILES_MS_PER_MROW_THREAD",
    "PUSHED_LARGE_GROUPS_MS_PER_MROW_THREAD",
    "PUSHED_FILTER_EVAL_MS_PER_MROW_THREAD",
    "PUSHED_FILTER_MATCH_MS_PER_MROW",
    "PUSHED_MERGE_MS_PER_MGROUP",
    "PUSHED_CARDINALITY_HASH_MS_PER_MROW_THREAD",
    "PUSHED_CARDINALITY_MS_PER_MVALUE",
]
LUCENE_NAMES = [
    "LUCENE_FIXED_MS",
    "OBJECT_STORE_OPEN_MS",
    "LUCENE_COLUMN_MS_PER_MROW_THREAD",
    "LUCENE_NUMERIC_KEY_MS_PER_MROW_THREAD",
    "LUCENE_DATE_KEY_MS_PER_MROW_THREAD",
    "LUCENE_RANGE_KEY_MS_PER_MROW_THREAD",
    "LUCENE_FILTERS_KEY_MS_PER_MROW_THREAD",
    "LUCENE_COMPOSITE_SOURCE_MS_PER_MROW_THREAD",
    "LUCENE_NESTED_LEVEL_MS_PER_MROW_THREAD",
    "LUCENE_SIMPLE_METRIC_MS_PER_MROW_THREAD",
    "LUCENE_BUCKET_METRIC_MS_PER_MROW_THREAD",
    "LUCENE_EXTENDED_STATS_MS_PER_MROW_THREAD",
    "LUCENE_PERCENTILES_MS_PER_MROW_THREAD",
    "LUCENE_CARDINALITY_MS_PER_MROW_THREAD",
    "LUCENE_LARGE_GROUPS_MS_PER_MROW_NODE",
    "LUCENE_FILTER_EVAL_MS_PER_MROW_THREAD",
]
ALL_NAMES = PUSHED_NAMES + [n for n in LUCENE_NAMES if n not in PUSHED_NAMES]

UNITS = {
    "MS": "ms per request",
    "MS_PER_GB_PER_NODE": "ms per GB of column bytes one node reads from the object store",
    "MS_PER_MROW_THREAD_PER_8_BYTES": "ms per million rows per thread per 8 bytes of row width",
    "MS_PER_MROW_THREAD": "ms per million rows one thread processes",
    "MS_PER_MROW_NODE": "ms per million rows one node holds",
    "MS_PER_MROW": "ms per million rows of the whole table",
    "MS_PER_MGROUP": "ms per million group rows merged",
    "MS_PER_MVALUE": "ms per million distinct values fed to the sketch",
}


def unit_of(name):
    """The unit a coefficient name ends with, as the UNITS table spells it."""
    for suffix in sorted(UNITS, key=len, reverse=True):
        if name.endswith(suffix):
            return UNITS[suffix]
    raise ValueError(name)


def num(row, key):
    return float(row[key])


def features(row):
    """The per operator quantities of one row, in the order of PUSHED_NAMES / LUCENE_NAMES."""
    rows = num(row, "rows")
    nodes = num(row, "nodes")
    par = num(row, "parallelism")
    slices = num(row, "slices")
    obj = 1.0 if row["storage"] == "object_store" else 0.0
    groups = num(row, "groups")
    columns = num(row, "columns_read")
    bytes_per_row = num(row, "bytes_per_row")
    passes = num(row, "scan_passes")
    string_keys = num(row, "string_keys")
    numeric_keys = num(row, "numeric_keys")
    date_keys = num(row, "date_keys")
    range_keys = num(row, "range_keys")
    filter_keys = num(row, "filter_keys")
    composite_date_keys = num(row, "composite_date_keys")
    composite = num(row, "composite")
    simple_metrics = num(row, "simple_metrics")
    extended_stats = num(row, "extended_stats")
    percentiles = num(row, "percentiles")
    cardinality = num(row, "cardinality")
    distinct = num(row, "distinct_values")
    sel = num(row, "filter_selectivity")
    filtered = 1.0 if sel < 1.0 else 0.0

    rows_per_node = rows / nodes
    mrow_node = rows_per_node / 1e6
    mrow_thread_p = rows_per_node / par / 1e6
    mrow_thread_l = rows_per_node / slices / 1e6
    mrow_table = rows / 1e6
    gb_per_node = rows * bytes_per_row * passes / nodes / 1e9
    large = 1.0 if groups > LARGE_GROUPS else 0.0
    merged_mgroups = min(num(row, "merged_groups"), rows_per_node) / 1e6
    non_composite_keys = (string_keys + numeric_keys + date_keys + range_keys + filter_keys) * (1.0 - composite)
    composite_sources = (string_keys + numeric_keys + composite_date_keys) * composite
    nested_levels = max(0.0, non_composite_keys - 1.0)
    any_key = 1.0 if string_keys + numeric_keys + date_keys + range_keys + filter_keys + composite_date_keys > 0 else 0.0
    bucket_metrics = simple_metrics * any_key
    cardinality_hashed = cardinality if distinct > LUCENE_CARDINALITY_ORDINALS_MAX_DISTINCT else 0.0

    if row["path"] == "pushed":
        values = [
            1.0,
            obj,
            gb_per_node * obj,
            mrow_thread_p * bytes_per_row / 8.0 * passes,
            mrow_thread_p * string_keys * sel,
            mrow_thread_p * numeric_keys * sel,
            mrow_thread_p * date_keys * sel,
            mrow_thread_p * range_keys * sel,
            mrow_thread_p * filter_keys * sel,
            mrow_thread_p * composite_date_keys * sel,
            mrow_thread_p * extended_stats * sel,
            mrow_thread_p * percentiles * sel,
            mrow_thread_p * large * sel,
            mrow_thread_p * filtered,
            mrow_table * sel * filtered,
            merged_mgroups * par,
            mrow_thread_p * cardinality * sel,
            min(distinct, mrow_thread_p * 1e6) * par / 1e6 * cardinality,
        ]
        return dict(zip(PUSHED_NAMES, values))
    values = [
        1.0,
        obj,
        mrow_thread_l * columns * sel,
        mrow_thread_l * numeric_keys * (1.0 - composite) * sel,
        mrow_thread_l * date_keys * sel,
        mrow_thread_l * range_keys * sel,
        mrow_thread_l * filter_keys * sel,
        mrow_thread_l * composite_sources * sel,
        mrow_thread_l * nested_levels * sel,
        mrow_thread_l * simple_metrics * sel,
        mrow_thread_l * bucket_metrics * sel,
        mrow_thread_l * extended_stats * sel,
        mrow_thread_l * percentiles * sel,
        mrow_thread_l * cardinality_hashed * sel,
        mrow_node * large * sel,
        mrow_thread_l * filtered,
    ]
    return dict(zip(LUCENE_NAMES, values))


def predict(coefficients, row):
    return sum(coefficients[name] * value for name, value in features(row).items())


# ---------------------------------------------------------------------------
# Non negative least squares (Lawson and Hanson active set), pure Python.
# ---------------------------------------------------------------------------


def solve_least_squares(a, b):
    """Unconstrained least squares of a (m x n) and b (m) through the normal equations."""
    n = len(a[0])
    ata = [[0.0] * n for _ in range(n)]
    atb = [0.0] * n
    for i, row in enumerate(a):
        for j in range(n):
            if row[j] == 0.0:
                continue
            atb[j] += row[j] * b[i]
            for k in range(n):
                ata[j][k] += row[j] * row[k]
    for j in range(n):
        ata[j][j] += 1e-12
    return gauss(ata, atb)


def gauss(matrix, rhs):
    n = len(rhs)
    m = [row[:] + [rhs[i]] for i, row in enumerate(matrix)]
    for col in range(n):
        pivot = max(range(col, n), key=lambda r: abs(m[r][col]))
        m[col], m[pivot] = m[pivot], m[col]
        if abs(m[col][col]) < 1e-300:
            continue
        for r in range(n):
            if r != col and m[r][col] != 0.0:
                factor = m[r][col] / m[col][col]
                for c in range(col, n + 1):
                    m[r][c] -= factor * m[col][c]
    return [m[i][n] / m[i][i] if abs(m[i][i]) > 1e-300 else 0.0 for i in range(n)]


def nnls(a, b, max_iterations=500):
    m = len(a)
    n = len(a[0])
    x = [0.0] * n
    passive = set()
    tolerance = 1e-10

    def residual(vec):
        return [b[i] - sum(a[i][j] * vec[j] for j in range(n)) for i in range(m)]

    def gradient(res):
        return [sum(a[i][j] * res[i] for i in range(m)) for j in range(n)]

    w = gradient(residual(x))
    iterations = 0
    while iterations < max_iterations:
        iterations += 1
        candidates = [j for j in range(n) if j not in passive and w[j] > tolerance]
        if not candidates:
            break
        passive.add(max(candidates, key=lambda j: w[j]))
        while True:
            cols = sorted(passive)
            sub = [[a[i][j] for j in cols] for i in range(m)]
            z_sub = solve_least_squares(sub, b)
            z = [0.0] * n
            for j, value in zip(cols, z_sub):
                z[j] = value
            if all(z[j] > tolerance for j in cols):
                x = z
                break
            alpha = min(x[j] / (x[j] - z[j]) for j in cols if z[j] <= tolerance and x[j] - z[j] > 0)
            x = [x[j] + alpha * (z[j] - x[j]) for j in range(n)]
            for j in cols:
                if x[j] <= tolerance:
                    x[j] = 0.0
                    passive.discard(j)
        w = gradient(residual(x))
    return x


def round_to_significant(value, digits=2):
    if value == 0.0:
        return 0.0
    magnitude = math.floor(math.log10(abs(value)))
    factor = 10 ** (digits - 1 - magnitude)
    return round(value * factor) / factor


def fit(rows):
    a = []
    b = []
    for row in rows:
        f = features(row)
        latency = num(row, "latency_ms")
        a.append([f.get(name, 0.0) / latency for name in ALL_NAMES])
        b.append(1.0)
    x = nnls(a, b)
    return dict(zip(ALL_NAMES, x))


def format_ms(value):
    return f"{value / 1000:.2f} s" if value >= 10_000 else f"{value:.0f} ms"


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--csv", default="src/test/resources/cost/measurements.csv")
    parser.add_argument("--report", default=None)
    args = parser.parse_args()

    with open(args.csv, newline="") as handle:
        rows = list(csv.DictReader(handle))
    fitted_rows = [r for r in rows if r["excluded"] == "0"]
    raw = fit(fitted_rows)
    rounded = {name: round_to_significant(value) for name, value in raw.items()}

    out = []
    out.append("# Cost coefficient fit")
    out.append("")
    out.append(
        f"Fitted on {len(fitted_rows)} measured rows ({len(rows) - len(fitted_rows)} rows excluded, see the note column) "
        "by non negative least squares on residuals scaled by 1 / measured latency (relative error). "
        "Coefficients are rounded to two significant digits; the rounded values are what CostCoefficients.java carries "
        "and what the residual and choice tables below use."
    )
    out.append("")
    out.append("## Coefficients")
    out.append("")
    out.append("| coefficient | fitted | rounded | unit |")
    out.append("|---|---|---|---|")
    for name in ALL_NAMES:
        out.append(f"| {name} | {raw[name]:.4g} | {rounded[name]:.4g} | {unit_of(name)} |")
    out.append("")

    out.append("## Residuals")
    out.append("")
    out.append("Per fitted row: predicted / measured with the rounded coefficients. Rows are in CSV order.")
    out.append("")
    out.append("| round | table | cluster | shape | path | slices | measured | predicted | pred / meas |")
    out.append("|---|---|---|---|---|---|---|---|---|")
    log_errors = []
    for row in fitted_rows:
        measured = num(row, "latency_ms")
        predicted = predict(rounded, row)
        ratio = predicted / measured
        log_errors.append(abs(math.log(ratio)))
        out.append(
            f"| {row['round']} | {row['table']} | {row['cluster']} | {row['shape']} | {row['path']} | {row['slices']} "
            f"| {format_ms(measured)} | {format_ms(predicted)} | {ratio:.2f} |"
        )
    out.append("")
    median = sorted(log_errors)[len(log_errors) // 2]
    within_2x = sum(1 for e in log_errors if e <= math.log(2.0))
    out.append(
        f"Median absolute log ratio {median:.3f} (a factor of {math.exp(median):.2f}); "
        f"{within_2x} of {len(log_errors)} rows within a factor of 2."
    )
    out.append("")

    out.append("## Choices")
    out.append("")
    out.append(
        f"Every (shape, table, cluster) with both a pushed and a Lucene measurement (excluded rows on either side "
        f"stay out). Pairs whose measured latencies "
        f"differ by more than {CHOICE_MARGIN}x are asserted by CostModelTests; pairs inside that band are reported only. "
        "A pair is listed once per Lucene slices value measured."
    )
    out.append("")
    out.append("| table | cluster | shape | slices | measured pushed | measured lucene | measured winner | model pushed | model lucene | model winner | verdict |")
    out.append("|---|---|---|---|---|---|---|---|---|---|---|")
    pairs = defaultdict(dict)
    for row in rows:
        if row["excluded"] != "0":
            continue
        key = (row["table"], row["cluster"], row["shape"])
        pairs[key].setdefault(row["path"], []).append(row)
    disagreements = []
    agreed = 0
    reported = 0
    for key in sorted(pairs, key=lambda k: (["20M", "100M", "1B"].index(k[0]), k[1], k[2])):
        entry = pairs[key]
        if "pushed" not in entry or "lucene" not in entry:
            continue
        pushed_row = sorted(entry["pushed"], key=lambda r: r["round"])[-1]
        for lucene_row in sorted(entry["lucene"], key=lambda r: (int(r["slices"]), r["round"])):
            mp = num(pushed_row, "latency_ms")
            ml = num(lucene_row, "latency_ms")
            pp = predict(rounded, pushed_row)
            pl = predict(rounded, lucene_row)
            measured_winner = "pushed" if mp < ml else "lucene"
            model_winner = "pushed" if pp <= pl else "lucene"
            margin = max(mp, ml) / min(mp, ml)
            if margin <= CHOICE_MARGIN:
                verdict = f"reported ({margin:.2f}x)"
                reported += 1
            elif measured_winner == model_winner:
                verdict = "agrees"
                agreed += 1
            else:
                verdict = f"DISAGREES ({margin:.2f}x)"
                disagreements.append((key, lucene_row["slices"], mp, ml, pp, pl))
            out.append(
                f"| {key[0]} | {key[1]} | {key[2]} | {lucene_row['slices']} | {format_ms(mp)} | {format_ms(ml)} | {measured_winner} "
                f"| {format_ms(pp)} | {format_ms(pl)} | {model_winner} | {verdict} |"
            )
    out.append("")
    out.append(f"{agreed} pairs agree, {len(disagreements)} disagree, {reported} inside the {CHOICE_MARGIN}x band.")
    if disagreements:
        out.append("")
        out.append("Disagreements:")
        for (table, cluster, shape), slices, mp, ml, pp, pl in disagreements:
            out.append(
                f"- {shape} on {table} / {cluster} (slices {slices}): measured pushed {format_ms(mp)} vs lucene {format_ms(ml)}, "
                f"model pushed {format_ms(pp)} vs lucene {format_ms(pl)}"
            )
    text = "\n".join(out) + "\n"
    if args.report:
        with open(args.report, "w") as handle:
            handle.write(text)
    else:
        sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
