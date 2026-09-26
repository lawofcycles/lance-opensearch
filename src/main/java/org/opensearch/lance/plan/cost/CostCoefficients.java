/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.cost;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The coefficients of {@link CostModel}, fitted to the measured warm
 * latencies of the aggregation shapes on the 20M, 100M and 1B row
 * tables ({@code src/test/resources/cost/measurements.csv}) by
 * {@code scripts/fit-cost-coefficients.py} and rounded to two
 * significant digits; the fit and its residuals are in
 * {@code src/test/resources/cost/fit-report.md}. Every name ends in its
 * unit. "Per Mrow thread" means per million rows one thread processes:
 * the per node row share divided by the path's parallelism (the pushed
 * scan's {@code pushdown_parallelism}, the aggregator path's
 * {@code slices}); "per Mrow node" per million rows one node holds,
 * and "per Mrow" per million rows of the whole table.
 *
 * <p>The structural constants at the end are not fitted: they are the
 * assumptions the model makes where the statistics carry no figure, and
 * the thresholds that switch between its regimes.
 */
public final class CostCoefficients {

    private CostCoefficients() {}

    // ---- both paths ------------------------------------------------------

    /** Request latency an object store adds for opening the table and its fragments. */
    public static final double OBJECT_STORE_OPEN_MS = 74;

    // ---- pushed scan (Lance dataset scan with the aggregate inside) --------

    /** Fixed cost of a pushed request: transport, scan setup, reply. */
    public static final double PUSHED_FIXED_MS = 19;
    /** Transfer of the columns the scan reads, per GB one node pulls from the object store; not divided by the parallelism. */
    public static final double OBJECT_STORE_READ_MS_PER_GB_PER_NODE = 170;
    /** Decoding the scanned columns, per 8 bytes of row width. */
    public static final double PUSHED_DECODE_MS_PER_MROW_THREAD_PER_8_BYTES = 9;
    /** Hashing a string group key. */
    public static final double PUSHED_STRING_KEY_MS_PER_MROW_THREAD = 17;
    /** Hashing a numeric group key. */
    public static final double PUSHED_NUMERIC_KEY_MS_PER_MROW_THREAD = 1.8;
    /** Truncating a timestamp to a histogram bucket and hashing it. */
    public static final double PUSHED_DATE_KEY_MS_PER_MROW_THREAD = 45;
    /** Evaluating the range bands of a range key. */
    public static final double PUSHED_RANGE_KEY_MS_PER_MROW_THREAD = 18;
    /** Evaluating the predicates of a filters key. */
    public static final double PUSHED_FILTERS_KEY_MS_PER_MROW_THREAD = 12;
    /** A composite date histogram source. */
    public static final double PUSHED_COMPOSITE_DATE_KEY_MS_PER_MROW_THREAD = 100;
    /** The sums of squares an extended_stats adds over a stats. */
    public static final double PUSHED_EXTENDED_STATS_MS_PER_MROW_THREAD = 0.89;
    /** The bin counts of a percentiles, on top of its second scan pass. */
    public static final double PUSHED_PERCENTILES_MS_PER_MROW_THREAD = 17;
    /** Hash table misses once the groups exceed {@link #LARGE_GROUPS}. */
    public static final double PUSHED_LARGE_GROUPS_MS_PER_MROW_THREAD = 100;
    /** Evaluating a query filter inside the scan, over every row. */
    public static final double PUSHED_FILTER_EVAL_MS_PER_MROW_THREAD = 12;
    /**
     * Materialising the row address set the filter's scalar index
     * answers with, per million matching rows of the whole table: the
     * index covers the table, so every node reads the full set whatever
     * its share of the rows, and the parallel scans of a node share it.
     * The 1B filtered aggregate measured 7.0 s on one node, 4.3 s on
     * four and 3.8 s on six, a floor the per thread terms cannot reach.
     */
    public static final double PUSHED_FILTER_MATCH_MS_PER_MROW = 17;
    /** Merging the group rows the parallel scans of one node return, per million rows merged. */
    public static final double PUSHED_MERGE_MS_PER_MGROUP = 460;
    /** Hashing every row's value into the HyperLogLog++ sketch, the same work as a string group key. */
    public static final double PUSHED_CARDINALITY_HASH_MS_PER_MROW_THREAD = 19;
    /** Feeding distinct values into the HyperLogLog++ sketch on one thread, per million values. */
    public static final double PUSHED_CARDINALITY_MS_PER_MVALUE = 290;

    // ---- Lucene aggregator path (leaf readers over the warm column store) --

    /** Fixed cost of an aggregator request: transport, reader open, reply. */
    public static final double LUCENE_FIXED_MS = 18;
    /** Reading one column from the off heap column store; a keyword terms key costs nothing beyond this. */
    public static final double LUCENE_COLUMN_MS_PER_MROW_THREAD = 8.3;
    /** Hashing a numeric terms key. */
    public static final double LUCENE_NUMERIC_KEY_MS_PER_MROW_THREAD = 17;
    /** Rounding a timestamp to a histogram bucket. */
    public static final double LUCENE_DATE_KEY_MS_PER_MROW_THREAD = 14;
    /** Placing a value in its range band. */
    public static final double LUCENE_RANGE_KEY_MS_PER_MROW_THREAD = 26;
    /** Evaluating the filters of a filters key. */
    public static final double LUCENE_FILTERS_KEY_MS_PER_MROW_THREAD = 31;
    /** One source of a composite aggregation. */
    public static final double LUCENE_COMPOSITE_SOURCE_MS_PER_MROW_THREAD = 18;
    /** One nested bucket level below the first. */
    public static final double LUCENE_NESTED_LEVEL_MS_PER_MROW_THREAD = 17;
    /** One sum / avg / min / max / value_count / stats metric. */
    public static final double LUCENE_SIMPLE_METRIC_MS_PER_MROW_THREAD = 3.6;
    /**
     * One simple metric collected under a bucket key, on top of the
     * metric's own cost: the bucket aggregator hands every row to the
     * metric with its bucket ordinal, so the metric's arrays are
     * addressed per bucket rather than accumulated in place. On the 20M
     * table a terms(category) alone collected in 38 ms and with an
     * avg or sum under it in 146 to 201 ms.
     */
    public static final double LUCENE_BUCKET_METRIC_MS_PER_MROW_THREAD = 32;
    /** One extended_stats metric. */
    public static final double LUCENE_EXTENDED_STATS_MS_PER_MROW_THREAD = 19;
    /** One percentiles / percentile_ranks TDigest. */
    public static final double LUCENE_PERCENTILES_MS_PER_MROW_THREAD = 97;
    /** One cardinality HyperLogLog++ hashing every row's value; see {@link #LUCENE_CARDINALITY_ORDINALS_MAX_DISTINCT}. */
    public static final double LUCENE_CARDINALITY_MS_PER_MROW_THREAD = 130;
    /**
     * Hash table misses once the groups exceed {@link #LARGE_GROUPS},
     * per million rows one node holds rather than one thread: the
     * slices' tables compete for the node's memory, and the measured
     * terms over ten million distinct values ran only twice as fast
     * with eight slices as with one, not eight times.
     */
    public static final double LUCENE_LARGE_GROUPS_MS_PER_MROW_NODE = 95;
    /** Evaluating a query filter on the Lucene side, over every row. */
    public static final double LUCENE_FILTER_EVAL_MS_PER_MROW_THREAD = 200;

    // ---- structural constants (not fitted) --------------------------------

    /**
     * Tables below this row count keep the placeholder costs the planner
     * used before the fit: the measurements start at 20M rows, and on a
     * table this small every path answers in the fixed cost, so the
     * fitted model has nothing to say and the fixed preference order
     * (pushed when a rule folds, Lucene otherwise) stands.
     */
    public static final long FITTED_MODEL_MIN_ROWS = 1_000_000L;
    /** Group count above which the per row hash table misses apply on both paths. */
    public static final long LARGE_GROUPS = 1_000_000L;
    /**
     * Distinct values above which the Lucene cardinality aggregator
     * hashes every row's bytes instead of collecting ordinals. The
     * aggregator takes the ordinals path when one bit per ordinal stays
     * under a quarter of its precision 14 sketch (2^14 bytes), that is
     * 2^14 / 4 bytes of bits, 32,768 ordinals; a numeric column always
     * hashes, and cheaply, which the model does not tell apart.
     */
    public static final long LUCENE_CARDINALITY_ORDINALS_MAX_DISTINCT = 32_768L;
    /** Bytes per row assumed for a string column with a bitmap index (dictionary encoded). */
    public static final double DICTIONARY_STRING_BYTES_PER_ROW = 2;
    /** Bytes per row assumed for a string or binary column without one. */
    public static final double STRING_BYTES_PER_ROW = 16;
    /** Bytes per row assumed for a column of a type the width table does not cover (struct, list, decimal). */
    public static final double OTHER_BYTES_PER_ROW = 16;
    /** Years a date histogram's span is assumed to cover when the statistics carry no bounds. */
    public static final double DATE_SPAN_ASSUMED_YEARS = 10;
    /** Group rows one pushed scan keeps for a single level terms ordered by count or by a metric: {@code shard_size} times this. */
    public static final int TERMS_TOP_K_RETENTION_FACTOR = 4;
    /** Share of the rows Calcite assumes a group key of unknown cardinality has, its default aggregate estimate. */
    public static final double UNKNOWN_KEY_DISTINCT_SHARE = 0.1;

    // ---- pushed filter encodings (see CostModel#filterEncodingMillis) ------

    /**
     * Shipping one KB of a plan's filter encoding to one data node
     * inside the fragment request: a structural assumption of 1 GB/s
     * transport throughput, not a measurement. The request's fixed
     * transport cost is inside the fixed terms above; only the
     * encoding's size varies between the two forms of a pushed filter.
     * The Substrait form ships its SQL as well (the executor's column
     * loads take SQL only), so the two forms differ by the Substrait
     * bytes alone, and the SQL form is chosen once those bytes times the
     * data node count exceed {@link #FILTER_SQL_TIE_BREAK_MS} divided by
     * this constant, 50 KB: about 4,000 short {@code terms} values on
     * one node, about 1,000 on four. A 200 value {@code terms} list
     * (under 3 KB of Substrait) ships as Substrait on any cluster of
     * fewer than about 18 data nodes.
     */
    public static final double FILTER_WIRE_MS_PER_KB_PER_NODE = 0.001;
    /**
     * Charged to the SQL encoding of a pushed filter so that the
     * Substrait encoding wins when the two otherwise cost the same,
     * which the measurements say they do: on the build farm a count
     * only scan of a small table planned a filter of 7 to 650
     * characters in 0.6 to 2.4 ms over the unfiltered scan in either
     * encoding, with the Substrait form 30 to 70 microseconds slower
     * on the smallest predicates and inside the run to run noise on
     * the larger ones, because Lance turns both into the same
     * DataFusion expression and the planning of that expression is
     * what costs. The constant is a preference for the encoding whose
     * field references are positional and whose message is typed, not
     * a measured difference; the wire term overtakes it once the
     * Substrait bytes times the data node count pass 50 KB
     * ({@code 0.05 / 0.001} KB), so only a {@code terms} list in the
     * thousands of values on one node, or in the high hundreds on a
     * wide cluster, ships as SQL.
     */
    public static final double FILTER_SQL_TIE_BREAK_MS = 0.05;

    /** The fitted coefficients by name, for the tests that check them against the fit report. */
    public static Map<String, Double> fitted() {
        Map<String, Double> named = new LinkedHashMap<>();
        named.put("PUSHED_FIXED_MS", PUSHED_FIXED_MS);
        named.put("OBJECT_STORE_OPEN_MS", OBJECT_STORE_OPEN_MS);
        named.put("OBJECT_STORE_READ_MS_PER_GB_PER_NODE", OBJECT_STORE_READ_MS_PER_GB_PER_NODE);
        named.put("PUSHED_DECODE_MS_PER_MROW_THREAD_PER_8_BYTES", PUSHED_DECODE_MS_PER_MROW_THREAD_PER_8_BYTES);
        named.put("PUSHED_STRING_KEY_MS_PER_MROW_THREAD", PUSHED_STRING_KEY_MS_PER_MROW_THREAD);
        named.put("PUSHED_NUMERIC_KEY_MS_PER_MROW_THREAD", PUSHED_NUMERIC_KEY_MS_PER_MROW_THREAD);
        named.put("PUSHED_DATE_KEY_MS_PER_MROW_THREAD", PUSHED_DATE_KEY_MS_PER_MROW_THREAD);
        named.put("PUSHED_RANGE_KEY_MS_PER_MROW_THREAD", PUSHED_RANGE_KEY_MS_PER_MROW_THREAD);
        named.put("PUSHED_FILTERS_KEY_MS_PER_MROW_THREAD", PUSHED_FILTERS_KEY_MS_PER_MROW_THREAD);
        named.put("PUSHED_COMPOSITE_DATE_KEY_MS_PER_MROW_THREAD", PUSHED_COMPOSITE_DATE_KEY_MS_PER_MROW_THREAD);
        named.put("PUSHED_EXTENDED_STATS_MS_PER_MROW_THREAD", PUSHED_EXTENDED_STATS_MS_PER_MROW_THREAD);
        named.put("PUSHED_PERCENTILES_MS_PER_MROW_THREAD", PUSHED_PERCENTILES_MS_PER_MROW_THREAD);
        named.put("PUSHED_LARGE_GROUPS_MS_PER_MROW_THREAD", PUSHED_LARGE_GROUPS_MS_PER_MROW_THREAD);
        named.put("PUSHED_FILTER_EVAL_MS_PER_MROW_THREAD", PUSHED_FILTER_EVAL_MS_PER_MROW_THREAD);
        named.put("PUSHED_FILTER_MATCH_MS_PER_MROW", PUSHED_FILTER_MATCH_MS_PER_MROW);
        named.put("PUSHED_MERGE_MS_PER_MGROUP", PUSHED_MERGE_MS_PER_MGROUP);
        named.put("PUSHED_CARDINALITY_HASH_MS_PER_MROW_THREAD", PUSHED_CARDINALITY_HASH_MS_PER_MROW_THREAD);
        named.put("PUSHED_CARDINALITY_MS_PER_MVALUE", PUSHED_CARDINALITY_MS_PER_MVALUE);
        named.put("LUCENE_FIXED_MS", LUCENE_FIXED_MS);
        named.put("LUCENE_COLUMN_MS_PER_MROW_THREAD", LUCENE_COLUMN_MS_PER_MROW_THREAD);
        named.put("LUCENE_NUMERIC_KEY_MS_PER_MROW_THREAD", LUCENE_NUMERIC_KEY_MS_PER_MROW_THREAD);
        named.put("LUCENE_DATE_KEY_MS_PER_MROW_THREAD", LUCENE_DATE_KEY_MS_PER_MROW_THREAD);
        named.put("LUCENE_RANGE_KEY_MS_PER_MROW_THREAD", LUCENE_RANGE_KEY_MS_PER_MROW_THREAD);
        named.put("LUCENE_FILTERS_KEY_MS_PER_MROW_THREAD", LUCENE_FILTERS_KEY_MS_PER_MROW_THREAD);
        named.put("LUCENE_COMPOSITE_SOURCE_MS_PER_MROW_THREAD", LUCENE_COMPOSITE_SOURCE_MS_PER_MROW_THREAD);
        named.put("LUCENE_NESTED_LEVEL_MS_PER_MROW_THREAD", LUCENE_NESTED_LEVEL_MS_PER_MROW_THREAD);
        named.put("LUCENE_SIMPLE_METRIC_MS_PER_MROW_THREAD", LUCENE_SIMPLE_METRIC_MS_PER_MROW_THREAD);
        named.put("LUCENE_BUCKET_METRIC_MS_PER_MROW_THREAD", LUCENE_BUCKET_METRIC_MS_PER_MROW_THREAD);
        named.put("LUCENE_EXTENDED_STATS_MS_PER_MROW_THREAD", LUCENE_EXTENDED_STATS_MS_PER_MROW_THREAD);
        named.put("LUCENE_PERCENTILES_MS_PER_MROW_THREAD", LUCENE_PERCENTILES_MS_PER_MROW_THREAD);
        named.put("LUCENE_CARDINALITY_MS_PER_MROW_THREAD", LUCENE_CARDINALITY_MS_PER_MROW_THREAD);
        named.put("LUCENE_LARGE_GROUPS_MS_PER_MROW_NODE", LUCENE_LARGE_GROUPS_MS_PER_MROW_NODE);
        named.put("LUCENE_FILTER_EVAL_MS_PER_MROW_THREAD", LUCENE_FILTER_EVAL_MS_PER_MROW_THREAD);
        return named;
    }
}
