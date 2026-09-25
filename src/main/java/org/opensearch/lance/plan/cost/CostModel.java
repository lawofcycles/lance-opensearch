/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.cost;

import static org.opensearch.lance.plan.cost.CostCoefficients.FILTER_SQL_TIE_BREAK_MS;
import static org.opensearch.lance.plan.cost.CostCoefficients.FILTER_WIRE_MS_PER_KB_PER_NODE;
import static org.opensearch.lance.plan.cost.CostCoefficients.LUCENE_BUCKET_METRIC_MS_PER_MROW_THREAD;
import static org.opensearch.lance.plan.cost.CostCoefficients.LUCENE_CARDINALITY_MS_PER_MROW_THREAD;
import static org.opensearch.lance.plan.cost.CostCoefficients.LUCENE_COLUMN_MS_PER_MROW_THREAD;
import static org.opensearch.lance.plan.cost.CostCoefficients.LUCENE_COMPOSITE_SOURCE_MS_PER_MROW_THREAD;
import static org.opensearch.lance.plan.cost.CostCoefficients.LUCENE_DATE_KEY_MS_PER_MROW_THREAD;
import static org.opensearch.lance.plan.cost.CostCoefficients.LUCENE_EXTENDED_STATS_MS_PER_MROW_THREAD;
import static org.opensearch.lance.plan.cost.CostCoefficients.LUCENE_FILTERS_KEY_MS_PER_MROW_THREAD;
import static org.opensearch.lance.plan.cost.CostCoefficients.LUCENE_FILTER_EVAL_MS_PER_MROW_THREAD;
import static org.opensearch.lance.plan.cost.CostCoefficients.LUCENE_FIXED_MS;
import static org.opensearch.lance.plan.cost.CostCoefficients.LUCENE_LARGE_GROUPS_MS_PER_MROW_NODE;
import static org.opensearch.lance.plan.cost.CostCoefficients.LUCENE_NESTED_LEVEL_MS_PER_MROW_THREAD;
import static org.opensearch.lance.plan.cost.CostCoefficients.LUCENE_NUMERIC_KEY_MS_PER_MROW_THREAD;
import static org.opensearch.lance.plan.cost.CostCoefficients.LUCENE_PERCENTILES_MS_PER_MROW_THREAD;
import static org.opensearch.lance.plan.cost.CostCoefficients.LUCENE_RANGE_KEY_MS_PER_MROW_THREAD;
import static org.opensearch.lance.plan.cost.CostCoefficients.LUCENE_SIMPLE_METRIC_MS_PER_MROW_THREAD;
import static org.opensearch.lance.plan.cost.CostCoefficients.OBJECT_STORE_OPEN_MS;
import static org.opensearch.lance.plan.cost.CostCoefficients.OBJECT_STORE_READ_MS_PER_GB_PER_NODE;
import static org.opensearch.lance.plan.cost.CostCoefficients.PUSHED_CARDINALITY_HASH_MS_PER_MROW_THREAD;
import static org.opensearch.lance.plan.cost.CostCoefficients.PUSHED_CARDINALITY_MS_PER_MVALUE;
import static org.opensearch.lance.plan.cost.CostCoefficients.PUSHED_COMPOSITE_DATE_KEY_MS_PER_MROW_THREAD;
import static org.opensearch.lance.plan.cost.CostCoefficients.PUSHED_DATE_KEY_MS_PER_MROW_THREAD;
import static org.opensearch.lance.plan.cost.CostCoefficients.PUSHED_DECODE_MS_PER_MROW_THREAD_PER_8_BYTES;
import static org.opensearch.lance.plan.cost.CostCoefficients.PUSHED_EXTENDED_STATS_MS_PER_MROW_THREAD;
import static org.opensearch.lance.plan.cost.CostCoefficients.PUSHED_FILTERS_KEY_MS_PER_MROW_THREAD;
import static org.opensearch.lance.plan.cost.CostCoefficients.PUSHED_FILTER_EVAL_MS_PER_MROW_THREAD;
import static org.opensearch.lance.plan.cost.CostCoefficients.PUSHED_FILTER_MATCH_MS_PER_MROW;
import static org.opensearch.lance.plan.cost.CostCoefficients.PUSHED_FIXED_MS;
import static org.opensearch.lance.plan.cost.CostCoefficients.PUSHED_LARGE_GROUPS_MS_PER_MROW_THREAD;
import static org.opensearch.lance.plan.cost.CostCoefficients.PUSHED_MERGE_MS_PER_MGROUP;
import static org.opensearch.lance.plan.cost.CostCoefficients.PUSHED_NUMERIC_KEY_MS_PER_MROW_THREAD;
import static org.opensearch.lance.plan.cost.CostCoefficients.PUSHED_PERCENTILES_MS_PER_MROW_THREAD;
import static org.opensearch.lance.plan.cost.CostCoefficients.PUSHED_RANGE_KEY_MS_PER_MROW_THREAD;
import static org.opensearch.lance.plan.cost.CostCoefficients.PUSHED_STRING_KEY_MS_PER_MROW_THREAD;

import org.opensearch.lance.plan.rel.PushedOperation.PushedFilter;

/**
 * Predicted latency in milliseconds of the two physical forms of an
 * aggregation tree, from the shape's {@link AggregateProfile} and the
 * run's {@link CostInputs}. Every term is a coefficient of
 * {@link CostCoefficients} times a quantity; the coefficients were
 * fitted to the measured warm latencies of the aggregation shapes and
 * {@code scripts/fit-cost-coefficients.py} computes exactly these
 * formulas, so the fit report and this class agree by construction.
 *
 * <p>Both formulas share the fixed costs of a request and, over an
 * object store table, the store's open latency. Rows are charged per
 * thread: the table's rows divided by the nodes and by the path's
 * parallelism. Three quantities are not per thread. The pushed scan
 * pays the object store transfer of the columns it reads once per node
 * and not divided by the parallelism, because the transfer is
 * bandwidth bound rather than CPU bound (the parallelism sweep at 1B
 * rows moved it by a fifth where the local decode moved fourfold); the
 * aggregator path reads the warmed columns from the node's column store
 * and has no storage term. The aggregator path's hash table misses
 * above {@link CostCoefficients#LARGE_GROUPS} groups are per node too:
 * the slices' tables compete for the node's memory, and eight slices
 * measured twice as fast as one, not eight times. A pushed filter
 * materialises the row address set its scalar index answers with, and
 * that set covers the whole table on every node whatever the node's
 * share of the rows, so it is charged per matching row of the table.
 * The fan out floor the design named turned out not to be per node in
 * the measurements (its coefficient fitted to zero), so it lives in the
 * fixed terms; the merge of the per node answers on the coordinator is
 * likewise inside the fixed terms, and the merge the model charges is
 * the executor's merge of its own parallel scans' group rows.
 *
 * <p>Neither formula models the column store's warmth (a cold node pays
 * the storage read on the aggregator path too), the index cache state,
 * or concurrent requests: the coefficients are single request, warm
 * latencies.
 *
 * <p>Tables below {@link CostCoefficients#FITTED_MODEL_MIN_ROWS} rows
 * are outside the measured range and keep the placeholder costs the
 * operators used before the fit; {@link #usesFittedModel} says which
 * regime a table is in.
 */
public final class CostModel {

    private CostModel() {}

    /** Whether the fitted formulas apply to a table of {@code tableRows} rows, or the placeholder costs stand. */
    public static boolean usesFittedModel(double tableRows) {
        return tableRows >= CostCoefficients.FITTED_MODEL_MIN_ROWS;
    }

    /** Predicted milliseconds of the Lance dataset scan carrying the aggregate. */
    public static double pushedAggregateMillis(CostInputs inputs, AggregateProfile shape) {
        double objectStore = inputs.storage() == StorageKind.OBJECT_STORE ? 1.0 : 0.0;
        double rowsPerNode = shape.tableRows() / inputs.nodes();
        double mrowThread = rowsPerNode / inputs.pushdownParallelism() / 1e6;
        double mrowTable = shape.tableRows() / 1e6;
        double gigabytesPerNode = shape.tableRows() * shape.bytesPerRow() * shape.scanPasses() / inputs.nodes() / 1e9;
        double selectivity = shape.filterSelectivity();
        double aggregated = mrowThread * selectivity;
        double mergedMgroups = Math.min(shape.mergedGroups(), rowsPerNode) / 1e6;
        double sketchMvalues = shape.cardinality()
            ? Math.min(shape.cardinalityDistinct(), mrowThread * 1e6) * inputs.pushdownParallelism() / 1e6
            : 0.0;

        double millis = PUSHED_FIXED_MS;
        millis += OBJECT_STORE_OPEN_MS * objectStore;
        millis += OBJECT_STORE_READ_MS_PER_GB_PER_NODE * gigabytesPerNode * objectStore;
        millis += PUSHED_DECODE_MS_PER_MROW_THREAD_PER_8_BYTES * mrowThread * shape.bytesPerRow() / 8.0 * shape.scanPasses();
        millis += PUSHED_STRING_KEY_MS_PER_MROW_THREAD * aggregated * shape.stringKeys();
        millis += PUSHED_NUMERIC_KEY_MS_PER_MROW_THREAD * aggregated * shape.numericKeys();
        millis += PUSHED_DATE_KEY_MS_PER_MROW_THREAD * aggregated * shape.dateKeys();
        millis += PUSHED_RANGE_KEY_MS_PER_MROW_THREAD * aggregated * shape.rangeKeys();
        millis += PUSHED_FILTERS_KEY_MS_PER_MROW_THREAD * aggregated * shape.filterKeys();
        millis += PUSHED_COMPOSITE_DATE_KEY_MS_PER_MROW_THREAD * aggregated * shape.compositeDateKeys();
        millis += PUSHED_EXTENDED_STATS_MS_PER_MROW_THREAD * aggregated * (shape.extendedStats() ? 1 : 0);
        millis += PUSHED_PERCENTILES_MS_PER_MROW_THREAD * aggregated * (shape.percentiles() ? 1 : 0);
        millis += PUSHED_LARGE_GROUPS_MS_PER_MROW_THREAD * aggregated * (shape.largeGroups() ? 1 : 0);
        millis += PUSHED_FILTER_EVAL_MS_PER_MROW_THREAD * mrowThread * (shape.filtered() ? 1 : 0);
        millis += PUSHED_FILTER_MATCH_MS_PER_MROW * mrowTable * selectivity * (shape.filtered() ? 1 : 0);
        millis += PUSHED_MERGE_MS_PER_MGROUP * mergedMgroups * inputs.pushdownParallelism();
        millis += PUSHED_CARDINALITY_HASH_MS_PER_MROW_THREAD * aggregated * (shape.cardinality() ? 1 : 0);
        millis += PUSHED_CARDINALITY_MS_PER_MVALUE * sketchMvalues;
        return millis;
    }

    /** Predicted milliseconds of the Lucene aggregators over the fragment leaf readers. */
    public static double luceneAggregateMillis(CostInputs inputs, AggregateProfile shape) {
        double objectStore = inputs.storage() == StorageKind.OBJECT_STORE ? 1.0 : 0.0;
        double rowsPerNode = shape.tableRows() / inputs.nodes();
        double mrowNode = rowsPerNode / 1e6;
        double mrowThread = rowsPerNode / inputs.slices() / 1e6;
        double selectivity = shape.filterSelectivity();
        double aggregated = mrowThread * selectivity;
        int termsKeys = shape.composite() ? 0 : shape.numericKeys();

        double millis = LUCENE_FIXED_MS;
        millis += OBJECT_STORE_OPEN_MS * objectStore;
        millis += LUCENE_COLUMN_MS_PER_MROW_THREAD * aggregated * shape.columnsRead();
        millis += LUCENE_NUMERIC_KEY_MS_PER_MROW_THREAD * aggregated * termsKeys;
        millis += LUCENE_DATE_KEY_MS_PER_MROW_THREAD * aggregated * shape.dateKeys();
        millis += LUCENE_RANGE_KEY_MS_PER_MROW_THREAD * aggregated * shape.rangeKeys();
        millis += LUCENE_FILTERS_KEY_MS_PER_MROW_THREAD * aggregated * shape.filterKeys();
        millis += LUCENE_COMPOSITE_SOURCE_MS_PER_MROW_THREAD * aggregated * shape.compositeSources();
        millis += LUCENE_NESTED_LEVEL_MS_PER_MROW_THREAD * aggregated * shape.nestedLevels();
        millis += LUCENE_SIMPLE_METRIC_MS_PER_MROW_THREAD * aggregated * shape.simpleMetrics();
        millis += LUCENE_BUCKET_METRIC_MS_PER_MROW_THREAD * aggregated * shape.bucketedMetrics();
        millis += LUCENE_EXTENDED_STATS_MS_PER_MROW_THREAD * aggregated * (shape.extendedStats() ? 1 : 0);
        millis += LUCENE_PERCENTILES_MS_PER_MROW_THREAD * aggregated * (shape.percentiles() ? 1 : 0);
        millis += LUCENE_CARDINALITY_MS_PER_MROW_THREAD * aggregated * (shape.cardinalityHashesEveryRow() ? 1 : 0);
        millis += LUCENE_LARGE_GROUPS_MS_PER_MROW_NODE * mrowNode * selectivity * (shape.largeGroups() ? 1 : 0);
        millis += LUCENE_FILTER_EVAL_MS_PER_MROW_THREAD * mrowThread * (shape.filtered() ? 1 : 0);
        return millis;
    }

    /**
     * Predicted milliseconds a pushed filter's encoding adds to the scan
     * that carries it, in both cost regimes: shipping the encoding to
     * every data node inside the fragment request (a Substrait filter
     * that also carries its SQL ships both), plus, on the SQL encoding,
     * {@link CostCoefficients#FILTER_SQL_TIE_BREAK_MS}. Turning the
     * encoding into the expression Lance plans is not a term: both
     * encodings decode to the same DataFusion expression inside Lance
     * and run through the same coercion, simplification and scalar index
     * passes, and the measurements behind the tie break constant put
     * the decode of either inside the noise of that planning. The
     * evaluation of the predicate is likewise the same on either form.
     *
     * <p>The term is microseconds against the milliseconds of the rows
     * terms, so it never decides between a pushed filter and a Lucene
     * operator; it orders the two encodings of one predicate, which
     * otherwise cost the same: the Substrait form wins until its extra
     * bytes times the fan out outweigh the tie break, from where the
     * shorter SQL ships instead.
     */
    public static double filterEncodingMillis(CostInputs inputs, PushedFilter filter) {
        double sqlKb = filter.sql() == null ? 0.0 : filter.sql().length() / 1024.0;
        double substraitKb = filter.substraitLength() / 1024.0;
        double millis = FILTER_WIRE_MS_PER_KB_PER_NODE * (sqlKb + substraitKb) * inputs.nodes();
        if (!filter.usesSubstrait()) {
            millis += FILTER_SQL_TIE_BREAK_MS;
        }
        return millis;
    }
}
