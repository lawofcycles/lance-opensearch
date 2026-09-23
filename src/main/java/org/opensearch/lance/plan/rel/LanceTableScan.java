/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rel;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.opensearch.lance.plan.calcite.LanceConvention;
import org.opensearch.lance.plan.calcite.LanceRel;
import org.apache.calcite.rex.RexNode;
import org.opensearch.lance.plan.rel.PushedOperation.PushedAggregate;
import org.opensearch.lance.plan.rel.PushedOperation.PushedFilter;
import org.opensearch.lance.plan.rel.PushedOperation.PushedFts;
import org.opensearch.lance.plan.rel.PushedOperation.PushedKnn;
import org.opensearch.lance.plan.rel.PushedOperation.PushedTopK;
import org.lance.ipc.ColumnOrdering;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

/**
 * Scan of a Lance backed index, the leaf every plan over a Lance table
 * starts from. The scan carries an immutable list of
 * {@link PushedOperation}s the Lance dataset scan computes; a scan with
 * a pushed aggregate stands in for the whole aggregate it replaced, so
 * its row type is the aggregate's row type and its row estimate is the
 * aggregate's group estimate.
 */
public class LanceTableScan extends TableScan implements LanceRel {

    private final ImmutableList<PushedOperation> pushedOperations;

    /** Creates the bare scan with the {@link LanceConvention} trait. */
    public LanceTableScan(RelOptCluster cluster, RelOptTable table) {
        this(cluster, cluster.traitSetOf(LanceConvention.INSTANCE), table, ImmutableList.of());
    }

    private LanceTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, ImmutableList<PushedOperation> pushed) {
        super(cluster, traitSet, ImmutableList.of(), table);
        this.pushedOperations = pushed;
    }

    /** The pushed operations, in push order; empty for a bare scan. */
    public List<PushedOperation> pushedOperations() {
        return pushedOperations;
    }

    /** The pushed aggregate, when the scan carries one. */
    public Optional<PushedAggregate> pushedAggregate() {
        for (PushedOperation operation : pushedOperations) {
            if (operation instanceof PushedAggregate aggregate) {
                return Optional.of(aggregate);
            }
        }
        return Optional.empty();
    }

    /** The pushed filter, when the scan carries one. */
    public Optional<PushedFilter> pushedFilter() {
        for (PushedOperation operation : pushedOperations) {
            if (operation instanceof PushedFilter filter) {
                return Optional.of(filter);
            }
        }
        return Optional.empty();
    }

    /** The pushed full text match, when the scan carries one. */
    public Optional<PushedFts> pushedFts() {
        for (PushedOperation operation : pushedOperations) {
            if (operation instanceof PushedFts fts) {
                return Optional.of(fts);
            }
        }
        return Optional.empty();
    }

    /** The pushed knn search, when the scan carries one. */
    public Optional<PushedKnn> pushedKnn() {
        for (PushedOperation operation : pushedOperations) {
            if (operation instanceof PushedKnn knn) {
                return Optional.of(knn);
            }
        }
        return Optional.empty();
    }

    /** The pushed top-k page, when the scan carries one. */
    public Optional<PushedTopK> pushedTopK() {
        for (PushedOperation operation : pushedOperations) {
            if (operation instanceof PushedTopK topK) {
                return Optional.of(topK);
            }
        }
        return Optional.empty();
    }

    /**
     * The same scan with {@code aggregate} pushed into it: the scan's
     * row type becomes the aggregate's and the plan above no longer
     * contains the aggregate. {@code aggregate} is the node the rule
     * matched with its input rebuilt to the concrete tree; {@code bytes}
     * is what the Substrait producer encoded for it. Refuses when the
     * scan already carries any pushed operation: the push rule matches
     * bare scans, and an aggregate consumes every matching row, so it
     * combines with nothing.
     */
    public LanceTableScan withPushedAggregate(LanceAggregate aggregate, ByteBuffer bytes) {
        if (pushedAggregate().isPresent()) {
            throw new IllegalStateException("the scan already carries a pushed aggregate");
        }
        if (pushedTopK().isPresent()) {
            throw new IllegalStateException("a pushed aggregate does not combine with a pushed top-k");
        }
        if (!pushedOperations.isEmpty()) {
            throw new IllegalStateException("the scan already carries a pushed operation: " + pushedOperations);
        }
        return new LanceTableScan(getCluster(), getTraitSet(), table, ImmutableList.of(new PushedAggregate(aggregate, bytes)));
    }

    /**
     * The same scan with {@code condition} pushed as its filter,
     * spelled as {@code sql} for the executor's
     * {@code ScanOptions.filter}. The row type does not change. Refuses
     * when the scan already carries any pushed operation: the push
     * rule matches bare scans, so a filter never combines with an
     * already pushed query kind or top-k.
     */
    public LanceTableScan withPushedFilter(RexNode condition, String sql) {
        if (pushedFilter().isPresent()) {
            throw new IllegalStateException("the scan already carries a pushed filter");
        }
        if (pushedTopK().isPresent()) {
            throw new IllegalStateException("a filter cannot push below a pushed top-k, which already cut the page");
        }
        if (!pushedOperations.isEmpty()) {
            throw new IllegalStateException("the scan already carries a pushed operation: " + pushedOperations);
        }
        return new LanceTableScan(getCluster(), getTraitSet(), table, ImmutableList.of(new PushedFilter(condition, sql)));
    }

    /**
     * The same scan with {@code fts} pushed into it: the Lance dataset
     * scan runs the inverted-index lookup with {@code filterSql} as a
     * prefilter (none when null), the scan's row type becomes the FTS
     * node's, and the plan above no longer contains the node. Only one
     * operation may be pushed: the fuse rule matches bare scans.
     */
    public LanceTableScan withPushedFts(LanceFtsMatch fts, String filterSql) {
        if (!pushedOperations.isEmpty()) {
            throw new IllegalStateException("the scan already carries a pushed operation: " + pushedOperations);
        }
        return new LanceTableScan(getCluster(), getTraitSet(), table, ImmutableList.of(new PushedFts(fts, filterSql)));
    }

    /**
     * The same scan with {@code knn} pushed into it: the Lance dataset
     * scan runs the nearest lookup with {@code filterSql} as a
     * prefilter evaluated before the top-k cutoff (none when null), the
     * scan's row type becomes the knn node's, and the plan above no
     * longer contains the node. Only one operation may be pushed: the
     * fuse rule matches bare scans.
     */
    public LanceTableScan withPushedKnn(LanceKnnSearch knn, String filterSql) {
        if (!pushedOperations.isEmpty()) {
            throw new IllegalStateException("the scan already carries a pushed operation: " + pushedOperations);
        }
        return new LanceTableScan(getCluster(), getTraitSet(), table, ImmutableList.of(new PushedKnn(knn, filterSql)));
    }

    /**
     * The same scan with the hits page pushed into it: the Lance
     * dataset scan returns the top {@code fetch} rows already in
     * {@code orderings} order (or in the FTS / knn scan's score order
     * when {@code orderings} is empty), with {@code cursorSql} ANDed
     * into the scan filter for a {@code search_after} continuation.
     * The page cuts the rows of exactly one query, so the scan must be
     * bare or carry a single pushed filter, FTS or knn; a pushed
     * aggregate consumes every matching row and does not combine with
     * a page. With {@code hitShape} the scan stands in for the whole
     * hits plan and takes the hit shape's row type.
     */
    public LanceTableScan withPushedTopK(LanceTopK topK, LanceHitShape hitShape, List<ColumnOrdering> orderings, String cursorSql) {
        if (pushedTopK().isPresent()) {
            throw new IllegalStateException("the scan already carries a pushed top-k");
        }
        if (pushedAggregate().isPresent()) {
            throw new IllegalStateException("a pushed top-k does not combine with a pushed aggregate");
        }
        if (pushedOperations.size() > 1) {
            throw new IllegalStateException("a pushed top-k combines with at most one pushed operation: " + pushedOperations);
        }
        ImmutableList<PushedOperation> pushed = ImmutableList.<PushedOperation>builder()
            .addAll(pushedOperations)
            .add(new PushedTopK(topK, hitShape, orderings, cursorSql))
            .build();
        return new LanceTableScan(getCluster(), getTraitSet(), table, pushed);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.isEmpty();
        return new LanceTableScan(getCluster(), traitSet, table, pushedOperations);
    }

    /**
     * The hit shape's row type when a pushed top-k carries one, then
     * the aggregate's, FTS node's or knn node's row type when one is
     * pushed, the table row type otherwise.
     */
    @Override
    public RelDataType deriveRowType() {
        Optional<PushedTopK> topK = pushedTopK();
        if (topK.isPresent() && topK.get().hitShape() != null) {
            return topK.get().hitShape().getRowType();
        }
        Optional<PushedAggregate> pushed = pushedAggregate();
        if (pushed.isPresent()) {
            return pushed.get().aggregate().getRowType();
        }
        Optional<PushedFts> fts = pushedFts();
        if (fts.isPresent()) {
            return fts.get().fts().getRowType();
        }
        Optional<PushedKnn> knn = pushedKnn();
        if (knn.isPresent()) {
            return knn.get().knn().getRowType();
        }
        return super.deriveRowType();
    }

    /**
     * Row count from the table's statistic (the Lance fragment row
     * counts) for a bare scan, scaled by the pushed filter's guessed
     * selectivity when one is pushed; a pushed aggregate returns one
     * row per group, a pushed FTS its match estimate and a pushed knn
     * at most {@code k} rows, so their own estimates stand. A pushed
     * top-k caps whatever the query below it yields at its page.
     */
    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        double rows = estimateQueryRowCount(mq);
        Optional<PushedTopK> topK = pushedTopK();
        if (topK.isPresent()) {
            rows = Math.min(rows, (double) topK.get().topK().fetch() + topK.get().topK().offset());
        }
        return rows;
    }

    /** The row estimate of the pushed query alone, before any pushed top-k cut. */
    private double estimateQueryRowCount(RelMetadataQuery mq) {
        Optional<PushedAggregate> pushed = pushedAggregate();
        if (pushed.isPresent()) {
            return pushed.get().aggregate().estimateRowCount(mq);
        }
        Optional<PushedFts> fts = pushedFts();
        if (fts.isPresent()) {
            return fts.get().fts().estimateRowCount(mq);
        }
        Optional<PushedKnn> knn = pushedKnn();
        if (knn.isPresent()) {
            return knn.get().knn().estimateRowCount(mq);
        }
        double rows = table.getRowCount();
        Optional<PushedFilter> filter = pushedFilter();
        if (filter.isPresent()) {
            rows *= RelMdUtil.guessSelectivity(filter.get().condition());
        }
        return rows;
    }

    /**
     * Rows read (a bare scan) or groups returned (a pushed aggregate)
     * stand in for predicted milliseconds until the cost model gets
     * real coefficients, with a constant per pushed operation in the
     * second slot so two scans over the same table order by how much
     * work was pushed. Native and heap bytes are not modelled yet, so
     * the byte slots stay at the constant model and the budget check in
     * the cost ordering cannot fire on a scan.
     */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rows = estimateRowCount(mq);
        return planner.getCostFactory().makeCost(rows, pushedOperations.size(), 0);
    }

    /** Prints the pushed operations, so the digest and the explain output carry them. */
    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).itemIf("pushed", pushedOperations, !pushedOperations.isEmpty());
    }
}
