/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rel.physical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.opensearch.lance.plan.calcite.LuceneConvention;
import org.opensearch.lance.plan.calcite.LuceneRel;
import org.opensearch.lance.plan.rel.LanceFtsMatch;
import org.opensearch.lance.plan.rel.LanceHitShape;
import org.opensearch.lance.plan.rel.LanceKnnSearch;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.rel.LanceTopK;

import java.util.List;
import java.util.Objects;

/**
 * A hits page ordered and cut on the Java heap: Lucene's top docs
 * collector over the per-fragment leaf readers instead of an ordered,
 * limited Lance dataset scan. The node wraps the {@link LanceTopK} it
 * stands in for (with the top-k's input rebuilt to the concrete query
 * tree, exactly like a pushed top-k carries it) and the
 * {@link LanceHitShape} folded with it when the plan carried one (null
 * for the executor's own top-k route), and takes the bare
 * {@link LanceTableScan} as its input, because the collector consumes
 * the fragment readers the scan exposes. The runtime path is the
 * existing Lucene collector fallback in
 * {@code TransportLanceFragmentQueryAction}; no executor traverses
 * this node yet, it makes the fallback visible to the Volcano
 * planner's cost comparison.
 */
public final class HeapTopKExec extends SingleRel implements LuceneRel {

    private final LanceTopK topK;
    private final LanceHitShape hitShape;

    /**
     * @param traitSet must carry {@link LuceneConvention#INSTANCE}
     * @param input the bare scan whose fragment readers the collector
     *     runs over
     * @param topK the top-k the node stands in for, its input rebuilt
     *     to the concrete query tree
     * @param hitShape the hit envelope folded with the top-k, or null
     *     when the plan carried none
     */
    public HeapTopKExec(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, LanceTopK topK, LanceHitShape hitShape) {
        super(cluster, traitSet, input);
        this.topK = Objects.requireNonNull(topK, "topK");
        this.hitShape = hitShape;
    }

    /** The top-k the node stands in for, with its concrete query tree. */
    public LanceTopK topK() {
        return topK;
    }

    /** The hit envelope folded with the top-k, or null when the plan carried none. */
    public LanceHitShape hitShape() {
        return hitShape;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new HeapTopKExec(getCluster(), traitSet, sole(inputs), topK, hitShape);
    }

    /**
     * The hit envelope's row type when the plan carried one (the node
     * then stands in for the whole hits plan), the top-k's own row
     * type otherwise.
     */
    @Override
    protected RelDataType deriveRowType() {
        return hitShape != null ? hitShape.getRowType() : topK.getRowType();
    }

    /** At most the page passes, fewer when the query below matches fewer rows. */
    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        return Math.min((double) topK.fetch() + topK.offset(), mq.getRowCount(getInput()));
    }

    /**
     * A constant until the cost model gets real coefficients: the tiny
     * cost plus one unit in every slot. The offset keeps this
     * alternative strictly above the zero cost of
     * {@link LuceneHandoffExec}, so whenever the top-k pushdown rule
     * folds the same tree into the Lance scan, the Lance plan costs
     * less and the Volcano planner never has to break a tie between
     * the two conventions.
     */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        RelOptCostFactory factory = planner.getCostFactory();
        return factory.makeTinyCost().plus(factory.makeCost(1, 1, 1));
    }

    /**
     * Prints the page (collations, bounds, cursor), the hit envelope
     * when one is carried, and the wrapped query tree's filter / FTS /
     * knn parameters, so two nodes wrapping different pages never
     * share a digest and the explain output shows what the collector
     * would answer.
     */
    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        pw.item("collations", topK.collations());
        pw.item("fetch", topK.fetch());
        pw.item("offset", topK.offset());
        pw.itemIf("searchAfter", topK.searchAfter(), topK.searchAfter() != null);
        if (hitShape != null) {
            pw.item("columns", hitShape.outputColumns());
            pw.item("source", hitShape.includeSource());
            pw.item("id", hitShape.includeId());
            pw.item("score", hitShape.includeScore());
            pw.item("sortValues", hitShape.includeSortValues());
        }
        RelNode node = topK.getInput();
        while (true) {
            if (node instanceof Filter filter) {
                pw.item("filter", filter.getCondition());
            } else if (node instanceof LanceFtsMatch fts) {
                pw.item("fts", fts.queryJson());
            } else if (node instanceof LanceKnnSearch knn) {
                pw.item("knn", knn.queryJson());
            } else {
                break;
            }
            node = node.getInput(0);
        }
        return pw;
    }
}
