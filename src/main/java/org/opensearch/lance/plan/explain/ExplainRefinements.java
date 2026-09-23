/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.explain;

import org.opensearch.lance.plan.execute.FragmentPlan;
import org.opensearch.lance.plan.execute.FragmentPlanRefiner;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Predicts, from the plan and the mapping, which of the data node's
 * refinements ({@link FragmentPlanRefiner}) would move a pushed
 * operation of a {@link FragmentPlan} to the Lucene side. The
 * prediction replays the refiner's order on what the coordinating node
 * can see: {@link FragmentPlanRefiner.Reason#SECURITY_WRAPPER} when a
 * reader wrapper is installed and the plan carries a pushed aggregate,
 * a pushed page or a full text clause (a pushed knn and the scalar
 * filter SQL survive the wrapper, as on the data node), and
 * {@link FragmentPlanRefiner.Reason#SORT_FIELD_TYPE} when the page that
 * survives orders by an {@code ip} override column, whose Lucene sort
 * field carries the {@code ip} format the executor's guard refuses.
 *
 * <p>This is a prediction; the data node decides. Two of the executor's
 * guards depend on inputs only it has and are not predicted here: a
 * {@code search_after} cursor equal to a sort field's missing value
 * sentinel (the sentinel comes from the Lucene sort field the mapping
 * builds), and {@link FragmentPlanRefiner.Reason#AGGREGATE_RESOLUTION}
 * (the resolution of the pushed aggregate against the mapping and the
 * node's {@code lance.aggregation.pushdown_max_groups}).
 */
final class ExplainRefinements {

    private ExplainRefinements() {}

    /**
     * @param plan the plan the coordinator would ship
     * @param readerWrapperInstalled whether a reader wrapper is installed
     *     on the index ({@link ReaderWrapperProbe})
     * @param ipColumns the index's {@code ip} override columns
     * @return the predicted reasons in the refiner's order, empty when
     *     none applies
     */
    static List<FragmentPlanRefiner.Reason> predict(FragmentPlan plan, boolean readerWrapperInstalled, Set<String> ipColumns) {
        List<FragmentPlanRefiner.Reason> reasons = new ArrayList<>();
        FragmentPlan remaining = plan;
        if (readerWrapperInstalled) {
            FragmentPlan before = remaining;
            remaining = remaining.withoutAggregate().withoutTopK();
            if (!remaining.isKnn()) {
                remaining = remaining.withoutLanceClause();
            }
            if (remaining != before) {
                reasons.add(FragmentPlanRefiner.Reason.SECURITY_WRAPPER);
            }
        }
        if (remaining.topK() != null && ordersByAnIpColumn(remaining.topK(), ipColumns)) {
            reasons.add(FragmentPlanRefiner.Reason.SORT_FIELD_TYPE);
        }
        return reasons;
    }

    private static boolean ordersByAnIpColumn(FragmentPlan.TopK topK, Set<String> ipColumns) {
        for (FragmentPlan.ScanOrdering ordering : topK.orderings()) {
            if (ipColumns.contains(ordering.column())) {
                return true;
            }
        }
        return false;
    }
}
