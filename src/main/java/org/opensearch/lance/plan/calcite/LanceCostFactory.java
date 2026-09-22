/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.calcite;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;

/**
 * Builds {@link LanceCost}s carrying the node's memory budgets, so the
 * ordering of any two costs made by the same factory can treat the budgets
 * as hard constraints. The production caller passes the native memory
 * limit and the JVM heap maximum; tests pass small fixtures.
 */
public final class LanceCostFactory implements RelOptCostFactory {

    private final long nativeBudgetBytes;
    private final long heapBudgetBytes;

    /**
     * @param nativeBudgetBytes budget for predicted native (off-heap) bytes
     * @param heapBudgetBytes budget for predicted heap bytes
     */
    public LanceCostFactory(long nativeBudgetBytes, long heapBudgetBytes) {
        this.nativeBudgetBytes = nativeBudgetBytes;
        this.heapBudgetBytes = heapBudgetBytes;
    }

    /**
     * @param rows predicted latency in milliseconds
     * @param cpu predicted native bytes
     * @param io predicted heap bytes
     */
    @Override
    public RelOptCost makeCost(double rows, double cpu, double io) {
        return new LanceCost(rows, cpu, io, nativeBudgetBytes, heapBudgetBytes);
    }

    @Override
    public RelOptCost makeHugeCost() {
        return makeCost(Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE);
    }

    @Override
    public RelOptCost makeInfiniteCost() {
        return makeCost(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY);
    }

    @Override
    public RelOptCost makeTinyCost() {
        return makeCost(1, 1, 0);
    }

    @Override
    public RelOptCost makeZeroCost() {
        return makeCost(0, 0, 0);
    }
}
