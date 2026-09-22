/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.calcite;

import org.apache.calcite.plan.RelOptCost;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Pins the cost ordering: the memory budgets are hard constraints, so a
 * cost that fits them beats any cost that does not, and milliseconds only
 * decide between costs on the same side of the budgets.
 */
public class LanceCostTests extends OpenSearchTestCase {

    private static final long NATIVE_BUDGET = 1000;
    private static final long HEAP_BUDGET = 500;

    private final LanceCostFactory factory = new LanceCostFactory(NATIVE_BUDGET, HEAP_BUDGET);

    public void testBothFeasibleMillisDecide() {
        RelOptCost fast = factory.makeCost(10, 100, 50);
        RelOptCost slow = factory.makeCost(20, 900, 400);
        assertTrue(fast.isLe(slow));
        assertFalse(slow.isLe(fast));
        assertTrue(fast.isLt(slow));
        assertFalse(slow.isLt(fast));
    }

    public void testFeasibleBeatsNativeInfeasibleRegardlessOfMillis() {
        RelOptCost feasibleSlow = factory.makeCost(10_000, 100, 50);
        RelOptCost infeasibleFast = factory.makeCost(1, 2000, 50);
        assertTrue(feasibleSlow.isLe(infeasibleFast));
        assertFalse(infeasibleFast.isLe(feasibleSlow));
        assertTrue(feasibleSlow.isLt(infeasibleFast));
    }

    public void testFeasibleBeatsHeapInfeasibleRegardlessOfMillis() {
        RelOptCost feasibleSlow = factory.makeCost(10_000, 100, 50);
        RelOptCost infeasibleFast = factory.makeCost(1, 100, 5000);
        assertTrue(feasibleSlow.isLe(infeasibleFast));
        assertFalse(infeasibleFast.isLe(feasibleSlow));
    }

    public void testBothInfeasibleMillisDecide() {
        RelOptCost fast = factory.makeCost(10, 2000, 50);
        RelOptCost slow = factory.makeCost(20, 3000, 5000);
        assertTrue(fast.isLe(slow));
        assertFalse(slow.isLe(fast));
    }

    public void testEqualMillisIsLeBothWaysButNotLt() {
        RelOptCost a = factory.makeCost(10, 100, 50);
        RelOptCost b = factory.makeCost(10, 200, 60);
        assertTrue(a.isLe(b));
        assertTrue(b.isLe(a));
        assertFalse(a.isLt(b));
        assertFalse(b.isLt(a));
    }

    public void testPlus() {
        RelOptCost sum = factory.makeCost(1, 2, 3).plus(factory.makeCost(4, 5, 6));
        assertEquals(5.0, sum.getRows(), 0.0);
        assertEquals(7.0, sum.getCpu(), 0.0);
        assertEquals(9.0, sum.getIo(), 0.0);
    }

    public void testMinus() {
        RelOptCost diff = factory.makeCost(4, 5, 6).minus(factory.makeCost(1, 2, 3));
        assertEquals(3.0, diff.getRows(), 0.0);
        assertEquals(3.0, diff.getCpu(), 0.0);
        assertEquals(3.0, diff.getIo(), 0.0);
    }

    public void testMultiplyBy() {
        RelOptCost doubled = factory.makeCost(1, 2, 3).multiplyBy(2);
        assertEquals(2.0, doubled.getRows(), 0.0);
        assertEquals(4.0, doubled.getCpu(), 0.0);
        assertEquals(6.0, doubled.getIo(), 0.0);
    }

    public void testDivideBy() {
        RelOptCost cost = factory.makeCost(4, 4, 4);
        assertEquals(2.0, cost.divideBy(factory.makeCost(2, 2, 2)), 1.0e-9);
        assertEquals(1.0, cost.divideBy(factory.makeCost(0, 0, 0)), 0.0);
    }

    public void testToString() {
        assertEquals("{12.0 ms, 100.0 native bytes, 50.0 heap bytes}", factory.makeCost(12, 100, 50).toString());
    }

    public void testInfiniteDetection() {
        assertTrue(factory.makeInfiniteCost().isInfinite());
        assertTrue(factory.makeCost(Double.POSITIVE_INFINITY, 0, 0).isInfinite());
        assertTrue(factory.makeCost(0, Double.POSITIVE_INFINITY, 0).isInfinite());
        assertTrue(factory.makeCost(0, 0, Double.POSITIVE_INFINITY).isInfinite());
        assertFalse(factory.makeZeroCost().isInfinite());
        assertFalse(factory.makeHugeCost().isInfinite());
        assertFalse(factory.makeTinyCost().isInfinite());
    }

    public void testEqualsAndEpsilon() {
        RelOptCost a = factory.makeCost(1, 2, 3);
        RelOptCost b = factory.makeCost(1, 2, 3);
        assertTrue(a.equals(b));
        assertEquals(a.hashCode(), b.hashCode());
        assertFalse(a.equals(factory.makeCost(1, 2, 4)));
        assertTrue(a.isEqWithEpsilon(factory.makeCost(1.0000001, 2, 3)));
        assertFalse(a.isEqWithEpsilon(factory.makeCost(1.1, 2, 3)));
    }
}
