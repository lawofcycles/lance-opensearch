/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.calcite;

import org.apache.calcite.plan.RelOptCost;

import java.util.Objects;

/**
 * Cost of a Lance plan candidate. Calcite's {@link RelOptCost} has three
 * slots named rows, cpu, and io; this class reads them with a different
 * meaning: {@link #getRows()} is predicted latency in milliseconds,
 * {@link #getCpu()} is predicted native (off-heap) bytes, and
 * {@link #getIo()} is predicted heap bytes. Latency is the objective;
 * the byte components are constraints checked against the node budgets.
 *
 * <p>Ordering ({@link #isLe}): a cost is feasible when its native bytes and
 * heap bytes both fit the budgets the {@link LanceCostFactory} was built
 * with. A feasible cost is always smaller than an infeasible one; between
 * two feasible or two infeasible costs, milliseconds decide. This keeps the
 * order total, which the Volcano planner requires, while treating the
 * memory limits as hard constraints rather than as weighted terms.
 */
public final class LanceCost implements RelOptCost {

    private static final double EPSILON = 1.0e-5;

    private final double millis;
    private final double nativeBytes;
    private final double heapBytes;
    private final double nativeBudgetBytes;
    private final double heapBudgetBytes;

    LanceCost(double millis, double nativeBytes, double heapBytes, double nativeBudgetBytes, double heapBudgetBytes) {
        this.millis = millis;
        this.nativeBytes = nativeBytes;
        this.heapBytes = heapBytes;
        this.nativeBudgetBytes = nativeBudgetBytes;
        this.heapBudgetBytes = heapBudgetBytes;
    }

    /** Predicted latency in milliseconds (Calcite's rows slot). */
    @Override
    public double getRows() {
        return millis;
    }

    /** Predicted native bytes (Calcite's cpu slot). */
    @Override
    public double getCpu() {
        return nativeBytes;
    }

    /** Predicted heap bytes (Calcite's io slot). */
    @Override
    public double getIo() {
        return heapBytes;
    }

    @Override
    public boolean isInfinite() {
        return Double.isInfinite(millis) || Double.isInfinite(nativeBytes) || Double.isInfinite(heapBytes);
    }

    private boolean feasible() {
        return nativeBytes <= nativeBudgetBytes && heapBytes <= heapBudgetBytes;
    }

    @Override
    public boolean isLe(RelOptCost other) {
        LanceCost that = (LanceCost) other;
        boolean thisFeasible = feasible();
        boolean thatFeasible = that.feasible();
        if (thisFeasible != thatFeasible) {
            return thisFeasible;
        }
        return millis <= that.millis;
    }

    /**
     * Strictly smaller: smaller-or-equal in one direction but not the other,
     * so two costs with equal milliseconds and equal feasibility are never
     * strictly ordered even when their byte components differ.
     */
    @Override
    public boolean isLt(RelOptCost other) {
        return isLe(other) && ((LanceCost) other).isLe(this) == false;
    }

    @Override
    public boolean equals(RelOptCost other) {
        return other == this
            || (other instanceof LanceCost
                && millis == ((LanceCost) other).millis
                && nativeBytes == ((LanceCost) other).nativeBytes
                && heapBytes == ((LanceCost) other).heapBytes);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof LanceCost && equals((RelOptCost) obj);
    }

    @Override
    public int hashCode() {
        return Objects.hash(millis, nativeBytes, heapBytes);
    }

    @Override
    public boolean isEqWithEpsilon(RelOptCost other) {
        return other instanceof LanceCost
            && Math.abs(millis - ((LanceCost) other).millis) < EPSILON
            && Math.abs(nativeBytes - ((LanceCost) other).nativeBytes) < EPSILON
            && Math.abs(heapBytes - ((LanceCost) other).heapBytes) < EPSILON;
    }

    @Override
    public RelOptCost plus(RelOptCost other) {
        LanceCost that = (LanceCost) other;
        return new LanceCost(
            millis + that.millis,
            nativeBytes + that.nativeBytes,
            heapBytes + that.heapBytes,
            nativeBudgetBytes,
            heapBudgetBytes
        );
    }

    @Override
    public RelOptCost minus(RelOptCost other) {
        LanceCost that = (LanceCost) other;
        return new LanceCost(
            millis - that.millis,
            nativeBytes - that.nativeBytes,
            heapBytes - that.heapBytes,
            nativeBudgetBytes,
            heapBudgetBytes
        );
    }

    @Override
    public RelOptCost multiplyBy(double factor) {
        return new LanceCost(millis * factor, nativeBytes * factor, heapBytes * factor, nativeBudgetBytes, heapBudgetBytes);
    }

    /**
     * Geometric mean of the component ratios that are non-zero and finite
     * on both sides, the same shape as Calcite's {@code VolcanoCost}, so the
     * planner's cost improvement heuristics behave as they do on the stock
     * cost.
     */
    @Override
    public double divideBy(RelOptCost cost) {
        LanceCost that = (LanceCost) cost;
        double product = 1;
        int factors = 0;
        if (millis != 0 && Double.isInfinite(millis) == false && that.millis != 0 && Double.isInfinite(that.millis) == false) {
            product *= millis / that.millis;
            ++factors;
        }
        if (nativeBytes != 0
            && Double.isInfinite(nativeBytes) == false
            && that.nativeBytes != 0
            && Double.isInfinite(that.nativeBytes) == false) {
            product *= nativeBytes / that.nativeBytes;
            ++factors;
        }
        if (heapBytes != 0 && Double.isInfinite(heapBytes) == false && that.heapBytes != 0 && Double.isInfinite(that.heapBytes) == false) {
            product *= heapBytes / that.heapBytes;
            ++factors;
        }
        if (factors == 0) {
            return 1.0;
        }
        return Math.pow(product, 1.0 / factors);
    }

    @Override
    public String toString() {
        return "{" + millis + " ms, " + nativeBytes + " native bytes, " + heapBytes + " heap bytes}";
    }
}
