/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.cost;

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptPlanner;

import java.util.Objects;

/**
 * How the {@link CostInputs} of one planning run reach the operators'
 * {@code computeSelfCost}: the planner factory registers one holder in
 * the Volcano planner's {@link Context} when it builds the cluster, the
 * {@code plan(RelNode, CostInputs)} call fills it before the first cost
 * is computed, and every operator reads it back through
 * {@link #inputsOf(RelOptPlanner)}. A planner's context is fixed at
 * construction while the inputs are known only when the plan is
 * demanded, hence a mutable holder rather than the inputs themselves.
 * One cluster serves one planning run on one thread, so the holder is
 * not synchronised.
 *
 * <p>A planner whose context carries no holder (a Hep planner in a rule
 * unit test, a planner another factory built) and a holder nothing
 * filled both answer {@link CostInputs#local()}, so a cost can always be
 * computed.
 */
public final class CostInputsHolder {

    /**
     * The local defaults, built once: the Volcano planner asks for a cost
     * many times per plan and {@link CostInputs#local()} reads the CPU
     * count on every call.
     */
    private static final CostInputs LOCAL_DEFAULTS = CostInputs.local();

    private CostInputs inputs;

    /** An empty holder; {@link #inputsOf} answers the local defaults until {@link #set} is called. */
    public CostInputsHolder() {}

    /** Fixes the inputs of the planning run that is about to start. */
    public void set(CostInputs newInputs) {
        this.inputs = Objects.requireNonNull(newInputs, "inputs");
    }

    /** The inputs set for this run, or the local defaults when none were. */
    public CostInputs get() {
        return inputs != null ? inputs : LOCAL_DEFAULTS;
    }

    /** The inputs of the run {@code planner} is executing; see the class comment for the fallbacks. */
    public static CostInputs inputsOf(RelOptPlanner planner) {
        CostInputsHolder holder = planner.getContext().unwrap(CostInputsHolder.class);
        return holder != null ? holder.get() : LOCAL_DEFAULTS;
    }
}
