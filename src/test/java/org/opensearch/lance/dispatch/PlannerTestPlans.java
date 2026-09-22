/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.lance.dispatch;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Reaches the dispatch package's package private planning surface for
 * the planner package's tests: minimal {@link LanceAggregatePushdown.Plan}
 * instances with distinct identities and a plan's encoded scan bytes.
 */
public final class PlannerTestPlans {

    private PlannerTestPlans() {}

    /**
     * A plan whose only use is identity comparison in dispatcher
     * tests: it carries no Substrait bytes and no composite, so
     * executing it would fail. Never hand it to a scan runner.
     */
    public static LanceAggregatePushdown.Plan identityMarkerPlan() {
        return new LanceAggregatePushdown.Plan(null, List.of(), List.of(), null, List.of(), List.of(), 0, null);
    }

    /** The plan's encoded main scan, read only, for byte equivalence assertions. */
    public static ByteBuffer substraitBytes(LanceAggregatePushdown.Plan plan) {
        return plan.substraitPlan();
    }

    /** The plan's parsed composite after values in source order, for the composite paging assertions. */
    public static List<Comparable<?>> compositeAfterValues(LanceAggregatePushdown.Plan plan) {
        return plan.compositeAfterValues();
    }
}
