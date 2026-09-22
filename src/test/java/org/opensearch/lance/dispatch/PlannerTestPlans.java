/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.lance.dispatch;

import java.util.List;

/**
 * Builds minimal {@link LanceAggregatePushdown.Plan} instances for the
 * planner package's tests, which need distinct plan identities but
 * never execute one. Lives in this package because the plan
 * constructor is package private.
 */
public final class PlannerTestPlans {

    private PlannerTestPlans() {}

    /** A structurally empty plan: identity only, never executed. */
    public static LanceAggregatePushdown.Plan emptyPlan() {
        return new LanceAggregatePushdown.Plan(null, List.of(), List.of(), null, List.of(), List.of(), 0, null);
    }
}
