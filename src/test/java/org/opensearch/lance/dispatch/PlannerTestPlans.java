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

    /**
     * A plan whose only use is identity comparison in dispatcher
     * tests: it carries no Substrait bytes and no composite, so
     * executing it would fail. Never hand it to a scan runner.
     */
    public static LanceAggregatePushdown.Plan identityMarkerPlan() {
        return new LanceAggregatePushdown.Plan(null, List.of(), List.of(), null, List.of(), List.of(), 0, null);
    }
}
