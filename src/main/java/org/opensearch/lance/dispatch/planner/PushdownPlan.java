/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch.planner;

import org.opensearch.lance.dispatch.LanceAggregatePushdown;

/**
 * A rule's answer: the encoded scan plan, as a thin wrapper over the
 * dispatch package's plan type so rule engine callers do not depend on
 * the legacy type directly. The dispatcher unwraps with
 * {@link #asLegacyPlan()} at the single point where the two worlds
 * meet.
 */
public record PushdownPlan(LanceAggregatePushdown.Plan legacy) {

    /** The wrapped plan, for the dispatcher to execute. */
    public LanceAggregatePushdown.Plan asLegacyPlan() {
        return legacy;
    }

    public static PushdownPlan of(LanceAggregatePushdown.Plan plan) {
        return new PushdownPlan(plan);
    }
}
