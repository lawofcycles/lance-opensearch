/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch.planner;

import java.util.Optional;

/**
 * One aggregation tree shape the fragment path can plan as a Lance
 * scan. A rule inspects the request in the
 * {@link AggregationRewriteContext} and either produces the full
 * {@link PushdownPlan} for its shape or returns empty so the next rule
 * (or the legacy dispatcher) gets a look. Rules own disjoint shapes:
 * the first rule that matches wins, so no two rules may match the same
 * tree.
 */
public interface AggregationRewriteRule {

    /**
     * The plan for this rule's shape, or empty when the request is not
     * this rule's shape or a field check fails.
     */
    Optional<PushdownPlan> tryRewrite(AggregationRewriteContext ctx);

    /**
     * A name for logs and diagnostics: non empty and unique across the
     * registered rules, both enforced by the registry constructor.
     * Stability across releases is expected but not enforced.
     */
    String name();
}
