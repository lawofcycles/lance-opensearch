/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.lance.dispatch.planner;

import java.util.List;

/**
 * Builds {@link AggregationRewriteRegistry} instances with curated
 * rule sets for tests outside this package, which cannot reach the
 * package private constructor.
 */
public final class PlannerTestRegistries {

    private PlannerTestRegistries() {}

    public static AggregationRewriteRegistry registryOf(AggregationRewriteRule... rules) {
        return new AggregationRewriteRegistry(List.of(rules));
    }
}
