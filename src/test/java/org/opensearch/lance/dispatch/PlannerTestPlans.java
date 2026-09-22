/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.lance.dispatch;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.lance.dispatch.planner.PlannerTestRegistries;
import org.opensearch.search.aggregations.AggregatorFactories;

/**
 * Reaches the dispatch package's package private planning surface for
 * the planner package's tests: minimal {@link LanceAggregatePushdown.Plan}
 * instances with distinct identities, the legacy dispatcher without
 * any rule, and a plan's encoded scan bytes.
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

    /**
     * The legacy shape dispatcher's answer with an empty registry, so
     * no rule is consulted: the reference a rule's plan is compared
     * against in the byte equivalence tests.
     */
    public static LanceAggregatePushdown.Plan planWithoutRules(
        AggregatorFactories.Builder aggregations,
        Schema schema,
        Map<String, LinkedHashMap<String, String>> multiFields,
        QueryShardContext qsc,
        int maxGroups,
        int bins,
        int slack
    ) {
        return LanceAggregatePushdown.planViaRegistry(
            aggregations,
            schema,
            multiFields,
            qsc,
            PlannerTestRegistries.registryOf(),
            maxGroups,
            bins,
            slack
        );
    }

    /** The plan's encoded main scan, read only, for byte equivalence assertions. */
    public static ByteBuffer substraitBytes(LanceAggregatePushdown.Plan plan) {
        return plan.substraitPlan();
    }
}
