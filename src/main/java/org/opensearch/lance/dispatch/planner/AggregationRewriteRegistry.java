/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch.planner;

import java.util.List;
import java.util.Optional;

/**
 * The ordered rule set the aggregation dispatcher consults before the
 * legacy shape dispatcher. {@link #rewrite} asks each rule in
 * registration order and returns the first match; an empty answer
 * means no rule owns the request's shape and the caller falls through.
 * The production set is the {@link #instance() singleton}, currently
 * empty: rules are registered here as they are extracted from the
 * legacy dispatcher.
 */
public final class AggregationRewriteRegistry {

    private static final AggregationRewriteRegistry INSTANCE = new AggregationRewriteRegistry(List.of());

    /** The production rule set. */
    public static AggregationRewriteRegistry instance() {
        return INSTANCE;
    }

    private final List<AggregationRewriteRule> rules;

    /**
     * Package private so tests can build a registry with a curated rule
     * set; production callers use {@link #instance()} only. Copies the
     * list, so later changes to the argument do not reach the registry.
     */
    AggregationRewriteRegistry(List<AggregationRewriteRule> rules) {
        this.rules = List.copyOf(rules);
    }

    /**
     * The first rule's plan for the request, in registration order, or
     * empty when no rule matches.
     */
    public Optional<PushdownPlan> rewrite(AggregationRewriteContext ctx) {
        for (AggregationRewriteRule rule : rules) {
            Optional<PushdownPlan> out = rule.tryRewrite(ctx);
            if (out.isPresent()) {
                return out;
            }
        }
        return Optional.empty();
    }

    /** The registered rules, in dispatch order; immutable. */
    public List<AggregationRewriteRule> rules() {
        return rules;
    }
}
