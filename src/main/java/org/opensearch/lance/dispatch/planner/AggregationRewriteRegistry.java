/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch.planner;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * The ordered rule set behind the aggregation pushdown: {@link #rewrite}
 * asks each rule in registration order and returns the first match; an
 * empty answer means no rule owns the request's shape and the Lucene
 * aggregators run. The production set is the {@link #instance()
 * singleton}: the metric only rule, the composite rule, then the
 * nested bucket rule. The shapes are disjoint (a composite top is
 * never a pushdown metric, and the nested bucket rule refuses both),
 * so the order documents the shape space rather than resolves an
 * overlap; together the three rules answer every shape the structural
 * gate accepts.
 */
public final class AggregationRewriteRegistry {

    private static final AggregationRewriteRegistry INSTANCE = new AggregationRewriteRegistry(
        List.of(new MetricOnlyRule(), new CompositeRule(), new NestedBucketRule())
    );

    /** The production rule set. */
    public static AggregationRewriteRegistry instance() {
        return INSTANCE;
    }

    private final List<AggregationRewriteRule> rules;

    /**
     * Package private so tests can build a registry with a curated rule
     * set; production callers use {@link #instance()} only. Copies the
     * list, so later changes to the argument do not reach the registry.
     * Rejects a rule whose {@link AggregationRewriteRule#name() name}
     * is null or empty, and two rules sharing a name, because
     * diagnostics could not tell them apart.
     */
    AggregationRewriteRegistry(List<AggregationRewriteRule> rules) {
        this.rules = List.copyOf(rules);
        Set<String> names = new HashSet<>();
        for (AggregationRewriteRule rule : this.rules) {
            String name = rule.name();
            if (name == null || name.isEmpty()) {
                throw new IllegalArgumentException("a rule name must be non empty");
            }
            if (!names.add(name)) {
                throw new IllegalArgumentException("duplicate rule name [" + name + "]");
            }
        }
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
