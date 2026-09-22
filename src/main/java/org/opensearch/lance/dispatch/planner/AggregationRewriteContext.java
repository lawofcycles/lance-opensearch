/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch.planner;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.AggregatorFactories;

/**
 * Everything a rule needs to decide whether it matches and to encode
 * its plan: the request's aggregation builders, the table's Arrow
 * schema, the index's keyword sub-field spec, the mapping context, and
 * the node's planning bounds. Immutable in what it owns: the multi
 * field map is copied at construction, and the primitives cannot
 * change. The {@code AggregatorFactories.Builder} is mutable by type
 * and is held by reference (OpenSearch offers no API to clone one);
 * rules treat it as read only. Every reference field is required, so a
 * rule never sees a null and a missing input fails at construction,
 * close to its source.
 *
 * <p>The three int bounds carry no range validation here: production
 * callers read them from the {@code LancePlugin} settings, whose
 * {@code Setting<Integer>} validators enforce the ranges, and that
 * setting layer is the single source of truth. A direct caller (a
 * test) accepts the responsibility of providing sane values.
 */
public final class AggregationRewriteContext {

    private final AggregatorFactories.Builder aggregations;
    private final Schema schema;
    private final Map<String, LinkedHashMap<String, String>> multiFields;
    private final QueryShardContext queryShardContext;
    private final int maxGroups;
    private final int percentilesBins;
    private final int topKSlack;

    public AggregationRewriteContext(
        AggregatorFactories.Builder aggregations,
        Schema schema,
        Map<String, LinkedHashMap<String, String>> multiFields,
        QueryShardContext queryShardContext,
        int maxGroups,
        int percentilesBins,
        int topKSlack
    ) {
        this.aggregations = Objects.requireNonNull(aggregations, "aggregations");
        this.schema = Objects.requireNonNull(schema, "schema");
        this.multiFields = Map.copyOf(Objects.requireNonNull(multiFields, "multiFields"));
        this.queryShardContext = Objects.requireNonNull(queryShardContext, "queryShardContext");
        this.maxGroups = maxGroups;
        this.percentilesBins = percentilesBins;
        this.topKSlack = topKSlack;
    }

    /** The request's aggregation builders; mutable by type, treated as read only by rules. */
    public AggregatorFactories.Builder aggregations() {
        return aggregations;
    }

    /** The dataset's Arrow schema, every top level column in order. */
    public Schema schema() {
        return schema;
    }

    /** The index's keyword sub-field spec, base column to sub-field name to type; an immutable copy. */
    public Map<String, LinkedHashMap<String, String>> multiFields() {
        return multiFields;
    }

    /** The mapping of the index the request targets. */
    public QueryShardContext queryShardContext() {
        return queryShardContext;
    }

    /** The node's bound on the estimated number of groups. */
    public int maxGroups() {
        return maxGroups;
    }

    /** Bins of a pushed down percentiles histogram. */
    public int percentilesBins() {
        return percentilesBins;
    }

    /** How many times {@code shard_size} groups a single level terms scan keeps. */
    public int topKSlack() {
        return topKSlack;
    }
}
