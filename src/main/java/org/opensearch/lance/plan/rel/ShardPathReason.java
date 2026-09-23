/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rel;

/**
 * Why a request shape answers through OpenSearch's regular shard search
 * path instead of the fragment fan-out. Each constant names one request
 * element the fragment executor does not serve; the translator detects
 * them on the search body and the planner carries them on
 * {@link LanceShardPathShape} and
 * {@link org.opensearch.lance.plan.rel.physical.ShardPathFallbackExec},
 * so the routing decision is visible in the plan.
 */
public enum ShardPathReason {

    /**
     * {@code suggest} needs FTS candidate APIs Lance does not surface
     * yet.
     */
    SUGGEST,

    /**
     * {@code highlight} needs FTS positional APIs Lance does not
     * surface yet.
     */
    HIGHLIGHT,

    /**
     * {@code collapse} groups hits by a field and can materialise
     * {@code inner_hits} per group. The fragment executor does not
     * synthesise the collapsing collector state, so hits would come
     * back ungrouped and {@code inner_hits} would disappear silently.
     */
    COLLAPSE,

    /**
     * {@code rescore} layers a second-pass query on top of the first
     * Sort/TopDocs window. The fragment executor drives a plain
     * {@code IndexSearcher.search} and never runs the rescorer, so
     * scores would stay at their first-pass values.
     */
    RESCORE,

    /**
     * A pipeline aggregation (sibling like {@code avg_bucket} or parent
     * like {@code cumulative_sum}) hits an "Already been replayed"
     * IllegalStateException in the coordinator merge, because the
     * fragment path replays the InternalAggregations tree in a way the
     * pipeline aggregators don't expect. The shard path's standard
     * reduce loop handles them.
     */
    PIPELINE_AGG
}
