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
    HIGHLIGHT
}
