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
     * {@code search_after} without {@code sort}: the per-fragment
     * executor drives {@code IndexSearcher.searchAfter}, which requires
     * a matching Sort. Without one the shard path's score-order
     * {@code search_after} is used instead.
     */
    SEARCH_AFTER_SCORE,

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
    PIPELINE_AGG,

    /**
     * {@code min_score} filters hits by score threshold. The fragment
     * executor's matched counting comes from Lance metadata (or a
     * Lucene count), which sees every doc that matches the query
     * regardless of score, so the pre-filter total would be reported
     * next to post-filter hits.
     */
    MIN_SCORE,

    /**
     * {@code terminate_after} cuts the collector short after N docs on
     * each shard. The fragment path does not thread the terminate count
     * into its scan, so both {@code hits.total.value} and the
     * {@code terminated_early} flag would be silently wrong.
     */
    TERMINATE_AFTER,

    /**
     * {@code stored_fields} projects a specific list of stored fields
     * per hit (or {@code _none_} to hide {@code _source} entirely). The
     * fragment executor materialises hits by copying the raw
     * {@code _source} bytes, dropping the stored-fields context.
     */
    STORED_FIELDS,

    /**
     * {@code docvalue_fields} loads named doc values into
     * {@code hits.fields}, which the fragment executor does not
     * populate.
     */
    DOCVALUE_FIELDS,

    /**
     * {@code "explain": true} returns a per-hit scoring explanation.
     * The fragment executor never calls {@code searcher.explain}, so
     * hits would come back without any {@code _explanation}.
     */
    EXPLAIN_PER_HIT
}
