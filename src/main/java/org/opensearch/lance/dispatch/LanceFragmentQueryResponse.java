/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.InternalAggregations;

/**
 * Per-node dispatch response. Carries the partial numbers the
 * coordinator needs to merge into a shard-shape-free
 * {@link org.opensearch.action.search.SearchResponse}:
 * <ul>
 *   <li>{@link #matched()} — number of rows in the node's fragment
 *       subset that satisfied the query, counted up to the request's
 *       {@link LanceFragmentQueryRequest#trackTotalHitsUpTo()} bound.
 *       {@link #matchedIsLowerBound()} says whether the executor
 *       stopped counting at the bound, in which case the true count
 *       is at least {@code matched}. The coordinator sums the values
 *       across all nodes to populate {@code hits.total.value} and
 *       turns the relation into {@code gte} when any node reported a
 *       lower bound or the sum exceeds the bound.</li>
 *   <li>{@link #fragmentCount()} — number of fragments the node
 *       scanned. Coordinator sums these to populate
 *       {@code _shards.total} on the merged response so operators
 *       can see the fragment fan-out.</li>
 *   <li>{@link #hits()} — top-{@code size} hits from the node's
 *       fragment subset. Coordinator concatenates all node lists
 *       and truncates to the request's {@code size}.</li>
 *   <li>{@link #aggregations()} — per-node
 *       {@link InternalAggregations} produced by driving OpenSearch's
 *       stock aggregator machinery against a per-fragment
 *       {@link org.apache.lucene.index.DirectoryReader}
 *       (see {@link TransportLanceFragmentQueryAction}). Every
 *       {@link org.opensearch.search.aggregations.InternalAggregation}
 *       inside is already Writeable, so the transport layer
 *       serialises the whole tree in one call. Coordinator merges
 *       them with
 *       {@link InternalAggregations#topLevelReduce(java.util.List,
 *       org.opensearch.search.aggregations.InternalAggregation.ReduceContext)}
 *       — the same reduction path the shard fan-out uses. May be
 *       {@code null} for hits-only requests where no aggregations
 *       were requested.</li>
 * </ul>
 *
 * <p>{@link SearchHit} is already {@link org.opensearch.core.common.io.stream.Writeable
 * Writeable} — the transport layer serialises the whole list in
 * one call and the coordinator rebuilds hits without any per-hit
 * conversion.
 */
public final class LanceFragmentQueryResponse extends ActionResponse {

    private final long matched;
    private final boolean matchedIsLowerBound;
    private final int fragmentCount;
    private final List<SearchHit> hits;
    private final InternalAggregations aggregations;

    public LanceFragmentQueryResponse(
        long matched,
        boolean matchedIsLowerBound,
        int fragmentCount,
        List<SearchHit> hits,
        InternalAggregations aggregations
    ) {
        this.matched = matched;
        this.matchedIsLowerBound = matchedIsLowerBound;
        this.fragmentCount = fragmentCount;
        this.hits = List.copyOf(hits);
        this.aggregations = aggregations;
    }

    public LanceFragmentQueryResponse(StreamInput in) throws IOException {
        super(in);
        this.matched = in.readVLong();
        this.matchedIsLowerBound = in.readBoolean();
        this.fragmentCount = in.readVInt();
        int hitCount = in.readVInt();
        List<SearchHit> readHits = new ArrayList<>(hitCount);
        for (int i = 0; i < hitCount; i++) {
            readHits.add(new SearchHit(in));
        }
        this.hits = List.copyOf(readHits);
        this.aggregations = in.readBoolean() ? InternalAggregations.readFrom(in) : null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(matched);
        out.writeBoolean(matchedIsLowerBound);
        out.writeVInt(fragmentCount);
        out.writeVInt(hits.size());
        for (SearchHit hit : hits) {
            hit.writeTo(out);
        }
        if (aggregations == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            aggregations.writeTo(out);
        }
    }

    public long matched() {
        return matched;
    }

    /**
     * Whether {@link #matched()} is a lower bound rather than the
     * exact count: the executor stopped counting once it had seen more
     * than the request's {@code trackTotalHitsUpTo} matches.
     */
    public boolean matchedIsLowerBound() {
        return matchedIsLowerBound;
    }

    public int fragmentCount() {
        return fragmentCount;
    }

    public List<SearchHit> hits() {
        return hits;
    }

    /**
     * Per-node aggregation results. May be {@code null} when the
     * request carried no aggregations. Never returns an empty
     * {@link InternalAggregations} — a request with aggregations
     * always produces at least one {@link
     * org.opensearch.search.aggregations.InternalAggregation}.
     */
    public InternalAggregations aggregations() {
        return aggregations;
    }
}
