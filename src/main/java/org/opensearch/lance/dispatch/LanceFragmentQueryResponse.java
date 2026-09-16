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
import org.opensearch.lance.dispatch.LanceMetricAggregator.PartialState;
import org.opensearch.search.SearchHit;

/**
 * Per-node dispatch response. Carries the partial numbers the
 * coordinator needs to merge into a shard-shape-free
 * {@link org.opensearch.action.search.SearchResponse}:
 * <ul>
 *   <li>{@link #matched()} — number of rows in the node's fragment
 *       subset that satisfied the filter. Coordinator sums these
 *       across all nodes to populate
 *       {@code hits.total.value}.</li>
 *   <li>{@link #fragmentCount()} — number of fragments the node
 *       scanned. Coordinator sums these to populate
 *       {@code _shards.total} on the merged response so operators
 *       can see the fragment fan-out.</li>
 *   <li>{@link #hits()} — top-{@code size} hits from the node's
 *       fragment subset. Coordinator concatenates all node lists
 *       and truncates to the request's {@code size}.</li>
 *   <li>{@link #partials()} — one {@link PartialState} per metric
 *       spec (parallel to the request's {@code metrics}), ready to
 *       feed into
 *       {@link LanceMetricAggregator#mergePartials(List, List)}.</li>
 * </ul>
 *
 * <p>{@link SearchHit} is already {@link org.opensearch.core.common.io.stream.Writeable
 * Writeable} — the transport layer serialises the whole list in
 * one call and the coordinator rebuilds hits without any per-hit
 * conversion.
 */
public final class LanceFragmentQueryResponse extends ActionResponse {

    private final long matched;
    private final int fragmentCount;
    private final List<SearchHit> hits;
    private final List<PartialState> partials;

    public LanceFragmentQueryResponse(long matched, int fragmentCount, List<SearchHit> hits, List<PartialState> partials) {
        this.matched = matched;
        this.fragmentCount = fragmentCount;
        this.hits = List.copyOf(hits);
        this.partials = List.copyOf(partials);
    }

    public LanceFragmentQueryResponse(StreamInput in) throws IOException {
        super(in);
        this.matched = in.readVLong();
        this.fragmentCount = in.readVInt();
        int hitCount = in.readVInt();
        List<SearchHit> readHits = new ArrayList<>(hitCount);
        for (int i = 0; i < hitCount; i++) {
            readHits.add(new SearchHit(in));
        }
        this.hits = List.copyOf(readHits);
        int partialCount = in.readVInt();
        List<PartialState> readPartials = new ArrayList<>(partialCount);
        for (int i = 0; i < partialCount; i++) {
            readPartials.add(new PartialState(in));
        }
        this.partials = List.copyOf(readPartials);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(matched);
        out.writeVInt(fragmentCount);
        out.writeVInt(hits.size());
        for (SearchHit hit : hits) {
            hit.writeTo(out);
        }
        out.writeVInt(partials.size());
        for (PartialState partial : partials) {
            partial.writeTo(out);
        }
    }

    public long matched() {
        return matched;
    }

    public int fragmentCount() {
        return fragmentCount;
    }

    public List<SearchHit> hits() {
        return hits;
    }

    public List<PartialState> partials() {
        return partials;
    }
}
