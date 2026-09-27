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
import org.opensearch.lance.WireVersion;
import org.opensearch.search.SearchHit;

/**
 * The fetch round's answer from one data node: the rendered hit of every
 * row address of the {@link LanceFragmentFetchRequest}, in the request's
 * order, and what the node spent on them. The hits carry the per hit
 * projections of the body ({@code _id}, {@code _source}, stored fields,
 * doc value fields, {@code fields}) and neither score nor sort values;
 * the coordinator stamps those from the query round. The profile counts
 * under the same figures as a query round response's fetch phase
 * ({@link LanceFragmentQueryResponse.Profile}: the fetch milliseconds and
 * the take scans, rows, columns and milliseconds; the query figures are
 * zero), so the coordinator adds it to the node's entry under
 * {@code profile.lance.nodes}. The stream opens with {@link #WIRE_VERSION}
 * (see {@link WireVersion}).
 */
public final class LanceFragmentFetchResponse extends ActionResponse {

    /** The wire format's version, the first field the response writes. */
    public static final int WIRE_VERSION = 1;

    private final List<SearchHit> hits;
    private final LanceFragmentQueryResponse.Profile profile;

    public LanceFragmentFetchResponse(List<SearchHit> hits, LanceFragmentQueryResponse.Profile profile) {
        this.hits = List.copyOf(hits);
        this.profile = profile == null ? LanceFragmentQueryResponse.Profile.NONE : profile;
    }

    public LanceFragmentFetchResponse(StreamInput in) throws IOException {
        super(in);
        WireVersion.Reader reader = WireVersion.read(in, "LanceFragmentFetchResponse", WIRE_VERSION);
        int hitCount = in.readVInt();
        List<SearchHit> readHits = new ArrayList<>(hitCount);
        for (int i = 0; i < hitCount; i++) {
            readHits.add(new SearchHit(in));
        }
        this.hits = List.copyOf(readHits);
        // The profile's five base figures, then the columns, laid out
        // inline: this message started with them, so they are base
        // fields of its own version 1, not a block.
        this.profile = new LanceFragmentQueryResponse.Profile(in).withTakeColumns(in.readVLong());
        reader.finish();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        WireVersion.write(out, WIRE_VERSION);
        out.writeVInt(hits.size());
        for (SearchHit hit : hits) {
            hit.writeTo(out);
        }
        profile.writeTo(out);
        out.writeVLong(profile.takeColumns());
    }

    /** One rendered hit per requested row address, in request order. */
    public List<SearchHit> hits() {
        return hits;
    }

    /** What the node spent on the round: its fetch milliseconds and take figures, query figures zero. */
    public LanceFragmentQueryResponse.Profile profile() {
        return profile;
    }
}
