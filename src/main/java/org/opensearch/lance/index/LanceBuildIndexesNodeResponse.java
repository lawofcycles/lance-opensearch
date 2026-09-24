/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.index;

import java.io.IOException;

import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.lance.WireVersion;

/**
 * One data node's outcome of a {@code node_local} index build: which
 * Lance indexes were built into this node's clone, which columns were
 * skipped or failed, and the status the node would have answered on its
 * own ({@link TransportLanceBuildIndexesAction#statusOf}). Opens with
 * {@link #WIRE_VERSION} (see {@link WireVersion}); the nested
 * {@link LanceBuildIndexesResponse.KindResult} has no marker of its
 * own, so a change to its fields bumps this number.
 */
public final class LanceBuildIndexesNodeResponse extends BaseNodeResponse {

    /** The wire format's version, the first field the response writes after its base class. */
    public static final int WIRE_VERSION = 1;

    private final LanceBuildIndexesResponse.KindResult fts;
    private final LanceBuildIndexesResponse.KindResult scalar;
    private final LanceBuildIndexesResponse.KindResult vector;
    private final RestStatus status;
    /**
     * Mapping re-derived from this node's clone after the build, so the
     * coordinator can apply the keyword to lance_text flip a first FTS
     * build causes; the source's version never moves in a node-local
     * build, so the namespace poll's re-derivation would not fire.
     */
    private final String mappingJson;

    public LanceBuildIndexesNodeResponse(
        DiscoveryNode node,
        LanceBuildIndexesResponse.KindResult fts,
        LanceBuildIndexesResponse.KindResult scalar,
        LanceBuildIndexesResponse.KindResult vector,
        RestStatus status,
        String mappingJson
    ) {
        super(node);
        this.fts = fts;
        this.scalar = scalar;
        this.vector = vector;
        this.status = status;
        this.mappingJson = mappingJson;
    }

    public LanceBuildIndexesNodeResponse(StreamInput in) throws IOException {
        super(in);
        WireVersion.Reader reader = WireVersion.read(in, "LanceBuildIndexesNodeResponse", WIRE_VERSION);
        this.fts = new LanceBuildIndexesResponse.KindResult(in);
        this.scalar = new LanceBuildIndexesResponse.KindResult(in);
        this.vector = new LanceBuildIndexesResponse.KindResult(in);
        this.status = RestStatus.readFrom(in);
        this.mappingJson = in.readOptionalString();
        reader.finish();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        WireVersion.write(out, WIRE_VERSION);
        fts.writeTo(out);
        scalar.writeTo(out);
        vector.writeTo(out);
        RestStatus.writeTo(out, status);
        out.writeOptionalString(mappingJson);
    }

    public String mappingJson() {
        return mappingJson;
    }

    public LanceBuildIndexesResponse.KindResult fts() {
        return fts;
    }

    public LanceBuildIndexesResponse.KindResult scalar() {
        return scalar;
    }

    public LanceBuildIndexesResponse.KindResult vector() {
        return vector;
    }

    public RestStatus status() {
        return status;
    }
}
