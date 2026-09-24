/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.index;

import java.io.IOException;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.lance.WireVersion;
import org.opensearch.transport.TransportRequest;

/**
 * The per node leg of {@link LanceBuildIndexesNodesRequest}: the build
 * request and the source version the node's clone must be at. Opens
 * with {@link #WIRE_VERSION} (see {@link WireVersion}); the nested
 * {@link LanceBuildIndexesRequest} has no marker of its own, so a
 * change to its fields bumps this number.
 */
public final class LanceBuildIndexesNodeRequest extends TransportRequest {

    /** The wire format's version, the first field the request writes after its base class. */
    public static final int WIRE_VERSION = 1;

    private final LanceBuildIndexesRequest request;
    private final long sourceVersion;

    public LanceBuildIndexesNodeRequest(LanceBuildIndexesRequest request, long sourceVersion) {
        super();
        this.request = request;
        this.sourceVersion = sourceVersion;
    }

    public LanceBuildIndexesNodeRequest(StreamInput in) throws IOException {
        super(in);
        WireVersion.read(in, "LanceBuildIndexesNodeRequest", WIRE_VERSION);
        this.request = new LanceBuildIndexesRequest(in);
        this.sourceVersion = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        WireVersion.write(out, WIRE_VERSION);
        request.writeTo(out);
        out.writeLong(sourceVersion);
    }

    public LanceBuildIndexesRequest request() {
        return request;
    }

    public long sourceVersion() {
        return sourceVersion;
    }
}
