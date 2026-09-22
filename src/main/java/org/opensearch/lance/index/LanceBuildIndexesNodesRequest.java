/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.index;

import java.io.IOException;

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Request for {@link LanceBuildIndexesNodesAction}: the original build
 * request plus the source manifest version the coordinator resolved, so
 * every node clones (or confirms its clone of) the same version before
 * building.
 */
public final class LanceBuildIndexesNodesRequest extends BaseNodesRequest<LanceBuildIndexesNodesRequest> {

    private final LanceBuildIndexesRequest request;
    private final long sourceVersion;

    public LanceBuildIndexesNodesRequest(LanceBuildIndexesRequest request, long sourceVersion, String... nodeIds) {
        super(nodeIds);
        this.request = request;
        this.sourceVersion = sourceVersion;
    }

    public LanceBuildIndexesNodesRequest(StreamInput in) throws IOException {
        super(in);
        this.request = new LanceBuildIndexesRequest(in);
        this.sourceVersion = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
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
