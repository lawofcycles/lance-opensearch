/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.io.IOException;
import java.util.List;

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Request for {@link LanceRequestCacheClearAction}: the uuids of the
 * indexes whose entries every node drops. Always sent to every node
 * (the cache is per node), so the node list is left empty.
 */
public final class LanceRequestCacheClearRequest extends BaseNodesRequest<LanceRequestCacheClearRequest> {

    private final List<String> indexUuids;

    public LanceRequestCacheClearRequest(List<String> indexUuids) {
        super(new String[0]);
        this.indexUuids = List.copyOf(indexUuids);
    }

    public LanceRequestCacheClearRequest(StreamInput in) throws IOException {
        super(in);
        this.indexUuids = in.readStringList();
    }

    public List<String> indexUuids() {
        return indexUuids;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringCollection(indexUuids);
    }
}
