/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.io.IOException;
import java.util.List;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Response of {@link LanceRequestCacheClearAction}: one
 * {@link LanceRequestCacheClearNodeResponse} per node that answered.
 * {@link #dropped()} sums them.
 */
public final class LanceRequestCacheClearResponse extends BaseNodesResponse<LanceRequestCacheClearNodeResponse> {

    public LanceRequestCacheClearResponse(
        ClusterName clusterName,
        List<LanceRequestCacheClearNodeResponse> nodes,
        List<FailedNodeException> failures
    ) {
        super(clusterName, nodes, failures);
    }

    public LanceRequestCacheClearResponse(StreamInput in) throws IOException {
        super(in);
    }

    /** Entries dropped over every node that answered. */
    public int dropped() {
        int total = 0;
        for (LanceRequestCacheClearNodeResponse node : getNodes()) {
            total += node.dropped();
        }
        return total;
    }

    @Override
    protected List<LanceRequestCacheClearNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(LanceRequestCacheClearNodeResponse::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<LanceRequestCacheClearNodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }
}
