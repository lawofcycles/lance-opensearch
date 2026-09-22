/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.index;

import java.io.IOException;
import java.util.List;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Response of {@link LanceBuildIndexesNodesAction}: the build outcome of
 * every data node that answered. {@link TransportLanceBuildIndexesAction}
 * merges it into the {@code POST /_lance/build_indexes/{index}} response
 * body, keyed under {@code nodes} by node id.
 */
public final class LanceBuildIndexesNodesResponse extends BaseNodesResponse<LanceBuildIndexesNodeResponse> {

    public LanceBuildIndexesNodesResponse(
        ClusterName clusterName,
        List<LanceBuildIndexesNodeResponse> nodes,
        List<FailedNodeException> failures
    ) {
        super(clusterName, nodes, failures);
    }

    public LanceBuildIndexesNodesResponse(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected List<LanceBuildIndexesNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(LanceBuildIndexesNodeResponse::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<LanceBuildIndexesNodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }
}
