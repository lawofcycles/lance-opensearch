/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.stats;

import java.io.IOException;
import java.util.List;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

/**
 * Response of {@link LanceStatsAction}: the {@link LanceStatsNodeResponse}
 * of every node that answered, rendered under {@code nodes} keyed by node
 * id. {@code RestActions.nodesResponse} adds the {@code _nodes} header and
 * {@code cluster_name} around it, the same envelope {@code _nodes/stats}
 * uses.
 */
public final class LanceStatsResponse extends BaseNodesResponse<LanceStatsNodeResponse> implements ToXContentFragment {

    public LanceStatsResponse(ClusterName clusterName, List<LanceStatsNodeResponse> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    public LanceStatsResponse(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected List<LanceStatsNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(LanceStatsNodeResponse::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<LanceStatsNodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("nodes");
        for (LanceStatsNodeResponse node : getNodes()) {
            builder.startObject(node.getNode().getId());
            node.toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }
}
