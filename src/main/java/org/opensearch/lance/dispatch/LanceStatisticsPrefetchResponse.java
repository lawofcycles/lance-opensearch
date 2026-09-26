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
import org.opensearch.lance.dispatch.LanceStatisticsPrefetchNodeResponse.Outcome;

/**
 * Response of {@link LanceStatisticsPrefetchAction}: one
 * {@link LanceStatisticsPrefetchNodeResponse} per node that answered.
 * {@link #count} tallies them by outcome.
 */
public final class LanceStatisticsPrefetchResponse extends BaseNodesResponse<LanceStatisticsPrefetchNodeResponse> {

    public LanceStatisticsPrefetchResponse(
        ClusterName clusterName,
        List<LanceStatisticsPrefetchNodeResponse> nodes,
        List<FailedNodeException> failures
    ) {
        super(clusterName, nodes, failures);
    }

    public LanceStatisticsPrefetchResponse(StreamInput in) throws IOException {
        super(in);
    }

    /** How many nodes that answered reported {@code outcome}. */
    public int count(Outcome outcome) {
        int total = 0;
        for (LanceStatisticsPrefetchNodeResponse node : getNodes()) {
            if (node.outcome() == outcome) {
                total++;
            }
        }
        return total;
    }

    @Override
    protected List<LanceStatisticsPrefetchNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(LanceStatisticsPrefetchNodeResponse::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<LanceStatisticsPrefetchNodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }
}
