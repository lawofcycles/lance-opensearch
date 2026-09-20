/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.stats;

import java.io.IOException;

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Request for {@link LanceStatsAction}: which nodes to ask, nothing else.
 * An empty node list means every node, as for {@code _nodes/stats}.
 */
public final class LanceStatsRequest extends BaseNodesRequest<LanceStatsRequest> {

    public LanceStatsRequest(String... nodeIds) {
        super(nodeIds);
    }

    public LanceStatsRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }
}
