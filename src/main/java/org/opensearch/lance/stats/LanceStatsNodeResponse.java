/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.stats;

import java.io.IOException;

import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

/** One node's {@link LanceNodeStats} with the node that produced it. */
public final class LanceStatsNodeResponse extends BaseNodeResponse implements ToXContentFragment {

    private final LanceNodeStats stats;

    public LanceStatsNodeResponse(DiscoveryNode node, LanceNodeStats stats) {
        super(node);
        this.stats = stats;
    }

    public LanceStatsNodeResponse(StreamInput in) throws IOException {
        super(in);
        this.stats = new LanceNodeStats(in);
    }

    public LanceNodeStats stats() {
        return stats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        stats.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("name", getNode().getName());
        return stats.toXContent(builder, params);
    }
}
