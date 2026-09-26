/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.io.IOException;

import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.lance.WireVersion;

/**
 * One node's answer to {@link LanceRequestCacheClearNodeRequest}: how
 * many entries it dropped. The stream opens with {@link #WIRE_VERSION}
 * after the node the OpenSearch base class writes.
 */
public final class LanceRequestCacheClearNodeResponse extends BaseNodeResponse {

    /** The wire format's version, the first field the response writes after its base class. */
    public static final int WIRE_VERSION = 1;

    private final int dropped;

    public LanceRequestCacheClearNodeResponse(DiscoveryNode node, int dropped) {
        super(node);
        this.dropped = dropped;
    }

    public LanceRequestCacheClearNodeResponse(StreamInput in) throws IOException {
        super(in);
        WireVersion.Reader reader = WireVersion.read(in, "LanceRequestCacheClearNodeResponse", WIRE_VERSION);
        this.dropped = in.readVInt();
        reader.finish();
    }

    /** Entries this node dropped. */
    public int dropped() {
        return dropped;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        WireVersion.write(out, WIRE_VERSION);
        out.writeVInt(dropped);
    }
}
