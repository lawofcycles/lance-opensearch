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
 * One node's answer to {@link LanceStatisticsPrefetchNodeRequest}: what
 * the node's statistics cache did with the version. The stream opens
 * with {@link #WIRE_VERSION} after the node the OpenSearch base class
 * writes.
 */
public final class LanceStatisticsPrefetchNodeResponse extends BaseNodeResponse {

    /** The wire format's version, the first field the response writes after its base class. */
    public static final int WIRE_VERSION = 1;

    /** What the node's cache did with the version it was asked to collect. */
    public enum Outcome {
        /** A collection was started. */
        STARTED,
        /** The cache already held the version's statistics. */
        HELD,
        /** A collection of the version was already queued or running. */
        PENDING
    }

    private final Outcome outcome;

    public LanceStatisticsPrefetchNodeResponse(DiscoveryNode node, Outcome outcome) {
        super(node);
        this.outcome = outcome;
    }

    public LanceStatisticsPrefetchNodeResponse(StreamInput in) throws IOException {
        super(in);
        WireVersion.Reader reader = WireVersion.read(in, "LanceStatisticsPrefetchNodeResponse", WIRE_VERSION);
        this.outcome = in.readEnum(Outcome.class);
        reader.finish();
    }

    public Outcome outcome() {
        return outcome;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        WireVersion.write(out, WIRE_VERSION);
        out.writeEnum(outcome);
    }
}
